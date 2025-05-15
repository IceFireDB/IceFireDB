package messagequeue

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	bswl "github.com/ipfs/boxo/bitswap/client/wantlist"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("bitswap/client/msgq")

const (
	// maxMessageSize is the maximum message size in bytes
	maxMessageSize = 1024 * 1024 * 2
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
	// maxRetries is the number of times to attempt to send a message before
	// giving up
	maxRetries = 3
	// The maximum amount of time in which to accept a response as being valid
	// for latency calculation (as opposed to discarding it as an outlier)
	maxValidLatency = 30 * time.Second
	// rebroadcastInterval is the minimum amount of time that must elapse before
	// resending wants to a peer
	rebroadcastInterval = 30 * time.Second
	// sendErrorBackoff is the time to wait before retrying to connect after
	// an error when trying to send a message
	sendErrorBackoff = 100 * time.Millisecond
	// when we reach sendMessageCutoff wants/cancels, we'll send the message immediately.
	sendMessageCutoff = 256
	// wait this long before sending next message
	sendTimeout = 30 * time.Second

	defaultPerPeerDelay = time.Millisecond / 8
	maxSendMessageDelay = 2 * time.Second
	minSendMessageDelay = 20 * time.Millisecond
)

var peerCount atomic.Int64

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	Connect(context.Context, peer.AddrInfo) error
	NewMessageSender(context.Context, peer.ID, *bsnet.MessageSenderOpts) (bsnet.MessageSender, error)
	Latency(peer.ID) time.Duration
	Ping(context.Context, peer.ID) ping.Result
	Self() peer.ID
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	ctx          context.Context
	shutdown     func()
	p            peer.ID
	network      MessageNetwork
	dhTimeoutMgr DontHaveTimeoutManager

	// The maximum size of a message in bytes. Any overflow is put into the
	// next message
	maxMessageSize int

	// The amount of time to wait when there's an error sending to a peer
	// before retrying
	sendErrorBackoff time.Duration

	// The maximum amount of time in which to accept a response as being valid
	// for latency calculation
	maxValidLatency time.Duration

	// Signals that there are outgoing wants / cancels ready to be processed
	outgoingWork chan struct{}

	// Channel of CIDs of blocks / HAVEs / DONT_HAVEs received from the peer
	responses chan []cid.Cid

	// Take lock whenever any of these variables are modified
	wllock    sync.Mutex
	bcstWants recallWantlist
	peerWants recallWantlist
	cancels   *cid.Set
	priority  int32

	// Don't touch any of these variables outside of run loop
	sender         bsnet.MessageSender
	rebroadcastNow chan struct{}
	// For performance reasons we just clear out the fields of the message
	// instead of creating a new one every time.
	msg bsmsg.BitSwapMessage

	// For simulating time -- uses mock in test
	clock clock.Clock

	// Used to track things that happen asynchronously -- used only in test
	events chan<- messageEvent

	perPeerDelay time.Duration
}

// recallWantlist keeps a list of pending wants and a list of sent wants
type recallWantlist struct {
	// The list of wants that have not yet been sent
	pending *bswl.Wantlist
	// The list of wants that have been sent
	sent *bswl.Wantlist
	// The time at which each want was sent
	sentAt map[cid.Cid]time.Time
}

func newRecallWantList() recallWantlist {
	return recallWantlist{
		pending: bswl.New(),
		sent:    bswl.New(),
		sentAt:  make(map[cid.Cid]time.Time),
	}
}

// add want to the pending list
func (r *recallWantlist) add(c cid.Cid, priority int32, wtype pb.Message_Wantlist_WantType) {
	r.pending.Add(c, priority, wtype)
}

// remove wants from both the pending list and the list of sent wants
func (r *recallWantlist) remove(c cid.Cid) {
	r.pending.Remove(c)
	r.sent.Remove(c)
	delete(r.sentAt, c)
}

// remove wants by type from both the pending list and the list of sent wants
func (r *recallWantlist) removeType(c cid.Cid, wtype pb.Message_Wantlist_WantType) {
	r.pending.RemoveType(c, wtype)
	r.sent.RemoveType(c, wtype)
	if !r.sent.Has(c) {
		delete(r.sentAt, c)
	}
}

// markSent moves the want from the pending to the sent list
//
// Returns true if the want was marked as sent. Returns false if the want wasn't
// pending.
func (r *recallWantlist) markSent(e bswl.Entry) bool {
	if !r.pending.RemoveType(e.Cid, e.WantType) {
		return false
	}
	r.sent.Add(e.Cid, e.Priority, e.WantType)
	return true
}

// setSentAt records the time at which a want was sent
func (r *recallWantlist) setSentAt(c cid.Cid, at time.Time) {
	// The want may have been canceled in the interim
	if r.sent.Has(c) {
		if _, ok := r.sentAt[c]; !ok {
			r.sentAt[c] = at
		}
	}
}

// clearSentAt clears out the record of the time a want was sent.
// We clear the sent at time when we receive a response for a key as we
// only need the first response for latency measurement.
func (r *recallWantlist) clearSentAt(c cid.Cid) {
	delete(r.sentAt, c)
}

// refresh moves wants from the sent list back to the pending list.
// If a want has been sent for longer than the interval, it is moved back to the pending list.
// Returns the number of wants that were refreshed.
func (r *recallWantlist) refresh(now time.Time, interval time.Duration) int {
	var refreshed int
	for _, want := range r.sent.Entries() {
		wantCid := want.Cid
		sentAt, ok := r.sentAt[wantCid]
		if ok && now.Sub(sentAt) >= interval {
			r.sent.Remove(wantCid)
			r.pending.Add(wantCid, want.Priority, want.WantType)
			refreshed++
		}
	}

	return refreshed
}

type peerConn struct {
	p       peer.ID
	network MessageNetwork
}

func newPeerConnection(p peer.ID, network MessageNetwork) *peerConn {
	return &peerConn{p, network}
}

func (pc *peerConn) Ping(ctx context.Context) ping.Result {
	return pc.network.Ping(ctx, pc.p)
}

func (pc *peerConn) Latency() time.Duration {
	return pc.network.Latency(pc.p)
}

// Fires when a timeout occurs waiting for a response from a peer running an
// older version of Bitswap that doesn't support DONT_HAVE messages.
type OnDontHaveTimeout func(peer.ID, []cid.Cid)

// DontHaveTimeoutManager pings a peer to estimate latency so it can set a reasonable
// upper bound on when to consider a DONT_HAVE request as timed out (when connected to
// a peer that doesn't support DONT_HAVE messages)
type DontHaveTimeoutManager interface {
	// Start the manager (idempotent)
	Start()
	// Shutdown the manager (Shutdown is final, manager cannot be restarted)
	Shutdown()
	// AddPending adds the wants as pending a response. If the are not
	// canceled before the timeout, the OnDontHaveTimeout method will be called.
	AddPending([]cid.Cid)
	// CancelPending removes the wants
	CancelPending([]cid.Cid)
	// UpdateMessageLatency informs the manager of a new latency measurement
	UpdateMessageLatency(time.Duration)
}

type optsConfig struct {
	dhtConfig    *DontHaveTimeoutConfig
	perPeerDelay time.Duration
}

type option func(*optsConfig)

func WithDontHaveTimeoutConfig(dhtConfig *DontHaveTimeoutConfig) option {
	return func(cfg *optsConfig) {
		cfg.dhtConfig = dhtConfig
	}
}

func WithPerPeerSendDelay(perPeerDelay time.Duration) option {
	return func(cfg *optsConfig) {
		if perPeerDelay == 0 {
			perPeerDelay = defaultPerPeerDelay
		}
		cfg.perPeerDelay = perPeerDelay
	}
}

// New creates a new MessageQueue.
//
// If onDontHaveTimeout is nil, then the dontHaveTimeoutMrg is disabled.
func New(ctx context.Context, p peer.ID, network MessageNetwork, onDontHaveTimeout OnDontHaveTimeout, options ...option) *MessageQueue {
	opts := optsConfig{
		perPeerDelay: defaultPerPeerDelay,
	}
	for _, o := range options {
		o(&opts)
	}

	var onTimeout func([]cid.Cid, time.Duration)
	var dhTimeoutMgr DontHaveTimeoutManager
	if onDontHaveTimeout != nil {
		onTimeout = func(ks []cid.Cid, timeout time.Duration) {
			log.Infow("Bitswap: timeout waiting for blocks", "timeout", timeout.String(), "cids", ks, "peer", p)
			onDontHaveTimeout(p, ks)
		}
		dhTimeoutMgr = newDontHaveTimeoutMgr(newPeerConnection(p, network), onTimeout, opts.dhtConfig)
	}
	mq := newMessageQueue(ctx, p, network, maxMessageSize, sendErrorBackoff, maxValidLatency, dhTimeoutMgr, nil, nil)
	mq.perPeerDelay = opts.perPeerDelay
	return mq
}

type messageEvent int

const (
	messageQueued messageEvent = iota
	messageFinishedSending
	latenciesRecorded
)

// This constructor is used by the tests
func newMessageQueue(
	ctx context.Context,
	p peer.ID,
	network MessageNetwork,
	maxMsgSize int,
	sendErrorBackoff time.Duration,
	maxValidLatency time.Duration,
	dhTimeoutMgr DontHaveTimeoutManager,
	clk clock.Clock,
	events chan<- messageEvent,
) *MessageQueue {
	if clk == nil {
		clk = clock.New()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &MessageQueue{
		ctx:              ctx,
		shutdown:         cancel,
		p:                p,
		network:          network,
		dhTimeoutMgr:     dhTimeoutMgr,
		maxMessageSize:   maxMsgSize,
		bcstWants:        newRecallWantList(),
		peerWants:        newRecallWantList(),
		cancels:          cid.NewSet(),
		outgoingWork:     make(chan struct{}, 1),
		responses:        make(chan []cid.Cid, 8),
		rebroadcastNow:   make(chan struct{}),
		sendErrorBackoff: sendErrorBackoff,
		maxValidLatency:  maxValidLatency,
		priority:         maxPriority,
		// For performance reasons we just clear out the fields of the message
		// after using it, instead of creating a new one every time.
		msg:    bsmsg.New(false),
		clock:  clk,
		events: events,
	}
}

// Add want-haves that are part of a broadcast to all connected peers
func (mq *MessageQueue) AddBroadcastWantHaves(wantHaves []cid.Cid) {
	if len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()

	for _, c := range wantHaves {
		mq.bcstWants.add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--

		// We're adding a want-have for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}

	mq.wllock.Unlock()

	// Schedule a message send
	mq.signalWorkReady()
}

// Add want-haves and want-blocks for the peer for this message queue.
func (mq *MessageQueue) AddWants(wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	if len(wantBlocks) == 0 && len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()

	for _, c := range wantHaves {
		mq.peerWants.add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--

		// We're adding a want-have for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}
	for _, c := range wantBlocks {
		mq.peerWants.add(c, mq.priority, pb.Message_Wantlist_Block)
		mq.priority--

		// We're adding a want-block for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}

	mq.wllock.Unlock()

	// Schedule a message send
	mq.signalWorkReady()
}

// Add cancel messages for the given keys.
func (mq *MessageQueue) AddCancels(cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	// Cancel any outstanding DONT_HAVE timers
	if mq.dhTimeoutMgr != nil {
		mq.dhTimeoutMgr.CancelPending(cancelKs)
	}

	var workReady bool

	mq.wllock.Lock()

	// Remove keys from broadcast and peer wants, and add to cancels
	for _, c := range cancelKs {
		// Check if a want for the key was sent
		wasSentBcst := mq.bcstWants.sent.Has(c)
		wasSentPeer := mq.peerWants.sent.Has(c)

		// Remove the want from tracking wantlists
		mq.bcstWants.remove(c)
		mq.peerWants.remove(c)

		// Only send a cancel if a want was sent
		if wasSentBcst || wasSentPeer {
			mq.cancels.Add(c)
			workReady = true
		}
	}

	mq.wllock.Unlock()

	// Unlock first to be nice to the scheduler.

	// Schedule a message send
	if workReady {
		mq.signalWorkReady()
	}
}

// ResponseReceived is called when a message is received from the network.
// ks is the set of blocks, HAVEs and DONT_HAVEs in the message
// Note that this is just used to calculate latency.
func (mq *MessageQueue) ResponseReceived(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}

	// These messages are just used to approximate latency, so if we get so
	// many responses that they get backed up, just ignore the overflow.
	select {
	case mq.responses <- ks:
	default:
	}
}

func (mq *MessageQueue) RebroadcastNow() {
	select {
	case mq.rebroadcastNow <- struct{}{}:
	case <-mq.ctx.Done():
	}
}

// Startup starts the processing of messages and rebroadcasting.
func (mq *MessageQueue) Startup() {
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	mq.shutdown()
}

func (mq *MessageQueue) onShutdown() {
	if mq.dhTimeoutMgr != nil {
		// Shut down the DONT_HAVE timeout manager
		mq.dhTimeoutMgr.Shutdown()
	}

	// Reset the streamMessageSender
	if mq.sender != nil {
		_ = mq.sender.Reset()
	}
}

func (mq *MessageQueue) runQueue() {
	const runRebroadcastsInterval = rebroadcastInterval / 2

	peerCount.Add(1)
	defer peerCount.Add(-1)

	defer mq.onShutdown()

	// Create a timer for debouncing scheduled work.
	scheduleWork := mq.clock.Timer(0)
	if !scheduleWork.Stop() {
		// Need to drain the timer if Stop() returns false
		// See: https://golang.org/pkg/time/#Timer.Stop
		<-scheduleWork.C
	}

	perPeerDelay := mq.perPeerDelay
	hasWorkChan := mq.outgoingWork

	rebroadcastTimer := mq.clock.Timer(runRebroadcastsInterval)
	defer rebroadcastTimer.Stop()

	for {
		select {
		case now := <-rebroadcastTimer.C:
			mq.rebroadcastWantlist(now, rebroadcastInterval)
			rebroadcastTimer.Reset(runRebroadcastsInterval)

		case <-mq.rebroadcastNow:
			mq.rebroadcastWantlist(mq.clock.Now(), 0)

		case <-hasWorkChan:
			if mq.events != nil {
				mq.events <- messageQueued
			}

			mq.sendMessage()
			hasWorkChan = nil

			delay := time.Duration(peerCount.Load()) * perPeerDelay
			delay = max(minSendMessageDelay, min(maxSendMessageDelay, delay))
			scheduleWork.Reset(delay)

		case <-scheduleWork.C:
			hasWorkChan = mq.outgoingWork

		case res := <-mq.responses:
			// We received a response from the peer, calculate latency
			mq.handleResponse(res)

		case <-mq.ctx.Done():
			return
		}
	}
}

// Periodically resend the list of wants to the peer
func (mq *MessageQueue) rebroadcastWantlist(now time.Time, interval time.Duration) {
	mq.wllock.Lock()
	// Transfer wants from the rebroadcast lists into the pending lists.
	toRebroadcast := mq.bcstWants.refresh(now, interval) + mq.peerWants.refresh(now, interval)
	mq.wllock.Unlock()

	// If some wants were transferred from the rebroadcast list
	if toRebroadcast > 0 {
		// Send them out
		mq.sendMessage()
		log.Infow("Rebroadcasting wants", "amount", toRebroadcast, "peer", mq.p)
	}
}

func (mq *MessageQueue) signalWorkReady() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

func (mq *MessageQueue) sendMessage() {
	sender, err := mq.initializeSender()
	if err != nil {
		// If we fail to initialize the sender, the networking layer will
		// emit a Disconnect event and the MessageQueue will get cleaned up
		log.Infof("Could not open message sender to peer %s: %s", mq.p, err)
		mq.Shutdown()
		return
	}

	if mq.dhTimeoutMgr != nil {
		// Make sure the DONT_HAVE timeout manager has started
		// Note: Start is idempotent
		mq.dhTimeoutMgr.Start()
	}

	supportsHave := mq.sender.SupportsHave()

	// After processing the message, clear out its fields to save memory
	defer mq.msg.Reset(false)

	var wantlist []bsmsg.Entry

	for {
		// Convert want lists to a Bitswap Message
		message, onSent := mq.extractOutgoingMessage(supportsHave)
		if message.Empty() {
			return
		}

		wantlist = message.FillWantlist(wantlist)
		mq.logOutgoingMessage(wantlist)

		if err = sender.SendMsg(mq.ctx, message); err != nil {
			// If the message couldn't be sent, the networking layer will
			// emit a Disconnect event and the MessageQueue will get cleaned up
			log.Infof("Could not send message to peer %s: %s", mq.p, err)
			mq.Shutdown()
			return
		}

		// Record sent time so as to calculate message latency
		onSent()

		// Set a timer to wait for responses
		mq.simulateDontHaveWithTimeout(wantlist)

		// If the message was too big and only a subset of wants could be sent,
		// send more if the the workcount is above the cutoff. Otherwise,
		// schedule sending the rest of the wants in the next iteration of the
		// event loop.
		pendingWork := mq.pendingWorkCount()
		if pendingWork < sendMessageCutoff {
			if pendingWork > 0 {
				mq.signalWorkReady()
			}
			return
		}

		mq.msg.Reset(false)
	}
}

// If want-block times out, simulate a DONT_HAVE response.
// This is necessary when making requests to peers running an older version of
// Bitswap that doesn't support the DONT_HAVE response, and is also useful to
// mitigate getting blocked by a peer that takes a long time to respond.
func (mq *MessageQueue) simulateDontHaveWithTimeout(wantlist []bsmsg.Entry) {
	// Get the CID of each want-block that expects a DONT_HAVE response
	wants := make([]cid.Cid, 0, len(wantlist))

	mq.wllock.Lock()

	for _, entry := range wantlist {
		if entry.WantType == pb.Message_Wantlist_Block && entry.SendDontHave {
			// Unlikely, but just in case check that the block hasn't been
			// received in the interim
			c := entry.Cid
			if mq.peerWants.sent.Has(c) {
				wants = append(wants, c)
			}
		}
	}

	mq.wllock.Unlock()

	if mq.dhTimeoutMgr != nil {
		// Add wants to DONT_HAVE timeout manager
		mq.dhTimeoutMgr.AddPending(wants)
	}
}

// handleResponse is called when a response is received from the peer,
// with the CIDs of received blocks / HAVEs / DONT_HAVEs
func (mq *MessageQueue) handleResponse(ks []cid.Cid) {
	now := mq.clock.Now()
	earliest := time.Time{}

	mq.wllock.Lock()

	// Check if the keys in the response correspond to any request that was
	// sent to the peer.
	//
	// - Find the earliest request so as to calculate the longest latency as
	//   we want to be conservative when setting the timeout
	// - Ignore latencies that are very long, as these are likely to be outliers
	//   caused when
	//   - we send a want to peer A
	//   - peer A does not have the block
	//   - peer A later receives the block from peer B
	//   - peer A sends us HAVE / block
	for _, c := range ks {
		if at, ok := mq.bcstWants.sentAt[c]; ok {
			if (earliest.IsZero() || at.Before(earliest)) && now.Sub(at) < mq.maxValidLatency {
				earliest = at
			}
			mq.bcstWants.clearSentAt(c)
		}
		if at, ok := mq.peerWants.sentAt[c]; ok {
			if (earliest.IsZero() || at.Before(earliest)) && now.Sub(at) < mq.maxValidLatency {
				earliest = at
			}
			// Clear out the sent time for the CID because we only want to
			// record the latency between the request and the first response
			// for that CID (not subsequent responses)
			mq.peerWants.clearSentAt(c)
		}
	}

	mq.wllock.Unlock()

	if !earliest.IsZero() && mq.dhTimeoutMgr != nil {
		// Inform the timeout manager of the calculated latency
		mq.dhTimeoutMgr.UpdateMessageLatency(now.Sub(earliest))
	}
	if mq.events != nil {
		mq.events <- latenciesRecorded
	}
}

func (mq *MessageQueue) logOutgoingMessage(wantlist []bsmsg.Entry) {
	// Save some CPU cycles and allocations if log level is higher than debug
	if !log.Level().Enabled(zapcore.DebugLevel) {
		return
	}

	self := mq.network.Self()
	for _, e := range wantlist {
		if e.Cancel {
			if e.WantType == pb.Message_Wantlist_Have {
				log.Debugw("sent message",
					"type", "CANCEL_WANT_HAVE",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			} else {
				log.Debugw("sent message",
					"type", "CANCEL_WANT_BLOCK",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			}
		} else {
			if e.WantType == pb.Message_Wantlist_Have {
				log.Debugw("sent message",
					"type", "WANT_HAVE",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			} else {
				log.Debugw("sent message",
					"type", "WANT_BLOCK",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			}
		}
	}
}

// The amount of work that is waiting to be processed
func (mq *MessageQueue) pendingWorkCount() int {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	return mq.bcstWants.pending.Len() + mq.peerWants.pending.Len() + mq.cancels.Len()
}

// Convert the lists of wants into a Bitswap message
func (mq *MessageQueue) extractOutgoingMessage(supportsHave bool) (bsmsg.BitSwapMessage, func()) {
	// Get broadcast and regular wantlist entries.
	mq.wllock.Lock()
	peerEntries := mq.peerWants.pending.Entries()
	bcstEntries := mq.bcstWants.pending.Entries()
	cancels := mq.cancels.Keys()
	if !supportsHave {
		filteredPeerEntries := peerEntries[:0]
		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// don't send want-haves (only send want-blocks)
		//
		// Doing this here under the lock makes everything else in this
		// function simpler.
		//
		// TODO: We should _try_ to avoid recording these in the first
		// place if possible.
		for _, e := range peerEntries {
			if e.WantType == pb.Message_Wantlist_Have {
				mq.peerWants.removeType(e.Cid, pb.Message_Wantlist_Have)
			} else {
				filteredPeerEntries = append(filteredPeerEntries, e)
			}
		}
		peerEntries = filteredPeerEntries
	}
	mq.wllock.Unlock()

	// We prioritize cancels, then regular wants, then broadcast wants.

	var (
		msgSize         = 0 // size of message so far
		sentCancels     = 0 // number of cancels in message
		sentPeerEntries = 0 // number of peer entries in message
		sentBcstEntries = 0 // number of broadcast entries in message
	)

	// Add each cancel to the message
	for _, c := range cancels {
		msgSize += mq.msg.Cancel(c)
		sentCancels++

		if msgSize >= mq.maxMessageSize {
			goto FINISH
		}
	}

	// Next, add the wants. If we have too many entries to fit into a single
	// message, sort by priority and include the high priority ones first.

	for _, e := range peerEntries {
		msgSize += mq.msg.AddEntry(e.Cid, e.Priority, e.WantType, true)
		sentPeerEntries++

		if msgSize >= mq.maxMessageSize {
			goto FINISH
		}
	}

	// Add each broadcast want-have to the message
	for _, e := range bcstEntries {
		// Broadcast wants are sent as want-have
		wantType := pb.Message_Wantlist_Have

		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// send a want-block instead
		if !supportsHave {
			wantType = pb.Message_Wantlist_Block
		}

		msgSize += mq.msg.AddEntry(e.Cid, e.Priority, wantType, false)
		sentBcstEntries++

		if msgSize >= mq.maxMessageSize {
			goto FINISH
		}
	}

FINISH:

	// Finally, re-take the lock, mark sent and remove any entries from our
	// message that we've decided to cancel at the last minute.
	mq.wllock.Lock()
	for i, e := range peerEntries[:sentPeerEntries] {
		if !mq.peerWants.markSent(e) {
			// It changed.
			mq.msg.Remove(e.Cid)
			peerEntries[i].Cid = cid.Undef
		}
	}

	for i, e := range bcstEntries[:sentBcstEntries] {
		if !mq.bcstWants.markSent(e) {
			mq.msg.Remove(e.Cid)
			bcstEntries[i].Cid = cid.Undef
		}
	}

	for _, c := range cancels[:sentCancels] {
		if !mq.cancels.Has(c) {
			mq.msg.Remove(c)
		} else {
			mq.cancels.Remove(c)
		}
	}
	mq.wllock.Unlock()

	// When the message has been sent, record the time at which each want was
	// sent so we can calculate message latency
	onSent := func() {
		now := mq.clock.Now()

		mq.wllock.Lock()

		for _, e := range peerEntries[:sentPeerEntries] {
			if e.Cid.Defined() { // Check if want was canceled in the interim
				mq.peerWants.setSentAt(e.Cid, now)
			}
		}

		for _, e := range bcstEntries[:sentBcstEntries] {
			if e.Cid.Defined() { // Check if want was canceled in the interim
				mq.bcstWants.setSentAt(e.Cid, now)
			}
		}

		mq.wllock.Unlock()

		if mq.events != nil {
			mq.events <- messageFinishedSending
		}
	}

	return mq.msg, onSent
}

func (mq *MessageQueue) initializeSender() (bsnet.MessageSender, error) {
	if mq.sender == nil {
		opts := &bsnet.MessageSenderOpts{
			MaxRetries:       maxRetries,
			SendTimeout:      sendTimeout,
			SendErrorBackoff: sendErrorBackoff,
		}
		nsender, err := mq.network.NewMessageSender(mq.ctx, mq.p, opts)
		if err != nil {
			return nil, err
		}

		mq.sender = nsender
	}
	return mq.sender, nil
}
