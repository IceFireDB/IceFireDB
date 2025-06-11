// Package decision implements the decision engine for the bitswap service.
package decision

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	wl "github.com/ipfs/boxo/bitswap/client/wantlist"
	"github.com/ipfs/boxo/bitswap/internal/defaults"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bmetrics "github.com/ipfs/boxo/bitswap/metrics"
	bstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// TODO consider taking responsibility for other types of requests. For
// example, there could be a |cancelQueue| for all of the cancellation
// messages that need to go out. There could also be a |wantlistQueue| for
// the local peer's wantlists. Alternatively, these could all be bundled
// into a single, intelligent global queue that efficiently
// batches/combines and takes all of these into consideration.
//
// Right now, messages go onto the network for four reasons:
// 1. an initial `sendwantlist` message to a provider of the first key in a
//    request
// 2. a periodic full sweep of `sendwantlist` messages to all providers
// 3. upon receipt of blocks, a `cancel` message to all peers
// 4. draining the priority queue of `blockrequests` from peers
//
// Presently, only `blockrequests` are handled by the decision engine.
// However, there is an opportunity to give it more responsibility! If the
// decision engine is given responsibility for all of the others, it can
// intelligently decide how to combine requests efficiently.
//
// Some examples of what would be possible:
//
// * when sending out the wantlists, include `cancel` requests
// * when handling `blockrequests`, include `sendwantlist` and `cancel` as
//   appropriate
// * when handling `cancel`, if we recently received a wanted block from a
//   peer, include a partial wantlist that contains a few other high priority
//   blocks
//
// In a sense, if we treat the decision engine as a black box, it could do
// whatever it sees fit to produce desired outcomes (get wanted keys
// quickly, maintain good relationships with peers, etc).

var log = logging.Logger("bitswap/server/decision")

const (
	// outboxChanBuffer must be 0 to prevent stale messages from being sent
	outboxChanBuffer = 0
	// targetMessageSize is the ideal size of the batched payload. We try to
	// pop this much data off the request queue, but it may be a little more
	// or less depending on what's in the queue.
	defaultTargetMessageSize = 16 * 1024
	// tagFormat is the tag given to peers associated an engine
	tagFormat = "bs-engine-%s-%s"

	// queuedTagWeight is the default weight for peers that have work queued
	// on their behalf.
	queuedTagWeight = 10
)

// Envelope contains a message for a Peer.
type Envelope struct {
	// Peer is the intended recipient.
	Peer peer.ID

	// Message is the payload.
	Message bsmsg.BitSwapMessage

	// A callback to notify the decision queue that the task is complete
	Sent func()
}

// PeerTagger covers the methods on the connection manager used by the decision
// engine to tag peers
type PeerTagger interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(p peer.ID, tag string)
}

// Assigns a specific score to a peer
type ScorePeerFunc func(peer.ID, int)

// ScoreLedger is an external ledger dealing with peer scores.
type ScoreLedger interface {
	// Returns aggregated data communication with a given peer.
	GetReceipt(p peer.ID) *Receipt
	// Increments the sent counter for the given peer.
	AddToSentBytes(p peer.ID, n int)
	// Increments the received counter for the given peer.
	AddToReceivedBytes(p peer.ID, n int)
	// PeerConnected should be called when a new peer connects,
	// meaning the ledger should open accounting.
	PeerConnected(p peer.ID)
	// PeerDisconnected should be called when a peer disconnects to
	// clean up the accounting.
	PeerDisconnected(p peer.ID)
	// Starts the ledger sampling process.
	Start(scorePeer ScorePeerFunc)
	// Stops the sampling process.
	Stop()
}

type PeerEntry struct {
	Peer     peer.ID
	Priority int32
	WantType pb.Message_Wantlist_WantType
}

// PeerLedger is an external ledger dealing with peers and their want lists.
type PeerLedger interface {
	// Wants informs the ledger that [peer.ID] wants [wl.Entry].
	// If peer ledger exceed internal limit, then the entry is not added
	// and false is returned.
	Wants(p peer.ID, e wl.Entry) bool

	// CancelWant returns true if the [cid.Cid] was removed from the wantlist of [peer.ID].
	CancelWant(p peer.ID, k cid.Cid) bool

	// CancelWantWithType will not cancel WantBlock if we sent a HAVE message.
	CancelWantWithType(p peer.ID, k cid.Cid, typ pb.Message_Wantlist_WantType)

	// Peers returns all peers that want [cid.Cid].
	Peers(k cid.Cid) []PeerEntry

	// CollectPeerIDs returns all peers that the ledger has an active session with.
	CollectPeerIDs() []peer.ID

	// WantlistSizeForPeer returns the size of the wantlist for [peer.ID].
	WantlistSizeForPeer(p peer.ID) int

	// WantlistForPeer returns the wantlist for [peer.ID].
	WantlistForPeer(p peer.ID) []wl.Entry

	// ClearPeerWantlist clears the wantlist for [peer.ID].
	ClearPeerWantlist(p peer.ID)

	// PeerDisconnected informs the ledger that [peer.ID] is no longer connected.
	PeerDisconnected(p peer.ID)
}

// Engine manages sending requested blocks to peers.
type Engine struct {
	// peerRequestQueue is a priority queue of requests received from peers.
	// Requests are popped from the queue, packaged up, and placed in the
	// outbox.
	peerRequestQueue *peertaskqueue.PeerTaskQueue

	// FIXME it's a bit odd for the client and the worker to both share memory
	// (both modify the peerRequestQueue) and also to communicate over the
	// workSignal channel. consider sending requests over the channel and
	// allowing the worker to have exclusive access to the peerRequestQueue. In
	// that case, no lock would be required.
	workSignal chan struct{}

	// outbox contains outgoing messages to peers. This is owned by the
	// taskWorker goroutine
	outbox chan (<-chan *Envelope)

	bsm *blockstoreManager

	peerTagger PeerTagger

	tagQueued, tagUseful string

	lock sync.RWMutex // protects the fields immediately below

	// peerLedger saves which peers are waiting for a Cid
	peerLedger PeerLedger

	// an external ledger dealing with peer scores
	scoreLedger ScoreLedger

	ticker *time.Ticker

	taskWorkerLock  sync.Mutex
	taskWorkerCount int
	waitWorkers     sync.WaitGroup
	cancel          context.CancelFunc
	closeOnce       sync.Once

	targetMessageSize int

	// wantHaveReplaceSize is the maximum size of the block in bytes up to
	// which to replace a WantHave with a WantBlock.
	wantHaveReplaceSize int

	sendDontHaves bool

	self peer.ID

	// metrics gauge for total pending tasks across all workers
	pendingGauge metrics.Gauge

	// metrics gauge for total pending tasks across all workers
	activeGauge metrics.Gauge

	// used to ensure metrics are reported each fixed number of operation
	metricUpdateCounter atomic.Uint32

	taskComparator TaskComparator

	peerBlockRequestFilter PeerBlockRequestFilter

	bstoreWorkerCount          int
	maxOutstandingBytesPerPeer int

	maxQueuedWantlistEntriesPerPeer uint
	maxCidSize                      uint
}

// TaskInfo represents the details of a request from a peer.
type TaskInfo struct {
	Peer peer.ID
	// The CID of the block
	Cid cid.Cid
	// Tasks can be want-have or want-block
	IsWantBlock bool
	// Whether to immediately send a response if the block is not found
	SendDontHave bool
	// The size of the block corresponding to the task
	BlockSize int
	// Whether the block was found
	HaveBlock bool
}

// TaskComparator is used for task prioritization.
// It should return true if task 'ta' has higher priority than task 'tb'
type TaskComparator func(ta, tb *TaskInfo) bool

// PeerBlockRequestFilter is used to accept / deny requests for a CID coming from a PeerID
// It should return true if the request should be fulfilled.
type PeerBlockRequestFilter func(p peer.ID, c cid.Cid) bool

type Option func(*Engine)

func WithTaskComparator(comparator TaskComparator) Option {
	return func(e *Engine) {
		e.taskComparator = comparator
	}
}

func WithPeerBlockRequestFilter(pbrf PeerBlockRequestFilter) Option {
	return func(e *Engine) {
		e.peerBlockRequestFilter = pbrf
	}
}

func WithTargetMessageSize(size int) Option {
	return func(e *Engine) {
		e.targetMessageSize = size
	}
}

func WithScoreLedger(scoreledger ScoreLedger) Option {
	return func(e *Engine) {
		e.scoreLedger = scoreledger
	}
}

// WithPeerLedger sets a custom [PeerLedger] to be used with this [Engine].
func WithPeerLedger(peerLedger PeerLedger) Option {
	return func(e *Engine) {
		e.peerLedger = peerLedger
	}
}

// WithBlockstoreWorkerCount sets the number of worker threads used for
// blockstore operations in the decision engine
func WithBlockstoreWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("Engine blockstore worker count is %d but must be > 0", count))
	}
	return func(e *Engine) {
		e.bstoreWorkerCount = count
	}
}

// WithTaskWorkerCount sets the number of worker threads used inside the engine
func WithTaskWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("Engine task worker count is %d but must be > 0", count))
	}
	return func(e *Engine) {
		e.taskWorkerCount = count
	}
}

// WithMaxOutstandingBytesPerPeer describes approximately how much work we are will to have outstanding to a peer at any
// given time. Setting it to 0 will disable any limiting.
func WithMaxOutstandingBytesPerPeer(count int) Option {
	if count < 0 {
		panic(fmt.Sprintf("max outstanding bytes per peer is %d but must be >= 0", count))
	}
	return func(e *Engine) {
		e.maxOutstandingBytesPerPeer = count
	}
}

// WithMaxQueuedWantlistEntriesPerPeer limits how many individual entries each
// peer is allowed to send. If a peer sends more than this, then the lowest
// priority entries are truncated to this limit. If there is insufficient space
// to enqueue new entries, then older existing wants with no associated blocks,
// and lower priority wants, are canceled to make room for the new wants.
func WithMaxQueuedWantlistEntriesPerPeer(count uint) Option {
	return func(e *Engine) {
		e.maxQueuedWantlistEntriesPerPeer = count
	}
}

// WithMaxQueuedWantlistEntriesPerPeer limits how much individual entries each peer is allowed to send.
// If a peer send us more than this we will truncate newest entries.
func WithMaxCidSize(n uint) Option {
	return func(e *Engine) {
		e.maxCidSize = n
	}
}

func WithSetSendDontHave(send bool) Option {
	return func(e *Engine) {
		e.sendDontHaves = send
	}
}

// WithWantHaveReplaceSize sets the maximum size of a block in bytes up to
// which to replace a WantHave with a WantBlock response.
func WithWantHaveReplaceSize(size int) Option {
	return func(e *Engine) {
		e.wantHaveReplaceSize = size
	}
}

// wrapTaskComparator wraps a TaskComparator so it can be used as a QueueTaskComparator
func wrapTaskComparator(tc TaskComparator) peertask.QueueTaskComparator {
	return func(a, b *peertask.QueueTask) bool {
		taskDataA := a.Task.Data.(*taskData)
		taskInfoA := &TaskInfo{
			Peer:         a.Target,
			Cid:          a.Task.Topic.(cid.Cid),
			IsWantBlock:  taskDataA.IsWantBlock,
			SendDontHave: taskDataA.SendDontHave,
			BlockSize:    taskDataA.BlockSize,
			HaveBlock:    taskDataA.HaveBlock,
		}
		taskDataB := b.Task.Data.(*taskData)
		taskInfoB := &TaskInfo{
			Peer:         b.Target,
			Cid:          b.Task.Topic.(cid.Cid),
			IsWantBlock:  taskDataB.IsWantBlock,
			SendDontHave: taskDataB.SendDontHave,
			BlockSize:    taskDataB.BlockSize,
			HaveBlock:    taskDataB.HaveBlock,
		}
		return tc(taskInfoA, taskInfoB)
	}
}

// NewEngine creates a new block sending engine for the given block store.
// maxOutstandingBytesPerPeer hints to the peer task queue not to give a peer
// more tasks if it has some maximum work already outstanding.
func NewEngine(
	ctx context.Context,
	bs bstore.Blockstore,
	peerTagger PeerTagger,
	self peer.ID,
	opts ...Option,
) *Engine {
	ctx, cancel := context.WithCancel(ctx)

	e := &Engine{
		scoreLedger:                     NewDefaultScoreLedger(),
		bstoreWorkerCount:               defaults.BitswapEngineBlockstoreWorkerCount,
		maxOutstandingBytesPerPeer:      defaults.BitswapMaxOutstandingBytesPerPeer,
		peerTagger:                      peerTagger,
		outbox:                          make(chan (<-chan *Envelope), outboxChanBuffer),
		workSignal:                      make(chan struct{}, 1),
		ticker:                          time.NewTicker(time.Millisecond * 100),
		wantHaveReplaceSize:             defaults.DefaultWantHaveReplaceSize,
		taskWorkerCount:                 defaults.BitswapEngineTaskWorkerCount,
		sendDontHaves:                   true,
		self:                            self,
		pendingGauge:                    bmetrics.PendingEngineGauge(ctx),
		activeGauge:                     bmetrics.ActiveEngineGauge(ctx),
		targetMessageSize:               defaultTargetMessageSize,
		tagQueued:                       fmt.Sprintf(tagFormat, "queued", uuid.New().String()),
		tagUseful:                       fmt.Sprintf(tagFormat, "useful", uuid.New().String()),
		maxQueuedWantlistEntriesPerPeer: defaults.MaxQueuedWantlistEntiresPerPeer,
		maxCidSize:                      defaults.MaximumAllowedCid,
		cancel:                          cancel,
	}

	for _, opt := range opts {
		opt(e)
	}

	// If peerLedger was not set by option, then create a default instance.
	if e.peerLedger == nil {
		e.peerLedger = NewDefaultPeerLedger(e.maxQueuedWantlistEntriesPerPeer)
	}

	e.bsm = newBlockstoreManager(bs, e.bstoreWorkerCount, bmetrics.PendingBlocksGauge(ctx), bmetrics.ActiveBlocksGauge(ctx))

	// default peer task queue options
	peerTaskQueueOpts := []peertaskqueue.Option{
		peertaskqueue.OnPeerAddedHook(e.onPeerAdded),
		peertaskqueue.OnPeerRemovedHook(e.onPeerRemoved),
		peertaskqueue.TaskMerger(newTaskMerger()),
		peertaskqueue.IgnoreFreezing(true),
		peertaskqueue.MaxOutstandingWorkPerPeer(e.maxOutstandingBytesPerPeer),
	}

	if e.taskComparator != nil {
		queueTaskComparator := wrapTaskComparator(e.taskComparator)
		peerTaskQueueOpts = append(peerTaskQueueOpts, peertaskqueue.PeerComparator(peertracker.TaskPriorityPeerComparator(queueTaskComparator)))
		peerTaskQueueOpts = append(peerTaskQueueOpts, peertaskqueue.TaskComparator(queueTaskComparator))
	}

	e.peerRequestQueue = peertaskqueue.New(peerTaskQueueOpts...)

	if e.wantHaveReplaceSize == 0 {
		log.Info("Replace WantHave with WantBlock is disabled")
	} else {
		log.Infow("Replace WantHave with WantBlock is enabled", "maxSize", e.wantHaveReplaceSize)
	}

	e.startWorkers(ctx)

	return e
}

func (e *Engine) updateMetrics() {
	c := e.metricUpdateCounter.Add(1)
	if c%100 == 0 {
		stats := e.peerRequestQueue.Stats()
		e.activeGauge.Set(float64(stats.NumActive))
		e.pendingGauge.Set(float64(stats.NumPending))
	}
}

// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// Older versions of Bitswap did not respond, so this allows us to simulate
// those older versions for testing.
func (e *Engine) SetSendDontHaves(send bool) {
	e.sendDontHaves = send
}

// Starts the score ledger. Before start the function checks and,
// if it is unset, initializes the scoreLedger with the default
// implementation.
func (e *Engine) startScoreLedger() {
	e.scoreLedger.Start(func(p peer.ID, score int) {
		if score == 0 {
			e.peerTagger.UntagPeer(p, e.tagUseful)
		} else {
			e.peerTagger.TagPeer(p, e.tagUseful, score)
		}
	})
}

// startWorkers starts workers to handle requests from other nodes for the data
// on this node.
func (e *Engine) startWorkers(ctx context.Context) {
	e.bsm.start()
	e.startScoreLedger()

	e.taskWorkerLock.Lock()
	defer e.taskWorkerLock.Unlock()

	e.waitWorkers.Add(e.taskWorkerCount)
	for i := 0; i < e.taskWorkerCount; i++ {
		go e.taskWorker(ctx)
	}
}

// Close shuts down the decision engine and returns after all workers have
// finished. Safe to call multiple times/concurrently.
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		e.cancel()
		e.bsm.stop()
		e.scoreLedger.Stop()
	})
	e.waitWorkers.Wait()
}

func (e *Engine) onPeerAdded(p peer.ID) {
	e.peerTagger.TagPeer(p, e.tagQueued, queuedTagWeight)
}

func (e *Engine) onPeerRemoved(p peer.ID) {
	e.peerTagger.UntagPeer(p, e.tagQueued)
}

// WantlistForPeer returns the list of keys that the given peer has asked for
func (e *Engine) WantlistForPeer(p peer.ID) []wl.Entry {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return e.peerLedger.WantlistForPeer(p)
}

// LedgerForPeer returns aggregated data communication with a given peer.
func (e *Engine) LedgerForPeer(p peer.ID) *Receipt {
	return e.scoreLedger.GetReceipt(p)
}

// Each taskWorker pulls items off the request queue up to the maximum size
// and adds them to an envelope that is passed off to the bitswap workers,
// which send the message to the network.
func (e *Engine) taskWorker(ctx context.Context) {
	defer e.waitWorkers.Done()
	defer e.taskWorkerExit()
	for {
		oneTimeUse := make(chan *Envelope, 1) // buffer to prevent blocking
		select {
		case <-ctx.Done():
			return
		case e.outbox <- oneTimeUse:
		}
		// receiver is ready for an outoing envelope. let's prepare one. first,
		// we must acquire a task from the PQ...
		envelope, err := e.nextEnvelope(ctx)
		if err != nil {
			close(oneTimeUse)
			return // ctx cancelled
		}
		oneTimeUse <- envelope // buffered. won't block
		close(oneTimeUse)
	}
}

// taskWorkerExit handles cleanup of task workers
func (e *Engine) taskWorkerExit() {
	e.taskWorkerLock.Lock()
	defer e.taskWorkerLock.Unlock()

	e.taskWorkerCount--
	if e.taskWorkerCount == 0 {
		close(e.outbox)
	}
}

// nextEnvelope runs in the taskWorker goroutine. Returns an error if the
// context is cancelled before the next Envelope can be created.
func (e *Engine) nextEnvelope(ctx context.Context) (*Envelope, error) {
	for {
		// Pop some tasks off the request queue
		p, nextTasks, pendingBytes := e.peerRequestQueue.PopTasks(e.targetMessageSize)
		e.updateMetrics()
		for len(nextTasks) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.workSignal:
				p, nextTasks, pendingBytes = e.peerRequestQueue.PopTasks(e.targetMessageSize)
				e.updateMetrics()
			case <-e.ticker.C:
				// When a task is cancelled, the queue may be "frozen" for a
				// period of time. We periodically "thaw" the queue to make
				// sure it doesn't get stuck in a frozen state.
				e.peerRequestQueue.ThawRound()
				p, nextTasks, pendingBytes = e.peerRequestQueue.PopTasks(e.targetMessageSize)
				e.updateMetrics()
			}
		}

		// Create a new message
		msg := bsmsg.New(false)

		log.Debugw("Bitswap process tasks", "local", e.self, "taskCount", len(nextTasks))

		// Amount of data in the request queue still waiting to be popped
		msg.SetPendingBytes(int32(pendingBytes))

		// Split out want-blocks, want-haves and DONT_HAVEs
		blockCids := make([]cid.Cid, 0, len(nextTasks))
		blockTasks := make(map[cid.Cid]*taskData, len(nextTasks))
		for _, t := range nextTasks {
			c := t.Topic.(cid.Cid)
			td := t.Data.(*taskData)
			if td.HaveBlock {
				if td.IsWantBlock {
					blockCids = append(blockCids, c)
					blockTasks[c] = td
				} else {
					// Add HAVES to the message
					msg.AddHave(c)
				}
			} else {
				// Add DONT_HAVEs to the message
				msg.AddDontHave(c)
			}
		}

		// Fetch blocks from datastore
		blks, err := e.bsm.getBlocks(ctx, blockCids)
		if err != nil {
			// we're dropping the envelope but that's not an issue in practice.
			return nil, err
		}

		for c, t := range blockTasks {
			blk := blks[c]
			// If the block was not found (it has been removed)
			if blk == nil {
				// If the client requested DONT_HAVE, add DONT_HAVE to the message
				if t.SendDontHave {
					msg.AddDontHave(c)
				}
			} else {
				// Add the block to the message
				// log.Debugf("  make evlp %s->%s block: %s (%d bytes)", e.self, p, c, len(blk.RawData()))
				msg.AddBlock(blk)
			}
		}

		// If there's nothing in the message, bail out
		if msg.Empty() {
			e.peerRequestQueue.TasksDone(p, nextTasks...)
			continue
		}

		log.Debugw("Bitswap engine -> msg", "local", e.self, "to", p, "blockCount", len(msg.Blocks()), "presenceCount", len(msg.BlockPresences()), "size", msg.Size())
		return &Envelope{
			Peer:    p,
			Message: msg,
			Sent: func() {
				// Once the message has been sent, signal the request queue so
				// it can be cleared from the queue
				e.peerRequestQueue.TasksDone(p, nextTasks...)

				// Signal the worker to check for more work
				e.signalNewWork()
			},
		}, nil
	}
}

// Outbox returns a channel of one-time use Envelope channels.
func (e *Engine) Outbox() <-chan (<-chan *Envelope) {
	return e.outbox
}

// Peers returns a slice of Peers with whom the local node has active sessions.
func (e *Engine) Peers() []peer.ID {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return e.peerLedger.CollectPeerIDs()
}

// MessageReceived is called when a message is received from a remote peer.
// For each item in the wantlist, add a want-have or want-block entry to the
// request queue (this is later popped off by the workerTasks). Returns true
// if the connection to the server must be closed.
func (e *Engine) MessageReceived(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) bool {
	if m.Empty() {
		log.Infof("received empty message from %s", p)
		return false
	}

	wants, cancels, denials, err := e.splitWantsCancelsDenials(p, m)
	if err != nil {
		// This is a truly broken client, let's kill the connection.
		log.Warnw(err.Error(), "local", e.self, "remote", p)
		return true
	}

	noReplace := e.wantHaveReplaceSize == 0

	// Get block sizes for unique CIDs.
	wantKs := make([]cid.Cid, 0, len(wants))
	var haveKs []cid.Cid
	for _, entry := range wants {
		if noReplace && entry.WantType == pb.Message_Wantlist_Have {
			haveKs = append(haveKs, entry.Cid)
		} else {
			wantKs = append(wantKs, entry.Cid)
		}
	}
	blockSizes, err := e.bsm.getBlockSizes(ctx, wantKs)
	if err != nil {
		log.Info("aborting message processing", err)
		return false
	}
	if len(haveKs) != 0 {
		hasBlocks, err := e.bsm.hasBlocks(ctx, haveKs)
		if err != nil {
			log.Info("aborting message processing", err)
			return false
		}
		if len(hasBlocks) != 0 {
			if blockSizes == nil {
				blockSizes = make(map[cid.Cid]int, len(hasBlocks))
			}
			for blkCid := range hasBlocks {
				blockSizes[blkCid] = 0
			}
		}
	}

	e.lock.Lock()

	if m.Full() {
		e.peerLedger.ClearPeerWantlist(p)
	}

	var overflow []bsmsg.Entry
	wants, overflow = e.filterOverflow(p, wants, overflow)

	if len(overflow) != 0 {
		log.Infow("handling wantlist overflow", "local", e.self, "from", p, "wantlistSize", len(wants), "overflowSize", len(overflow))
		wants = e.handleOverflow(ctx, p, overflow, wants)
	}

	for _, entry := range cancels {
		c := entry.Cid
		log.Debugw("Bitswap engine <- cancel", "local", e.self, "from", p, "cid", c)
		if e.peerLedger.CancelWant(p, c) {
			e.peerRequestQueue.Remove(c, p)
		}
	}

	e.lock.Unlock()

	var activeEntries []peertask.Task

	// Cancel a block operation
	sendDontHave := func(entry bsmsg.Entry) {
		// Only add the task to the queue if the requester wants a DONT_HAVE
		if e.sendDontHaves && entry.SendDontHave {
			c := entry.Cid
			activeEntries = append(activeEntries, peertask.Task{
				Topic:    c,
				Priority: int(entry.Priority),
				Work:     bsmsg.BlockPresenceSize(c),
				Data: &taskData{
					BlockSize:    0,
					HaveBlock:    false,
					IsWantBlock:  entry.WantType == pb.Message_Wantlist_Block,
					SendDontHave: entry.SendDontHave,
				},
			})
		}
	}

	// Deny access to blocks
	for _, entry := range denials {
		log.Debugw("Bitswap engine: block denied access", "local", e.self, "from", p, "cid", entry.Cid, "sendDontHave", entry.SendDontHave)
		sendDontHave(entry)
	}

	// For each want-block
	for _, entry := range wants {
		c := entry.Cid
		blockSize, found := blockSizes[c]

		// If the block was not found
		if !found {
			log.Debugw("Bitswap engine: block not found", "local", e.self, "from", p, "cid", c, "sendDontHave", entry.SendDontHave)
			sendDontHave(entry)
			continue
		}
		// The block was found, add it to the queue

		// Check if this is a want-block or a have-block that can be converted
		// to a want-block.
		isWantBlock := blockSize != 0 && e.sendAsBlock(entry.WantType, blockSize)

		log.Debugw("Bitswap engine: block found", "local", e.self, "from", p, "cid", c, "isWantBlock", isWantBlock)

		// entrySize is the amount of space the entry takes up in the
		// message we send to the recipient. If we're sending a block, the
		// entrySize is the size of the block. Otherwise it's the size of
		// a block presence entry.
		entrySize := blockSize
		if !isWantBlock {
			entrySize = bsmsg.BlockPresenceSize(c)
		}
		activeEntries = append(activeEntries, peertask.Task{
			Topic:    c,
			Priority: int(entry.Priority),
			Work:     entrySize,
			Data: &taskData{
				BlockSize:    blockSize,
				HaveBlock:    true,
				IsWantBlock:  isWantBlock,
				SendDontHave: entry.SendDontHave,
			},
		})
	}

	// Push entries onto the request queue and signal network that new work is ready.
	if len(activeEntries) != 0 {
		e.peerRequestQueue.PushTasksTruncated(e.maxQueuedWantlistEntriesPerPeer, p, activeEntries...)
		e.updateMetrics()
		e.signalNewWork()
	}
	return false
}

func (e *Engine) filterOverflow(p peer.ID, wants, overflow []bsmsg.Entry) ([]bsmsg.Entry, []bsmsg.Entry) {
	if len(wants) == 0 {
		return wants, overflow
	}

	filteredWants := wants[:0] // shift inplace
	for _, entry := range wants {
		if !e.peerLedger.Wants(p, entry.Entry) {
			// Cannot add entry because it would exceed size limit.
			overflow = append(overflow, entry)
			continue
		}
		filteredWants = append(filteredWants, entry)
	}
	// Clear truncated entries - early GC.
	clear(wants[len(filteredWants):])
	return filteredWants, overflow
}

// handleOverflow processes incoming wants that could not be addded to the peer
// ledger without exceeding the peer want limit. These are handled by trying to
// make room by canceling existing wants for which there is no block. If this
// does not make sufficient room, then any lower priority wants that have
// blocks are canceled.
//
// Important: handleOverflwo must be called e.lock is locked.
func (e *Engine) handleOverflow(ctx context.Context, p peer.ID, overflow, wants []bsmsg.Entry) []bsmsg.Entry {
	// Sort overflow from most to least important.
	slices.SortFunc(overflow, func(a, b bsmsg.Entry) int {
		return cmp.Compare(b.Entry.Priority, a.Entry.Priority)
	})
	// Sort existing wants from least to most important, to try to replace
	// lowest priority items first.
	existingWants := e.peerLedger.WantlistForPeer(p)
	slices.SortFunc(existingWants, func(a, b wl.Entry) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	queuedWantKs := cid.NewSet()
	for _, entry := range existingWants {
		queuedWantKs.Add(entry.Cid)
	}
	queuedBlockSizes, err := e.bsm.getBlockSizes(ctx, queuedWantKs.Keys())
	if err != nil {
		log.Info("aborting overflow processing", err)
		return wants
	}

	// Remove entries for blocks that are not present to make room for overflow.
	var removed []int
	for i, w := range existingWants {
		if _, found := queuedBlockSizes[w.Cid]; !found {
			// Cancel lowest priority dont-have.
			if e.peerLedger.CancelWant(p, w.Cid) {
				e.peerRequestQueue.Remove(w.Cid, p)
			}
			removed = append(removed, i)
			// Pop hoghest priority overflow.
			firstOver := overflow[0]
			overflow = overflow[1:]
			// Add highest priority overflow to wants.
			e.peerLedger.Wants(p, firstOver.Entry)
			wants = append(wants, firstOver)
			if len(overflow) == 0 {
				return wants
			}
		}
	}

	// Replace existing entries, that are a lower priority, with overflow
	// entries.
	var replace int
	for _, overflowEnt := range overflow {
		// Do not compare with removed existingWants entry.
		for len(removed) != 0 && replace == removed[0] {
			replace++
			removed = removed[1:]
		}
		if overflowEnt.Entry.Priority < existingWants[replace].Priority {
			// All overflow entries have too low of priority to replace any
			// existing wants.
			break
		}
		entCid := existingWants[replace].Cid
		replace++
		if e.peerLedger.CancelWant(p, entCid) {
			e.peerRequestQueue.Remove(entCid, p)
		}
		e.peerLedger.Wants(p, overflowEnt.Entry)
		wants = append(wants, overflowEnt)
	}

	return wants
}

// Split the want, cancel, and deny entries.
func (e *Engine) splitWantsCancelsDenials(p peer.ID, m bsmsg.BitSwapMessage) ([]bsmsg.Entry, []bsmsg.Entry, []bsmsg.Entry, error) {
	entries := m.Wantlist() // creates copy; safe to modify
	if len(entries) == 0 {
		return nil, nil, nil, nil
	}

	log.Debugw("Bitswap engine <- msg", "local", e.self, "from", p, "entryCount", len(entries))

	wants := entries[:0] // shift in-place
	var cancels, denials []bsmsg.Entry

	for _, et := range entries {
		c := et.Cid
		if e.maxCidSize != 0 && uint(c.ByteLen()) > e.maxCidSize {
			// Ignore requests about CIDs that big.
			continue
		}
		if c.Prefix().MhType == mh.IDENTITY {
			return nil, nil, nil, errors.New("peer canceled an identity CID")
		}

		if et.Cancel {
			cancels = append(cancels, et)
			continue
		}

		if e.peerBlockRequestFilter != nil && !e.peerBlockRequestFilter(p, c) {
			denials = append(denials, et)
			continue
		}

		if et.WantType == pb.Message_Wantlist_Have {
			log.Debugw("Bitswap engine <- want-have", "local", e.self, "from", p, "cid", c)
		} else {
			log.Debugw("Bitswap engine <- want-block", "local", e.self, "from", p, "cid", c)
		}

		// Do not take more wants that can be handled.
		if len(wants) < int(e.maxQueuedWantlistEntriesPerPeer) {
			wants = append(wants, et)
		}
	}

	if len(wants) == 0 {
		wants = nil
	}

	// Clear truncated entries.
	clear(entries[len(wants):])

	return wants, cancels, denials, nil
}

// ReceivedBlocks is called when new blocks are received from the network.
// This function also updates the receive side of the ledger.
func (e *Engine) ReceivedBlocks(from peer.ID, blks []blocks.Block) {
	if len(blks) == 0 {
		return
	}

	// Record how many bytes were received in the ledger
	for _, blk := range blks {
		log.Debugw("Bitswap engine <- block", "local", e.self, "from", from, "cid", blk.Cid(), "size", len(blk.RawData()))
		e.scoreLedger.AddToReceivedBytes(from, len(blk.RawData()))
	}
}

// NotifyNewBlocks is called when new blocks becomes available locally, and in particular when the caller of bitswap
// decide to store those blocks and make them available on the network.
func (e *Engine) NotifyNewBlocks(blks []blocks.Block) {
	if len(blks) == 0 {
		return
	}

	// Get the size of each block
	blockSizes := make(map[cid.Cid]int, len(blks))
	for _, blk := range blks {
		blockSizes[blk.Cid()] = len(blk.RawData())
	}

	// Check each peer to see if it wants one of the blocks we received
	var work bool
	for _, b := range blks {
		k := b.Cid()
		blockSize := blockSizes[k]

		e.lock.RLock()
		peers := e.peerLedger.Peers(k)
		e.lock.RUnlock()

		for _, entry := range peers {
			work = true

			isWantBlock := e.sendAsBlock(entry.WantType, blockSize)

			entrySize := blockSize
			if !isWantBlock {
				entrySize = bsmsg.BlockPresenceSize(k)
			}

			e.peerRequestQueue.PushTasksTruncated(e.maxQueuedWantlistEntriesPerPeer, entry.Peer, peertask.Task{
				Topic:    k,
				Priority: int(entry.Priority),
				Work:     entrySize,
				Data: &taskData{
					BlockSize:    blockSize,
					HaveBlock:    true,
					IsWantBlock:  isWantBlock,
					SendDontHave: false,
				},
			})
			e.updateMetrics()
		}
	}

	if work {
		e.signalNewWork()
	}
}

// TODO add contents of m.WantList() to my local wantlist? NB: could introduce
// race conditions where I send a message, but MessageSent gets handled after
// MessageReceived. The information in the local wantlist could become
// inconsistent. Would need to ensure that Sends and acknowledgement of the
// send happen atomically

// MessageSent is called when a message has successfully been sent out, to record
// changes.
func (e *Engine) MessageSent(p peer.ID, m bsmsg.BitSwapMessage) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Remove sent blocks from the want list for the peer
	for _, block := range m.Blocks() {
		e.scoreLedger.AddToSentBytes(p, len(block.RawData()))
		e.peerLedger.CancelWantWithType(p, block.Cid(), pb.Message_Wantlist_Block)
	}

	// Remove sent block presences from the want list for the peer
	for _, bp := range m.BlockPresences() {
		// Don't record sent data. We reserve that for data blocks.
		if bp.Type == pb.Message_Have {
			e.peerLedger.CancelWantWithType(p, bp.Cid, pb.Message_Wantlist_Have)
		}
	}
}

// PeerConnected is called when a new peer connects, meaning we should start
// sending blocks.
func (e *Engine) PeerConnected(p peer.ID) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.scoreLedger.PeerConnected(p)
}

// PeerDisconnected is called when a peer disconnects.
func (e *Engine) PeerDisconnected(p peer.ID) {
	e.peerRequestQueue.Clear(p)

	e.lock.Lock()
	defer e.lock.Unlock()

	e.peerLedger.PeerDisconnected(p)
	e.scoreLedger.PeerDisconnected(p)
}

// If the want is a want-have, and it's below a certain size, send the full
// block (instead of sending a HAVE)
func (e *Engine) sendAsBlock(wantType pb.Message_Wantlist_WantType, blockSize int) bool {
	return wantType == pb.Message_Wantlist_Block || blockSize <= e.wantHaveReplaceSize
}

func (e *Engine) numBytesSentTo(p peer.ID) uint64 {
	return e.LedgerForPeer(p).Sent
}

func (e *Engine) numBytesReceivedFrom(p peer.ID) uint64 {
	return e.LedgerForPeer(p).Recv
}

func (e *Engine) signalNewWork() {
	// Signal task generation to restart (if stopped!)
	select {
	case e.workSignal <- struct{}{}:
	default:
	}
}
