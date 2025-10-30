package session

import (
	"context"
	"time"

	"github.com/ipfs/boxo/bitswap/client/internal"
	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	bsgetter "github.com/ipfs/boxo/bitswap/client/internal/getter"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bspm "github.com/ipfs/boxo/bitswap/client/internal/peermanager"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/trace"
)

var log = logging.Logger("bitswap/session")

const (
	broadcastLiveWantsLimit = 64
)

// PeerManager keeps track of which sessions are interested in which peers and
// takes care of sending wants for the sessions.
type PeerManager interface {
	// RegisterSession tells the PeerManager that the session is interested in
	// a peer's connection state.
	RegisterSession(peer.ID, bspm.Session)
	// UnregisterSession tells the PeerManager that the session is no longer
	// interested in a peer's connection state.
	UnregisterSession(uint64)
	// SendWants tells the PeerManager to send wants to the given peer.
	SendWants(peerId peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) bool
	// BroadcastWantHaves sends want-haves to all connected peers (used for
	// session discovery).
	BroadcastWantHaves([]cid.Cid)
	// SendCancels tells the PeerManager to send cancels to all peers.
	SendCancels([]cid.Cid)
}

// SessionManager manages all the sessions,
type SessionManager interface {
	// RemoveSession removes a session (called when the session shuts down).
	RemoveSession(sesid uint64)
	// CancelSessionWants cancels the specified session's wants (called when a
	// call to GetBlocks() is cancelled).
	CancelSessionWants(sid uint64, wants []cid.Cid)
}

// SessionPeerManager keeps track of peers in the session
type SessionPeerManager interface {
	// PeersDiscovered indicates if any peers have been discovered yet,
	PeersDiscovered() bool
	// Shutdown stops the SessionPeerManager.
	Shutdown()
	// AddPeer adds a peer to the session, returning true if the peer is new.
	AddPeer(peer.ID) bool
	// RemovePeer removes a peer from the session, returning true if the peer
	// existed.
	RemovePeer(peer.ID) bool
	// Peers returns all peers in the session.
	Peers() []peer.ID
	// HasPeers returns true if there are any peers in the session.
	HasPeers() bool
	// ProtectConnection prevents a connection from being pruned by the
	// connection manager.
	ProtectConnection(peer.ID)
}

// opType is the kind of operation that is being processed by the event loop.
type opType int

const (
	// Receive blocks
	opReceive opType = iota
	// Want blocks
	opWant
	// Cancel wants
	opCancel
	// Broadcast want-haves
	opBroadcast
	// Wants sent to peers
	opWantsSent
)

type op struct {
	op   opType
	keys []cid.Cid
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
//
// Sessions manage their own lifecycle independently of any request contexts.
// Each session maintains an internal context for its operations and must be
// explicitly closed via the Close() method when no longer needed. The session's
// internal context is used for provider discovery and other long-running operations.
type Session struct {
	// dependencies
	ctx            context.Context
	cancel         context.CancelFunc
	sm             SessionManager
	pm             PeerManager
	sprm           SessionPeerManager
	providerFinder routing.ContentDiscovery
	sim            *bssim.SessionInterestManager

	sw  sessionWants
	sws sessionWantSender

	latencyTrkr latencyTracker

	// channels
	incoming      chan op
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	idleTick            *time.Timer
	baseTickDelay       time.Duration
	consecutiveTicks    int
	initialSearchDelay  time.Duration
	periodicSearchDelay time.Duration
	// identifiers
	notif notifications.PubSub
	id    uint64

	self peer.ID
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
//
// The caller MUST call Close() or cancel the context when the session is no
// longer needed to ensure proper cleanup.
//
// The retrievalState parameter, if provided, enables diagnostic tracking of
// the retrieval process. It is attached to the session's internal context and
// used to track provider discovery, connection attempts, and data retrieval
// phases. This is particularly useful for debugging timeout errors and
// understanding retrieval performance.
func New(
	ctx context.Context,
	sm SessionManager,
	id uint64,
	sprm SessionPeerManager,
	providerFinder routing.ContentDiscovery,
	sim *bssim.SessionInterestManager,
	pm PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay time.Duration,
	self peer.ID,
	havesReceivedGauge bspm.Gauge,
) *Session {
	ctx, cancel := context.WithCancel(ctx)

	s := &Session{
		sw:                  newSessionWants(broadcastLiveWantsLimit),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		cancel:              cancel,
		sm:                  sm,
		pm:                  pm,
		sprm:                sprm,
		providerFinder:      providerFinder,
		sim:                 sim,
		incoming:            make(chan op, 128),
		latencyTrkr:         latencyTracker{},
		notif:               notif,
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
		self:                self,
	}
	s.sws = newSessionWantSender(id, pm, sprm, sm, bpm, s.onWantsSent, s.onPeersExhausted, havesReceivedGauge)

	go s.run(ctx)

	return s
}

func (s *Session) ID() uint64 {
	return s.id
}

// Close terminates the session and cleans up its resources. This method must
// be called, or the context used to create the session must be canceled, when
// the session is no longer needed to avoid resource leaks. After calling
// Close, the session should not be used anymore.
func (s *Session) Close() {
	s.cancel()
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// The SessionManager tells each Session about all keys that it may be
	// interested in. Here the Session filters the keys to the ones that this
	// particular Session is interested in.
	interestedRes := s.sim.FilterSessionInterested(s.id, ks, haves, dontHaves)
	ks = interestedRes[0]
	haves = interestedRes[1]
	dontHaves = interestedRes[2]
	s.logReceiveFrom(from, ks, haves, dontHaves)

	// Inform the session want sender that a message has been received.
	s.sws.Update(from, ks, haves, dontHaves)

	if len(ks) == 0 {
		return
	}

	// Inform the session that blocks have been received.
	select {
	case s.incoming <- op{op: opReceive, keys: ks}:
	case <-s.ctx.Done():
	}
}

func (s *Session) logReceiveFrom(from peer.ID, interestedKs []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	if !log.LevelEnabled(logging.LevelDebug) {
		return
	}

	for _, c := range interestedKs {
		log.Debugw("Bitswap <- block", "local", s.self, "from", from, "cid", c, "session", s.id)
	}
	for _, c := range haves {
		log.Debugw("Bitswap <- HAVE", "local", s.self, "from", from, "cid", c, "session", s.id)
	}
	for _, c := range dontHaves {
		log.Debugw("Bitswap <- DONT_HAVE", "local", s.self, "from", from, "cid", c, "session", s.id)
	}
}

// GetBlock fetches a single block.
func (s *Session) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlock")
	defer span.End()
	return bsgetter.SyncGetBlock(ctx, k, s.GetBlocks)
}

// GetBlocks fetches a set of blocks within the context of this session and
// returns a channel that found blocks will be returned on. No order is
// guaranteed on the returned blocks.
func (s *Session) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlocks")
	defer span.End()

	return bsgetter.AsyncGetBlocks(ctx, s.ctx, keys, s.notif,
		func(ctx context.Context, keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opWant, keys: keys}:
			case <-ctx.Done():
			case <-s.ctx.Done():
			}
		},
		func(keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opCancel, keys: keys}:
			case <-s.ctx.Done():
			}
		},
	)
}

// SetBaseTickDelay changes the rate at which ticks happen.
func (s *Session) SetBaseTickDelay(baseTickDelay time.Duration) {
	select {
	case s.tickDelayReqs <- baseTickDelay:
	case <-s.ctx.Done():
	}
}

// onWantsSent is called when wants are sent to a peer by the session wants sender
func (s *Session) onWantsSent(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	allBlks := append(wantBlocks[:len(wantBlocks):len(wantBlocks)], wantHaves...)
	s.nonBlockingEnqueue(op{op: opWantsSent, keys: allBlks})
}

// onPeersExhausted is called when all available peers have sent DONT_HAVE for
// a set of cids (or all peers become unavailable)
func (s *Session) onPeersExhausted(ks []cid.Cid) {
	s.nonBlockingEnqueue(op{op: opBroadcast, keys: ks})
}

// nonBlockingEnqueue enqueues an operation on the incoming queue without
// blocking. We do not want to block the sessionWantSender if the incoming
// channel is full. So if we cannot immediately send on the incoming channel
// spin it off into a goroutine.
func (s *Session) nonBlockingEnqueue(o op) {
	select {
	case s.incoming <- o:
	default:
		go func() {
			select {
			case s.incoming <- o:
			case <-s.ctx.Done():
			}
		}()
	}
}

// run is the session run loop. Everything in this function should not be
// called outside of this loop.
func (s *Session) run(ctx context.Context) {
	go s.sws.Run()

	s.idleTick = time.NewTimer(s.initialSearchDelay)
	periodicSearchTimer := time.NewTimer(s.periodicSearchDelay)
	sessionSpan := trace.SpanFromContext(ctx)
	for {
		select {
		case oper := <-s.incoming:
			switch oper.op {
			case opReceive:
				// Received blocks
				sessionSpan.AddEvent("Session.ReceiveOp")
				s.handleReceive(oper.keys)
			case opWant:
				// Client wants blocks
				sessionSpan.AddEvent("Session.WantOp")
				s.wantBlocks(ctx, oper.keys)
			case opCancel:
				// Wants were cancelled
				sessionSpan.AddEvent("Session.WantCancelOp")
				s.sw.CancelPending(oper.keys)
				s.sws.Cancel(oper.keys)
			case opWantsSent:
				// Wants were sent to a peer
				sessionSpan.AddEvent("Session.WantsSentOp")
				s.sw.WantsSent(oper.keys)
			case opBroadcast:
				// Broadcast want-haves to all peers
				opCtx, span := internal.StartSpan(ctx, "Session.BroadcastOp")
				s.broadcast(opCtx, oper.keys)
				span.End()
			default:
				panic("unhandled operation")
			}
		case <-s.idleTick.C:
			// The session hasn't received blocks for a while, broadcast
			opCtx, span := internal.StartSpan(ctx, "Session.IdleBroadcast") // ProbeLab: don't delete/change span without notice
			s.broadcast(opCtx, nil)
			span.End()
		case <-periodicSearchTimer.C:
			// Periodically search for a random live want
			opCtx, span := internal.StartSpan(ctx, "Session.PeriodicSearch")
			s.handlePeriodicSearch(opCtx)
			periodicSearchTimer.Reset(s.periodicSearchDelay)
			span.End()
		case baseTickDelay := <-s.tickDelayReqs:
			// Set the base tick delay
			s.baseTickDelay = baseTickDelay
		case <-ctx.Done():
			periodicSearchTimer.Stop()
			// Shutdown
			s.handleShutdown()
			return
		}
	}
}

// broadcast is called when the session has not received any blocks for some
// time, or when all peers in the session have sent DONT_HAVE for a particular
// set of CIDs. Send want-haves to all connected peers, and search for new
// peers with the CID.
func (s *Session) broadcast(ctx context.Context, wants []cid.Cid) {
	// If this broadcast is because of an idle timeout (we haven't received any
	// blocks for a while) then broadcast all pending wants.
	if wants == nil {
		wants = s.sw.PrepareBroadcast()
	}

	// Broadcast a want-have for the live wants to connected peers.
	s.broadcastWantHaves(ctx, wants)

	// Do not find providers on consecutive ticks. Instead rely on periodic
	// search widening.
	if len(wants) > 0 && (s.consecutiveTicks == 0) {
		// Search for providers who have the first want in the list. Typically
		// if the provider has the first block they will have the rest of the
		// blocks also.
		log.Debugw("FindMorePeers", "session", s.id, "cid", wants[0], "pending", len(wants))
		s.findMorePeers(ctx, wants[0])
	}

	// If there are live wants record a consecutive tick.
	if s.sw.HasLiveWants() {
		s.resetIdleTick() // call before incrementing s.consecutiveTicks
		s.consecutiveTicks++
	}
}

// handlePeriodicSearch is called periodically to search for providers of a
// randomly chosen CID in the sesssion.
func (s *Session) handlePeriodicSearch(ctx context.Context) {
	randomWant := s.sw.RandomLiveWant()
	if !randomWant.Defined() {
		return
	}

	// TODO: come up with a better strategy for determining when to search
	// for new providers for blocks.
	s.findMorePeers(ctx, randomWant)

	s.broadcastWantHaves(ctx, []cid.Cid{randomWant})
}

// findMorePeers attempts to find more peers for a session by searching for
// providers for the given CID.
func (s *Session) findMorePeers(ctx context.Context, c cid.Cid) {
	// noop when provider finder is disabled
	if s.providerFinder == nil {
		return
	}
	go func(k cid.Cid) {
		ctx, span := internal.StartSpan(ctx, "Session.FindMorePeers")
		defer span.End()
		for p := range s.providerFinder.FindProvidersAsync(ctx, k, 0) {
			// When a provider indicates that it has a cid, it is equivalent to
			// the providing peer sending a HAVE.
			log.Infow("Found peer for CID", "peer", p, "cid", k)
			span.AddEvent("FoundPeer")
			s.sws.Update(p.ID, nil, []cid.Cid{c}, nil)
		}
	}(c)
}

// handleShutdown is called when the session shuts down.
func (s *Session) handleShutdown() {
	// Stop the idle timer
	s.idleTick.Stop()
	// Shut down the session peer manager
	s.sprm.Shutdown()
	// Shut down the sessionWantSender (blocks until sessionWantSender stops
	// sending)
	s.sws.Shutdown()
	// Signal to the SessionManager that the session has been shutdown
	// and can be cleaned up
	s.sm.RemoveSession(s.id)
}

// handleReceive is called when the session receives blocks from a peer.
func (s *Session) handleReceive(ks []cid.Cid) {
	// Record which blocks have been received and figure out the total latency
	// for fetching the blocks.
	wanted, totalLatency := s.sw.BlocksReceived(ks)
	if len(wanted) == 0 {
		return
	}

	// Record latency
	s.latencyTrkr.receiveUpdate(len(wanted), totalLatency)

	// Inform the SessionManager that this session is no longer expecting to
	// receive the wanted keys, since we now have them,
	s.sm.CancelSessionWants(s.id, wanted)

	s.idleTick.Stop()

	// We have received new wanted blocks, so reset the number of ticks that
	// have occurred since the last new block,
	s.consecutiveTicks = 0

	// Reset rebroadcast timer if there are still outstanding wants.
	if s.sw.HasLiveWants() {
		s.resetIdleTick()
	}
}

// wantBlocks is called when blocks are requested by the client
func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	if len(newks) > 0 {
		// Inform the SessionInterestManager that this session is interested in the keys
		s.sim.RecordSessionInterest(s.id, newks)
		// Tell the sessionWants tracker that that the wants have been requested
		s.sw.BlocksRequested(newks)
		// Tell the sessionWantSender that the blocks have been requested
		s.sws.Add(newks)
	}

	// If we have discovered peers already, the sessionWantSender will
	// send wants to them
	if s.sprm.PeersDiscovered() {
		return
	}

	// No peers discovered yet, broadcast some want-haves
	ks := s.sw.GetNextWants()
	if len(ks) > 0 {
		log.Infow("No peers - broadcasting", "session", s.id, "want-count", len(ks))
		s.broadcastWantHaves(ctx, ks)
	}
}

// broadcastWantHaves sends want-haves to all connected peers.
func (s *Session) broadcastWantHaves(ctx context.Context, wants []cid.Cid) {
	log.Debugw("broadcastWantHaves", "session", s.id, "cids", wants)
	s.pm.BroadcastWantHaves(wants)
}

// resetIdleTick sets the idle tick time based on average latency.
//
// The session will broadcast if it has outstanding wants and doesn't receive
// any blocks for some time.
// The length of time is calculated
//   - initially
//     as a fixed delay
//   - once some blocks are received
//     from a base delay and average latency, with a backoff
func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if !s.latencyTrkr.hasLatency() {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.latencyTrkr.averageLatency()
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay *= time.Duration(1 + s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

// latencyTracker keeps track of the average latency between sending a want
// and receiving the corresponding block
type latencyTracker struct {
	totalLatency time.Duration
	count        int
}

func (lt *latencyTracker) hasLatency() bool {
	return lt.totalLatency > 0 && lt.count > 0
}

func (lt *latencyTracker) averageLatency() time.Duration {
	return lt.totalLatency / time.Duration(lt.count)
}

func (lt *latencyTracker) receiveUpdate(count int, totalLatency time.Duration) {
	lt.totalLatency += totalLatency
	lt.count += count
}
