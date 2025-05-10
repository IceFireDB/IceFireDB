package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/internal/defaults"
	"github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bmetrics "github.com/ipfs/boxo/bitswap/metrics"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/server/internal/decision"
	"github.com/ipfs/boxo/bitswap/tracer"
	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

var (
	log   = logging.Logger("bitswap/server")
	sflog = log.Desugar()
)

type Option func(*Server)

type Server struct {
	sentHistogram     metrics.Histogram
	sendTimeHistogram metrics.Histogram

	// the engine is the bit of logic that decides who to send which blocks to
	engine *decision.Engine

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// External statistics interface
	tracer tracer.Tracer

	// Counters for various statistics
	counterLk sync.Mutex
	counters  Stat

	// the total number of simultaneous threads sending outgoing messages
	taskWorkerCount int

	// Cancel stops the server
	cancel    context.CancelFunc
	closing   chan struct{}
	closeOnce sync.Once
	// waitWorkers waits for all worker goroutines to exit.
	waitWorkers sync.WaitGroup

	// Extra options to pass to the decision manager
	engineOptions []decision.Option
}

func New(ctx context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Server {
	ctx, cancel := context.WithCancel(ctx)

	s := &Server{
		sentHistogram:     bmetrics.SentHist(ctx),
		sendTimeHistogram: bmetrics.SendTimeHist(ctx),
		taskWorkerCount:   defaults.BitswapTaskWorkerCount,
		network:           network,
		cancel:            cancel,
		closing:           make(chan struct{}),
	}

	for _, o := range options {
		o(s)
	}

	s.engine = decision.NewEngine(
		ctx,
		bstore,
		network,
		network.Self(),
		s.engineOptions...,
	)
	s.engineOptions = nil

	s.startWorkers(ctx)

	return s
}

func TaskWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("task worker count is %d but must be > 0", count))
	}
	return func(bs *Server) {
		bs.taskWorkerCount = count
	}
}

func WithTracer(tap tracer.Tracer) Option {
	return func(bs *Server) {
		bs.tracer = tap
	}
}

func WithPeerBlockRequestFilter(pbrf decision.PeerBlockRequestFilter) Option {
	o := decision.WithPeerBlockRequestFilter(pbrf)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// WithTaskComparator configures custom task prioritization logic.
func WithTaskComparator(comparator decision.TaskComparator) Option {
	o := decision.WithTaskComparator(comparator)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// Configures the engine to use the given score decision logic.
func WithScoreLedger(scoreLedger decision.ScoreLedger) Option {
	o := decision.WithScoreLedger(scoreLedger)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// WithPeerLedger configures the engine with a custom [decision.PeerLedger].
func WithPeerLedger(peerLedger decision.PeerLedger) Option {
	o := decision.WithPeerLedger(peerLedger)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// LedgerForPeer returns aggregated data about blocks swapped and communication
// with a given peer.
func (bs *Server) LedgerForPeer(p peer.ID) *decision.Receipt {
	return bs.engine.LedgerForPeer(p)
}

// EngineTaskWorkerCount sets the number of worker threads used inside the engine
func EngineTaskWorkerCount(count int) Option {
	o := decision.WithTaskWorkerCount(count)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// This option is only used for testing.
func SetSendDontHaves(send bool) Option {
	o := decision.WithSetSendDontHave(send)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// EngineBlockstoreWorkerCount sets the number of worker threads used for
// blockstore operations in the decision engine
func EngineBlockstoreWorkerCount(count int) Option {
	o := decision.WithBlockstoreWorkerCount(count)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

func WithTargetMessageSize(tms int) Option {
	o := decision.WithTargetMessageSize(tms)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// MaxOutstandingBytesPerPeer describes approximately how much work we are will to have outstanding to a peer at any
// given time. Setting it to 0 will disable any limiting.
func MaxOutstandingBytesPerPeer(count int) Option {
	o := decision.WithMaxOutstandingBytesPerPeer(count)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// MaxQueuedWantlistEntriesPerPeer limits how much individual entries each peer is allowed to send.
// If a peer send us more than this we will truncate newest entries.
// It defaults to defaults.MaxQueuedWantlistEntiresPerPeer.
func MaxQueuedWantlistEntriesPerPeer(count uint) Option {
	o := decision.WithMaxQueuedWantlistEntriesPerPeer(count)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// MaxCidSize limits how big CIDs we are willing to serve.
// We will ignore CIDs over this limit.
// It defaults to [defaults.MaxCidSize].
// If it is 0 no limit is applied.
func MaxCidSize(n uint) Option {
	o := decision.WithMaxCidSize(n)
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, o)
	}
}

// WithWantHaveReplaceSize sets the maximum size of a block in bytes up to
// which the bitswap server will replace a WantHave with a WantBlock response.
//
// Behavior:
//   - If size > 0: The server may send full blocks instead of just confirming possession
//     for blocks up to the specified size.
//   - If size = 0: WantHave replacement is disabled entirely. This allows the server to
//     skip reading block sizes during WantHave request processing, which can be more
//     efficient if the data storage bills "possession" checks and "reads" differently.
//
// Performance considerations:
//   - Enabling replacement (size > 0) may reduce network round-trips but requires
//     checking block sizes for each WantHave request to decide if replacement should occur.
//   - Disabling replacement (size = 0) optimizes server performance by avoiding
//     block size checks, potentially reducing infrastructure costs if possession checks
//     are less expensive than full reads.
//
// It defaults to [defaults.DefaultWantHaveReplaceSize]
// and the value may change in future releases.
//
// Use this option to set explicit behavior to balance between network
// efficiency, server performance, and potential storage cost optimizations
// based on your specific use case and storage backend.
func WithWantHaveReplaceSize(size int) Option {
	if size < 0 {
		size = 0
	}
	return func(bs *Server) {
		bs.engineOptions = append(bs.engineOptions, decision.WithWantHaveReplaceSize(size))
	}
}

// WantlistForPeer returns the currently understood list of blocks requested by a
// given peer.
func (bs *Server) WantlistForPeer(p peer.ID) []cid.Cid {
	var out []cid.Cid
	for _, e := range bs.engine.WantlistForPeer(p) {
		out = append(out, e.Cid)
	}
	return out
}

func (bs *Server) startWorkers(ctx context.Context) {
	// Start up workers to handle requests from other nodes for the data on this node
	bs.waitWorkers.Add(bs.taskWorkerCount)
	for i := 0; i < bs.taskWorkerCount; i++ {
		i := i
		go bs.taskWorker(ctx, i)
	}
}

func (bs *Server) taskWorker(ctx context.Context, id int) {
	defer bs.waitWorkers.Done()

	for {
		select {
		case nextEnvelope := <-bs.engine.Outbox():
			select {
			case envelope, ok := <-nextEnvelope:
				if !ok {
					continue
				}

				start := time.Now()

				// TODO: Only record message as sent if there was no error?
				// Ideally, yes. But we'd need some way to trigger a retry and/or drop
				// the peer.
				bs.engine.MessageSent(envelope.Peer, envelope.Message)
				if bs.tracer != nil {
					bs.tracer.MessageSent(envelope.Peer, envelope.Message)
				}
				bs.sendBlocks(ctx, envelope)

				bs.sendTimeHistogram.Observe(time.Since(start).Seconds())

			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Server) logOutgoingBlocks(env *decision.Envelope) {
	if ce := sflog.Check(zap.DebugLevel, "sent message"); ce == nil {
		return
	}

	self := bs.network.Self()

	for _, blockPresence := range env.Message.BlockPresences() {
		c := blockPresence.Cid
		switch blockPresence.Type {
		case pb.Message_Have:
			log.Debugw("sent message",
				"type", "HAVE",
				"cid", c,
				"local", self,
				"to", env.Peer,
			)
		case pb.Message_DontHave:
			log.Debugw("sent message",
				"type", "DONT_HAVE",
				"cid", c,
				"local", self,
				"to", env.Peer,
			)
		default:
			panic(fmt.Sprintf("unrecognized BlockPresence type %v", blockPresence.Type))
		}

	}
	for _, block := range env.Message.Blocks() {
		log.Debugw("sent message",
			"type", "BLOCK",
			"cid", block.Cid(),
			"local", self,
			"to", env.Peer,
		)
	}
}

func (bs *Server) sendBlocks(ctx context.Context, env *decision.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	err := bs.network.SendMessage(ctx, env.Peer, env.Message)
	if err != nil {
		log.Debugw("failed to send blocks message",
			"peer", env.Peer,
			"error", err,
		)
		return
	}

	bs.logOutgoingBlocks(env)

	dataSent := 0
	blocks := env.Message.Blocks()
	for _, b := range blocks {
		dataSent += len(b.RawData())
	}
	bs.counterLk.Lock()
	bs.counters.BlocksSent += uint64(len(blocks))
	bs.counters.DataSent += uint64(dataSent)
	bs.counterLk.Unlock()
	bs.sentHistogram.Observe(float64(env.Message.Size()))
	log.Debugw("sent message", "peer", env.Peer)
}

type Stat struct {
	Peers      []string
	BlocksSent uint64
	DataSent   uint64
}

// Stat returns aggregated statistics about bitswap operations
func (bs *Server) Stat() (Stat, error) {
	bs.counterLk.Lock()
	s := bs.counters
	bs.counterLk.Unlock()

	peers := bs.engine.Peers()
	peersStr := make([]string, len(peers))
	for i, p := range peers {
		peersStr[i] = p.String()
	}
	slices.Sort(peersStr)
	s.Peers = peersStr

	return s, nil
}

// NotifyNewBlocks announces the existence of blocks to this bitswap service. The
// service will potentially notify its peers.
// Bitswap itself doesn't store new blocks. It's the caller responsibility to ensure
// that those blocks are available in the blockstore before calling this function.
func (bs *Server) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	select {
	case <-bs.closing:
		return errors.New("bitswap is closed")
	default:
	}

	// Send wanted blocks to decision engine
	bs.engine.NotifyNewBlocks(blks)

	return nil
}

func (bs *Server) ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage) {
	// This call records changes to wantlists, blocks received,
	// and number of bytes transferred.
	mustKillConnection := bs.engine.MessageReceived(ctx, p, incoming)
	if mustKillConnection {
		bs.network.DisconnectFrom(ctx, p)
	}
	// TODO: this is bad, and could be easily abused.
	// Should only track *useful* messages in ledger

	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}
}

// ReceivedBlocks notify the decision engine that a peer is well behaving
// and gave us useful data, potentially increasing it's score and making us
// send them more data in exchange.
func (bs *Server) ReceivedBlocks(from peer.ID, blks []blocks.Block) {
	bs.engine.ReceivedBlocks(from, blks)
}

func (*Server) ReceiveError(err error) {
	log.Infof("Bitswap Client ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

func (bs *Server) PeerConnected(p peer.ID) {
	bs.engine.PeerConnected(p)
}

func (bs *Server) PeerDisconnected(p peer.ID) {
	bs.engine.PeerDisconnected(p)
}

// Close is called to shutdown the Server. Returns when all workers and
// decision engine have finished. Safe to calling multiple times/concurrently.
func (bs *Server) Close() {
	bs.closeOnce.Do(func() {
		close(bs.closing)
		bs.cancel()
	})
	bs.engine.Close()
	bs.waitWorkers.Wait()
}
