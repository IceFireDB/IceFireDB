package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-bitswap/internal/defaults"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bmetrics "github.com/ipfs/go-bitswap/metrics"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server/internal/decision"
	"github.com/ipfs/go-bitswap/tracer"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"
)

var provideKeysBufferSize = 2048

var log = logging.Logger("bitswap-server")
var sflog = log.Desugar()

const provideWorkerMax = 6

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

	process process.Process

	// newBlocks is a channel for newly added blocks to be provided to the
	// network.  blocks pushed down this channel get buffered and fed to the
	// provideKeys channel later on to avoid too much network activity
	newBlocks chan cid.Cid
	// provideKeys directly feeds provide workers
	provideKeys chan cid.Cid

	// Extra options to pass to the decision manager
	engineOptions []decision.Option

	// the size of channel buffer to use
	hasBlockBufferSize int
	// whether or not to make provide announcements
	provideEnabled bool
}

func New(ctx context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Server {
	ctx, cancel := context.WithCancel(ctx)

	px := process.WithTeardown(func() error {
		return nil
	})
	go func() {
		<-px.Closing() // process closes first
		cancel()
	}()

	s := &Server{
		sentHistogram:      bmetrics.SentHist(ctx),
		sendTimeHistogram:  bmetrics.SendTimeHist(ctx),
		taskWorkerCount:    defaults.BitswapTaskWorkerCount,
		network:            network,
		process:            px,
		provideEnabled:     true,
		hasBlockBufferSize: defaults.HasBlockBufferSize,
		provideKeys:        make(chan cid.Cid, provideKeysBufferSize),
	}
	s.newBlocks = make(chan cid.Cid, s.hasBlockBufferSize)

	for _, o := range options {
		o(s)
	}

	s.engine = decision.NewEngine(
		ctx,
		bstore,
		network.ConnectionManager(),
		network.Self(),
		s.engineOptions...,
	)
	s.engineOptions = nil

	s.startWorkers(ctx, px)

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

// ProvideEnabled is an option for enabling/disabling provide announcements
func ProvideEnabled(enabled bool) Option {
	return func(bs *Server) {
		bs.provideEnabled = enabled
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

// HasBlockBufferSize configure how big the new blocks buffer should be.
func HasBlockBufferSize(count int) Option {
	if count < 0 {
		panic("cannot have negative buffer size")
	}
	return func(bs *Server) {
		bs.hasBlockBufferSize = count
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

func (bs *Server) startWorkers(ctx context.Context, px process.Process) {
	bs.engine.StartWorkers(ctx, px)

	// Start up workers to handle requests from other nodes for the data on this node
	for i := 0; i < bs.taskWorkerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			bs.taskWorker(ctx, i)
		})
	}

	if bs.provideEnabled {
		// Start up a worker to manage sending out provides messages
		px.Go(func(px process.Process) {
			bs.provideCollector(ctx)
		})

		// Spawn up multiple workers to handle incoming blocks
		// consider increasing number if providing blocks bottlenecks
		// file transfers
		px.Go(bs.provideWorker)
	}
}

func (bs *Server) taskWorker(ctx context.Context, id int) {
	defer log.Debug("bitswap task worker shutting down...")
	log := log.With("ID", id)
	for {
		log.Debug("Bitswap.TaskWorker.Loop")
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

				dur := time.Since(start)
				bs.sendTimeHistogram.Observe(dur.Seconds())

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
	Peers         []string
	ProvideBufLen int
	BlocksSent    uint64
	DataSent      uint64
}

// Stat returns aggregated statistics about bitswap operations
func (bs *Server) Stat() (Stat, error) {
	bs.counterLk.Lock()
	s := bs.counters
	bs.counterLk.Unlock()
	s.ProvideBufLen = len(bs.newBlocks)

	peers := bs.engine.Peers()
	peersStr := make([]string, len(peers))
	for i, p := range peers {
		peersStr[i] = p.Pretty()
	}
	sort.Strings(peersStr)
	s.Peers = peersStr

	return s, nil
}

// NotifyNewBlocks announces the existence of blocks to this bitswap service. The
// service will potentially notify its peers.
// Bitswap itself doesn't store new blocks. It's the caller responsibility to ensure
// that those blocks are available in the blockstore before calling this function.
func (bs *Server) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	// Send wanted blocks to decision engine
	bs.engine.NotifyNewBlocks(blks)

	// If the reprovider is enabled, send block to reprovider
	if bs.provideEnabled {
		for _, blk := range blks {
			select {
			case bs.newBlocks <- blk.Cid():
				// send block off to be reprovided
			case <-bs.process.Closing():
				return bs.process.Close()
			}
		}
	}

	return nil
}

func (bs *Server) provideCollector(ctx context.Context) {
	defer close(bs.provideKeys)
	var toProvide []cid.Cid
	var nextKey cid.Cid
	var keysOut chan cid.Cid

	for {
		select {
		case blkey, ok := <-bs.newBlocks:
			if !ok {
				log.Debug("newBlocks channel closed")
				return
			}

			if keysOut == nil {
				nextKey = blkey
				keysOut = bs.provideKeys
			} else {
				toProvide = append(toProvide, blkey)
			}
		case keysOut <- nextKey:
			if len(toProvide) > 0 {
				nextKey = toProvide[0]
				toProvide = toProvide[1:]
			} else {
				keysOut = nil
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Server) provideWorker(px process.Process) {
	// FIXME: OnClosingContext returns a _custom_ context type.
	// Unfortunately, deriving a new cancelable context from this custom
	// type fires off a goroutine. To work around this, we create a single
	// cancelable context up-front and derive all sub-contexts from that.
	//
	// See: https://github.com/ipfs/go-ipfs/issues/5810
	ctx := procctx.OnClosingContext(px)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	limit := make(chan struct{}, provideWorkerMax)

	limitedGoProvide := func(k cid.Cid, wid int) {
		defer func() {
			// replace token when done
			<-limit
		}()

		log.Debugw("Bitswap.ProvideWorker.Start", "ID", wid, "cid", k)
		defer log.Debugw("Bitswap.ProvideWorker.End", "ID", wid, "cid", k)

		ctx, cancel := context.WithTimeout(ctx, defaults.ProvideTimeout) // timeout ctx
		defer cancel()

		if err := bs.network.Provide(ctx, k); err != nil {
			log.Warn(err)
		}
	}

	// worker spawner, reads from bs.provideKeys until it closes, spawning a
	// _ratelimited_ number of workers to handle each key.
	for wid := 2; ; wid++ {
		log.Debug("Bitswap.ProvideWorker.Loop")

		select {
		case <-px.Closing():
			return
		case k, ok := <-bs.provideKeys:
			if !ok {
				log.Debug("provideKeys channel closed")
				return
			}
			select {
			case <-px.Closing():
				return
			case limit <- struct{}{}:
				go limitedGoProvide(k, wid)
			}
		}
	}
}

func (bs *Server) ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage) {
	// This call records changes to wantlists, blocks received,
	// and number of bytes transfered.
	bs.engine.MessageReceived(ctx, p, incoming)
	// TODO: this is bad, and could be easily abused.
	// Should only track *useful* messages in ledger

	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}
}

// ReceivedBlocks notify the decision engine that a peer is well behaving
// and gave us usefull data, potentially increasing it's score and making us
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

// Close is called to shutdown the Client
func (bs *Server) Close() error {
	return bs.process.Close()
}
