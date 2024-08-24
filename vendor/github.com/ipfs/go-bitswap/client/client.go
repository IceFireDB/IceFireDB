// Package bitswap implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package client

import (
	"context"
	"errors"

	"sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	bsbpm "github.com/ipfs/go-bitswap/client/internal/blockpresencemanager"
	bsgetter "github.com/ipfs/go-bitswap/client/internal/getter"
	bsmq "github.com/ipfs/go-bitswap/client/internal/messagequeue"
	"github.com/ipfs/go-bitswap/client/internal/notifications"
	bspm "github.com/ipfs/go-bitswap/client/internal/peermanager"
	bspqm "github.com/ipfs/go-bitswap/client/internal/providerquerymanager"
	bssession "github.com/ipfs/go-bitswap/client/internal/session"
	bssim "github.com/ipfs/go-bitswap/client/internal/sessioninterestmanager"
	bssm "github.com/ipfs/go-bitswap/client/internal/sessionmanager"
	bsspm "github.com/ipfs/go-bitswap/client/internal/sessionpeermanager"
	"github.com/ipfs/go-bitswap/internal"
	"github.com/ipfs/go-bitswap/internal/defaults"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bmetrics "github.com/ipfs/go-bitswap/metrics"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/tracer"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap-client")

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*Client)

// ProviderSearchDelay overwrites the global provider search delay
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return func(bs *Client) {
		bs.provSearchDelay = newProvSearchDelay
	}
}

// RebroadcastDelay overwrites the global provider rebroadcast delay
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return func(bs *Client) {
		bs.rebroadcastDelay = newRebroadcastDelay
	}
}

func SetSimulateDontHavesOnTimeout(send bool) Option {
	return func(bs *Client) {
		bs.simulateDontHavesOnTimeout = send
	}
}

// Configures the Client to use given tracer.
// This provides methods to access all messages sent and received by the Client.
// This interface can be used to implement various statistics (this is original intent).
func WithTracer(tap tracer.Tracer) Option {
	return func(bs *Client) {
		bs.tracer = tap
	}
}

func WithBlockReceivedNotifier(brn BlockReceivedNotifier) Option {
	return func(bs *Client) {
		bs.blockReceivedNotifier = brn
	}
}

type BlockReceivedNotifier interface {
	// ReceivedBlocks notifies the decision engine that a peer is well-behaving
	// and gave us useful data, potentially increasing its score and making us
	// send them more data in exchange.
	ReceivedBlocks(peer.ID, []blocks.Block)
}

// New initializes a Bitswap client that runs until client.Close is called.
func New(parent context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Client {
	// important to use provided parent context (since it may include important
	// loggable data). It's probably not a good idea to allow bitswap to be
	// coupled to the concerns of the ipfs daemon in this way.
	//
	// FIXME(btc) Now that bitswap manages itself using a process, it probably
	// shouldn't accept a context anymore. Clients should probably use Close()
	// exclusively. We should probably find another way to share logging data
	ctx, cancelFunc := context.WithCancel(parent)

	px := process.WithTeardown(func() error {
		return nil
	})

	// onDontHaveTimeout is called when a want-block is sent to a peer that
	// has an old version of Bitswap that doesn't support DONT_HAVE messages,
	// or when no response is received within a timeout.
	var sm *bssm.SessionManager
	var bs *Client
	onDontHaveTimeout := func(p peer.ID, dontHaves []cid.Cid) {
		// Simulate a message arriving with DONT_HAVEs
		if bs.simulateDontHavesOnTimeout {
			sm.ReceiveFrom(ctx, p, nil, nil, dontHaves)
		}
	}
	peerQueueFactory := func(ctx context.Context, p peer.ID) bspm.PeerQueue {
		return bsmq.New(ctx, p, network, onDontHaveTimeout)
	}

	sim := bssim.New()
	bpm := bsbpm.New()
	pm := bspm.New(ctx, peerQueueFactory, network.Self())
	pqm := bspqm.New(ctx, network)

	sessionFactory := func(
		sessctx context.Context,
		sessmgr bssession.SessionManager,
		id uint64,
		spm bssession.SessionPeerManager,
		sim *bssim.SessionInterestManager,
		pm bssession.PeerManager,
		bpm *bsbpm.BlockPresenceManager,
		notif notifications.PubSub,
		provSearchDelay time.Duration,
		rebroadcastDelay delay.D,
		self peer.ID) bssm.Session {
		return bssession.New(sessctx, sessmgr, id, spm, pqm, sim, pm, bpm, notif, provSearchDelay, rebroadcastDelay, self)
	}
	sessionPeerManagerFactory := func(ctx context.Context, id uint64) bssession.SessionPeerManager {
		return bsspm.New(id, network.ConnectionManager())
	}
	notif := notifications.New()
	sm = bssm.New(ctx, sessionFactory, sim, sessionPeerManagerFactory, bpm, pm, notif, network.Self())

	bs = &Client{
		blockstore:                 bstore,
		network:                    network,
		process:                    px,
		pm:                         pm,
		pqm:                        pqm,
		sm:                         sm,
		sim:                        sim,
		notif:                      notif,
		counters:                   new(counters),
		dupMetric:                  bmetrics.DupHist(ctx),
		allMetric:                  bmetrics.AllHist(ctx),
		provSearchDelay:            defaults.ProvSearchDelay,
		rebroadcastDelay:           delay.Fixed(time.Minute),
		simulateDontHavesOnTimeout: true,
	}

	// apply functional options before starting and running bitswap
	for _, option := range options {
		option(bs)
	}

	bs.pqm.Startup()

	// bind the context and process.
	// do it over here to avoid closing before all setup is done.
	go func() {
		<-px.Closing() // process closes first
		sm.Shutdown()
		cancelFunc()
		notif.Shutdown()
	}()
	procctx.CloseAfterContext(px, ctx) // parent cancelled first

	return bs
}

// Client instances implement the bitswap protocol.
type Client struct {
	pm *bspm.PeerManager

	// the provider query manager manages requests to find providers
	pqm *bspqm.ProviderQueryManager

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

	// manages channels of outgoing blocks for sessions
	notif notifications.PubSub

	process process.Process

	// Counters for various statistics
	counterLk sync.Mutex
	counters  *counters

	// Metrics interface metrics
	dupMetric metrics.Histogram
	allMetric metrics.Histogram

	// External statistics interface
	tracer tracer.Tracer

	// the SessionManager routes requests to interested sessions
	sm *bssm.SessionManager

	// the SessionInterestManager keeps track of which sessions are interested
	// in which CIDs
	sim *bssim.SessionInterestManager

	// how long to wait before looking for providers in a session
	provSearchDelay time.Duration

	// how often to rebroadcast providing requests to find more optimized providers
	rebroadcastDelay delay.D

	blockReceivedNotifier BlockReceivedNotifier

	// whether we should actually simulate dont haves on request timeout
	simulateDontHavesOnTimeout bool
}

type counters struct {
	blocksRecvd    uint64
	dupBlocksRecvd uint64
	dupDataRecvd   uint64
	dataRecvd      uint64
	messagesRecvd  uint64
}

// GetBlock attempts to retrieve a particular block from peers within the
// deadline enforced by the context.
func (bs *Client) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "GetBlock", trace.WithAttributes(attribute.String("Key", k.String())))
	defer span.End()
	return bsgetter.SyncGetBlock(ctx, k, bs.GetBlocks)
}

// GetBlocks returns a channel where the caller may receive blocks that
// correspond to the provided |keys|. Returns an error if BitSwap is unable to
// begin this request within the deadline enforced by the context.
//
// NB: Your request remains open until the context expires. To conserve
// resources, provide a context with a reasonably short deadline (ie. not one
// that lasts throughout the lifetime of the server)
func (bs *Client) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "GetBlocks", trace.WithAttributes(attribute.Int("NumKeys", len(keys))))
	defer span.End()
	session := bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
	return session.GetBlocks(ctx, keys)
}

// NotifyNewBlocks announces the existence of blocks to this bitswap service.
// Bitswap itself doesn't store new blocks. It's the caller responsibility to ensure
// that those blocks are available in the blockstore before calling this function.
func (bs *Client) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "NotifyNewBlocks")
	defer span.End()

	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	blkCids := make([]cid.Cid, len(blks))
	for i, blk := range blks {
		blkCids[i] = blk.Cid()
	}

	// Send all block keys (including duplicates) to any sessions that want them.
	// (The duplicates are needed by sessions for accounting purposes)
	bs.sm.ReceiveFrom(ctx, "", blkCids, nil, nil)

	// Publish the block to any Bitswap clients that had requested blocks.
	// (the sessions use this pubsub mechanism to inform clients of incoming
	// blocks)
	bs.notif.Publish(blks...)

	return nil
}

// receiveBlocksFrom process blocks received from the network
func (bs *Client) receiveBlocksFrom(ctx context.Context, from peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) error {
	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	wanted, notWanted := bs.sim.SplitWantedUnwanted(blks)
	for _, b := range notWanted {
		log.Debugf("[recv] block not in wantlist; cid=%s, peer=%s", b.Cid(), from)
	}

	allKs := make([]cid.Cid, 0, len(blks))
	for _, b := range blks {
		allKs = append(allKs, b.Cid())
	}

	// Inform the PeerManager so that we can calculate per-peer latency
	combined := make([]cid.Cid, 0, len(allKs)+len(haves)+len(dontHaves))
	combined = append(combined, allKs...)
	combined = append(combined, haves...)
	combined = append(combined, dontHaves...)
	bs.pm.ResponseReceived(from, combined)

	// Send all block keys (including duplicates) to any sessions that want them for accounting purpose.
	bs.sm.ReceiveFrom(ctx, from, allKs, haves, dontHaves)

	if bs.blockReceivedNotifier != nil {
		bs.blockReceivedNotifier.ReceivedBlocks(from, wanted)
	}

	// Publish the block to any Bitswap clients that had requested blocks.
	// (the sessions use this pubsub mechanism to inform clients of incoming
	// blocks)
	for _, b := range wanted {
		bs.notif.Publish(b)
	}

	for _, b := range wanted {
		log.Debugw("Bitswap.GetBlockRequest.End", "cid", b.Cid())
	}

	return nil
}

// ReceiveMessage is called by the network interface when a new message is
// received.
func (bs *Client) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
	bs.counterLk.Lock()
	bs.counters.messagesRecvd++
	bs.counterLk.Unlock()

	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}

	iblocks := incoming.Blocks()

	if len(iblocks) > 0 {
		bs.updateReceiveCounters(iblocks)
		for _, b := range iblocks {
			log.Debugf("[recv] block; cid=%s, peer=%s", b.Cid(), p)
		}
	}

	haves := incoming.Haves()
	dontHaves := incoming.DontHaves()
	if len(iblocks) > 0 || len(haves) > 0 || len(dontHaves) > 0 {
		// Process blocks
		err := bs.receiveBlocksFrom(ctx, p, iblocks, haves, dontHaves)
		if err != nil {
			log.Warnf("ReceiveMessage recvBlockFrom error: %s", err)
			return
		}
	}
}

func (bs *Client) updateReceiveCounters(blocks []blocks.Block) {
	// Check which blocks are in the datastore
	// (Note: any errors from the blockstore are simply logged out in
	// blockstoreHas())
	blocksHas := bs.blockstoreHas(blocks)

	bs.counterLk.Lock()
	defer bs.counterLk.Unlock()

	// Do some accounting for each block
	for i, b := range blocks {
		has := blocksHas[i]

		blkLen := len(b.RawData())
		bs.allMetric.Observe(float64(blkLen))
		if has {
			bs.dupMetric.Observe(float64(blkLen))
		}

		c := bs.counters

		c.blocksRecvd++
		c.dataRecvd += uint64(blkLen)
		if has {
			c.dupBlocksRecvd++
			c.dupDataRecvd += uint64(blkLen)
		}
	}
}

func (bs *Client) blockstoreHas(blks []blocks.Block) []bool {
	res := make([]bool, len(blks))

	wg := sync.WaitGroup{}
	for i, block := range blks {
		wg.Add(1)
		go func(i int, b blocks.Block) {
			defer wg.Done()

			has, err := bs.blockstore.Has(context.TODO(), b.Cid())
			if err != nil {
				log.Infof("blockstore.Has error: %s", err)
				has = false
			}

			res[i] = has
		}(i, block)
	}
	wg.Wait()

	return res
}

// PeerConnected is called by the network interface
// when a peer initiates a new connection to bitswap.
func (bs *Client) PeerConnected(p peer.ID) {
	bs.pm.Connected(p)
}

// PeerDisconnected is called by the network interface when a peer
// closes a connection
func (bs *Client) PeerDisconnected(p peer.ID) {
	bs.pm.Disconnected(p)
}

// ReceiveError is called by the network interface when an error happens
// at the network layer. Currently just logs error.
func (bs *Client) ReceiveError(err error) {
	log.Infof("Bitswap Client ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

// Close is called to shutdown the Client
func (bs *Client) Close() error {
	return bs.process.Close()
}

// GetWantlist returns the current local wantlist (both want-blocks and
// want-haves).
func (bs *Client) GetWantlist() []cid.Cid {
	return bs.pm.CurrentWants()
}

// GetWantBlocks returns the current list of want-blocks.
func (bs *Client) GetWantBlocks() []cid.Cid {
	return bs.pm.CurrentWantBlocks()
}

// GetWanthaves returns the current list of want-haves.
func (bs *Client) GetWantHaves() []cid.Cid {
	return bs.pm.CurrentWantHaves()
}

// IsOnline is needed to match go-ipfs-exchange-interface
func (bs *Client) IsOnline() bool {
	return true
}

// NewSession generates a new Bitswap session. You should use this, rather
// that calling Client.GetBlocks, any time you intend to do several related
// block requests in a row. The session returned will have it's own GetBlocks
// method, but the session will use the fact that the requests are related to
// be more efficient in its requests to peers. If you are using a session
// from go-blockservice, it will create a bitswap session automatically.
func (bs *Client) NewSession(ctx context.Context) exchange.Fetcher {
	ctx, span := internal.StartSpan(ctx, "NewSession")
	defer span.End()
	return bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
}
