// Package client implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package client

import (
	"context"
	"sync"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	bsgetter "github.com/ipfs/boxo/bitswap/client/internal/getter"
	bsmq "github.com/ipfs/boxo/bitswap/client/internal/messagequeue"
	"github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bspm "github.com/ipfs/boxo/bitswap/client/internal/peermanager"
	bssession "github.com/ipfs/boxo/bitswap/client/internal/session"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	bssm "github.com/ipfs/boxo/bitswap/client/internal/sessionmanager"
	bsspm "github.com/ipfs/boxo/bitswap/client/internal/sessionpeermanager"
	"github.com/ipfs/boxo/bitswap/internal"
	"github.com/ipfs/boxo/bitswap/internal/defaults"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	bmetrics "github.com/ipfs/boxo/bitswap/metrics"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/tracer"
	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	rpqm "github.com/ipfs/boxo/routing/providerquerymanager"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("bitswap/client")

type DontHaveTimeoutConfig = bsmq.DontHaveTimeoutConfig

func DefaultDontHaveTimeoutConfig() *DontHaveTimeoutConfig {
	return bsmq.DefaultDontHaveTimeoutConfig()
}

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*Client)

// ProviderSearchDelay sets the initial dely before triggering a provider
// search to find more peers and broadcast the want list. It also partially
// controls re-broadcasts delay when the session idles (does not receive any
// blocks), but these have back-off logic to increase the interval. See
// [defaults.ProvSearchDelay] for the default.
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return func(bs *Client) {
		bs.provSearchDelay = newProvSearchDelay
	}
}

// RebroadcastDelay sets a custom delay for periodic search of a random want.
// When the value ellapses, a random CID from the wantlist is chosen and the
// client attempts to find more peers for it and sends them the single want.
// [defaults.RebroadcastDelay] for the default.
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

func WithDontHaveTimeoutConfig(cfg *DontHaveTimeoutConfig) Option {
	return func(bs *Client) {
		bs.dontHaveTimeoutConfig = cfg
	}
}

// WithPerPeerSendDelay determines how long to wait, based on the number of
// peers, for wants to accumulate before sending a bitswap message to peers. A
// value of 0 uses bitswap messagequeue default.
func WithPerPeerSendDelay(delay time.Duration) Option {
	return func(bs *Client) {
		bs.perPeerSendDelay = delay
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

// WithTraceBlock, if enabled is true, configures the client's GetBLock and
// GetBlocks functions to returns a
// [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable
// [blocks.Block].
func WithTraceBlock(enable bool) Option {
	return func(bs *Client) {
		bs.traceBlock = enable
	}
}

func WithBlockReceivedNotifier(brn BlockReceivedNotifier) Option {
	return func(bs *Client) {
		bs.blockReceivedNotifier = brn
	}
}

// WithoutDuplicatedBlockStats disable collecting counts of duplicated blocks
// received. This counter requires triggering a blockstore.Has() call for
// every block received by launching goroutines in parallel. In the worst case
// (no caching/blooms etc), this is an expensive call for the datastore to
// answer. In a normal case (caching), this has the power of evicting a
// different block from intermediary caches. In the best case, it doesn't
// affect performance. Use if this stat is not relevant.
func WithoutDuplicatedBlockStats() Option {
	return func(bs *Client) {
		bs.skipDuplicatedBlocksStats = true
	}
}

// WithDefaultProviderQueryManager indicates whether to use the default
// ProviderQueryManager as a wrapper of the content Router. The default bitswap
// ProviderQueryManager provides bounded parallelism and limits for these
// lookups. The bitswap default ProviderQueryManager uses these options, which
// may be more conservative than the ProviderQueryManager defaults:
//
//   - WithMaxProviders(defaults.BitswapClientDefaultMaxProviders)
//
// To use a custom ProviderQueryManager, set to false and wrap directly the
// content router provided with the WithContentRouting() option. Only takes
// effect if WithContentRouting is set.
func WithDefaultProviderQueryManager(defaultProviderQueryManager bool) Option {
	return func(bs *Client) {
		bs.defaultProviderQueryManager = defaultProviderQueryManager
	}
}

// BroadcastControlEnable enables or disables broadcast reduction logic.
// Setting this to false restores the previous broadcast behavior of sending
// broadcasts to all peers, and ignores all other BroadcastControl options.
// Default is false (disabled).
func BroadcastControlEnable(enable bool) Option {
	return func(bs *Client) {
		bs.bcastControl.Enable = enable
	}
}

// BroadcastControlMaxPeers sets a hard limit on the number of peers to send
// broadcasts to. A value of 0 means no broadcasts are sent. A value of -1
// means there is no limit. Default is -1 (unlimited).
func BroadcastControlMaxPeers(limit int) Option {
	return func(bs *Client) {
		bs.bcastControl.MaxPeers = limit
	}
}

// BroadcastControlLocalPeers enables or disables broadcast control for peers
// on the local network. If false, than always broadcast to peers on the local
// network. If true, apply broadcast control to local peers. Default is false
// (always broadcast to local peers).
func BroadcastControlLocalPeers(enable bool) Option {
	return func(bs *Client) {
		bs.bcastControl.LocalPeers = enable
	}
}

// BroadcastControlPeeredPeers enables or disables broadcast control for peers
// configured for peering. If false, than always broadcast to peers configured
// for peering. If true, apply broadcast control to peered peers. Default is
// false (always broadcast to peered peers).
func BroadcastControlPeeredPeers(enable bool) Option {
	return func(bs *Client) {
		bs.bcastControl.PeeredPeers = enable
	}
}

// BroadcastControlMaxRandomPeers sets the number of peers to broadcast to
// anyway, even though broadcast control logic has determined that they are
// not broadcast targets. Setting this to a non-zero value ensures at least
// this number of random peers receives a broadcast. This may be helpful in
// cases where peers that are not receiving broadcasts may have wanted blocks.
// Default is 0 (no random broadcasts).
func BroadcastControlMaxRandomPeers(n int) Option {
	return func(bs *Client) {
		bs.bcastControl.MaxRandomPeers = n
	}
}

// BroadcastControlSendToPendingPeers, enables or disables sending broadcasts
// to any peers to which there is a pending message to send. When enabled, this
// sends broadcasts to many more peers, but does so in a way that does not
// increase the number of separate broadcast messages. There is still the
// increased cost of the recipients having to process and respond to the
// broadcasts. Default is false.
func BroadcastControlSendToPendingPeers(enable bool) Option {
	return func(bs *Client) {
		bs.bcastControl.SendToPendingPeers = enable
	}
}

type BlockReceivedNotifier interface {
	// ReceivedBlocks notifies the decision engine that a peer is well-behaving
	// and gave us useful data, potentially increasing its score and making us
	// send them more data in exchange.
	ReceivedBlocks(peer.ID, []blocks.Block)
}

// New initializes a Bitswap client that runs until client.Close is called.
// The Content providerFinder paramteter can be nil to disable content-routing
// lookups for content (rely only on bitswap for discovery).
func New(parent context.Context, network bsnet.BitSwapNetwork, providerFinder routing.ContentDiscovery, bstore blockstore.Blockstore, options ...Option) *Client {
	// important to use provided parent context (since it may include important
	// loggable data). It's probably not a good idea to allow bitswap to be
	// coupled to the concerns of the ipfs daemon in this way.
	//
	// FIXME(btc) Now that bitswap manages itself using a process, it probably
	// shouldn't accept a context anymore. Clients should probably use Close()
	// exclusively. We should probably find another way to share logging data
	ctx, cancelFunc := context.WithCancel(parent)

	bs := &Client{
		network:                     network,
		providerFinder:              providerFinder,
		blockstore:                  bstore,
		cancel:                      cancelFunc,
		closing:                     make(chan struct{}),
		counters:                    new(counters),
		dupMetric:                   bmetrics.DupHist(ctx),
		allMetric:                   bmetrics.AllHist(ctx),
		havesReceivedGauge:          bmetrics.HavesReceivedGauge(ctx),
		blocksReceivedGauge:         bmetrics.BlocksReceivedGauge(ctx),
		provSearchDelay:             defaults.ProvSearchDelay,
		rebroadcastDelay:            delay.Fixed(defaults.RebroadcastDelay),
		simulateDontHavesOnTimeout:  true,
		defaultProviderQueryManager: true,

		bcastControl: bspm.BroadcastControl{
			MaxPeers: -1,
		},
	}

	// apply functional options before starting and running bitswap
	for _, option := range options {
		option(bs)
	}

	if bs.bcastControl.Enable {
		if bs.bcastControl.NeedHost() {
			bs.bcastControl.Host = network.Host()
		}
		bs.bcastControl.SkipGauge = bmetrics.BroadcastSkipGauge(ctx)
	}

	// onDontHaveTimeout is called when a want-block is sent to a peer that
	// has an old version of Bitswap that doesn't support DONT_HAVE messages,
	// or when no response is received within a timeout.
	//
	// When set to nil (when bs.simulateDontHavesOnTimeout is false), then
	// disable the dontHaveTimoutMgr and do not simulate DONT_HAVE messages on
	// timeout.
	var onDontHaveTimeout func(peer.ID, []cid.Cid)

	var sm *bssm.SessionManager
	if bs.simulateDontHavesOnTimeout {
		// Simulate a message arriving with DONT_HAVEs
		onDontHaveTimeout = func(p peer.ID, dontHaves []cid.Cid) {
			sm.ReceiveFrom(ctx, p, nil, nil, dontHaves)
		}
	}
	peerQueueFactory := func(ctx context.Context, p peer.ID) bspm.PeerQueue {
		return bsmq.New(ctx, p, network, onDontHaveTimeout,
			bsmq.WithDontHaveTimeoutConfig(bs.dontHaveTimeoutConfig),
			bsmq.WithPerPeerSendDelay(bs.perPeerSendDelay))
	}

	sim := bssim.New()
	bpm := bsbpm.New()
	pm := bspm.New(ctx, peerQueueFactory, bs.bcastControl)

	if bs.providerFinder != nil && bs.defaultProviderQueryManager {
		// network can do dialing.
		pqm, err := rpqm.New(network, bs.providerFinder,
			rpqm.WithMaxProviders(defaults.BitswapClientDefaultMaxProviders))
		if err != nil {
			// Should not be possible to hit this
			panic(err)
		}
		bs.pqm = pqm
	}

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
		self peer.ID,
	) bssm.Session {
		// careful when bs.pqm is nil. Since we are type-casting it
		// into routing.ContentDiscovery when passing it, it will become
		// not nil. Related:
		// https://groups.google.com/g/golang-nuts/c/wnH302gBa4I?pli=1
		var sessionProvFinder routing.ContentDiscovery
		if bs.pqm != nil {
			sessionProvFinder = bs.pqm
		} else if providerFinder != nil {
			sessionProvFinder = providerFinder
		}
		return bssession.New(sessctx, sessmgr, id, spm, sessionProvFinder, sim, pm, bpm, notif, provSearchDelay, rebroadcastDelay, self, bs.havesReceivedGauge)
	}
	sessionPeerManagerFactory := func(ctx context.Context, id uint64) bssession.SessionPeerManager {
		return bsspm.New(id, network)
	}
	notif := notifications.New(bs.traceBlock)
	sm = bssm.New(ctx, sessionFactory, sim, sessionPeerManagerFactory, bpm, pm, notif, network.Self())

	bs.sm = sm
	bs.notif = notif
	bs.pm = pm
	bs.sim = sim

	return bs
}

// Client instances implement the bitswap protocol.
type Client struct {
	pm *bspm.PeerManager

	providerFinder routing.ContentDiscovery

	// the provider query manager manages requests to find providers
	pqm                         *rpqm.ProviderQueryManager
	defaultProviderQueryManager bool

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

	// manages channels of outgoing blocks for sessions
	notif notifications.PubSub

	cancel    context.CancelFunc
	closing   chan struct{}
	closeOnce sync.Once

	// Counters for various statistics
	counterLk sync.Mutex
	counters  *counters

	// Metrics interface metrics
	dupMetric metrics.Histogram
	allMetric metrics.Histogram

	havesReceivedGauge  bspm.Gauge
	blocksReceivedGauge bspm.Gauge

	// External statistics interface
	tracer tracer.Tracer

	// Enable GetBLock to return
	// [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable
	// [blocks.Block].
	traceBlock bool

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
	dontHaveTimeoutConfig      *DontHaveTimeoutConfig

	// dupMetric will stay at 0
	skipDuplicatedBlocksStats bool

	perPeerSendDelay time.Duration

	// Broadcast control configuration.
	bcastControl bspm.BroadcastControl
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
//
// If [WithTraceBlock] option is set true, then returns a
// [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable
// [blocks.Block].
func (bs *Client) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "GetBlock", trace.WithAttributes(attribute.String("Key", k.String())))
	defer span.End()
	return bsgetter.SyncGetBlock(ctx, k, bs.GetBlocks)
}

// GetBlocks returns a channel where the caller may receive blocks that
// correspond to the provided |keys|. Returns an error if BitSwap is unable to
// begin this request within the deadline enforced by the context.
//
// If [WithTraceBlock] option is set true, then returns a channel of
// [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable
// [blocks.Block].
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
	case <-bs.closing:
		return nil
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
	var zero peer.ID
	bs.notif.Publish(zero, blks...)

	return nil
}

// receiveBlocksFrom processes blocks received from the network
func (bs *Client) receiveBlocksFrom(ctx context.Context, from peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) {
	select {
	case <-bs.closing:
		return
	default:
	}

	if len(blks) != 0 || len(haves) != 0 {
		bs.pm.MarkBroadcastTarget(from)
	}

	wanted, notWanted := bs.sim.SplitWantedUnwanted(blks)
	if log.Level().Enabled(zapcore.DebugLevel) {
		for _, b := range notWanted {
			log.Debugf("[recv] block not in wantlist; cid=%s, peer=%s", b.Cid(), from)
		}
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
	bs.notif.Publish(from, wanted...)
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
		if log.Level().Enabled(zapcore.DebugLevel) {
			for _, b := range iblocks {
				log.Debugf("[recv] block; cid=%s, peer=%s", b.Cid(), p)
			}
		}
	}

	haves := incoming.Haves()
	dontHaves := incoming.DontHaves()
	if len(iblocks) > 0 || len(haves) > 0 || len(dontHaves) > 0 {
		// Process blocks
		bs.receiveBlocksFrom(ctx, p, iblocks, haves, dontHaves)
	}
}

func (bs *Client) updateReceiveCounters(blocks []blocks.Block) {
	// Check which blocks are in the datastore
	// (Note: any errors from the blockstore are simply logged out in
	// blockstoreHas())
	var blocksHas []bool
	if !bs.skipDuplicatedBlocksStats {
		blocksHas = bs.blockstoreHas(blocks)
	}

	bs.counterLk.Lock()
	defer bs.counterLk.Unlock()

	// Do some accounting for each block
	for i, b := range blocks {
		has := (blocksHas != nil) && blocksHas[i]

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
		bs.blocksReceivedGauge.Inc()
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
	bs.closeOnce.Do(func() {
		close(bs.closing)
		bs.sm.Shutdown()
		bs.cancel()
		if bs.pqm != nil {
			bs.pqm.Close()
		}
		bs.notif.Shutdown()
	})
	return nil
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
// from blockservice, it will create a bitswap session automatically.
func (bs *Client) NewSession(ctx context.Context) exchange.Fetcher {
	ctx, span := internal.StartSpan(ctx, "NewSession")
	defer span.End()
	return bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
}
