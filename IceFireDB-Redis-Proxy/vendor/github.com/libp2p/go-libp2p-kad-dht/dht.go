package dht

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-routing-helpers/tracing"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	dhtcfg "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	"github.com/libp2p/go-libp2p-kad-dht/rtrefresh"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-base32"
	ma "github.com/multiformats/go-multiaddr"
	"go.opencensus.io/tag"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const tracer = tracing.Tracer("go-libp2p-kad-dht")
const dhtName = "IpfsDHT"

var (
	logger     = logging.Logger("dht")
	baseLogger = logger.Desugar()

	rtFreezeTimeout = 1 * time.Minute
)

const (
	// BaseConnMgrScore is the base of the score set on the connection
	// manager "kbucket" tag. It is added with the common prefix length
	// between two peer IDs.
	baseConnMgrScore = 5
)

type mode int

const (
	modeServer mode = iota + 1
	modeClient
)

const (
	kad1 protocol.ID = "/kad/1.0.0"
)

const (
	kbucketTag       = "kbucket"
	protectedBuckets = 2
)

// IpfsDHT is an implementation of Kademlia with S/Kademlia modifications.
// It is used to implement the base Routing module.
type IpfsDHT struct {
	host      host.Host // the network services we need
	self      peer.ID   // Local peer (yourself)
	selfKey   kb.ID
	peerstore peerstore.Peerstore // Peer Registry

	datastore ds.Datastore // Local data

	routingTable *kb.RoutingTable // Array of routing tables for differently distanced nodes
	// providerStore stores & manages the provider records for this Dht peer.
	providerStore providers.ProviderStore

	// manages Routing Table refresh
	rtRefreshManager *rtrefresh.RtRefreshManager

	birth time.Time // When this peer started up

	Validator record.Validator

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	protoMessenger *pb.ProtocolMessenger
	msgSender      pb.MessageSenderWithDisconnect

	stripedPutLocks [256]sync.Mutex

	// DHT protocols we query with. We'll only add peers to our routing
	// table if they speak these protocols.
	protocols []protocol.ID

	// DHT protocols we can respond to.
	serverProtocols []protocol.ID

	auto   ModeOpt
	mode   mode
	modeLk sync.Mutex

	bucketSize int
	alpha      int // The concurrency parameter per path
	beta       int // The number of peers closest to a target that must have responded for a query path to terminate

	queryPeerFilter        QueryFilterFunc
	routingTablePeerFilter RouteTableFilterFunc
	rtPeerDiversityFilter  peerdiversity.PeerIPGroupFilter

	autoRefresh bool

	// timeout for the lookupCheck operation
	lookupCheckTimeout time.Duration
	// number of concurrent lookupCheck operations
	lookupCheckCapacity int
	lookupChecksLk      sync.Mutex

	// A function returning a set of bootstrap peers to fallback on if all other attempts to fix
	// the routing table fail (or, e.g., this is the first time this node is
	// connecting to the network).
	bootstrapPeers func() []peer.AddrInfo

	maxRecordAge time.Duration

	// Allows disabling dht subsystems. These should _only_ be set on
	// "forked" DHTs (e.g., DHTs with custom protocols and/or private
	// networks).
	enableProviders, enableValues bool

	disableFixLowPeers bool
	fixLowPeersChan    chan struct{}

	addPeerToRTChan   chan peer.ID
	refreshFinishedCh chan struct{}

	rtFreezeTimeout time.Duration

	// network size estimator
	nsEstimator   *netsize.Estimator
	enableOptProv bool

	// a bound channel to limit asynchronicity of in-flight ADD_PROVIDER RPCs
	optProvJobsPool chan struct{}

	// configuration variables for tests
	testAddressUpdateProcessing bool

	// addrFilter is used to filter the addresses we put into the peer store.
	// Mostly used to filter out localhost and local addresses.
	addrFilter func([]ma.Multiaddr) []ma.Multiaddr
}

// Assert that IPFS assumptions about interfaces aren't broken. These aren't a
// guarantee, but we can use them to aid refactoring.
var (
	_ routing.ContentRouting = (*IpfsDHT)(nil)
	_ routing.Routing        = (*IpfsDHT)(nil)
	_ routing.PeerRouting    = (*IpfsDHT)(nil)
	_ routing.PubKeyFetcher  = (*IpfsDHT)(nil)
	_ routing.ValueStore     = (*IpfsDHT)(nil)
)

// New creates a new DHT with the specified host and options.
// Please note that being connected to a DHT peer does not necessarily imply that it's also in the DHT Routing Table.
// If the Routing Table has more than "minRTRefreshThreshold" peers, we consider a peer as a Routing Table candidate ONLY when
// we successfully get a query response from it OR if it send us a query.
func New(ctx context.Context, h host.Host, options ...Option) (*IpfsDHT, error) {
	var cfg dhtcfg.Config
	if err := cfg.Apply(append([]Option{dhtcfg.Defaults}, options...)...); err != nil {
		return nil, err
	}
	if err := cfg.ApplyFallbacks(h); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	dht, err := makeDHT(h, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT, err=%s", err)
	}

	dht.autoRefresh = cfg.RoutingTable.AutoRefresh

	dht.maxRecordAge = cfg.MaxRecordAge
	dht.enableProviders = cfg.EnableProviders
	dht.enableValues = cfg.EnableValues
	dht.disableFixLowPeers = cfg.DisableFixLowPeers

	dht.Validator = cfg.Validator
	dht.msgSender = cfg.MsgSenderBuilder(h, dht.protocols)
	dht.protoMessenger, err = pb.NewProtocolMessenger(dht.msgSender)
	if err != nil {
		return nil, err
	}

	dht.testAddressUpdateProcessing = cfg.TestAddressUpdateProcessing

	dht.auto = cfg.Mode
	switch cfg.Mode {
	case ModeAuto, ModeClient:
		dht.mode = modeClient
	case ModeAutoServer, ModeServer:
		dht.mode = modeServer
	default:
		return nil, fmt.Errorf("invalid dht mode %d", cfg.Mode)
	}

	if dht.mode == modeServer {
		if err := dht.moveToServerMode(); err != nil {
			return nil, err
		}
	}

	// register for event bus and network notifications
	if err := dht.startNetworkSubscriber(); err != nil {
		return nil, err
	}

	// go-routine to make sure we ALWAYS have RT peer addresses in the peerstore
	// since RT membership is decoupled from connectivity
	go dht.persistRTPeersInPeerStore()

	dht.rtPeerLoop()

	// Fill routing table with currently connected peers that are DHT servers
	for _, p := range dht.host.Network().Peers() {
		dht.peerFound(p)
	}

	dht.rtRefreshManager.Start()

	// listens to the fix low peers chan and tries to fix the Routing Table
	if !dht.disableFixLowPeers {
		dht.runFixLowPeersLoop()
	}

	return dht, nil
}

// NewDHT creates a new DHT object with the given peer as the 'local' host.
// IpfsDHT's initialized with this function will respond to DHT requests,
// whereas IpfsDHT's initialized with NewDHTClient will not.
func NewDHT(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, Datastore(dstore))
	if err != nil {
		panic(err)
	}
	return dht
}

// NewDHTClient creates a new DHT object with the given peer as the 'local'
// host. IpfsDHT clients initialized with this function will not respond to DHT
// requests. If you need a peer to respond to DHT requests, use NewDHT instead.
func NewDHTClient(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, Datastore(dstore), Mode(ModeClient))
	if err != nil {
		panic(err)
	}
	return dht
}

func makeDHT(h host.Host, cfg dhtcfg.Config) (*IpfsDHT, error) {
	var protocols, serverProtocols []protocol.ID

	v1proto := cfg.ProtocolPrefix + kad1

	if cfg.V1ProtocolOverride != "" {
		v1proto = cfg.V1ProtocolOverride
	}

	protocols = []protocol.ID{v1proto}
	serverProtocols = []protocol.ID{v1proto}

	dht := &IpfsDHT{
		datastore:              cfg.Datastore,
		self:                   h.ID(),
		selfKey:                kb.ConvertPeerID(h.ID()),
		peerstore:              h.Peerstore(),
		host:                   h,
		birth:                  time.Now(),
		protocols:              protocols,
		serverProtocols:        serverProtocols,
		bucketSize:             cfg.BucketSize,
		alpha:                  cfg.Concurrency,
		beta:                   cfg.Resiliency,
		lookupCheckCapacity:    cfg.LookupCheckConcurrency,
		queryPeerFilter:        cfg.QueryPeerFilter,
		routingTablePeerFilter: cfg.RoutingTable.PeerFilter,
		rtPeerDiversityFilter:  cfg.RoutingTable.DiversityFilter,
		addrFilter:             cfg.AddressFilter,

		fixLowPeersChan: make(chan struct{}, 1),

		addPeerToRTChan:   make(chan peer.ID),
		refreshFinishedCh: make(chan struct{}),

		enableOptProv:   cfg.EnableOptimisticProvide,
		optProvJobsPool: nil,
	}

	var maxLastSuccessfulOutboundThreshold time.Duration

	// The threshold is calculated based on the expected amount of time that should pass before we
	// query a peer as part of our refresh cycle.
	// To grok the Math Wizardy that produced these exact equations, please be patient as a document explaining it will
	// be published soon.
	if cfg.Concurrency < cfg.BucketSize { // (alpha < K)
		l1 := math.Log(float64(1) / float64(cfg.BucketSize))                              // (Log(1/K))
		l2 := math.Log(float64(1) - (float64(cfg.Concurrency) / float64(cfg.BucketSize))) // Log(1 - (alpha / K))
		maxLastSuccessfulOutboundThreshold = time.Duration(l1 / l2 * float64(cfg.RoutingTable.RefreshInterval))
	} else {
		maxLastSuccessfulOutboundThreshold = cfg.RoutingTable.RefreshInterval
	}

	// construct routing table
	// use twice the theoritical usefulness threhold to keep older peers around longer
	rt, err := makeRoutingTable(dht, cfg, 2*maxLastSuccessfulOutboundThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to construct routing table,err=%s", err)
	}
	dht.routingTable = rt
	dht.bootstrapPeers = cfg.BootstrapPeers

	dht.lookupCheckTimeout = cfg.RoutingTable.RefreshQueryTimeout

	// init network size estimator
	dht.nsEstimator = netsize.NewEstimator(h.ID(), rt, cfg.BucketSize)

	if dht.enableOptProv {
		dht.optProvJobsPool = make(chan struct{}, cfg.OptimisticProvideJobsPoolSize)
	}

	// rt refresh manager
	dht.rtRefreshManager, err = makeRtRefreshManager(dht, cfg, maxLastSuccessfulOutboundThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to construct RT Refresh Manager,err=%s", err)
	}

	// create a tagged context derived from the original context
	// the DHT context should be done when the process is closed
	dht.ctx, dht.cancel = context.WithCancel(dht.newContextWithLocalTags(context.Background()))

	if cfg.ProviderStore != nil {
		dht.providerStore = cfg.ProviderStore
	} else {
		dht.providerStore, err = providers.NewProviderManager(h.ID(), dht.peerstore, cfg.Datastore)
		if err != nil {
			return nil, fmt.Errorf("initializing default provider manager (%v)", err)
		}
	}

	dht.rtFreezeTimeout = rtFreezeTimeout

	return dht, nil
}

// lookupCheck performs a lookup request to a remote peer.ID, verifying that it is able to
// answer it correctly
func (dht *IpfsDHT) lookupCheck(ctx context.Context, p peer.ID) error {
	// lookup request to p requesting for its own peer.ID
	peerids, err := dht.protoMessenger.GetClosestPeers(ctx, p, p)
	// p is expected to return at least 1 peer id, unless our routing table has
	// less than bucketSize peers, in which case we aren't picky about who we
	// add to the routing table.
	if err == nil && len(peerids) == 0 && dht.routingTable.Size() >= dht.bucketSize {
		return fmt.Errorf("peer %s failed to return its closest peers, got %d", p, len(peerids))
	}
	return err
}

func makeRtRefreshManager(dht *IpfsDHT, cfg dhtcfg.Config, maxLastSuccessfulOutboundThreshold time.Duration) (*rtrefresh.RtRefreshManager, error) {
	keyGenFnc := func(cpl uint) (string, error) {
		p, err := dht.routingTable.GenRandPeerID(cpl)
		return string(p), err
	}

	queryFnc := func(ctx context.Context, key string) error {
		_, err := dht.GetClosestPeers(ctx, key)
		return err
	}

	r, err := rtrefresh.NewRtRefreshManager(
		dht.host, dht.routingTable, cfg.RoutingTable.AutoRefresh,
		keyGenFnc,
		queryFnc,
		dht.lookupCheck,
		cfg.RoutingTable.RefreshQueryTimeout,
		cfg.RoutingTable.RefreshInterval,
		maxLastSuccessfulOutboundThreshold,
		dht.refreshFinishedCh)

	return r, err
}

func makeRoutingTable(dht *IpfsDHT, cfg dhtcfg.Config, maxLastSuccessfulOutboundThreshold time.Duration) (*kb.RoutingTable, error) {
	// make a Routing Table Diversity Filter
	var filter *peerdiversity.Filter
	if dht.rtPeerDiversityFilter != nil {
		df, err := peerdiversity.NewFilter(dht.rtPeerDiversityFilter, "rt/diversity", func(p peer.ID) int {
			return kb.CommonPrefixLen(dht.selfKey, kb.ConvertPeerID(p))
		})

		if err != nil {
			return nil, fmt.Errorf("failed to construct peer diversity filter: %w", err)
		}

		filter = df
	}

	rt, err := kb.NewRoutingTable(cfg.BucketSize, dht.selfKey, time.Minute, dht.host.Peerstore(), maxLastSuccessfulOutboundThreshold, filter)
	if err != nil {
		return nil, err
	}

	cmgr := dht.host.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		commonPrefixLen := kb.CommonPrefixLen(dht.selfKey, kb.ConvertPeerID(p))
		if commonPrefixLen < protectedBuckets {
			cmgr.Protect(p, kbucketTag)
		} else {
			cmgr.TagPeer(p, kbucketTag, baseConnMgrScore)
		}
	}
	rt.PeerRemoved = func(p peer.ID) {
		cmgr.Unprotect(p, kbucketTag)
		cmgr.UntagPeer(p, kbucketTag)

		// try to fix the RT
		dht.fixRTIfNeeded()
	}

	return rt, err
}

// ProviderStore returns the provider storage object for storing and retrieving provider records.
func (dht *IpfsDHT) ProviderStore() providers.ProviderStore {
	return dht.providerStore
}

// GetRoutingTableDiversityStats returns the diversity stats for the Routing Table.
func (dht *IpfsDHT) GetRoutingTableDiversityStats() []peerdiversity.CplDiversityStats {
	return dht.routingTable.GetDiversityStats()
}

// Mode allows introspection of the operation mode of the DHT
func (dht *IpfsDHT) Mode() ModeOpt {
	return dht.auto
}

// runFixLowPeersLoop manages simultaneous requests to fixLowPeers
func (dht *IpfsDHT) runFixLowPeersLoop() {
	dht.wg.Add(1)
	go func() {
		defer dht.wg.Done()

		dht.fixLowPeers()

		ticker := time.NewTicker(periodicBootstrapInterval)
		defer ticker.Stop()

		for {
			select {
			case <-dht.fixLowPeersChan:
			case <-ticker.C:
			case <-dht.ctx.Done():
				return
			}

			dht.fixLowPeers()
		}
	}()
}

// fixLowPeers tries to get more peers into the routing table if we're below the threshold
func (dht *IpfsDHT) fixLowPeers() {
	if dht.routingTable.Size() > minRTRefreshThreshold {
		return
	}

	// we try to add all peers we are connected to to the Routing Table
	// in case they aren't already there.
	for _, p := range dht.host.Network().Peers() {
		dht.peerFound(p)
	}

	// TODO Active Bootstrapping
	// We should first use non-bootstrap peers we knew of from previous
	// snapshots of the Routing Table before we connect to the bootstrappers.
	// See https://github.com/libp2p/go-libp2p-kad-dht/issues/387.
	if dht.routingTable.Size() == 0 && dht.bootstrapPeers != nil {
		bootstrapPeers := dht.bootstrapPeers()
		if len(bootstrapPeers) == 0 {
			// No point in continuing, we have no peers!
			return
		}

		found := 0
		for _, i := range rand.Perm(len(bootstrapPeers)) {
			ai := bootstrapPeers[i]
			err := dht.Host().Connect(dht.ctx, ai)
			if err == nil {
				found++
			} else {
				logger.Warnw("failed to bootstrap", "peer", ai.ID, "error", err)
			}

			// Wait for two bootstrap peers, or try them all.
			//
			// Why two? In theory, one should be enough
			// normally. However, if the network were to
			// restart and everyone connected to just one
			// bootstrapper, we'll end up with a mostly
			// partitioned network.
			//
			// So we always bootstrap with two random peers.
			if found == maxNBoostrappers {
				break
			}
		}
	}

	// if we still don't have peers in our routing table(probably because Identify hasn't completed),
	// there is no point in triggering a Refresh.
	if dht.routingTable.Size() == 0 {
		return
	}

	if dht.autoRefresh {
		dht.rtRefreshManager.RefreshNoWait()
	}
}

// TODO This is hacky, horrible and the programmer needs to have his mother called a hamster.
// SHOULD be removed once https://github.com/libp2p/go-libp2p/issues/800 goes in.
func (dht *IpfsDHT) persistRTPeersInPeerStore() {
	tickr := time.NewTicker(peerstore.RecentlyConnectedAddrTTL / 3)
	defer tickr.Stop()

	for {
		select {
		case <-tickr.C:
			ps := dht.routingTable.ListPeers()
			for _, p := range ps {
				dht.peerstore.UpdateAddrs(p, peerstore.RecentlyConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
			}
		case <-dht.ctx.Done():
			return
		}
	}
}

// getLocal attempts to retrieve the value from the datastore.
//
// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *IpfsDHT) getLocal(ctx context.Context, key string) (*recpb.Record, error) {
	logger.Debugw("finding value in datastore", "key", internal.LoggableRecordKeyString(key))

	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(key))
	if err != nil {
		logger.Warnw("get local failed", "key", internal.LoggableRecordKeyString(key), "error", err)
		return nil, err
	}

	// Double check the key. Can't hurt.
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorw("BUG: found a DHT record that didn't match it's key", "expected", internal.LoggableRecordKeyString(key), "got", rec.GetKey())
		return nil, nil

	}
	return rec, nil
}

// putLocal stores the key value pair in the datastore
func (dht *IpfsDHT) putLocal(ctx context.Context, key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("failed to put marshal record for local put", "error", err, "key", internal.LoggableRecordKeyString(key))
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

func (dht *IpfsDHT) rtPeerLoop() {
	dht.wg.Add(1)
	go func() {
		defer dht.wg.Done()

		var bootstrapCount uint
		var isBootsrapping bool
		var timerCh <-chan time.Time

		for {
			select {
			case <-timerCh:
				dht.routingTable.MarkAllPeersIrreplaceable()
			case p := <-dht.addPeerToRTChan:
				if dht.routingTable.Size() == 0 {
					isBootsrapping = true
					bootstrapCount = 0
					timerCh = nil
				}
				// queryPeer set to true as we only try to add queried peers to the RT
				newlyAdded, err := dht.routingTable.TryAddPeer(p, true, isBootsrapping)
				if err != nil {
					// peer not added.
					continue
				}
				if newlyAdded {
					// peer was added to the RT, it can now be fixed if needed.
					dht.fixRTIfNeeded()
				} else {
					// the peer is already in our RT, but we just successfully queried it and so let's give it a
					// bump on the query time so we don't ping it too soon for a liveliness check.
					dht.routingTable.UpdateLastSuccessfulOutboundQueryAt(p, time.Now())
				}
			case <-dht.refreshFinishedCh:
				bootstrapCount = bootstrapCount + 1
				if bootstrapCount == 2 {
					timerCh = time.NewTimer(dht.rtFreezeTimeout).C
				}

				old := isBootsrapping
				isBootsrapping = false
				if old {
					dht.rtRefreshManager.RefreshNoWait()
				}

			case <-dht.ctx.Done():
				return
			}
		}
	}()
}

// peerFound verifies whether the found peer advertises DHT protocols
// and probe it to make sure it answers DHT queries as expected. If
// it fails to answer, it isn't added to the routingTable.
func (dht *IpfsDHT) peerFound(p peer.ID) {
	// if the peer is already in the routing table or the appropriate bucket is
	// already full, don't try to add the new peer.ID
	if !dht.routingTable.UsefulNewPeer(p) {
		return
	}

	// verify whether the remote peer advertises the right dht protocol
	b, err := dht.validRTPeer(p)
	if err != nil {
		logger.Errorw("failed to validate if peer is a DHT peer", "peer", p, "error", err)
	} else if b {

		// check if the maximal number of concurrent lookup checks is reached
		dht.lookupChecksLk.Lock()
		if dht.lookupCheckCapacity == 0 {
			dht.lookupChecksLk.Unlock()
			// drop the new peer.ID if the maximal number of concurrent lookup
			// checks is reached
			return
		}
		dht.lookupCheckCapacity--
		dht.lookupChecksLk.Unlock()

		go func() {
			livelinessCtx, cancel := context.WithTimeout(dht.ctx, dht.lookupCheckTimeout)
			defer cancel()

			// performing a FIND_NODE query
			err := dht.lookupCheck(livelinessCtx, p)

			dht.lookupChecksLk.Lock()
			dht.lookupCheckCapacity++
			dht.lookupChecksLk.Unlock()

			if err != nil {
				logger.Debugw("connected peer not answering DHT request as expected", "peer", p, "error", err)
				return
			}

			// if the FIND_NODE succeeded, the peer is considered as valid
			dht.validPeerFound(p)
		}()
	}
}

// validPeerFound signals the routingTable that we've found a peer that
// supports the DHT protocol, and just answered correctly to a DHT FindPeers
func (dht *IpfsDHT) validPeerFound(p peer.ID) {
	if c := baseLogger.Check(zap.DebugLevel, "peer found"); c != nil {
		c.Write(zap.String("peer", p.String()))
	}

	select {
	case dht.addPeerToRTChan <- p:
	case <-dht.ctx.Done():
		return
	}
}

// peerStoppedDHT signals the routing table that a peer is unable to responsd to DHT queries anymore.
func (dht *IpfsDHT) peerStoppedDHT(p peer.ID) {
	logger.Debugw("peer stopped dht", "peer", p)
	// A peer that does not support the DHT protocol is dead for us.
	// There's no point in talking to anymore till it starts supporting the DHT protocol again.
	dht.routingTable.RemovePeer(p)
}

func (dht *IpfsDHT) fixRTIfNeeded() {
	select {
	case dht.fixLowPeersChan <- struct{}{}:
	default:
	}
}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
func (dht *IpfsDHT) FindLocal(ctx context.Context, id peer.ID) peer.AddrInfo {
	_, span := internal.StartSpan(ctx, "IpfsDHT.FindLocal", trace.WithAttributes(attribute.Stringer("PeerID", id)))
	defer span.End()

	if hasValidConnectedness(dht.host, id) {
		return dht.peerstore.PeerInfo(id)
	}
	return peer.AddrInfo{}
}

// nearestPeersToQuery returns the routing tables closest peers.
func (dht *IpfsDHT) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	closer := dht.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)
	return closer
}

// betterPeersToQuery returns nearestPeersToQuery with some additional filtering
func (dht *IpfsDHT) betterPeersToQuery(pmes *pb.Message, from peer.ID, count int) []peer.ID {
	closer := dht.nearestPeersToQuery(pmes, count)

	// no node? nil
	if closer == nil {
		logger.Infow("no closer peers to send", from)
		return nil
	}

	filtered := make([]peer.ID, 0, len(closer))
	for _, clp := range closer {

		// == to self? thats bad
		if clp == dht.self {
			logger.Error("BUG betterPeersToQuery: attempted to return self! this shouldn't happen...")
			return nil
		}
		// Dont send a peer back themselves
		if clp == from {
			continue
		}

		filtered = append(filtered, clp)
	}

	// ok seems like closer nodes
	return filtered
}

func (dht *IpfsDHT) setMode(m mode) error {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()

	if m == dht.mode {
		return nil
	}

	switch m {
	case modeServer:
		return dht.moveToServerMode()
	case modeClient:
		return dht.moveToClientMode()
	default:
		return fmt.Errorf("unrecognized dht mode: %d", m)
	}
}

// moveToServerMode advertises (via libp2p identify updates) that we are able to respond to DHT queries and sets the appropriate stream handlers.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (dht *IpfsDHT) moveToServerMode() error {
	dht.mode = modeServer
	for _, p := range dht.serverProtocols {
		dht.host.SetStreamHandler(p, dht.handleNewStream)
	}
	return nil
}

// moveToClientMode stops advertising (and rescinds advertisements via libp2p identify updates) that we are able to
// respond to DHT queries and removes the appropriate stream handlers. We also kill all inbound streams that were
// utilizing the handled protocols.
// Note: We may support responding to queries with protocols aside from our primary ones in order to support
// interoperability with older versions of the DHT protocol.
func (dht *IpfsDHT) moveToClientMode() error {
	dht.mode = modeClient
	for _, p := range dht.serverProtocols {
		dht.host.RemoveStreamHandler(p)
	}

	pset := make(map[protocol.ID]bool)
	for _, p := range dht.serverProtocols {
		pset[p] = true
	}

	for _, c := range dht.host.Network().Conns() {
		for _, s := range c.GetStreams() {
			if pset[s.Protocol()] {
				if s.Stat().Direction == network.DirInbound {
					_ = s.Reset()
				}
			}
		}
	}
	return nil
}

func (dht *IpfsDHT) getMode() mode {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()
	return dht.mode
}

// Context returns the DHT's context.
func (dht *IpfsDHT) Context() context.Context {
	return dht.ctx
}

// RoutingTable returns the DHT's routingTable.
func (dht *IpfsDHT) RoutingTable() *kb.RoutingTable {
	return dht.routingTable
}

// Close calls Process Close.
func (dht *IpfsDHT) Close() error {
	dht.cancel()
	dht.wg.Wait()

	var wg sync.WaitGroup
	closes := [...]func() error{
		dht.rtRefreshManager.Close,
		dht.providerStore.Close,
	}
	var errors [len(closes)]error
	wg.Add(len(errors))
	for i, c := range closes {
		go func(i int, c func() error) {
			defer wg.Done()
			errors[i] = c()
		}(i, c)
	}
	wg.Wait()

	return multierr.Combine(errors[:]...)
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

// PeerID returns the DHT node's Peer ID.
func (dht *IpfsDHT) PeerID() peer.ID {
	return dht.self
}

// PeerKey returns a DHT key, converted from the DHT node's Peer ID.
func (dht *IpfsDHT) PeerKey() []byte {
	return kb.ConvertPeerID(dht.self)
}

// Host returns the libp2p host this DHT is operating with.
func (dht *IpfsDHT) Host() host.Host {
	return dht.host
}

// Ping sends a ping message to the passed peer and waits for a response.
func (dht *IpfsDHT) Ping(ctx context.Context, p peer.ID) error {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.Ping", trace.WithAttributes(attribute.Stringer("PeerID", p)))
	defer span.End()
	return dht.protoMessenger.Ping(ctx, p)
}

// NetworkSize returns the most recent estimation of the DHT network size.
// EXPERIMENTAL: We do not provide any guarantees that this method will
// continue to exist in the codebase. Use it at your own risk.
func (dht *IpfsDHT) NetworkSize() (int32, error) {
	return dht.nsEstimator.NetworkSize()
}

// newContextWithLocalTags returns a new context.Context with the InstanceID and
// PeerID keys populated. It will also take any extra tags that need adding to
// the context as tag.Mutators.
func (dht *IpfsDHT) newContextWithLocalTags(ctx context.Context, extraTags ...tag.Mutator) context.Context {
	extraTags = append(
		extraTags,
		tag.Upsert(metrics.KeyPeerID, dht.self.String()),
		tag.Upsert(metrics.KeyInstanceID, fmt.Sprintf("%p", dht)),
	)
	ctx, _ = tag.New(
		ctx,
		extraTags...,
	) // ignoring error as it is unrelated to the actual function of this code.
	return ctx
}

func (dht *IpfsDHT) maybeAddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// Don't add addresses for self or our connected peers. We have better ones.
	if p == dht.self || hasValidConnectedness(dht.host, p) {
		return
	}
	dht.peerstore.AddAddrs(p, dht.filterAddrs(addrs), ttl)
}

func (dht *IpfsDHT) filterAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	if f := dht.addrFilter; f != nil {
		return f(addrs)
	}
	return addrs
}
