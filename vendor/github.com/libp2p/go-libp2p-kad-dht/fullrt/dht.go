package fullrt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multiformats/go-base32"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multihash"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	"github.com/libp2p/go-libp2p-routing-helpers/tracing"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	"google.golang.org/protobuf/proto"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	internalConfig "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	kb "github.com/libp2p/go-libp2p-kbucket"

	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"

	"github.com/libp2p/go-libp2p-xor/kademlia"
	kadkey "github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var logger = logging.Logger("fullrtdht")

const (
	tracer  = tracing.Tracer("go-libp2p-kad-dht/fullrt")
	dhtName = "FullRT"
)

const rtRefreshLimitsMsg = `Accelerated DHT client was unable to fully refresh its routing table due to Resource Manager limits, which may degrade content routing. Consider increasing resource limits. See debug logs for the "dht-crawler" subsystem for details.`

// FullRT is an experimental DHT client that is under development. Expect breaking changes to occur in this client
// until it stabilizes.
//
// Running FullRT by itself (i.e. without a companion IpfsDHT) will run into some issues. The most critical is that
// running a FullRT node will not currently keep you connected to the k closest peers which means that your peer's
// addresses may not be discoverable in the DHT. Additionally, FullRT is only a DHT client and not a server which means it does not contribute capacity to the network.
// If you want to run a server you should also run an IpfsDHT instance in server mode.
//
// FullRT has a Ready function that indicates the routing table has been refreshed recently. It is currently within the
// discretion of the application as to how much they care about whether the routing table is ready.
// One approach taken to "readiness" is to not insist on it for ad-hoc operations but to insist on it for larger bulk
// operations.
type FullRT struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	enableValues, enableProviders bool
	Validator                     record.Validator
	ProviderManager               *records.ProviderManager
	datastore                     ds.Datastore
	h                             host.Host

	crawlerInterval time.Duration
	lastCrawlTime   time.Time

	crawler        crawler.Crawler
	protoMessenger *dht_pb.ProtocolMessenger
	messageSender  dht_pb.MessageSender

	filterFromTable kaddht.QueryFilterFunc
	rtLk            sync.RWMutex
	rt              *trie.Trie

	kMapLk       sync.RWMutex
	keyToPeerMap map[string]peer.ID

	peerAddrsLk sync.RWMutex
	peerAddrs   map[peer.ID][]ma.Multiaddr

	bootstrapPeers []*peer.AddrInfo

	bucketSize int

	triggerRefresh chan struct{}

	waitFrac     float64
	timeoutPerOp time.Duration

	bulkSendParallelism int

	self peer.ID

	peerConnectednessSubscriber event.Subscription

	ipDiversityFilterLimit int
}

// NewFullRT creates a DHT client that tracks the full network. It takes a protocol prefix for the given network,
// For example, the protocol /ipfs/kad/1.0.0 has the prefix /ipfs.
//
// FullRT is an experimental DHT client that is under development. Expect breaking changes to occur in this client
// until it stabilizes.
//
// Not all of the standard DHT options are supported in this DHT.
//
// DHT options passed in should be suitable for your network (e.g. protocol prefix, validators, bucket size, and
// bootstrap peers).
func NewFullRT(h host.Host, protocolPrefix protocol.ID, options ...Option) (*FullRT, error) {
	fullrtcfg := config{
		crawlInterval:          time.Hour,
		bulkSendParallelism:    20,
		waitFrac:               0.3,
		timeoutPerOp:           5 * time.Second,
		ipDiversityFilterLimit: amino.DefaultMaxPeersPerIPGroup,
	}
	if err := fullrtcfg.apply(options...); err != nil {
		return nil, err
	}

	dhtcfg := &internalConfig.Config{
		Datastore:        dssync.MutexWrap(ds.NewMapDatastore()),
		Validator:        record.NamespacedValidator{},
		ValidatorChanged: false,
		EnableProviders:  true,
		EnableValues:     true,
		ProtocolPrefix:   protocolPrefix,
	}

	if err := dhtcfg.Apply(fullrtcfg.dhtOpts...); err != nil {
		return nil, err
	}
	if err := dhtcfg.ApplyFallbacks(h); err != nil {
		return nil, err
	}

	if err := dhtcfg.Validate(); err != nil {
		return nil, err
	}

	ms := net.NewMessageSenderImpl(h, amino.Protocols)
	protoMessenger, err := dht_pb.NewProtocolMessenger(ms)
	if err != nil {
		return nil, err
	}

	if fullrtcfg.crawler == nil {
		fullrtcfg.crawler, err = crawler.NewDefaultCrawler(h, crawler.WithParallelism(200))
		if err != nil {
			return nil, err
		}
	}

	sub, err := h.EventBus().Subscribe(new(event.EvtPeerConnectednessChanged), eventbus.Name("fullrt-dht"))
	if err != nil {
		return nil, fmt.Errorf("peer connectedness subscription failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	self := h.ID()
	pm, err := records.NewProviderManager(ctx, self, h.Peerstore(), dhtcfg.Datastore, fullrtcfg.pmOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	var bsPeers []*peer.AddrInfo

	for _, ai := range dhtcfg.BootstrapPeers() {
		tmpai := ai
		bsPeers = append(bsPeers, &tmpai)
	}

	rt := &FullRT{
		ctx:    ctx,
		cancel: cancel,

		enableValues:    dhtcfg.EnableValues,
		enableProviders: dhtcfg.EnableProviders,
		Validator:       dhtcfg.Validator,
		ProviderManager: pm,
		datastore:       dhtcfg.Datastore,
		h:               h,
		crawler:         fullrtcfg.crawler,
		messageSender:   ms,
		protoMessenger:  protoMessenger,
		filterFromTable: kaddht.PublicQueryFilter,
		rt:              trie.New(),
		keyToPeerMap:    make(map[string]peer.ID),
		bucketSize:      dhtcfg.BucketSize,

		peerAddrs:      make(map[peer.ID][]ma.Multiaddr),
		bootstrapPeers: bsPeers,

		triggerRefresh: make(chan struct{}),

		waitFrac:     fullrtcfg.waitFrac,
		timeoutPerOp: fullrtcfg.timeoutPerOp,

		crawlerInterval: fullrtcfg.crawlInterval,

		bulkSendParallelism:         fullrtcfg.bulkSendParallelism,
		self:                        self,
		peerConnectednessSubscriber: sub,
	}

	rt.wg.Add(2)
	go rt.runCrawler(ctx)
	go rt.runSubscriber()
	return rt, nil
}

func (dht *FullRT) runSubscriber() {
	defer dht.wg.Done()
	ms, ok := dht.messageSender.(dht_pb.MessageSenderWithDisconnect)
	defer dht.peerConnectednessSubscriber.Close()
	if !ok {
		return
	}
	for {
		select {
		case e := <-dht.peerConnectednessSubscriber.Out():
			pc, ok := e.(event.EvtPeerConnectednessChanged)
			if !ok {
				logger.Errorf("invalid event message type: %T", e)
				continue
			}

			if pc.Connectedness != network.Connected {
				ms.OnDisconnect(dht.ctx, pc.Peer)
			}
		case <-dht.ctx.Done():
			return
		}
	}
}

func (dht *FullRT) TriggerRefresh(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case dht.triggerRefresh <- struct{}{}:
		return nil
	case <-dht.ctx.Done():
		return errors.New("dht is closed")
	}
}

func (dht *FullRT) Stat() map[string]peer.ID {
	dht.kMapLk.RLock()
	defer dht.kMapLk.RUnlock()
	return maps.Clone(dht.keyToPeerMap)
}

// Ready indicates that the routing table has been refreshed recently. It is recommended to be used for operations where
// it is important for the operation to be particularly accurate (e.g. bulk publishing where you do not want to
// republish for as long as you can).
func (dht *FullRT) Ready() bool {
	dht.rtLk.RLock()
	lastCrawlTime := dht.lastCrawlTime
	dht.rtLk.RUnlock()

	if time.Since(lastCrawlTime) > dht.crawlerInterval {
		return false
	}

	// TODO: This function needs to be better defined. Perhaps based on going through the peer map and seeing when the
	// last time we were connected to any of them was.
	dht.kMapLk.RLock()
	rtSize := len(dht.keyToPeerMap)
	dht.kMapLk.RUnlock()

	return rtSize > len(dht.bootstrapPeers)+1
}

func (dht *FullRT) Host() host.Host {
	return dht.h
}

func (dht *FullRT) runCrawler(ctx context.Context) {
	defer dht.wg.Done()
	t := time.NewTicker(dht.crawlerInterval)

	foundPeers := make(map[peer.ID][]ma.Multiaddr)
	foundPeersLk := sync.Mutex{}

	initialTrigger := make(chan struct{}, 1)
	initialTrigger <- struct{}{}

	for {
		select {
		case <-t.C:
		case <-initialTrigger:
		case <-dht.triggerRefresh:
		case <-ctx.Done():
			return
		}

		var addrs []*peer.AddrInfo
		foundPeersLk.Lock()
		for k := range foundPeers {
			addrs = append(addrs, &peer.AddrInfo{ID: k}) // Addrs: v.addrs
		}
		foundPeersLk.Unlock()
		addrs = append(addrs, dht.bootstrapPeers...)

		clear(foundPeers)

		start := time.Now()
		limitErrOnce := sync.Once{}
		dht.crawler.Run(ctx, addrs,
			func(p peer.ID, rtPeers []*peer.AddrInfo) {
				keep := kaddht.PublicRoutingTableFilter(dht, p)
				if !keep {
					return
				}

				foundPeersLk.Lock()
				defer foundPeersLk.Unlock()
				foundPeers[p] = dht.h.Peerstore().Addrs(p)
			},
			func(p peer.ID, err error) {
				dialErr, ok := err.(*swarm.DialError)
				if ok {
					for _, transportErr := range dialErr.DialErrors {
						if errors.Is(transportErr.Cause, network.ErrResourceLimitExceeded) {
							limitErrOnce.Do(func() { logger.Errorf(rtRefreshLimitsMsg) })
						}
					}
				}
				// note that DialError implements Unwrap() which returns the Cause, so this covers that case
				if errors.Is(err, network.ErrResourceLimitExceeded) {
					limitErrOnce.Do(func() { logger.Errorf(rtRefreshLimitsMsg) })
				}
			})
		dur := time.Since(start)
		logger.Infof("crawl took %v", dur)

		peerAddrs := make(map[peer.ID][]ma.Multiaddr)
		kPeerMap := make(map[string]peer.ID)
		newRt := trie.New()
		for peerID, foundAddrs := range foundPeers {
			kadKey := kadkey.KbucketIDToKey(kb.ConvertPeerID(peerID))
			peerAddrs[peerID] = foundAddrs
			kPeerMap[string(kadKey)] = peerID
			newRt.Add(kadKey)
		}

		dht.peerAddrsLk.Lock()
		dht.peerAddrs = peerAddrs
		dht.peerAddrsLk.Unlock()

		dht.kMapLk.Lock()
		dht.keyToPeerMap = kPeerMap
		dht.kMapLk.Unlock()

		dht.rtLk.Lock()
		dht.rt = newRt
		dht.lastCrawlTime = time.Now()
		dht.rtLk.Unlock()
	}
}

func (dht *FullRT) Close() error {
	dht.cancel()
	dht.wg.Wait()
	return dht.ProviderManager.Close()
}

func (dht *FullRT) Bootstrap(ctx context.Context) (err error) {
	_, end := tracer.Bootstrap(dhtName, ctx)
	defer func() { end(err) }()

	// TODO: This should block until the first crawl finish.

	return nil
}

// CheckPeers return (success, total)
func (dht *FullRT) CheckPeers(ctx context.Context, peers ...peer.ID) (int, int) {
	ctx, span := internal.StartSpan(ctx, "FullRT.CheckPeers", trace.WithAttributes(attribute.Int("NumPeers", len(peers))))
	defer span.End()

	var peerAddrs chan peer.AddrInfo
	var total int
	if len(peers) == 0 {
		dht.peerAddrsLk.RLock()
		total = len(dht.peerAddrs)
		peerAddrs = make(chan peer.AddrInfo, total)
		for k, v := range dht.peerAddrs {
			peerAddrs <- peer.AddrInfo{
				ID:    k,
				Addrs: v,
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	} else {
		total = len(peers)
		peerAddrs = make(chan peer.AddrInfo, total)
		dht.peerAddrsLk.RLock()
		for _, p := range peers {
			peerAddrs <- peer.AddrInfo{
				ID:    p,
				Addrs: dht.peerAddrs[p],
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	}

	var success uint64

	workers(100, func(ai peer.AddrInfo) {
		dialctx, dialcancel := context.WithTimeout(ctx, time.Second*3)
		if err := dht.h.Connect(dialctx, ai); err == nil {
			atomic.AddUint64(&success, 1)
		}
		dialcancel()
	}, peerAddrs)
	return int(success), total
}

func workers(numWorkers int, fn func(peer.AddrInfo), inputs <-chan peer.AddrInfo) {
	jobs := make(chan peer.AddrInfo)
	defer close(jobs)
	for range numWorkers {
		go func() {
			for j := range jobs {
				fn(j)
			}
		}()
	}
	for i := range inputs {
		jobs <- i
	}
}

// GetClosestPeers tries to return the `dht.bucketSize` closest known peers to
// the given key.
//
// If the IP diversity filter limit is set, the returned peers will contain at
// most `dht.ipDiversityFilterLimit` peers sharing the same IP group. Hence,
// the peers may not be the absolute closest peers to the given key, but they
// will be more diverse in terms of IP addresses.
func (dht *FullRT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	_, span := internal.StartSpan(ctx, "FullRT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	kbID := kb.ConvertKey(key)
	kadKey := kadkey.KbucketIDToKey(kbID)

	ipGroupCounts := make(map[peerdiversity.PeerIPGroupKey]map[peer.ID]struct{})
	peers := make([]peer.ID, 0, dht.bucketSize)

	// Lock routing table, keyToPeerMap and peer addresses so that they cannot be
	// changed during a GetClosestPeers call. Otherwise peers may be removed,
	// resulting in keys missing in maps.
	dht.rtLk.RLock()
	defer dht.rtLk.RUnlock()
	dht.kMapLk.RLock()
	defer dht.kMapLk.RUnlock()
	dht.peerAddrsLk.RLock()
	defer dht.peerAddrsLk.RUnlock()

	// If ipDiversityFilterLimit is non-zero, the step is slightly larger than
	// the bucket size, allowing to have a few backup peers in case some are
	// filtered out by the diversity filter. Multiple calls to ClosestN are
	// expensive, but increasing the `count` parameter is cheap.
	step := dht.bucketSize + 2*dht.ipDiversityFilterLimit
	for nClosest := 0; nClosest < dht.rt.Size(); nClosest += step {
		// Get the last `step` closest peers, because we already tried the `nClosest` closest peers
		closestKeys := kademlia.ClosestN(kadKey, dht.rt, nClosest+step)[nClosest:]

	PeersLoop:
		for _, k := range closestKeys {
			// Recover the peer ID from the key
			p, ok := dht.keyToPeerMap[string(k)]
			if !ok {
				logger.Warnf("key not found in map")
				continue
			}

			peerAddrs := dht.peerAddrs[p]

			if dht.ipDiversityFilterLimit > 0 {
				for _, addr := range peerAddrs {
					ip, err := manet.ToIP(addr)
					if err != nil {
						continue
					}
					ipGroup := peerdiversity.IPGroupKey(ip)
					if len(ipGroup) == 0 {
						continue
					}
					if _, ok := ipGroupCounts[ipGroup]; !ok {
						ipGroupCounts[ipGroup] = make(map[peer.ID]struct{})
					}
					if len(ipGroupCounts[ipGroup]) >= dht.ipDiversityFilterLimit {
						// This ip group is already overrepresented, skip this peer
						continue PeersLoop
					}
					ipGroupCounts[ipGroup][p] = struct{}{}
				}
			}

			// Add the peer's known addresses to the peerstore so that it can be
			// dialed by the caller.
			dht.h.Peerstore().AddAddrs(p, peerAddrs, peerstore.TempAddrTTL)
			peers = append(peers, p)

			if len(peers) == dht.bucketSize {
				return peers, nil
			}
		}
	}
	return peers, nil
}

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *FullRT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(dhtName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	logger.Debugw("putting value", "key", internal.LoggableRecordKeyString(key))

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(ctx, key)
	if err != nil {
		// Means something is wrong with the datastore.
		return err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return errors.New("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = internal.FormatRFC3339(time.Now())
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		return err
	}

	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.Value,
			ID:   p,
		})
		err := dht.protoMessenger.PutValue(ctx, p, rec)
		return err
	}, peers, true)

	if successes == 0 {
		return errors.New("failed to complete put")
	}

	return nil
}

// RecvdVal stores a value and the peer from which we got the value.
type RecvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue searches for the value corresponding to given Key.
func (dht *FullRT) GetValue(ctx context.Context, key string, opts ...routing.Option) (result []byte, err error) {
	ctx, end := tracer.GetValue(dhtName, ctx, key, opts...)
	defer func() { end(result, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, kaddht.Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *FullRT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, end := tracer.SearchValue(dhtName, ctx, key, opts...)
	defer func() { ch, err = end(ch, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key)

	out := make(chan []byte)
	go func() {
		defer close(out)

		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		dht.updatePeerValues(ctx, key, best, updatePeers)
		cancel()
	}()

	return out, nil
}

func (dht *FullRT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int,
) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

func (dht *FullRT) processValues(ctx context.Context, key string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, v RecvdVal, better bool) bool,
) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("failed to select best value", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *FullRT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			// TODO: Is this possible?
			if p == dht.h.ID() {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					logger.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

type lookupWithFollowupResult struct {
	peers []peer.ID // the top K not unreachable peers at the end of the query
}

func (dht *FullRT) getValues(ctx context.Context, key string) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("finding value", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Val:  rec.GetValue(),
			From: dht.h.ID(),
		}:
		case <-ctx.Done():
		}
	}
	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		lookupResCh <- &lookupWithFollowupResult{}
		close(valCh)
		close(lookupResCh)
		return valCh, lookupResCh
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		queryFn := func(ctx context.Context, p peer.ID) error {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
			if err != nil {
				return err
			}

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			if rec == nil {
				return nil
			}

			val := rec.GetValue()
			if val == nil {
				logger.Debug("received a nil record value")
				return nil
			}
			if err := dht.Validator.Validate(key, val); err != nil {
				// make sure record is valid
				logger.Debugw("received invalid record (discarded)", "error", err)
				return nil
			}

			// the record is present and valid, send it out for processing
			select {
			case valCh <- RecvdVal{
				Val:  val,
				From: p,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}

		dht.execOnMany(ctx, queryFn, peers, false)
		lookupResCh <- &lookupWithFollowupResult{peers: peers}
	}()
	return valCh, lookupResCh
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *FullRT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	ctx, end := tracer.Provide(dhtName, ctx, key, brdcst)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported
	} else if !key.Defined() {
		return errors.New("invalid cid: undefined")
	}
	keyMH := key.Hash()
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// add self locally
	dht.ProviderManager.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.h.ID()})
	if !brdcst {
		return nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeers(closerCtx, string(keyMH))
	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		err := dht.protoMessenger.PutProviderAddrs(ctx, p, keyMH, peer.AddrInfo{
			ID:    dht.self,
			Addrs: dht.h.Addrs(),
		})
		return err
	}, peers, true)

	if exceededDeadline {
		return context.DeadlineExceeded
	}

	if successes == 0 {
		return errors.New("failed to complete provide")
	}

	return ctx.Err()
}

// execOnMany executes the given function on each of the peers, although it may only wait for a certain chunk of peers
// to respond before considering the results "good enough" and returning.
//
// If sloppyExit is true then this function will return without waiting for all of its internal goroutines to close.
// If sloppyExit is true then the passed in function MUST be able to safely complete an arbitrary amount of time after
// execOnMany has returned (e.g. do not write to resources that might get closed or set to nil and therefore result in
// a panic instead of just returning an error).
func (dht *FullRT) execOnMany(ctx context.Context, fn func(context.Context, peer.ID) error, peers []peer.ID, sloppyExit bool) int {
	if len(peers) == 0 {
		return 0
	}

	// having a buffer that can take all of the elements is basically a hack to allow for sloppy exits that clean up
	// the goroutines after the function is done rather than before
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * dht.waitFrac)

	putctx, cancel := context.WithTimeout(ctx, dht.timeoutPerOp)
	defer cancel()

	for _, p := range peers {
		go func(p peer.ID) {
			errCh <- fn(putctx, p)
		}(p)
	}

	var numDone, numSuccess, successSinceLastTick int
	var ticker *time.Ticker
	var tickChan <-chan time.Time

	for numDone < len(peers) {
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				if numSuccess >= numSuccessfulToWaitFor && ticker == nil {
					// Once there are enough successes, wait a little longer
					ticker = time.NewTicker(time.Millisecond * 500)
					defer ticker.Stop()
					tickChan = ticker.C
					successSinceLastTick = numSuccess
				}
				// This is equivalent to numSuccess * 2 + numFailures >= len(peers) and is a heuristic that seems to be
				// performing reasonably.
				// TODO: Make this metric more configurable
				// TODO: Have better heuristics in this function whether determined from observing static network
				// properties or dynamically calculating them
				if numSuccess+numDone >= len(peers) {
					cancel()
					if sloppyExit {
						return numSuccess
					}
				}
			}
		case <-tickChan:
			if numSuccess > successSinceLastTick {
				// If there were additional successes, then wait another tick
				successSinceLastTick = numSuccess
			} else {
				cancel()
				if sloppyExit {
					return numSuccess
				}
			}
		}
	}
	return numSuccess
}

func (dht *FullRT) ProvideMany(ctx context.Context, keys []multihash.Multihash) (err error) {
	ctx, end := tracer.ProvideMany(dhtName, ctx, keys)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported
	}

	// Compute addresses once for all provides
	pi := peer.AddrInfo{
		ID:    dht.h.ID(),
		Addrs: dht.h.Addrs(),
	}
	pbPeers := dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{pi})

	// TODO: We may want to limit the type of addresses in our provider records
	// For example, in a WAN-only DHT prohibit sharing non-WAN addresses (e.g. 192.168.0.100)
	if len(pi.Addrs) < 1 {
		return errors.New("no known addresses for self, cannot put provider")
	}

	fn := func(ctx context.Context, p, k peer.ID) error {
		pmes := dht_pb.NewMessage(dht_pb.Message_ADD_PROVIDER, multihash.Multihash(k), 0)
		pmes.ProviderPeers = pbPeers

		return dht.messageSender.SendMessage(ctx, p, pmes)
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	for _, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn)
}

func (dht *FullRT) PutMany(ctx context.Context, keys []string, values [][]byte) error {
	ctx, span := internal.StartSpan(ctx, "FullRT.PutMany", trace.WithAttributes(attribute.Int("NumKeys", len(keys))))
	defer span.End()

	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	if len(keys) != len(values) {
		return errors.New("number of keys does not match the number of values")
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	keyRecMap := make(map[string][]byte)
	for i, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
		keyRecMap[k] = values[i]
	}

	if len(keys) != len(keyRecMap) {
		return errors.New("does not support duplicate keys")
	}

	fn := func(ctx context.Context, p, k peer.ID) error {
		keyStr := string(k)
		return dht.protoMessenger.PutValue(ctx, p, record.MakePutRecord(keyStr, keyRecMap[keyStr]))
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn)
}

func (dht *FullRT) bulkMessageSend(ctx context.Context, keys []peer.ID, fn func(ctx context.Context, target, k peer.ID) error) error {
	ctx, span := internal.StartSpan(ctx, "FullRT.BulkMessageSend")
	defer span.End()

	if len(keys) == 0 {
		return nil
	}

	type report struct {
		successes   int
		failures    int
		lastSuccess time.Time
		mx          sync.RWMutex
	}

	keySuccesses := make(map[peer.ID]*report, len(keys))
	var numSkipped int64

	for _, k := range keys {
		keySuccesses[k] = &report{}
	}

	logger.Infof("bulk send: number of keys %d, unique %d", len(keys), len(keySuccesses))
	numSuccessfulToWaitFor := int(float64(dht.bucketSize) * dht.waitFrac * 1.2)

	sortedKeys := make([]peer.ID, 0, len(keySuccesses))
	for k := range keySuccesses {
		sortedKeys = append(sortedKeys, k)
	}

	sortedKeys = kb.SortClosestPeers(sortedKeys, kb.ID(make([]byte, 32)))

	dht.kMapLk.RLock()
	numPeers := len(dht.keyToPeerMap)
	dht.kMapLk.RUnlock()

	chunkSize := (len(sortedKeys) * dht.bucketSize * 2) / numPeers
	if chunkSize == 0 {
		chunkSize = 1
	}

	connmgrTag := fmt.Sprintf("dht-bulk-provide-tag-%d", rand.Int())

	type workMessage struct {
		p    peer.ID
		keys []peer.ID
	}

	workCh := make(chan workMessage, 1)
	wg := sync.WaitGroup{}
	wg.Add(dht.bulkSendParallelism)
	for range dht.bulkSendParallelism {
		go func() {
			defer wg.Done()
			defer logger.Debugf("bulk send goroutine done")
			for wmsg := range workCh {
				p, workKeys := wmsg.p, wmsg.keys
				dht.peerAddrsLk.RLock()
				peerAddrs := dht.peerAddrs[p]
				dht.peerAddrsLk.RUnlock()
				dialCtx, dialCancel := context.WithTimeout(ctx, dht.timeoutPerOp)
				if err := dht.h.Connect(dialCtx, peer.AddrInfo{ID: p, Addrs: peerAddrs}); err != nil {
					dialCancel()
					atomic.AddInt64(&numSkipped, 1)
					continue
				}
				dialCancel()
				dht.h.ConnManager().Protect(p, connmgrTag)
				for _, k := range workKeys {
					keyReport := keySuccesses[k]

					queryTimeout := dht.timeoutPerOp
					keyReport.mx.RLock()
					if keyReport.successes >= numSuccessfulToWaitFor {
						if time.Since(keyReport.lastSuccess) > time.Millisecond*500 {
							keyReport.mx.RUnlock()
							continue
						}
						queryTimeout = time.Millisecond * 500
					}
					keyReport.mx.RUnlock()

					fnCtx, fnCancel := context.WithTimeout(ctx, queryTimeout)
					if err := fn(fnCtx, p, k); err == nil {
						keyReport.mx.Lock()
						keyReport.successes++
						if keyReport.successes >= numSuccessfulToWaitFor {
							keyReport.lastSuccess = time.Now()
						}
						keyReport.mx.Unlock()
					} else {
						keyReport.mx.Lock()
						keyReport.failures++
						keyReport.mx.Unlock()
						if ctx.Err() != nil {
							fnCancel()
							break
						}
					}
					fnCancel()
				}

				dht.h.ConnManager().Unprotect(p, connmgrTag)
			}
		}()
	}

	keyGroups := divideByChunkSize(sortedKeys, chunkSize)
	sendsSoFar := 0
	for _, g := range keyGroups {
		if ctx.Err() != nil {
			break
		}

		keysPerPeer := make(map[peer.ID][]peer.ID)
		for _, k := range g {
			peers, err := dht.GetClosestPeers(ctx, string(k))
			if err == nil {
				for _, p := range peers {
					keysPerPeer[p] = append(keysPerPeer[p], k)
				}
			}
		}

		logger.Debugf("bulk send: %d peers for group size %d", len(keysPerPeer), len(g))

	keyloop:
		for p, workKeys := range keysPerPeer {
			select {
			case workCh <- workMessage{p: p, keys: workKeys}:
			case <-ctx.Done():
				break keyloop
			}
		}
		sendsSoFar += len(g)
		logger.Infof("bulk sending: %.1f%% done - %d/%d done", 100*float64(sendsSoFar)/float64(len(keySuccesses)), sendsSoFar, len(keySuccesses))
	}

	close(workCh)

	logger.Debugf("bulk send complete, waiting on goroutines to close")

	wg.Wait()

	numSendsSuccessful := 0
	numFails := 0
	// generate a histogram of how many successful sends occurred per key
	successHist := make(map[int]int)
	// generate a histogram of how many failed sends occurred per key
	// this does not include sends to peers that were skipped and had no messages sent to them at all
	failHist := make(map[int]int)
	for _, v := range keySuccesses {
		if v.successes > 0 {
			numSendsSuccessful++
		}
		successHist[v.successes]++
		failHist[v.failures]++
		numFails += v.failures
	}

	if numSendsSuccessful == 0 {
		logger.Infof("bulk send failed")
		return errors.New("failed to complete bulk sending")
	}

	logger.Infof("bulk send complete: %d keys, %d unique, %d successful, %d skipped peers, %d fails",
		len(keys), len(keySuccesses), numSendsSuccessful, numSkipped, numFails)

	logger.Infof("bulk send summary: successHist %v, failHist %v", successHist, failHist)

	return nil
}

// divideByChunkSize divides the set of keys into groups of (at most) chunkSize. Chunk size must be greater than 0.
func divideByChunkSize(keys []peer.ID, chunkSize int) [][]peer.ID {
	if len(keys) == 0 {
		return nil
	}

	if chunkSize < 1 {
		panic(fmt.Sprintf("fullrt: divide into groups: invalid chunk size %d", chunkSize))
	}

	var keyChunks [][]peer.ID
	var nextChunk []peer.ID
	chunkProgress := 0
	for _, k := range keys {
		nextChunk = append(nextChunk, k)
		chunkProgress++
		if chunkProgress == chunkSize {
			keyChunks = append(keyChunks, nextChunk)
			chunkProgress = 0
			nextChunk = make([]peer.ID, 0, len(nextChunk))
		}
	}
	if chunkProgress != 0 {
		keyChunks = append(keyChunks, nextChunk)
	}
	return keyChunks
}

// FindProviders searches until the context expires.
func (dht *FullRT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, errors.New("invalid cid: undefined")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes. If count is zero then the query will run until it
// completes. Note: not reading from the returned channel may block the query
// from progressing.
func (dht *FullRT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	ctx, end := tracer.FindProvidersAsync(dhtName, ctx, key, count)
	defer func() { ch = end(ch, nil) }()

	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	peerOut := make(chan peer.AddrInfo)

	keyMH := key.Hash()

	logger.Debugw("finding providers", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

func (dht *FullRT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	// use a span here because unlike tracer.FindProvidersAsync we know who told us about it and that intresting to log.
	ctx, span := internal.StartSpan(ctx, "FullRT.FindProvidersAsyncRoutine")
	defer span.End()

	defer close(peerOut)

	findAll := count == 0
	ps := make(map[peer.ID]struct{})
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.ID) bool {
		psLock.Lock()
		defer psLock.Unlock()
		_, ok := ps[p]
		if !ok && (len(ps) < count || findAll) {
			ps[p] = struct{}{}
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.ProviderManager.GetProviders(ctx, key)
	if err != nil {
		return
	}
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if psTryAdd(p.ID) {
			select {
			case peerOut <- p:
				span.AddEvent("found provider", trace.WithAttributes(
					attribute.Stringer("peer", p.ID),
					attribute.Stringer("from", dht.self),
				))
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && psSize() >= count {
			return
		}
	}

	peers, err := dht.GetClosestPeers(ctx, string(key))
	if err != nil {
		return
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	fn := func(ctx context.Context, p peer.ID) error {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
		if err != nil {
			return err
		}

		logger.Debugf("%d provider entries", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
			logger.Debugf("got provider: %s", prov)
			if psTryAdd(prov.ID) {
				logger.Debugf("using provider: %s", prov)
				select {
				case peerOut <- *prov:
					span.AddEvent("found provider", trace.WithAttributes(
						attribute.Stringer("peer", prov.ID),
						attribute.Stringer("from", p),
					))
				case <-ctx.Done():
					logger.Debug("context timed out sending more providers")
					return ctx.Err()
				}
			}
			if !findAll && psSize() >= count {
				logger.Debugf("got enough providers (%d/%d)", psSize(), count)
				cancelquery()
				return nil
			}
		}

		// Give closer peers back to the query to be queried
		logger.Debugf("got closer peers: %d %s", len(closest), closest)

		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: closest,
		})
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)
}

// FindPeer searches for a peer with given ID.
func (dht *FullRT) FindPeer(ctx context.Context, id peer.ID) (pi peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(dhtName, ctx, id)
	defer func() { end(pi, err) }()

	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("finding peer", "peer", id)

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers, err := dht.GetClosestPeers(ctx, string(id))
	if err != nil {
		return peer.AddrInfo{}, err
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	addrsCh := make(chan *peer.AddrInfo, 1)
	newAddrs := make([]ma.Multiaddr, 0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		addrsSoFar := make(map[string]struct{})
		for {
			select {
			case ai, ok := <-addrsCh:
				if !ok {
					return
				}

				for _, a := range ai.Addrs {
					_, found := addrsSoFar[string(a.Bytes())]
					if !found {
						newAddrs = append(newAddrs, a)
						addrsSoFar[string(a.Bytes())] = struct{}{}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	fn := func(ctx context.Context, p peer.ID) error {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			return err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		for _, a := range peers {
			if a.ID == id {
				select {
				case addrsCh <- a:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
		}
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)

	close(addrsCh)
	wg.Wait()

	if len(newAddrs) > 0 {
		connctx, cancelconn := context.WithTimeout(ctx, time.Second*5)
		defer cancelconn()
		_ = dht.h.Connect(connctx, peer.AddrInfo{
			ID:    id,
			Addrs: newAddrs,
		})
	}

	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected
	// to the peer.
	if hasValidConnectedness(dht.h, id) {
		return dht.h.Peerstore().PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

var _ routing.Routing = (*FullRT)(nil)

// getLocal attempts to retrieve the value from the datastore.
//
// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *FullRT) getLocal(ctx context.Context, key string) (*recpb.Record, error) {
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
func (dht *FullRT) putLocal(ctx context.Context, key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("failed to put marshal record for local put", "error", err, "key", internal.LoggableRecordKeyString(key))
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *FullRT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorw("error retrieving record from datastore", "key", dskey, "error", err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		logger.Errorw("failed to unmarshal record from datastore", "key", dskey, "error", err)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		logger.Debugw("local record verify failed", "key", rec.GetKey(), "error", err)
		return nil, nil
	}

	return rec, nil
}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
func (dht *FullRT) FindLocal(id peer.ID) peer.AddrInfo {
	if hasValidConnectedness(dht.h, id) {
		return dht.h.Peerstore().PeerInfo(id)
	}
	return peer.AddrInfo{}
}

func (dht *FullRT) maybeAddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// Don't add addresses for self or our connected peers. We have better ones.
	if p == dht.h.ID() || hasValidConnectedness(dht.h, p) {
		return
	}
	dht.h.Peerstore().AddAddrs(p, addrs, ttl)
}

func hasValidConnectedness(host host.Host, id peer.ID) bool {
	connectedness := host.Network().Connectedness(id)
	return connectedness == network.Connected || connectedness == network.Limited
}
