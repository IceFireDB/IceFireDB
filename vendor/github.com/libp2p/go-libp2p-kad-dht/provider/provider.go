package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pool "github.com/guillaumemichel/reservedpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/connectivity"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/queue"
	"github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

const (
	// maxPrefixSize is the maximum size of a prefix used to define a keyspace
	// region.
	maxPrefixSize = 24

	// approxPrefixLenGCPCount is the number of GetClosestPeers calls run by the
	// approxPrefixLen function. This function makes GetClosestPeers requests to
	// get an estimate of the network size, to set the initial keyspace region
	// prefix length. A high number increases the precision of the measurement,
	// but adds network load and latency before the initial provide request can
	// be performed.
	approxPrefixLenGCPCount = 4
	// defaultPrefixLenValidity is the validity of the cached average region
	// prefix length computed from the schedule. It allows to avoid recomputing
	// the average everytime we need to average prefix length.
	defaultPrefixLenValidity = 5 * time.Minute

	// retryInterval is the interval at which the provider tries to perform any
	// previously failed work (provide or reprovide).
	retryInterval = 5 * time.Minute

	// individualProvideThreshold is the threshold for the number of keys to
	// trigger a region exploration. If the number of keys to provide for a
	// region is less or equal to the threshold, the keys will be individually
	// provided.
	individualProvideThreshold = 2

	// maxExplorationPrefixSearches is the maximum number of GetClosestPeers
	// operations that are allowed to explore a prefix, preventing an infinite
	// loop, since the exit condition depends on the network topology.
	// A lower bound estimate on the number of fresh peers returned by GCP is
	// replicationFactor/2. Hence, 64 GCP are expected to return at least
	// 32*replicatonFactor peers, which should be more than enough, even if the
	// supplied prefix is too short.
	maxExplorationPrefixSearches = 64

	// maxConsecutiveProvideFailuresAllowed is the maximum number of consecutive
	// provides that are allowed to fail to the same remote peer before cancelling
	// all pending requests to this peer.
	maxConsecutiveProvideFailuresAllowed = 2
	// minimalRegionReachablePeersRatio is the minimum ratio of reachable peers
	// in a region for the provide to be considered a success.
	minimalRegionReachablePeersRatio float32 = 0.2
)

var (
	// ErrClosed is returned when the provider is closed.
	ErrClosed = errors.New("provider: closed")
	// ErrOffline is returned when the provider is offline, and cannot process
	// the request. When a node is offline, operations on the keystore are
	// performed as usual, but keys aren't added to provide queue nor advertised
	// to the network.
	ErrOffline = errors.New("provider: offline")
)

const LoggerName = "dht/provider"

var logger = logging.Logger(LoggerName)

type KadClosestPeersRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
}

type workerType uint8

const (
	periodicWorker workerType = iota
	burstWorker
)

var _ internal.Provider = (*SweepingProvider)(nil)

type SweepingProvider struct {
	done         chan struct{}
	ctx          context.Context
	cancelCtx    context.CancelFunc
	closeOnce    sync.Once
	wg           sync.WaitGroup
	cleanupFuncs []func() error

	peerid peer.ID
	order  bit256.Key
	router KadClosestPeersRouter

	connectivity *connectivity.ConnectivityChecker

	keystore keystore.Keystore

	replicationFactor int

	provideQueue         *queue.ProvideQueue
	provideRunning       sync.Mutex
	reprovideQueue       *queue.ReprovideQueue
	lateReprovideRunning sync.Mutex

	workerPool               *pool.Pool[workerType]
	maxProvideConnsPerWorker int

	cycleStart        time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	schedule               *trie.Trie[bitstr.Key, time.Duration]
	scheduleLk             sync.Mutex
	scheduleCursor         bitstr.Key
	scheduleTimer          *time.Timer
	scheduleTimerStartedAt time.Time

	ongoingReprovides   *trie.Trie[bitstr.Key, struct{}]
	ongoingReprovidesLk sync.Mutex

	cachedAvgPrefixLen     int
	avgPrefixLenLk         sync.Mutex
	approxPrefixLenRunning sync.Mutex
	lastAvgPrefixLen       time.Time
	avgPrefixLenValidity   time.Duration

	msgSender      pb.MessageSender
	getSelfAddrs   func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error

	provideCounter metric.Int64Counter
}

// New creates a new SweepingProvider instance with the supplied options.
func New(opts ...Option) (*SweepingProvider, error) {
	cfg, err := getOpts(opts)
	if err != nil {
		return nil, err
	}
	cleanupFuncs := []func() error{}

	if cfg.keystore == nil {
		// Setup KeyStore if missing
		mapDs := ds.NewMapDatastore()
		cleanupFuncs = append(cleanupFuncs, mapDs.Close)
		cfg.keystore, err = keystore.NewKeystore(mapDs)
		if err != nil {
			cleanup(cleanupFuncs)
			return nil, err
		}
		cleanupFuncs = append(cleanupFuncs, cfg.keystore.Close)
	}
	meter := otel.Meter("github.com/libp2p/go-libp2p-kad-dht/provider")
	providerCounter, err := meter.Int64Counter(
		"total_provide_count",
		metric.WithDescription("Number of successful provides since node is running"),
	)
	if err != nil {
		cleanup(cleanupFuncs)
		return nil, err
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	connChecker, err := connectivity.New(
		func() bool {
			peers, err := cfg.router.GetClosestPeers(ctx, string(cfg.peerid))
			return err == nil && len(peers) > 0
		},
		connectivity.WithOfflineDelay(cfg.offlineDelay),
		connectivity.WithOnlineCheckInterval(cfg.connectivityCheckOnlineInterval),
	)
	if err != nil {
		cancelCtx()
		cleanup(cleanupFuncs)
		return nil, err
	}
	cleanupFuncs = append(cleanupFuncs, connChecker.Close)

	prov := &SweepingProvider{
		done:         make(chan struct{}),
		ctx:          ctx,
		cancelCtx:    cancelCtx,
		cleanupFuncs: cleanupFuncs,

		router: cfg.router,
		peerid: cfg.peerid,
		order:  keyspace.PeerIDToBit256(cfg.peerid),

		connectivity: connChecker,

		replicationFactor: cfg.replicationFactor,
		reprovideInterval: cfg.reprovideInterval,
		maxReprovideDelay: cfg.maxReprovideDelay,

		workerPool: pool.New(cfg.maxWorkers, map[workerType]int{
			periodicWorker: cfg.dedicatedPeriodicWorkers,
			burstWorker:    cfg.dedicatedBurstWorkers,
		}),
		maxProvideConnsPerWorker: cfg.maxProvideConnsPerWorker,

		avgPrefixLenValidity: defaultPrefixLenValidity,
		cachedAvgPrefixLen:   -1,

		cycleStart: time.Now(),

		msgSender:      cfg.msgSender,
		getSelfAddrs:   cfg.selfAddrs,
		addLocalRecord: cfg.addLocalRecord,

		keystore: cfg.keystore,

		schedule:      trie.New[bitstr.Key, time.Duration](),
		scheduleTimer: time.NewTimer(time.Hour),

		provideQueue:   queue.NewProvideQueue(),
		reprovideQueue: queue.NewReprovideQueue(),

		ongoingReprovides: trie.New[bitstr.Key, struct{}](),

		provideCounter: providerCounter,
	}
	// Set up callbacks after both provider and connectivity checker are initialized
	// This breaks the circular dependency between connectivity, onOnline, and approxPrefixLen
	prov.connectivity.SetCallbacks(prov.onOnline, prov.onOffline)
	prov.connectivity.Start()

	// Don't need to start schedule timer yet
	prov.scheduleTimer.Stop()

	prov.cleanupFuncs = append(prov.cleanupFuncs, prov.workerPool.Close)

	prov.wg.Add(1)
	go prov.run()

	return prov, nil
}

func (s *SweepingProvider) run() {
	defer s.wg.Done()

	logger.Debug("Starting SweepingProvider")
	retryTicker := time.NewTicker(retryInterval)
	defer retryTicker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-retryTicker.C:
			if s.connectivity.IsOnline() {
				s.catchupPendingWork()
			}
		case <-s.scheduleTimer.C:
			s.handleReprovide()
		}
	}
}

// Close stops the provider and releases all resources.
func (s *SweepingProvider) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		s.cancelCtx()
		s.wg.Wait()
		s.approxPrefixLenRunning.Lock()
		_ = struct{}{} // cannot have empty critical section
		s.approxPrefixLenRunning.Unlock()

		s.scheduleTimer.Stop()
		err = cleanup(s.cleanupFuncs)
	})
	return err
}

func cleanup(funcs []func() error) error {
	var errs []error
	for i := len(funcs) - 1; i >= 0; i-- { // LIFO: last-added is cleaned up first
		if f := funcs[i]; f != nil {
			if err := f(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (s *SweepingProvider) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// scheduleNextReprovideNoLock makes sure the scheduler wakes up in
// `timeUntilReprovide` to reprovide the region identified by `prefix`.
func (s *SweepingProvider) scheduleNextReprovideNoLock(prefix bitstr.Key, timeUntilReprovide time.Duration) {
	s.scheduleCursor = prefix
	s.scheduleTimer.Reset(timeUntilReprovide)
	s.scheduleTimerStartedAt = time.Now()
}

func (s *SweepingProvider) reschedulePrefix(prefix bitstr.Key) {
	s.scheduleLk.Lock()
	s.schedulePrefixNoLock(prefix, true)
	s.scheduleLk.Unlock()
}

// schedulePrefixNoLock adds the supplied prefix to the schedule, unless
// already present.
//
// If `justReprovided` is true, it will schedule the next reprovide at most
// s.reprovideInterval+s.maxReprovideDelay in the future, allowing the
// reprovide to be delayed of at most maxReprovideDelay.
//
// If the supplied prefix is the next prefix to be reprovided, update the
// schedule cursor and timer.
func (s *SweepingProvider) schedulePrefixNoLock(prefix bitstr.Key, justReprovided bool) {
	nextReprovideTime := s.reprovideTimeForPrefix(prefix)
	if justReprovided {
		// Schedule next reprovide given that the prefix was just reprovided on
		// schedule. In the case the next reprovide time should be delayed due to a
		// growth in the number of network peers matching the prefix, don't delay
		// more than s.maxReprovideDelay.
		nextReprovideTime = min(nextReprovideTime, s.currentTimeOffset()+s.reprovideInterval+s.maxReprovideDelay)
	}
	// If schedule contains keys starting with prefix, remove them to avoid
	// overlap.
	if _, ok := keyspace.FindPrefixOfKey(s.schedule, prefix); ok {
		// Already scheduled.
		return
	}
	// Unschedule superstrings in schedule if any.
	s.unscheduleSubsumedPrefixesNoLock(prefix)

	s.schedule.Add(prefix, nextReprovideTime)

	// Check if the prefix that was just added is the next one to be reprovided.
	if s.schedule.IsNonEmptyLeaf() {
		// The prefix we insterted is the only element in the schedule.
		timeUntilPrefixReprovide := s.timeUntil(nextReprovideTime)
		s.scheduleNextReprovideNoLock(prefix, timeUntilPrefixReprovide)
		return
	}
	followingKey := keyspace.NextNonEmptyLeaf(s.schedule, prefix, s.order).Key
	if followingKey == s.scheduleCursor {
		// The key following prefix is the schedule cursor.
		timeUntilPrefixReprovide := s.timeUntil(nextReprovideTime)
		_, scheduledAlarm := trie.Find(s.schedule, s.scheduleCursor)
		if timeUntilPrefixReprovide < s.timeUntil(scheduledAlarm) {
			s.scheduleNextReprovideNoLock(prefix, timeUntilPrefixReprovide)
		}
	}
}

// unscheduleSubsumedPrefixes removes all superstrings of `prefix` that are
// scheduled in the future. Assumes that the schedule lock is held.
func (s *SweepingProvider) unscheduleSubsumedPrefixesNoLock(prefix bitstr.Key) {
	// Pop prefixes scheduled in the future being covered by the explored peers.
	keyspace.PruneSubtrie(s.schedule, prefix)

	// If we removed s.scheduleCursor from schedule, select the next one
	if keyspace.IsBitstrPrefix(prefix, s.scheduleCursor) {
		next := keyspace.NextNonEmptyLeaf(s.schedule, s.scheduleCursor, s.order)
		if next == nil {
			s.scheduleNextReprovideNoLock(prefix, s.reprovideInterval)
		} else {
			timeUntilReprovide := s.timeUntil(next.Data)
			s.scheduleNextReprovideNoLock(next.Key, timeUntilReprovide)
			logger.Debugf("next scheduled prefix now is %s", s.scheduleCursor)
		}
	}
}

// currentTimeOffset returns the current time offset in the reprovide cycle.
func (s *SweepingProvider) currentTimeOffset() time.Duration {
	return s.timeOffset(time.Now())
}

// timeOffset returns the time offset in the reprovide cycle for the given
// time.
func (s *SweepingProvider) timeOffset(t time.Time) time.Duration {
	return t.Sub(s.cycleStart) % s.reprovideInterval
}

// timeUntil returns the time left (duration) until the given time offset.
func (s *SweepingProvider) timeUntil(d time.Duration) time.Duration {
	return s.timeBetween(s.currentTimeOffset(), d)
}

// timeBetween returns the duration between the two provided offsets, assuming
// it is no more than s.reprovideInterval.
func (s *SweepingProvider) timeBetween(from, to time.Duration) time.Duration {
	return (to-from+s.reprovideInterval-1)%s.reprovideInterval + 1
}

// reprovideTimeForPrefix calculates the scheduled time offset for reproviding
// keys associated with a given prefix based on its bitstring prefix. The
// function maps the given binary prefix to a fraction of the overall reprovide
// interval (s.reprovideInterval), such that keys with prefixes closer to a
// configured target s.order (in XOR distance) are scheduled earlier and those
// further away later in the cycle.
//
// For any prefix of bit length n, the function generates 2^n distinct
// reprovide times that evenly partition the entire reprovide interval. The
// process first truncates s.order to n bits and then XORs it with the provided
// prefix. The resulting binary string is converted to an integer,
// corresponding to the index of the 2^n possible reprovide times to use for
// the prefix.
//
// This method ensures a deterministic and evenly distributed reprovide
// schedule, where the temporal position within the cycle is based on the
// binary representation of the key's prefix.
func (s *SweepingProvider) reprovideTimeForPrefix(prefix bitstr.Key) time.Duration {
	if len(prefix) == 0 {
		// Empty prefix: all reprovides occur at the beginning of the cycle.
		return 0
	}
	if len(prefix) > maxPrefixSize {
		// Truncate the prefix to the maximum allowed size to avoid overly fine
		// slicing of time.
		prefix = prefix[:maxPrefixSize]
	}
	// Number of possible bitstrings of the same length as prefix.
	maxInt := int64(1 << len(prefix))
	// XOR the prefix with the order key to reorder the schedule: keys "close" to
	// s.order are scheduled first in the cycle, and those "far" from it are
	// scheduled later.
	order := bitstr.Key(key.BitString(s.order)[:len(prefix)])
	k := prefix.Xor(order)
	val, _ := strconv.ParseInt(string(k), 2, 64)
	// Calculate the time offset as a fraction of the overall reprovide interval.
	return time.Duration(int64(s.reprovideInterval) * val / maxInt)
}

// approxPrefixLen makes a few GetClosestPeers calls to get an estimate
// of the prefix length to be used in the network.
//
// This function blocks until GetClosestPeers succeeds or the provider is
// closed. No provide operation can happen until this function returns.
func (s *SweepingProvider) approxPrefixLen() {
	cplSum := atomic.Int32{}
	cplSamples := atomic.Int32{}
	wg := sync.WaitGroup{}
	wg.Add(approxPrefixLenGCPCount)
	for range approxPrefixLenGCPCount {
		go func() {
			defer wg.Done()
			randomMh := random.Multihashes(1)[0]
			for {
				if s.closed() || !s.connectivity.IsOnline() {
					return
				}
				peers, err := s.router.GetClosestPeers(s.ctx, string(randomMh))
				if err != nil {
					logger.Infof("GetClosestPeers failed during prefix len approximation measurement: %s", err)
				} else {
					if len(peers) < 2 {
						return // Ignore result if less than 2 other peers in DHT.
					}
					cpl := keyspace.KeyLen
					firstPeerKey := keyspace.PeerIDToBit256(peers[0])
					for _, p := range peers[1:] {
						cpl = min(cpl, key.CommonPrefixLength(firstPeerKey, keyspace.PeerIDToBit256(p)))
					}
					cplSum.Add(int32(cpl))
					cplSamples.Add(1)
					return
				}

				s.connectivity.TriggerCheck()
				time.Sleep(time.Second) // retry every second until success
			}
		}()
	}
	wg.Wait()

	nSamples := cplSamples.Load()
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()
	if nSamples == 0 {
		// At most 2 other peers in the DHT -> single region of prefix len 0
		s.cachedAvgPrefixLen = 0
	} else {
		s.cachedAvgPrefixLen = int(cplSum.Load() / nSamples)
	}
	logger.Debugf("prefix len approximation is %d", s.cachedAvgPrefixLen)
	s.lastAvgPrefixLen = time.Now()
}

// getAvgPrefixLenNoLock returns the average prefix length of all scheduled
// prefixes.
//
// Hangs until the first measurement is done if the average prefix length is
// missing.
func (s *SweepingProvider) getAvgPrefixLenNoLock() (int, error) {
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()

	if s.cachedAvgPrefixLen == -1 {
		return -1, ErrOffline
	}

	if s.lastAvgPrefixLen.Add(s.avgPrefixLenValidity).After(time.Now()) {
		// Return cached value if it is still valid.
		return s.cachedAvgPrefixLen, nil
	}
	prefixLenSum := 0
	if !s.schedule.IsEmptyLeaf() {
		// Take average prefix length of all scheduled prefixes.
		scheduleEntries := keyspace.AllEntries(s.schedule, s.order)
		for _, entry := range scheduleEntries {
			prefixLenSum += len(entry.Key)
		}
		s.cachedAvgPrefixLen = prefixLenSum / len(scheduleEntries)
		s.lastAvgPrefixLen = time.Now()
	}
	return s.cachedAvgPrefixLen, nil
}

// vanillaProvide provides a single key to the network without any
// optimization. It should be used for providing a small number of keys
// (typically 1 or 2), because exploring the keyspace would add too much
// overhead for a small number of keys.
func (s *SweepingProvider) vanillaProvide(k mh.Multihash) (bitstr.Key, error) {
	keys := []mh.Multihash{k}
	// Add provider record to local provider store.
	s.addLocalRecord(k)
	// Get peers to which the record will be allocated.
	peers, err := s.router.GetClosestPeers(s.ctx, string(k))
	if err != nil {
		return "", err
	}
	coveredPrefix, _ := keyspace.ShortestCoveredPrefix(bitstr.Key(key.BitString(keyspace.MhToBit256(k))), peers)
	addrInfo := peer.AddrInfo{ID: s.peerid, Addrs: s.getSelfAddrs()}
	keysAllocations := make(map[peer.ID][]mh.Multihash)
	for _, p := range peers {
		keysAllocations[p] = keys
	}
	err = s.sendProviderRecords(keysAllocations, addrInfo)
	if err == nil {
		logger.Debugw("sent provider record", "prefix", coveredPrefix, "count", 1, "keys", keys)
	}
	return coveredPrefix, err
}

// exploreSwarm finds all peers whose kademlia identifier matches `prefix` in
// the DHT swarm, and organizes them in keyspace regions.
//
// A region is identified by a keyspace prefix, and contains all the peers
// matching this prefix. A region always has at least s.replicationFactor
// peers. Regions are non-overlapping.
//
// If there less than s.replicationFactor peers match `prefix`, explore
// shorter prefixes until at least s.replicationFactor peers are included in
// the region.
//
// The returned `coveredPrefix` represents the keyspace prefix covered by all
// returned regions combined. It is different to the supplied `prefix` if there
// aren't enough peers matching `prefix`.
func (s *SweepingProvider) exploreSwarm(prefix bitstr.Key) (regions []keyspace.Region, coveredPrefix bitstr.Key, err error) {
	peers, err := s.closestPeersToPrefix(prefix)
	if err != nil {
		return nil, "", fmt.Errorf("exploreSwarm '%s': %w", prefix, err)
	}
	if len(peers) == 0 {
		return nil, "", fmt.Errorf("no peers found when exploring prefix %s", prefix)
	}
	regions, coveredPrefix = keyspace.RegionsFromPeers(peers, s.replicationFactor, s.order)
	return regions, coveredPrefix, nil
}

// closestPeersToPrefix returns at least s.replicationFactor peers
// corresponding to the branch of the network peers trie matching the provided
// prefix. In the case there aren't enough peers matching the provided prefix,
// it will find and return the closest peers to the prefix, even if they don't
// exactly match it.
func (s *SweepingProvider) closestPeersToPrefix(prefix bitstr.Key) ([]peer.ID, error) {
	allClosestPeers := make(map[peer.ID]struct{})

	nextPrefix := prefix
	startTime := time.Now()
	coverageTrie := trie.New[bitstr.Key, struct{}]()

	var gaps []bitstr.Key
	i := 0
	// Go down the trie to fully cover prefix.
	for ; i < maxExplorationPrefixSearches; i++ {
		if !s.connectivity.IsOnline() {
			return nil, ErrOffline
		}
		fullKey := keyspace.FirstFullKeyWithPrefix(nextPrefix, s.order)
		closestPeers, err := s.closestPeersToKey(fullKey)
		if err != nil {
			// We only get an err if something really bad happened, e.g no peers in
			// routing table, invalid key, etc.
			return nil, err
		}
		if len(closestPeers) == 0 {
			return nil, errors.New("dht lookup did not return any peers")
		}
		coveredPrefix, coveredPeers := keyspace.ShortestCoveredPrefix(fullKey, closestPeers)
		for _, p := range coveredPeers {
			allClosestPeers[p] = struct{}{}
		}

		if _, ok := keyspace.FindPrefixOfKey(coverageTrie, coveredPrefix); !ok {
			keyspace.PruneSubtrie(coverageTrie, coveredPrefix)
			coverageTrie.Add(coveredPrefix, struct{}{})
		}

		gaps = keyspace.TrieGaps(coverageTrie, prefix, s.order)
		if len(gaps) == 0 {
			if len(allClosestPeers) >= s.replicationFactor {
				// We have full coverage of `prefix`.
				break
			}
			for len(gaps) == 0 && len(prefix) > 0 {
				// We don't have enough peers, but we have covered the prefix. Let's
				// cover a shorter prefix.
				prefix = prefix[:len(prefix)-1]
				gaps = keyspace.TrieGaps(coverageTrie, prefix, s.order)
			}
			if len(gaps) == 0 {
				// We don't have enough peers, but we have covered the whole keyspace.
				break
			}
		}
		logger.Debugw("closestPeersToPrefix", "i", i, "prefix", prefix, "prevPrefix", nextPrefix, "fullKey[:12]", fullKey[:12], "coveredPrefix", coveredPrefix, "len(coveredPeers)", len(coveredPeers), "len(allClosestPeers)", len(allClosestPeers), "gaps", gaps)

		nextPrefix = gaps[0]
	}
	if i == maxExplorationPrefixSearches {
		logger.Warnw("closestPeersToPrefix needed more than maxPrefixSearches iterations", "gaps", gaps)
	}
	peers := make([]peer.ID, 0, len(allClosestPeers))
	for p := range allClosestPeers {
		peers = append(peers, p)
	}
	logger.Debugf("region %s exploration required %d requests to discover %d peers in %s", prefix, i+1, len(allClosestPeers), time.Since(startTime))
	return peers, nil
}

// closestPeersToKey returns a valid peer ID sharing a long common prefix with
// the provided key. Note that the returned peer IDs aren't random, they are
// taken from a static list of preimages.
func (s *SweepingProvider) closestPeersToKey(k bitstr.Key) ([]peer.ID, error) {
	p, _ := kb.GenRandPeerIDWithCPL(keyspace.KeyToBytes(k), kb.PeerIDPreimageMaxCpl)
	return s.router.GetClosestPeers(s.ctx, string(p))
}

type provideJob struct {
	pid  peer.ID
	keys []mh.Multihash
}

// sendProviderRecords manages reprovides for all given peer ids and allocated
// keys. Upon failure to reprovide a key, or to connect to a peer, it will NOT
// retry.
//
// Returns an error if we were unable to reprovide keys to a given threshold of
// peers. In this case, the region reprovide is considered failed and the
// caller is responsible for trying again. This allows detecting if we are
// offline.
func (s *SweepingProvider) sendProviderRecords(keysAllocations map[peer.ID][]mh.Multihash, addrInfo peer.AddrInfo) error {
	nPeers := len(keysAllocations)
	if nPeers == 0 {
		return nil
	}
	startTime := time.Now()
	errCount := atomic.Uint32{}
	nWorkers := s.maxProvideConnsPerWorker
	jobChan := make(chan provideJob, nWorkers)

	wg := sync.WaitGroup{}
	wg.Add(nWorkers)
	for range nWorkers {
		go func() {
			pmes := genProvideMessage(addrInfo)
			defer wg.Done()
			for job := range jobChan {
				err := s.provideKeysToPeer(job.pid, job.keys, pmes)
				if err != nil {
					errCount.Add(1)
				}
			}
		}()
	}

	for p, keys := range keysAllocations {
		jobChan <- provideJob{p, keys}
	}
	close(jobChan)
	wg.Wait()

	errCountLoaded := int(errCount.Load())
	logger.Infof("sent provider records to peers in %s, errors %d/%d", time.Since(startTime), errCountLoaded, len(keysAllocations))

	if errCountLoaded == nPeers || errCountLoaded > int(float32(nPeers)*(1-minimalRegionReachablePeersRatio)) {
		return fmt.Errorf("unable to provide to enough peers (%d/%d)", nPeers-errCountLoaded, nPeers)
	}
	return nil
}

// genProvideMessage generates a new provide message with the supplied
// AddrInfo. The message contains no keys, as they will be set later before
// sending the message.
func genProvideMessage(addrInfo peer.AddrInfo) *pb.Message {
	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, []byte{}, 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]peer.AddrInfo{addrInfo})
	return pmes
}

// provideKeysToPeer performs the network operation to advertise to the given
// DHT server (p) that we serve all the given keys.
func (s *SweepingProvider) provideKeysToPeer(p peer.ID, keys []mh.Multihash, pmes *pb.Message) error {
	errCount := 0
	for _, mh := range keys {
		pmes.Key = mh
		err := s.msgSender.SendMessage(s.ctx, p, pmes)
		if err != nil {
			errCount++

			if errCount == len(keys) || errCount > maxConsecutiveProvideFailuresAllowed {
				return fmt.Errorf("failed to provide to %s: %s", p, err.Error())
			}
		} else if errCount > 0 {
			// Reset error count
			errCount = 0
		}
	}
	return nil
}

// handleReprovide advances the reprovider schedule and (asynchronously)
// reprovides the region at the current schedule cursor.
//
// Behavior:
//   - Determines the next region to reprovide based on the current cursor and
//     the schedule, reprovides the region under the cursor, and moves the cursor
//     to the next region.
//   - Programs the schedule timer (alarm) for the next region’s reprovide
//     time. When the timer fires, this method must be invoked again.
//   - If the node has been blocked past the reprovide interval or if one or
//     more regions’ times are already in the past, those regions are added to
//     the reprovide queue for catch-up and a connectivity check is triggered.
//   - If the node is currently offline, it skips the immediate reprovide of
//     the current region and enqueues it to the reprovide queue for later.
//   - If the node is online it removes the current region from the reprovide
//     queue (if present) and starts an asynchronous batch reprovide using a
//     periodic worker.
func (s *SweepingProvider) handleReprovide() {
	s.scheduleLk.Lock()
	currentPrefix := s.scheduleCursor
	// Get next prefix to reprovide, and set timer for it.
	next := keyspace.NextNonEmptyLeaf(s.schedule, currentPrefix, s.order)

	if next == nil {
		// Schedule is empty, don't reprovide anything.
		s.scheduleLk.Unlock()
		return
	}

	var nextPrefix bitstr.Key
	var timeUntilNextReprovide time.Duration
	if next.Key == currentPrefix {
		// There is a single prefix in the schedule.
		nextPrefix = currentPrefix
		timeUntilNextReprovide = s.timeUntil(s.reprovideTimeForPrefix(currentPrefix))
	} else {
		currentTimeOffset := s.currentTimeOffset()
		timeSinceTimerRunning := s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), currentTimeOffset)
		timeSinceTimerUntilNext := s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), next.Data)

		if s.scheduleTimerStartedAt.Add(s.reprovideInterval).Before(time.Now()) {
			// Alarm was programmed more than reprovideInterval ago, which means that
			// no regions has been reprovided since. Add all regions to the reprovide
			// queue. This only happens if the main thread gets blocked for more than
			// reprovideInterval.
			nextKeyFound := false
			scheduleEntries := keyspace.AllEntries(s.schedule, s.order)
			next = scheduleEntries[0]
			for _, entry := range scheduleEntries {
				// Add all regions from the schedule to the reprovide queue. The next
				// region to be scheduled for reprovide is the one immediately
				// following the current time offset in the schedule.
				if !nextKeyFound && entry.Data > currentTimeOffset {
					next = entry
					nextKeyFound = true
				}
				s.reprovideQueue.Enqueue(entry.Key)
			}
			// Don't reprovide any region now, but schedule the next one. All regions
			// are expected to be reprovided when the provider is catching up with
			// failed regions.
			s.scheduleNextReprovideNoLock(next.Key, s.timeUntil(next.Data))
			s.scheduleLk.Unlock()
			return
		}
		if timeSinceTimerUntilNext < timeSinceTimerRunning {
			// next is scheduled in the past. While next is in the past, add next to
			// failedRegions and take nextLeaf as next.
			count := 0
			scheduleSize := s.schedule.Size()
			for timeSinceTimerUntilNext < timeSinceTimerRunning && count < scheduleSize {
				prefix := next.Key
				s.reprovideQueue.Enqueue(prefix)
				next = keyspace.NextNonEmptyLeaf(s.schedule, next.Key, s.order)
				timeSinceTimerUntilNext = s.timeBetween(s.timeOffset(s.scheduleTimerStartedAt), next.Data)
				count++
			}
		}

		// next is in the future
		nextPrefix = next.Key
		timeUntilNextReprovide = s.timeUntil(next.Data)
	}

	s.scheduleNextReprovideNoLock(nextPrefix, timeUntilNextReprovide)
	s.scheduleLk.Unlock()

	// If we are offline, don't try to reprovide region.
	if !s.connectivity.IsOnline() {
		s.reprovideQueue.Enqueue(currentPrefix)
		return
	}

	// Remove prefix that is about to be reprovided from the reprovide queue if
	// present.
	s.reprovideQueue.Remove(currentPrefix)

	s.wg.Add(1)
	go func() {
		if err := s.workerPool.Acquire(periodicWorker); err == nil {
			s.batchReprovide(currentPrefix, true)
			s.workerPool.Release(periodicWorker)
		}
		s.wg.Done()
	}()
}

// handleProvide provides supplied keys to the network if needed and schedules
// the keys to be reprovided if needed.
func (s *SweepingProvider) handleProvide(force, reprovide bool, keys ...mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	if reprovide {
		// Add keys to list of keys to be reprovided. Returned keys are deduplicated
		// newly added keys.
		newKeys, err := s.keystore.Put(s.ctx, keys...)
		if err != nil {
			return fmt.Errorf("couldn't add keys to keystore: %w", err)
		}
		if !force {
			keys = newKeys
		}
	}

	if s.isOffline() {
		return ErrOffline
	}
	prefixes, err := s.groupAndScheduleKeysByPrefix(keys, reprovide)
	if err != nil {
		return err
	}
	if len(prefixes) == 0 {
		return nil
	}
	// Sort prefixes by number of keys.
	sortedPrefixesAndKeys := keyspace.SortPrefixesBySize(prefixes)
	// Add keys to the provide queue.
	for _, prefixAndKeys := range sortedPrefixesAndKeys {
		s.provideQueue.Enqueue(prefixAndKeys.Prefix, prefixAndKeys.Keys...)
	}

	s.wg.Add(1)
	go s.provideLoop()
	return nil
}

// groupAndScheduleKeysByPrefix groups the supplied keys by their prefixes as
// present in the schedule, and if `schedule` is set to true, add these
// prefixes to the schedule to be reprovided.
func (s *SweepingProvider) groupAndScheduleKeysByPrefix(keys []mh.Multihash, schedule bool) (map[bitstr.Key][]mh.Multihash, error) {
	seen := make(map[string]struct{})
	prefixTrie := trie.New[bitstr.Key, struct{}]()
	prefixes := make(map[bitstr.Key][]mh.Multihash)
	avgPrefixLen := -1

	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		kStr := string(keyspace.KeyToBytes(k))
		// Don't add duplicates
		if _, ok := seen[kStr]; ok {
			continue
		}
		seen[kStr] = struct{}{}

		if prefix, ok := keyspace.FindPrefixOfKey(prefixTrie, k); ok {
			prefixes[prefix] = append(prefixes[prefix], h)
			continue
		}

		prefix, inSchedule := keyspace.FindPrefixOfKey(s.schedule, k)
		if !inSchedule {
			if avgPrefixLen == -1 {
				var err error
				avgPrefixLen, err = s.getAvgPrefixLenNoLock()
				if err != nil {
					return nil, err
				}
			}
			prefix = bitstr.Key(key.BitString(k)[:avgPrefixLen])
			if schedule {
				s.schedulePrefixNoLock(prefix, false)
			}
		}
		mhs := []mh.Multihash{h}
		if subtrie, ok := keyspace.FindSubtrie(prefixTrie, prefix); ok {
			// If prefixes already contains superstrings of prefix, consolidate the
			// keys to prefix.
			for _, entry := range keyspace.AllEntries(subtrie, s.order) {
				mhs = append(mhs, prefixes[entry.Key]...)
				delete(prefixes, entry.Key)
			}
			keyspace.PruneSubtrie(prefixTrie, prefix)
		}
		prefixTrie.Add(prefix, struct{}{})
		prefixes[prefix] = mhs
	}
	return prefixes, nil
}

func (s *SweepingProvider) isOffline() bool {
	s.avgPrefixLenLk.Lock()
	defer s.avgPrefixLenLk.Unlock()
	return s.cachedAvgPrefixLen == -1
}

func (s *SweepingProvider) onOffline() {
	s.provideQueue.Clear()

	s.avgPrefixLenLk.Lock()
	s.cachedAvgPrefixLen = -1 // Invalidate cached avg prefix len.
	s.avgPrefixLenLk.Unlock()
}

func (s *SweepingProvider) onOnline() {
	if s.closed() {
		return
	}

	s.avgPrefixLenLk.Lock()
	cachedAvgPrefixLen := s.cachedAvgPrefixLen
	s.avgPrefixLenLk.Unlock()

	if cachedAvgPrefixLen == -1 {
		// Provider was previously Offline (not Disconnected).
		// Run prefix length measurement, and refresh schedule afterwards.
		if !s.approxPrefixLenRunning.TryLock() {
			return
		}
		s.approxPrefixLen()
		s.approxPrefixLenRunning.Unlock()

		s.RefreshSchedule()
	}

	s.catchupPendingWork()
}

// catchupPendingWork is called when the provider comes back online after being offline.
//
// 1. Try again to reprovide regions that failed to be reprovided on time.
// 2. Try again to provide keys that failed to be provided.
//
// This function is guarded by s.lateReprovideRunning, ensuring the function
// cannot be called again while it is working on reproviding late regions.
func (s *SweepingProvider) catchupPendingWork() {
	if s.closed() || !s.lateReprovideRunning.TryLock() {
		return
	}
	s.wg.Add(2)
	go func() {
		// Reprovide late regions if any.
		s.reprovideLateRegions()
		s.lateReprovideRunning.Unlock()

		// Provides are handled after reprovides, because keys pending to be
		// provided will be provided as part of a region reprovide if they belong
		// to that region. Hence, the provideLoop will use less resources if run
		// after the reprovides.

		// Restart provide loop if it was stopped.
		s.provideLoop()
	}()
}

// provideLoop is the loop providing keys to the DHT swarm as long as the
// provide queue isn't empty.
//
// The s.provideRunning mutex prevents concurrent executions of the loop.
func (s *SweepingProvider) provideLoop() {
	defer s.wg.Done()
	if !s.provideRunning.TryLock() {
		// Ensure that only one goroutine is running the provide loop at a time.
		return
	}
	defer s.provideRunning.Unlock()

	for !s.provideQueue.IsEmpty() {
		if s.closed() {
			// Exit loop if provider is closed.
			return
		}
		if !s.connectivity.IsOnline() {
			// Don't try to provide if node is offline.
			return
		}
		// Block until we can acquire a worker from the pool.
		err := s.workerPool.Acquire(burstWorker)
		if err != nil {
			// Provider was closed while waiting for a worker.
			return
		}
		prefix, keys, ok := s.provideQueue.Dequeue()
		if ok {
			s.wg.Add(1)
			go func(prefix bitstr.Key, keys []mh.Multihash) {
				s.batchProvide(prefix, keys)
				s.workerPool.Release(burstWorker)
				s.wg.Done()
			}(prefix, keys)
		} else {
			s.workerPool.Release(burstWorker)
		}
	}
}

// reprovideLateRegions is the loop reproviding regions that failed to be
// reprovided on time. It returns once the reprovide queue is empty.
func (s *SweepingProvider) reprovideLateRegions() {
	defer s.wg.Done()
	for !s.reprovideQueue.IsEmpty() {
		if s.closed() {
			// Exit loop if provider is closed.
			return
		}
		if !s.connectivity.IsOnline() {
			// Don't try to reprovide a region if node is offline.
			return
		}
		// Block until we can acquire a worker from the pool.
		err := s.workerPool.Acquire(burstWorker)
		if err != nil {
			// Provider was closed while waiting for a worker.
			return
		}
		prefix, ok := s.reprovideQueue.Dequeue()
		if ok {
			s.wg.Add(1)
			go func(prefix bitstr.Key) {
				s.batchReprovide(prefix, false)
				s.workerPool.Release(burstWorker)
				s.wg.Done()
			}(prefix)
		} else {
			s.workerPool.Release(burstWorker)
		}
	}
}

func (s *SweepingProvider) batchProvide(prefix bitstr.Key, keys []mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	logger.Debugw("batchProvide called", "prefix", prefix, "count", len(keys))
	addrInfo, ok := s.selfAddrInfo()
	if !ok {
		// Don't provide if the node doesn't have a valid address to include in the
		// provider record.
		return
	}
	if len(keys) <= individualProvideThreshold {
		// Don't fully explore the region, execute simple DHT provides for these
		// keys. It isn't worth it to fully explore a region for just a few keys.
		s.individualProvide(prefix, keys, false, false)
		return
	}

	regions, coveredPrefix, err := s.exploreSwarm(prefix)
	if err != nil {
		s.failedProvide(prefix, keys, fmt.Errorf("provide '%s': %w", prefix, err))
		return
	}
	logger.Debugf("provide: requested prefix '%s' (len %d), prefix covered '%s' (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	// Add any key matching the covered prefix from the provide queue to the
	// current provide batch.
	extraKeys := s.provideQueue.DequeueMatching(coveredPrefix)
	keys = append(keys, extraKeys...)
	regions = keyspace.AssignKeysToRegions(regions, keys)

	if !s.provideRegions(regions, addrInfo, false, false) {
		logger.Warnf("failed to provide any region for prefix %s", prefix)
	}
}

func (s *SweepingProvider) batchReprovide(prefix bitstr.Key, periodicReprovide bool) {
	addrInfo, ok := s.selfAddrInfo()
	if !ok {
		// Don't provide if the node doesn't have a valid address to include in the
		// provider record.
		return
	}

	// Load keys matching prefix from the keystore.
	keys, err := s.keystore.Get(s.ctx, prefix)
	if err != nil {
		s.failedReprovide(prefix, fmt.Errorf("couldn't reprovide, error when loading keys: %s", err))
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
		return
	}
	if len(keys) == 0 {
		logger.Infof("No keys to reprovide for prefix %s", prefix)
		return
	}
	if len(keys) <= individualProvideThreshold {
		// Don't fully explore the region, execute simple DHT provides for these
		// keys. It isn't worth it to fully explore a region for just a few keys.
		s.individualProvide(prefix, keys, true, periodicReprovide)
		return
	}

	regions, coveredPrefix, err := s.exploreSwarm(prefix)
	if err != nil {
		s.failedReprovide(prefix, fmt.Errorf("reprovide '%s': %w", prefix, err))
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
		return
	}
	logger.Debugf("reprovide: requested prefix '%s' (len %d), prefix covered '%s' (len %d)", prefix, len(prefix), coveredPrefix, len(coveredPrefix))

	regions = s.claimRegionReprovide(regions)

	// Remove all keys matching coveredPrefix from provide queue. No need to
	// provide them anymore since they are about to be reprovided.
	s.provideQueue.DequeueMatching(coveredPrefix)
	// Remove covered prefix from the reprovide queue, so since we are about the
	// reprovide the region.
	s.reprovideQueue.Remove(coveredPrefix)

	// When reproviding a region, remove all scheduled regions starting with
	// the currently covered prefix.
	s.scheduleLk.Lock()
	s.unscheduleSubsumedPrefixesNoLock(coveredPrefix)
	s.scheduleLk.Unlock()

	if len(coveredPrefix) < len(prefix) {
		// Covered prefix is shorter than the requested one, load all the keys
		// matching the covered prefix from the keystore.
		keys, err = s.keystore.Get(s.ctx, coveredPrefix)
		if err != nil {
			err = fmt.Errorf("couldn't reprovide, error when loading keys: %s", err)
			s.failedReprovide(prefix, err)
			if periodicReprovide {
				s.reschedulePrefix(prefix)
			}
		}
	}
	regions = keyspace.AssignKeysToRegions(regions, keys)

	if !s.provideRegions(regions, addrInfo, true, periodicReprovide) {
		logger.Warnf("failed to reprovide any region for prefix %s", prefix)
	}
}

func (s *SweepingProvider) failedProvide(prefix bitstr.Key, keys []mh.Multihash, err error) {
	logger.Warn(err)
	// Put keys back to the provide queue.
	s.provideQueue.Enqueue(prefix, keys...)

	s.connectivity.TriggerCheck()
}

func (s *SweepingProvider) failedReprovide(prefix bitstr.Key, err error) {
	logger.Warn(err)
	// Put prefix in the reprovide queue.
	s.reprovideQueue.Enqueue(prefix)

	s.connectivity.TriggerCheck()
}

// selfAddrInfo returns the current peer.AddrInfo to be used in the provider
// records sent to remote peers.
//
// If the node currently has no valid multiaddress, return an empty AddrInfo
// and false.
func (s *SweepingProvider) selfAddrInfo() (peer.AddrInfo, bool) {
	addrs := s.getSelfAddrs()
	if len(addrs) == 0 {
		logger.Warn("provider: no self addresses available for providing keys")
		return peer.AddrInfo{}, false
	}
	return peer.AddrInfo{ID: s.peerid, Addrs: addrs}, true
}

// individualProvide provides the keys sharing the same prefix to the network
// without exploring the associated keyspace regions. It performs "normal" DHT
// provides for the supplied keys, handles failures and schedules next
// reprovide is necessary.
func (s *SweepingProvider) individualProvide(prefix bitstr.Key, keys []mh.Multihash, reprovide bool, periodicReprovide bool) {
	if len(keys) == 0 {
		return
	}

	var provideErr error
	if len(keys) == 1 {
		coveredPrefix, err := s.vanillaProvide(keys[0])
		if err == nil {
			s.provideCounter.Add(s.ctx, 1)
		} else if !reprovide {
			// Put the key back in the provide queue.
			s.failedProvide(prefix, keys, fmt.Errorf("individual provide failed for prefix '%s', %w", prefix, err))
		}
		provideErr = err
		if periodicReprovide {
			// Schedule next reprovide for the prefix that was actually covered by
			// the GCP, otherwise we may schedule a reprovide for a prefix too short
			// or too long.
			s.reschedulePrefix(coveredPrefix)
		}
	} else {
		wg := sync.WaitGroup{}
		success := atomic.Bool{}
		for _, key := range keys {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.vanillaProvide(key)
				if err == nil {
					s.provideCounter.Add(s.ctx, 1)
					success.Store(true)
				} else if !reprovide {
					// Individual provide failed, put key back in provide queue.
					s.failedProvide(prefix, []mh.Multihash{key}, err)
				}
			}()
		}
		wg.Wait()

		if !success.Load() {
			// Only errors if all provides failed.
			provideErr = fmt.Errorf("all individual provides failed for prefix %s", prefix)
		}
		if periodicReprovide {
			s.reschedulePrefix(prefix)
		}
	}
	if reprovide && provideErr != nil {
		s.failedReprovide(prefix, provideErr)
	}
}

// provideRegions contains common logic to batchProvide() and batchReprovide().
// It iterate over supplied regions, and allocates the regions provider records
// to the appropriate DHT servers.
func (s *SweepingProvider) provideRegions(regions []keyspace.Region, addrInfo peer.AddrInfo, reprovide, periodicReprovide bool) bool {
	errCount := 0
	for _, r := range regions {
		allKeys := keyspace.AllValues(r.Keys, s.order)
		if len(allKeys) == 0 {
			if reprovide {
				s.releaseRegionReprovide(r.Prefix)
			}
			continue
		}
		// Add keys to local provider store
		for _, h := range allKeys {
			s.addLocalRecord(h)
		}
		keysAllocations := keyspace.AllocateToKClosest(r.Keys, r.Peers, s.replicationFactor)
		err := s.sendProviderRecords(keysAllocations, addrInfo)
		if reprovide {
			s.releaseRegionReprovide(r.Prefix)
			if periodicReprovide {
				s.reschedulePrefix(r.Prefix)
			}
		}
		if err != nil {
			errCount++
			err = fmt.Errorf("cannot send provider records for region %s: %s", r.Prefix, err)
			if reprovide {
				s.failedReprovide(r.Prefix, err)
			} else { // provide operation
				s.failedProvide(r.Prefix, keyspace.AllValues(r.Keys, s.order), err)
			}
			continue
		}
		keyCount := len(allKeys)
		s.provideCounter.Add(s.ctx, int64(keyCount))
		logger.Debugw("sent provider records", "prefix", r.Prefix, "count", keyCount, "keys", allKeys)
	}
	// If at least 1 regions was provided, we don't consider it a failure.
	return errCount < len(regions)
}

// claimRegionReprovide checks if the region is already being reprovided by
// another thread. If not it marks the region as being currently reprovided.
func (s *SweepingProvider) claimRegionReprovide(regions []keyspace.Region) []keyspace.Region {
	out := regions[:0]
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	for _, r := range regions {
		if r.Peers.IsEmptyLeaf() {
			continue
		}
		if _, ok := keyspace.FindPrefixOfKey(s.ongoingReprovides, r.Prefix); !ok {
			// Prune superstrings of r.Prefix if any
			keyspace.PruneSubtrie(s.ongoingReprovides, r.Prefix)
			out = append(out, r)
			s.ongoingReprovides.Add(r.Prefix, struct{}{})
		}
	}
	return out
}

// releaseRegionReprovide marks the region as no longer being reprovided.
func (s *SweepingProvider) releaseRegionReprovide(prefix bitstr.Key) {
	s.ongoingReprovidesLk.Lock()
	defer s.ongoingReprovidesLk.Unlock()
	s.ongoingReprovides.Remove(prefix)
}

// ProvideOnce only sends provider records for the given keys out to the DHT
// swarm. It does NOT take the responsibility to reprovide these keys.
//
// Returns an error if the keys couldn't be added to the provide queue. This
// can happen if the provider is closed or if the node is currently Offline
// (either never bootstrapped, or disconnected since more than `OfflineDelay`).
// The schedule and provide queue depend on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) error {
	if s.closed() {
		return ErrClosed
	}
	return s.handleProvide(true, false, keys...)
}

// StartProviding provides the given keys to the DHT swarm unless they were
// already provided in the past. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
//
// Returns an error if the keys couldn't be added to the provide queue. This
// can happen if the provider is closed or if the node is currently Offline
// (either never bootstrapped, or disconnected since more than `OfflineDelay`).
// The schedule and provide queue depend on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	if s.closed() {
		return ErrClosed
	}
	return s.handleProvide(force, true, keys...)
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) error {
	if s.closed() {
		return ErrClosed
	}
	err := s.keystore.Delete(s.ctx, keys...)
	if err != nil {
		err = fmt.Errorf("failed to stop providing keys: %w", err)
	}
	s.provideQueue.Remove(keys...)
	return err
}

// Clear clears the all the keys from the provide queue and returns the number
// of keys that were cleared.
//
// The keys are not deleted from the keystore, so they will continue to be
// reprovided as scheduled.
func (s *SweepingProvider) Clear() int {
	if s.closed() {
		return 0
	}
	return s.provideQueue.Clear()
}

// AddToSchedule makes sure the prefixes associated with the supplied keys are
// scheduled to be reprovided.
//
// Returns an error if the provider is closed or if the node is currently
// Offline (either never bootstrapped, or disconnected since more than
// `OfflineDelay`). The schedule depends on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) AddToSchedule(keys ...mh.Multihash) error {
	if s.closed() {
		return ErrClosed
	}
	if s.isOffline() {
		return ErrOffline
	}
	_, err := s.groupAndScheduleKeysByPrefix(keys, true)
	return err
}

// RefreshSchedule scans the KeyStore for any keys that are not currently
// scheduled for reproviding. If such keys are found, it schedules their
// associated keyspace region to be reprovided.
//
// This function doesn't remove prefixes that have no keys from the schedule.
// This is done automatically during the reprovide operation if a region has no
// keys.
//
// Returns an error if the provider is closed or if the node is currently
// Offline (either never bootstrapped, or disconnected since more than
// `OfflineDelay`). The schedule depends on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) RefreshSchedule() error {
	if s.closed() {
		return ErrClosed
	}
	// Look for prefixes not included in the schedule
	s.scheduleLk.Lock()
	prefixLen, err := s.getAvgPrefixLenNoLock()
	if err != nil {
		s.scheduleLk.Unlock()
		return err
	}
	gaps := keyspace.TrieGaps(s.schedule, "", s.order)
	s.scheduleLk.Unlock()

	missing := make([]bitstr.Key, 0, len(gaps))
	for _, p := range gaps {
		if len(p) >= prefixLen {
			missing = append(missing, p)
		} else {
			missing = append(missing, keyspace.ExtendBinaryPrefix(p, prefixLen)...)
		}
	}

	// Only keep the missing prefixes for which there are keys in the KeyStore.
	toInsert := make([]bitstr.Key, 0)
	for _, p := range missing {
		ok, err := s.keystore.ContainsPrefix(s.ctx, p)
		if err != nil {
			logger.Warnf("couldn't refresh schedule for prefix %s: %s", p, err)
		}
		if ok {
			toInsert = append(toInsert, p)
		}
	}
	if len(toInsert) == 0 {
		return nil
	}

	// Insert prefixes into the schedule
	s.scheduleLk.Lock()
	for _, p := range toInsert {
		s.schedulePrefixNoLock(p, false)
	}
	s.scheduleLk.Unlock()
	return nil
}
