package provider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/boxo/provider/internal/queue"
	"github.com/ipfs/boxo/verifcid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	metrics "github.com/ipfs/go-metrics-interface"
	"github.com/multiformats/go-multihash"
)

const (
	// MAGIC: how long we wait before reproviding a key
	DefaultReproviderInterval = time.Hour * 22 // https://github.com/ipfs/kubo/pull/9326

	// MAGIC: If the reprovide ticker is larger than a minute (likely), provide
	// once after we've been up a minute. Don't provide _immediately_ as we
	// might be just about to stop.
	defaultInitialReprovideDelay = time.Minute

	// MAGIC: default number of provide operations that can run concurrently.
	// Note that each provide operation typically opens at least
	// `replication_factor` connections to remote peers.
	defaultProvideWorkerCount = 64

	// MAGIC: Maximum duration during which no workers are available to provide a
	// cid before a warning is triggered.
	provideDelayWarnDuration = 15 * time.Second
)

var log = logging.Logger("provider")

type reprovider struct {
	ctx     context.Context
	close   context.CancelFunc
	closewg sync.WaitGroup
	mu      sync.Mutex

	// reprovideInterval is the time between 2 reprovides. A value of 0 means
	// that no automatic reprovide will be performed.
	reprovideInterval        time.Duration
	initalReprovideDelay     time.Duration
	initialReprovideDelaySet bool

	allowlist   verifcid.Allowlist
	rsys        Provide
	keyProvider KeyChanFunc

	q  *queue.Queue
	ds datastore.Batching

	maxReprovideBatchSize uint
	provideWorkerCount    uint

	statLk                                      sync.Mutex
	totalReprovides, lastReprovideBatchSize     uint64
	avgReprovideDuration, lastReprovideDuration time.Duration
	lastRun                                     time.Time

	provideCounter   metrics.Counter
	reprovideCounter metrics.Counter

	throughputCallback ThroughputCallback
	// throughputProvideCurrentCount counts how many provides has been done since the last call to throughputCallback
	throughputReprovideCurrentCount uint
	// throughputDurationSum sums up durations between two calls to the throughputCallback
	throughputDurationSum     time.Duration
	throughputMinimumProvides uint

	keyPrefix datastore.Key
}

var _ System = (*reprovider)(nil)

type Provide interface {
	Provide(context.Context, cid.Cid, bool) error
}

type ProvideMany interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

type Ready interface {
	Ready() bool
}

// Option defines the functional option type that can be used to configure
// BatchProvidingSystem instances
type Option func(system *reprovider) error

var (
	lastReprovideKey = datastore.NewKey("/reprovide/lastreprovide")
	DefaultKeyPrefix = datastore.NewKey("/provider")
)

// New creates a new [System]. By default it is offline, that means it will
// enqueue tasks in ds.
// To have it publish records in the network use the [Online] option.
// If provider casts to [ProvideMany] the [ProvideMany.ProvideMany] method will
// be called instead.
//
// If provider casts to [Ready], it will wait until [Ready.Ready] is true.
func New(ds datastore.Batching, opts ...Option) (System, error) {
	ctx := metrics.CtxScope(context.Background(), "provider")
	s := &reprovider{
		allowlist:             verifcid.DefaultAllowlist,
		reprovideInterval:     DefaultReproviderInterval,
		maxReprovideBatchSize: math.MaxUint,
		provideWorkerCount:    defaultProvideWorkerCount,
		keyPrefix:             DefaultKeyPrefix,
		provideCounter:        metrics.NewCtx(ctx, "reprovider_provide_count", "Number of provides since node is running").Counter(),
		reprovideCounter:      metrics.NewCtx(ctx, "reprovider_reprovide_count", "Number of reprovides since node is running").Counter(),
	}

	var err error
	for _, o := range opts {
		if err = o(s); err != nil {
			return nil, err
		}
	}

	// Setup default behavior for the initial reprovide delay
	if !s.initialReprovideDelaySet && s.reprovideInterval > defaultInitialReprovideDelay {
		s.initalReprovideDelay = defaultInitialReprovideDelay
		s.initialReprovideDelaySet = true
	}

	if s.keyProvider == nil {
		s.keyProvider = func(ctx context.Context) (<-chan cid.Cid, error) {
			ch := make(chan cid.Cid)
			close(ch)
			return ch, nil
		}
	}

	s.ds = namespace.Wrap(ds, s.keyPrefix)
	s.q = queue.New(s.ds)

	// This is after the options processing so we do not have to worry about leaking a context if there is an
	// initialization error processing the options
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.close = cancel

	if s.rsys != nil {
		if _, ok := s.rsys.(ProvideMany); !ok {
			s.maxReprovideBatchSize = 1
		}

		s.run()
	}

	return s, nil
}

func Allowlist(allowlist verifcid.Allowlist) Option {
	return func(system *reprovider) error {
		system.allowlist = allowlist
		return nil
	}
}

func ReproviderInterval(duration time.Duration) Option {
	return func(system *reprovider) error {
		system.reprovideInterval = duration
		return nil
	}
}

func KeyProvider(fn KeyChanFunc) Option {
	return func(system *reprovider) error {
		system.keyProvider = fn
		return nil
	}
}

// DatastorePrefix sets a prefix for internal state stored in the Datastore.
// Defaults to [DefaultKeyPrefix].
func DatastorePrefix(k datastore.Key) Option {
	return func(system *reprovider) error {
		system.keyPrefix = k
		return nil
	}
}

// ProvideWorkerCount configures the number of concurrent workers that handle
// the initial provide of newly added CIDs. The maximum rate at which CIDs are
// advertised is determined by dividing the number of workers by the provide
// duration. Note that each provide typically opens connections to at least the
// configured replication factor of peers.
//
// Setting ProvideWorkerCount to 0 enables an unbounded number of workers,
// which can speed up provide operations under heavy load. Use this option
// carefully, as it may cause the libp2p node to open a very high number of
// connections to remote peers.
func ProvideWorkerCount(n int) Option {
	return func(system *reprovider) error {
		if n > 0 {
			system.provideWorkerCount = uint(n)
		}
		return nil
	}
}

// MaxBatchSize limits how big each batch is.
//
// Some content routers like acceleratedDHTClient have sub linear scalling and
// bigger sizes are thus faster per elements however smaller batch sizes can
// limit memory usage spike.
func MaxBatchSize(n uint) Option {
	return func(system *reprovider) error {
		system.maxReprovideBatchSize = n
		return nil
	}
}

// ThroughputReport fires the callback synchronously once at least limit
// multihashes have been advertised. It will then wait until a new set of at
// least limit multihashes has been advertised.
//
// While ThroughputReport is set, batches will be at most minimumProvides big.
// If it returns false it will not be called again.
func ThroughputReport(f ThroughputCallback, minimumProvides uint) Option {
	return func(system *reprovider) error {
		system.throughputCallback = f
		system.throughputMinimumProvides = minimumProvides
		return nil
	}
}

type ThroughputCallback = func(reprovide, complete bool, totalKeysProvided uint, totalDuration time.Duration) (continueWatching bool)

// Online will enables the router and makes it send publishes online. A nil
// value can be used to set the router offline. It is not possible to register
// multiple providers. If this option is passed multiple times it will error.
func Online(rsys Provide) Option {
	return func(system *reprovider) error {
		if system.rsys != nil {
			return errors.New("trying to register two providers on the same reprovider")
		}
		system.rsys = rsys
		return nil
	}
}

func (s *reprovider) run() {
	s.closewg.Add(1)
	go s.provideWorker()

	// don't start reprovide scheduling if reprovides are disabled (reprovideInterval == 0)
	if s.reprovideInterval > 0 {
		s.closewg.Add(1)
		go s.reprovideSchedulingWorker()
	}
}

func (s *reprovider) provideWorker() {
	defer s.closewg.Done()
	provCh := s.q.Dequeue()

	provideFunc := func(ctx context.Context, c cid.Cid) {
		log.Debugf("provider worker: providing %s", c)
		if err := s.rsys.Provide(ctx, c, true); err != nil {
			log.Errorf("failed to provide %s: %s", c, err)
		} else {
			s.provideCounter.Inc()
		}
	}

	var provideOperation func(context.Context, cid.Cid)
	if s.provideWorkerCount == 0 {
		// Unlimited workers
		provideOperation = func(ctx context.Context, c cid.Cid) {
			go provideFunc(ctx, c)
		}
	} else {
		provideQueue := make(chan cid.Cid)
		defer close(provideQueue)
		provideDelayTimer := time.NewTimer(provideDelayWarnDuration)
		provideDelayTimer.Stop()
		lateOnProvides := false

		// Assign cid to workers pool
		provideOperation = func(ctx context.Context, c cid.Cid) {
			provideDelayTimer.Reset(provideDelayWarnDuration)
			defer provideDelayTimer.Stop()
			select {
			case provideQueue <- c:
				if lateOnProvides {
					log.Warn("New provides are being processed again.")
				}
				lateOnProvides = false
			case <-provideDelayTimer.C:
				if !lateOnProvides {
					log.Warn("New provides are piling up in the queue, consider increasing the number of provide workers.")
					lateOnProvides = true
				}
				select {
				case provideQueue <- c:
				case <-ctx.Done():
				}
			case <-ctx.Done():
			}
		}
		// Start provide workers
		for range s.provideWorkerCount {
			go func(ctx context.Context) {
				for c := range provideQueue {
					provideFunc(ctx, c)
				}
			}(s.ctx)
		}
	}

	for c := range provCh {
		if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
			log.Errorf("insecure hash in reprovider, %s (%s)", c, err)
			continue
		}
		provideOperation(s.ctx, c)
	}
}

func (s *reprovider) reprovideSchedulingWorker() {
	defer s.closewg.Done()

	// read last reprovide time written to the datastore, and schedule the
	// first reprovide to happen reprovideInterval after that
	firstReprovideDelay := s.initalReprovideDelay
	lastReprovide, err := s.getLastReprovideTime()
	if err == nil && time.Since(lastReprovide) < s.reprovideInterval-s.initalReprovideDelay {
		firstReprovideDelay = time.Until(lastReprovide.Add(s.reprovideInterval))
	}
	firstReprovideTimer := time.NewTimer(firstReprovideDelay)

	select {
	case <-firstReprovideTimer.C:
	case <-s.ctx.Done():
		return
	}

	// after the first reprovide, schedule periodical reprovides
	nextReprovideTicker := time.NewTicker(s.reprovideInterval)

	for {
		err := s.Reprovide(context.Background())
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Errorf("failed to reprovide: %s", err)
		}
		select {
		case <-nextReprovideTicker.C:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *reprovider) waitUntilProvideSystemReady() {
	if r, ok := s.rsys.(Ready); ok {
		var ticker *time.Ticker
		for !r.Ready() {
			if ticker == nil {
				ticker = time.NewTicker(time.Minute)
				defer ticker.Stop()
			}
			log.Infof("reprovider system not ready, waiting 1m")
			select {
			case <-ticker.C:
			case <-s.ctx.Done():
				return
			}
		}
	}
}

func storeTime(t time.Time) []byte {
	val := []byte(strconv.FormatInt(t.UnixNano(), 10))
	return val
}

func parseTime(b []byte) (time.Time, error) {
	tns, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, tns), nil
}

func (s *reprovider) Close() error {
	s.close()
	err := s.q.Close()
	s.closewg.Wait()
	return err
}

func (s *reprovider) Provide(ctx context.Context, cid cid.Cid, announce bool) error {
	return s.q.Enqueue(cid)
}

func (s *reprovider) Reprovide(ctx context.Context) error {
	ok := s.mu.TryLock()
	if !ok {
		return fmt.Errorf("instance of reprovide already running")
	}
	defer s.mu.Unlock()

	kch, err := s.keyProvider(ctx)
	if err != nil {
		return err
	}

	batchSize := s.maxReprovideBatchSize
	if s.throughputCallback != nil && s.throughputMinimumProvides < batchSize {
		batchSize = s.throughputMinimumProvides
	}

	cids := make(map[cid.Cid]struct{}, min(batchSize, 1024))
	allCidsProcessed := false
	for !allCidsProcessed {
		for range batchSize {
			c, ok := <-kch
			if !ok {
				allCidsProcessed = true
				break
			}
			cids[c] = struct{}{}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ctx.Err(); err != nil {
			return errors.New("failed to reprovide: shutting down")
		}

		keys := make([]multihash.Multihash, 0, len(cids))
		for c := range cids {
			// hash security
			if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
				log.Errorf("insecure hash in reprovider, %s (%s)", c, err)
				continue
			}
			keys = append(keys, c.Hash())
			delete(cids, c)
		}

		// in case after removing all the invalid CIDs there are no valid ones left
		if len(keys) == 0 {
			continue
		}

		s.waitUntilProvideSystemReady()

		log.Infof("starting reprovide of %d keys", len(keys))
		start := time.Now()
		err := doProvideMany(s.ctx, s.rsys, keys)
		if err != nil {
			log.Errorf("reproviding failed %v", err)
			continue
		}
		dur := time.Since(start)
		recentAvgProvideDuration := dur / time.Duration(len(keys))
		log.Infof("finished reproviding %d keys. It took %v with an average of %v per provide", len(keys), dur, recentAvgProvideDuration)

		totalProvideTime := time.Duration(s.totalReprovides) * s.avgReprovideDuration
		s.statLk.Lock()
		s.avgReprovideDuration = (totalProvideTime + dur) / time.Duration(s.totalReprovides+uint64(len(keys)))
		s.totalReprovides += uint64(len(keys))
		s.lastReprovideBatchSize = uint64(len(keys))
		s.lastReprovideDuration = dur
		s.lastRun = time.Now()
		s.statLk.Unlock()

		s.reprovideCounter.Add(float64(len(keys)))

		// persist last reprovide time to disk to avoid unnecessary reprovides on restart
		if err := s.ds.Put(s.ctx, lastReprovideKey, storeTime(s.lastRun)); err != nil {
			log.Errorf("could not store last reprovide time: %v", err)
		}
		if err := s.ds.Sync(s.ctx, lastReprovideKey); err != nil {
			log.Errorf("could not perform sync of last reprovide time: %v", err)
		}

		s.throughputDurationSum += dur
		s.throughputReprovideCurrentCount += uint(len(keys))
		if s.throughputCallback != nil && s.throughputReprovideCurrentCount >= s.throughputMinimumProvides {
			if more := s.throughputCallback(true, allCidsProcessed, s.throughputReprovideCurrentCount, s.throughputDurationSum); !more {
				s.throughputCallback = nil
			}
			s.throughputReprovideCurrentCount = 0
			s.throughputDurationSum = 0
		}
	}
	return nil
}

// getLastReprovideTime gets the last time a reprovide was run from the datastore
func (s *reprovider) getLastReprovideTime() (time.Time, error) {
	val, err := s.ds.Get(s.ctx, lastReprovideKey)
	if errors.Is(err, datastore.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, errors.New("could not get last reprovide time")
	}

	t, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not decode last reprovide time, got %q", string(val))
	}

	return t, nil
}

type ReproviderStats struct {
	TotalReprovides, LastReprovideBatchSize                        uint64
	ReprovideInterval, AvgReprovideDuration, LastReprovideDuration time.Duration
	LastRun                                                        time.Time
}

// Stat returns various stats about this provider system
func (s *reprovider) Stat() (ReproviderStats, error) {
	s.statLk.Lock()
	defer s.statLk.Unlock()
	return ReproviderStats{
		TotalReprovides:        s.totalReprovides,
		LastReprovideBatchSize: s.lastReprovideBatchSize,
		ReprovideInterval:      s.reprovideInterval,
		AvgReprovideDuration:   s.avgReprovideDuration,
		LastReprovideDuration:  s.lastReprovideDuration,
		LastRun:                s.lastRun,
	}, nil
}

func doProvideMany(ctx context.Context, r Provide, keys []multihash.Multihash) error {
	if many, ok := r.(ProvideMany); ok {
		return many.ProvideMany(ctx, keys)
	}

	for _, k := range keys {
		log.Debugf("providing %s", k)
		if err := r.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
			return err
		}
	}
	return nil
}
