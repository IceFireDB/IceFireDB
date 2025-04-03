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
	"github.com/multiformats/go-multihash"
)

const (
	// MAGIC: how long we wait before reproviding a key
	DefaultReproviderInterval = time.Hour * 22 // https://github.com/ipfs/kubo/pull/9326

	// MAGIC: If the reprovide ticker is larger than a minute (likely), provide
	// once after we've been up a minute. Don't provide _immediately_ as we
	// might be just about to stop.
	defaultInitialReprovideDelay = time.Minute

	// MAGIC: how long we wait between the first provider we hear about and
	// batching up the provides to send out
	pauseDetectionThreshold = time.Millisecond * 500

	// MAGIC: how long we are willing to collect providers for the batch after
	// we receive the first one
	maxCollectionDuration = time.Minute * 10
)

var log = logging.Logger("provider.batched")

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

	reprovideCh         chan cid.Cid
	noReprovideInFlight chan struct{}

	maxReprovideBatchSize uint

	statLk                                    sync.Mutex
	totalProvides, lastReprovideBatchSize     uint64
	avgProvideDuration, lastReprovideDuration time.Duration
	lastRun                                   time.Time

	throughputCallback ThroughputCallback
	// throughputProvideCurrentCount counts how many provides has been done since the last call to throughputCallback
	throughputProvideCurrentCount uint
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
	s := &reprovider{
		allowlist:             verifcid.DefaultAllowlist,
		reprovideInterval:     DefaultReproviderInterval,
		maxReprovideBatchSize: math.MaxUint,
		keyPrefix:             DefaultKeyPrefix,
		reprovideCh:           make(chan cid.Cid),
		noReprovideInFlight:   make(chan struct{}),
	}

	for _, o := range opts {
		if err := o(s); err != nil {
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
	s.q = queue.NewQueue(s.ds)

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

// MaxBatchSize limit how big each batch is.
// Some content routers like acceleratedDHTClient have sub linear scalling and
// bigger sizes are thus faster per elements however smaller batch sizes can
// limit memory usage spike.
func MaxBatchSize(n uint) Option {
	return func(system *reprovider) error {
		system.maxReprovideBatchSize = n
		return nil
	}
}

// ThroughputReport will fire the callback synchronously once at least limit
// multihashes have been advertised, it will then wait until a new set of at least
// limit multihashes has been advertised.
// While ThroughputReport is set batches will be at most minimumProvides big.
// If it returns false it wont ever be called again.
func ThroughputReport(f ThroughputCallback, minimumProvides uint) Option {
	return func(system *reprovider) error {
		system.throughputCallback = f
		system.throughputMinimumProvides = minimumProvides
		return nil
	}
}

type ThroughputCallback = func(reprovide bool, complete bool, totalKeysProvided uint, totalDuration time.Duration) (continueWatching bool)

// Online will enable the router and make it send publishes online.
// nil can be used to turn the router offline.
// You can't register multiple providers, if this option is passed multiple times
// it will error.
func Online(rsys Provide) Option {
	return func(system *reprovider) error {
		if system.rsys != nil {
			return errors.New("trying to register two provider on the same reprovider")
		}
		system.rsys = rsys
		return nil
	}
}

func initialReprovideDelay(duration time.Duration) Option {
	return func(system *reprovider) error {
		system.initialReprovideDelaySet = true
		system.initalReprovideDelay = duration
		return nil
	}
}

func (s *reprovider) run() {
	provCh := s.q.Dequeue()

	s.closewg.Add(1)
	go func() {
		defer s.closewg.Done()

		m := make(map[cid.Cid]struct{})

		// setup stopped timers
		maxCollectionDurationTimer := time.NewTimer(time.Hour)
		pauseDetectTimer := time.NewTimer(time.Hour)
		stopAndEmptyTimer(maxCollectionDurationTimer)
		stopAndEmptyTimer(pauseDetectTimer)

		// make sure timers are cleaned up
		defer maxCollectionDurationTimer.Stop()
		defer pauseDetectTimer.Stop()

		resetTimersAfterReceivingProvide := func() {
			firstProvide := len(m) == 0
			if firstProvide {
				// after receiving the first provider start up the timers
				maxCollectionDurationTimer.Reset(maxCollectionDuration)
				pauseDetectTimer.Reset(pauseDetectionThreshold)
			} else {
				// otherwise just do a full restart of the pause timer
				stopAndEmptyTimer(pauseDetectTimer)
				pauseDetectTimer.Reset(pauseDetectionThreshold)
			}
		}

		for {
			performedReprovide := false
			complete := false

			batchSize := s.maxReprovideBatchSize
			if s.throughputCallback != nil && s.throughputMinimumProvides < batchSize {
				batchSize = s.throughputMinimumProvides
			}

			// at the start of every loop the maxCollectionDurationTimer and pauseDetectTimer should be already be
			// stopped and have empty channels
			for uint(len(m)) < batchSize {
				var noReprovideInFlight chan struct{}
				if len(m) == 0 {
					noReprovideInFlight = s.noReprovideInFlight
				}

				select {
				case c := <-provCh:
					resetTimersAfterReceivingProvide()
					m[c] = struct{}{}
				case c := <-s.reprovideCh:
					resetTimersAfterReceivingProvide()
					m[c] = struct{}{}
					performedReprovide = true
				case <-pauseDetectTimer.C:
					// if this timer has fired then the max collection timer has started so let's stop and empty it
					stopAndEmptyTimer(maxCollectionDurationTimer)
					complete = true
					goto AfterLoop
				case <-maxCollectionDurationTimer.C:
					// if this timer has fired then the pause timer has started so let's stop and empty it
					stopAndEmptyTimer(pauseDetectTimer)
					goto AfterLoop
				case <-s.ctx.Done():
					return
				case noReprovideInFlight <- struct{}{}:
					// if no reprovide is in flight get consumer asking for reprovides unstuck
				}
			}
			stopAndEmptyTimer(pauseDetectTimer)
			stopAndEmptyTimer(maxCollectionDurationTimer)
		AfterLoop:

			if len(m) == 0 {
				continue
			}

			keys := make([]multihash.Multihash, 0, len(m))
			for c := range m {
				delete(m, c)

				// hash security
				if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
					log.Errorf("insecure hash in reprovider, %s (%s)", c, err)
					continue
				}

				keys = append(keys, c.Hash())
			}

			// in case after removing all the invalid CIDs there are no valid ones left
			if len(keys) == 0 {
				continue
			}

			if r, ok := s.rsys.(Ready); ok {
				ticker := time.NewTicker(time.Minute)
				for !r.Ready() {
					log.Debugf("reprovider system not ready")
					select {
					case <-ticker.C:
					case <-s.ctx.Done():
						return
					}
				}
				ticker.Stop()
			}

			log.Debugf("starting provide of %d keys", len(keys))
			start := time.Now()
			err := doProvideMany(s.ctx, s.rsys, keys)
			if err != nil {
				log.Debugf("providing failed %v", err)
				continue
			}
			dur := time.Since(start)

			totalProvideTime := time.Duration(s.totalProvides) * s.avgProvideDuration
			recentAvgProvideDuration := dur / time.Duration(len(keys))

			s.statLk.Lock()
			s.avgProvideDuration = (totalProvideTime + dur) / (time.Duration(s.totalProvides) + time.Duration(len(keys)))
			s.totalProvides += uint64(len(keys))

			log.Debugf("finished providing of %d keys. It took %v with an average of %v per provide", len(keys), dur, recentAvgProvideDuration)

			if performedReprovide {
				s.lastReprovideBatchSize = uint64(len(keys))
				s.lastReprovideDuration = dur
				s.lastRun = time.Now()

				s.statLk.Unlock()

				// Don't hold the lock while writing to disk, consumers don't need to wait on IO to read thoses fields.

				if err := s.ds.Put(s.ctx, lastReprovideKey, storeTime(time.Now())); err != nil {
					log.Errorf("could not store last reprovide time: %v", err)
				}
				if err := s.ds.Sync(s.ctx, lastReprovideKey); err != nil {
					log.Errorf("could not perform sync of last reprovide time: %v", err)
				}
			} else {
				s.statLk.Unlock()
			}

			s.throughputDurationSum += dur
			s.throughputProvideCurrentCount += uint(len(keys))
			if s.throughputCallback != nil && s.throughputProvideCurrentCount >= s.throughputMinimumProvides {
				if more := s.throughputCallback(performedReprovide, complete, s.throughputProvideCurrentCount, s.throughputDurationSum); !more {
					s.throughputCallback = nil
				}
				s.throughputProvideCurrentCount = 0
				s.throughputDurationSum = 0
			}
		}
	}()

	s.closewg.Add(1)
	go func() {
		defer s.closewg.Done()

		var initialReprovideCh, reprovideCh <-chan time.Time

		// If reproviding is enabled (non-zero)
		if s.reprovideInterval > 0 {
			reprovideTicker := time.NewTicker(s.reprovideInterval)
			defer reprovideTicker.Stop()
			reprovideCh = reprovideTicker.C

			// if there is a non-zero initial reprovide time that was set in the initializer or if the fallback has been
			if s.initialReprovideDelaySet {
				initialReprovideTimer := time.NewTimer(s.initalReprovideDelay)
				defer initialReprovideTimer.Stop()

				initialReprovideCh = initialReprovideTimer.C
			}
		}

		for s.ctx.Err() == nil {
			select {
			case <-initialReprovideCh:
			case <-reprovideCh:
			case <-s.ctx.Done():
				return
			}

			err := s.reprovide(s.ctx, false)

			// only log if we've hit an actual error, otherwise just tell the client we're shutting down
			if s.ctx.Err() == nil && err != nil {
				log.Errorf("failed to reprovide: %s", err)
			}
		}
	}()
}

func stopAndEmptyTimer(t *time.Timer) {
	if !t.Stop() {
		<-t.C
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
	return s.reprovide(ctx, true)
}

func (s *reprovider) reprovide(ctx context.Context, force bool) error {
	if !s.shouldReprovide() && !force {
		return nil
	}

	ok := s.mu.TryLock()
	if !ok {
		return fmt.Errorf("instance of reprovide already running")
	}
	defer s.mu.Unlock()

	kch, err := s.keyProvider(ctx)
	if err != nil {
		return err
	}

reprovideCidLoop:
	for {
		select {
		case c, ok := <-kch:
			if !ok {
				break reprovideCidLoop
			}

			select {
			case s.reprovideCh <- c:
			case <-ctx.Done():
				return ctx.Err()
			case <-s.ctx.Done():
				return errors.New("failed to reprovide: shutting down")
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return errors.New("failed to reprovide: shutting down")
		}
	}

	// Wait until the underlying operation has completed
	select {
	case <-s.noReprovideInFlight:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return errors.New("failed to reprovide: shutting down")
	}
}

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

func (s *reprovider) shouldReprovide() bool {
	if s.reprovideInterval == 0 {
		return false
	}
	t, err := s.getLastReprovideTime()
	if err != nil {
		log.Debugf("getting last reprovide time failed: %s", err)
		return false
	}

	if time.Since(t) < s.reprovideInterval {
		return false
	}
	return true
}

type ReproviderStats struct {
	TotalProvides, LastReprovideBatchSize                        uint64
	ReprovideInterval, AvgProvideDuration, LastReprovideDuration time.Duration
	LastRun                                                      time.Time
}

// Stat returns various stats about this provider system
func (s *reprovider) Stat() (ReproviderStats, error) {
	s.statLk.Lock()
	defer s.statLk.Unlock()
	return ReproviderStats{
		TotalProvides:          s.totalProvides,
		LastReprovideBatchSize: s.lastReprovideBatchSize,
		ReprovideInterval:      s.reprovideInterval,
		AvgProvideDuration:     s.avgProvideDuration,
		LastReprovideDuration:  s.lastReprovideDuration,
		LastRun:                s.lastRun,
	}, nil
}

func doProvideMany(ctx context.Context, r Provide, keys []multihash.Multihash) error {
	if many, ok := r.(ProvideMany); ok {
		return many.ProvideMany(ctx, keys)
	}

	for _, k := range keys {
		if err := r.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
			return err
		}
	}
	return nil
}
