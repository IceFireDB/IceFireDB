package records

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-base32"
)

const (
	// providerNamespace is the reserved datastore key namespace for provider
	// records. No value record may use it (see valueDsKey), so a shared datastore
	// stays collision-free.
	providerNamespace = "providers"
	// ProvidersKeyPrefix is the datastore key prefix for ALL provider records.
	ProvidersKeyPrefix = "/" + providerNamespace + "/"
)

var (
	defaultCleanupInterval = time.Hour
	lruCacheSize           = 256
	log                    = logging.Logger("providers")
)

// ErrClosed is returned by AddProvider and GetProviders after Close.
var ErrClosed = errors.New("provider manager closed")

// ProviderStore represents a store that associates peers and their addresses to keys.
type ProviderStore interface {
	AddProvider(ctx context.Context, key []byte, prov peer.AddrInfo) error
	GetProviders(ctx context.Context, key []byte) ([]peer.AddrInfo, error)
	io.Closer
}

// ProviderManager adds and pulls providers out of the datastore, caching them
// in between.
//
// A ProviderManager is safe for concurrent use. AddProvider and GetProviders
// access the cache and datastore directly under mu; the datastore must be safe
// for concurrent use. Writes go straight to the datastore under mu with no
// write buffering, which favours durability and simplicity over write-burst
// throughput and keeps GC off the write lock. A background goroutine
// garbage-collects expired records on a parallel schedule: it sweeps the
// datastore without ever taking mu or touching the cache (reads drop expired
// providers by the same threshold), so GC never blocks reads or writes. GC
// racing a concurrent write may drop a just-refreshed record; this is accepted
// and self-heals on the next provide (tracked for a follow-up).
type ProviderManager struct {
	self peer.ID

	// mu guards cache (and the providerSets it holds) and stopped, and
	// serialises AddProvider and GetProviders access to the datastore so the
	// cache stays consistent with it. The background GC never takes mu (it only
	// sweeps the datastore).
	mu      sync.Mutex
	stopped bool
	cache   lru.LRUCache
	pstore  peerstore.Peerstore
	dstore  ds.Batching

	// shuffle randomises the provider order returned by GetProviders, so client
	// load is spread across a key's providers instead of always preferring the
	// datastore query's (lexicographic peer-ID) order. Defaults to the global,
	// concurrency-safe rand.Shuffle; tests inject a seeded source for
	// deterministic ordering. It is invoked under mu, so a test-injected
	// non-thread-safe source stays race-free.
	shuffle func(n int, swap func(i, j int))

	providerAddrTTL time.Duration
	provideValidity time.Duration
	cleanupInterval time.Duration

	cancel context.CancelFunc
	closed chan struct{}
}

var _ ProviderStore = (*ProviderManager)(nil)

// Option is a function that sets a provider manager option.
type Option func(*ProviderManager) error

func (pm *ProviderManager) applyOptions(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(pm); err != nil {
			return fmt.Errorf("provider manager option %d failed: %s", i, err)
		}
	}
	return nil
}

// CleanupInterval sets the time between GC runs.
// Defaults to 1h.
func CleanupInterval(d time.Duration) Option {
	return func(pm *ProviderManager) error {
		pm.cleanupInterval = d
		return nil
	}
}

// ProviderAddrTTL is the TTL to keep the multi addresses of provider
// peers around. Those addresses are returned alongside provider. After
// it expires, the returned records will require an extra lookup, to
// find the multiaddress associated with the returned peer id.
func ProviderAddrTTL(d time.Duration) Option {
	return func(pm *ProviderManager) error {
		pm.providerAddrTTL = d
		return nil
	}
}

// ProvideValidity is the default time that a Provider Record should last on DHT
// This value is also known as Provider Record Expiration Interval.
func ProvideValidity(d time.Duration) Option {
	return func(pm *ProviderManager) error {
		pm.provideValidity = d
		return nil
	}
}

// Cache sets the LRU cache implementation.
// Defaults to a simple LRU cache.
func Cache(c lru.LRUCache) Option {
	return func(pm *ProviderManager) error {
		pm.cache = c
		return nil
	}
}

// NewProviderManager creates a ProviderManager that runs until Close is
// called.
func NewProviderManager(local peer.ID, ps peerstore.Peerstore, dstore ds.Batching, opts ...Option) (*ProviderManager, error) {
	cache, err := lru.NewLRU(lruCacheSize, nil)
	if err != nil {
		return nil, err
	}
	pm := &ProviderManager{
		self:            local,
		pstore:          ps,
		dstore:          dstore,
		cache:           cache,
		shuffle:         rand.Shuffle,
		providerAddrTTL: amino.DefaultProviderAddrTTL,
		provideValidity: amino.DefaultProvideValidity,
		cleanupInterval: defaultCleanupInterval,
		closed:          make(chan struct{}),
	}
	if err := pm.applyOptions(opts...); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm.cancel = cancel
	go pm.gcLoop(ctx)
	return pm, nil
}

// Close stops the background GC, waits for it to exit, and fences the
// datastore: once Close returns, no AddProvider or GetProviders call touches
// the datastore, and late calls return ErrClosed. The backing datastore can
// therefore be closed as soon as Close returns. It is idempotent.
func (pm *ProviderManager) Close() error {
	pm.cancel()
	<-pm.closed
	pm.mu.Lock()
	pm.stopped = true
	pm.mu.Unlock()
	return nil
}

// AddProvider adds a provider for key k. The provider's addresses are recorded
// in the peerstore, and the (key, provider) pair is written to the cache (when
// the key is already cached) and the datastore. It returns ErrClosed after
// Close.
func (pm *ProviderManager) AddProvider(ctx context.Context, k []byte, provInfo peer.AddrInfo) error {
	ctx, span := internal.StartSpan(ctx, "ProviderManager.AddProvider")
	defer span.End()

	if provInfo.ID != pm.self { // don't add own addrs.
		pm.pstore.AddAddrs(provInfo.ID, provInfo.Addrs, pm.providerAddrTTL)
	}

	now := time.Now()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.stopped {
		return ErrClosed
	}
	if provs, ok := pm.cache.Get(string(k)); ok {
		provs.(*providerSet).setVal(provInfo.ID, now)
	} // else not cached, just write through
	return writeProviderEntry(ctx, pm.dstore, k, provInfo.ID, now)
}

// writeProviderEntry writes the provider into the datastore
func writeProviderEntry(ctx context.Context, dstore ds.Datastore, k []byte, p peer.ID, t time.Time) error {
	dsk := mkProvKeyFor(k, p)

	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())

	return dstore.Put(ctx, ds.NewKey(dsk), buf[:n])
}

func mkProvKeyFor(k []byte, p peer.ID) string {
	return mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))
}

func mkProvKey(k []byte) string {
	return ProvidersKeyPrefix + base32.RawStdEncoding.EncodeToString(k)
}

// GetProviders returns the set of providers for the given key. The returned
// slice is a fresh copy the caller may retain and modify. A datastore read
// failure yields an empty result rather than an error: the record simply
// appears absent, so a GET_PROVIDERS query falls back to other peers instead of
// failing. It returns ErrClosed after Close, and the context's error if ctx is
// cancelled.
func (pm *ProviderManager) GetProviders(ctx context.Context, k []byte) ([]peer.AddrInfo, error) {
	ctx, span := internal.StartSpan(ctx, "ProviderManager.GetProviders")
	defer span.End()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pm.mu.Lock()
	if pm.stopped {
		pm.mu.Unlock()
		return nil, ErrClosed
	}
	pset, err := pm.getProviderSetForKey(ctx, k)
	if err != nil {
		pm.mu.Unlock()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !errors.Is(err, ds.ErrNotFound) {
			log.Error("error reading providers: ", err)
		}
		return nil, nil
	}
	provs := slices.Clone(pset.providers)
	pm.mu.Unlock()
	// The datastore query (and thus the cached set built from it) yields
	// providers in an unspecified order that, for a datastore-backed store,
	// tends to be lexicographic by peer ID. Shuffle so callers spread load
	// across providers rather than always preferring the same ones; downstream
	// code must treat the order as arbitrary.
	pm.shuffle(len(provs), func(i, j int) { provs[i], provs[j] = provs[j], provs[i] })

	infos := make([]peer.AddrInfo, len(provs))
	for i, pid := range provs {
		ai := pm.pstore.PeerInfo(pid)
		infos[i] = peer.AddrInfo{
			ID:    ai.ID,
			Addrs: slices.Clone(ai.Addrs),
		}
	}
	return infos, nil
}

// getProviderSetForKey returns the ProviderSet for k, from the cache if present
// (dropping any entries that have since expired) or loaded from the datastore.
// The caller must hold pm.mu.
func (pm *ProviderManager) getProviderSetForKey(ctx context.Context, k []byte) (*providerSet, error) {
	cached, ok := pm.cache.Get(string(k))
	if ok {
		ps := cached.(*providerSet)
		providers := []peer.ID{}
		set := map[peer.ID]time.Time{}
		for k, v := range ps.set {
			if time.Since(v) > pm.provideValidity {
				continue
			}
			providers = append(providers, k)
			set[k] = v
		}
		ps.providers = providers
		ps.set = set
		return ps, nil
	}

	pset, err := loadProviderSet(ctx, pm.dstore, pm.provideValidity, k)
	if err != nil {
		return nil, err
	}

	if len(pset.providers) > 0 {
		pm.cache.Add(string(k), pset)
	}

	return pset, nil
}

// loadProviderSet loads the ProviderSet for k out of the datastore, discarding
// (and deleting) any entry that is expired or malformed.
func loadProviderSet(ctx context.Context, dstore ds.Datastore, provideValidity time.Duration, k []byte) (*providerSet, error) {
	res, err := dstore.Query(ctx, dsq.Query{Prefix: mkProvKey(k)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Close() }()

	now := time.Now()
	out := newProviderSet()
	for {
		e, ok := res.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			log.Error("got an error: ", e.Error)
			continue
		}

		// check expiration time
		t, err := readTimeValue(e.Value)
		switch {
		case err != nil:
			// couldn't parse the time
			log.Error("parsing providers record from disk: ", err)
			fallthrough
		case now.Sub(t) > provideValidity:
			// or just expired
			err = dstore.Delete(ctx, ds.RawKey(e.Key))
			if err != nil && !errors.Is(err, ds.ErrNotFound) {
				log.Error("failed to remove provider record from disk: ", err)
			}
			continue
		}

		lix := strings.LastIndex(e.Key, "/")

		decstr, err := base32.RawStdEncoding.DecodeString(e.Key[lix+1:])
		if err != nil {
			log.Error("base32 decoding error: ", err)
			err = dstore.Delete(ctx, ds.RawKey(e.Key))
			if err != nil && !errors.Is(err, ds.ErrNotFound) {
				log.Error("failed to remove provider record from disk: ", err)
			}
			continue
		}

		pid := peer.ID(decstr)

		out.setVal(pid, t)
	}

	return out, nil
}

func readTimeValue(data []byte) (time.Time, error) {
	nsec, n := binary.Varint(data)
	if n <= 0 {
		return time.Time{}, errors.New("failed to parse time")
	}

	return time.Unix(0, nsec), nil
}

// gcLoop periodically garbage-collects expired provider records until ctx is
// cancelled, then signals Close by closing pm.closed. A non-positive
// cleanupInterval disables collection but still honours Close.
func (pm *ProviderManager) gcLoop(ctx context.Context) {
	defer close(pm.closed)

	if pm.cleanupInterval <= 0 {
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(pm.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pm.collectExpired(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// collectExpired sweeps the provider subtree of the datastore, deleting every
// record older than provideValidity. It never touches the cache or mu, so it
// runs fully in parallel with AddProvider and GetProviders (relying on the
// datastore being safe for concurrent use). The cache needs no sweeping of its
// own: getProviderSetForKey drops expired providers on read by the same
// threshold, and unqueried entries age out of the LRU, so an expired record can
// never be served whether or not GC has reclaimed it from disk yet.
func (pm *ProviderManager) collectExpired(ctx context.Context) {
	res, err := pm.dstore.Query(ctx, dsq.Query{Prefix: ProvidersKeyPrefix})
	if err != nil {
		log.Error("provider record GC query failed: ", err)
		return
	}
	defer func() { _ = res.Close() }()

	now := time.Now()
	for e := range res.Next() {
		if ctx.Err() != nil {
			return
		}
		if e.Error != nil {
			log.Error("got error from GC query: ", e.Error)
			continue
		}

		t, err := readTimeValue(e.Value)
		if err != nil || now.Sub(t) > pm.provideValidity {
			if err := pm.dstore.Delete(ctx, ds.RawKey(e.Key)); err != nil && !errors.Is(err, ds.ErrNotFound) {
				log.Error("failed to remove provider record from disk: ", err)
			}
		}
	}
}
