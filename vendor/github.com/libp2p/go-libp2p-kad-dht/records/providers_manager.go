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
	batchBufferSize        = 256
	// maxPendingWrites is the hard cap on the pending write buffer. A flush
	// fires at batchBufferSize and, if it fails, keeps its entries so the next
	// write retries it; the headroom between the two is how many retries a
	// failing datastore gets before the buffer is dropped.
	maxPendingWrites = batchBufferSize + batchBufferSize/4
	// flushTimeout bounds a single flush of the pending buffer. AddProvider
	// flushes while holding mu, where an unbounded commit would stall every
	// concurrent GetProviders, and Close flushes on the shutdown path. It binds
	// only on a datastore that honours ctx: go-ds-leveldb's Commit ignores it,
	// so there a stalled write still blocks until the write itself returns.
	flushTimeout = 30 * time.Second
	log          = logging.Logger("providers")
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
// access the cache, a pending write buffer, and the datastore under mu; the
// datastore must be safe for concurrent use. Writes accumulate in pending and
// flush as one datastore batch at batchBufferSize or Close, so a burst of
// ADD_PROVIDER records costs one fsync rather than one per record; a flush
// that fails is retried by the writes that follow it, up to maxPendingWrites,
// past which the buffer is dropped so it cannot grow without bound.
// GetProviders overlays pending on the datastore and does not flush. A
// background goroutine garbage-collects expired records on a parallel schedule:
// it never takes mu, sweeping the datastore and committing deletes in batches
// of batchBufferSize.
// It may delete an on-disk record whose key has a fresher write still sitting
// in pending: deletes are chosen from a query snapshot and applied at commit,
// so a flush landing in between can be undone by it. See collectExpired for
// why that race is accepted. Reads are unaffected either way, because they
// overlay pending on top of the datastore and drop expired providers by the
// same threshold.
type ProviderManager struct {
	self peer.ID

	// mu guards cache (and the providerSets it holds), pending, and stopped,
	// and serialises AddProvider and GetProviders so the cache stays consistent
	// with pending and the datastore. The background GC never takes mu; it only
	// sweeps the datastore.
	mu      sync.Mutex
	stopped bool
	cache   lru.LRUCache
	pending map[string]time.Time
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
		pending:         make(map[string]time.Time),
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

// Close stops the background GC, flushes pending writes, and fences the
// datastore: once Close returns, no AddProvider or GetProviders call touches
// the datastore, and late calls return ErrClosed. The backing datastore can
// therefore be closed as soon as Close returns. The flush is bounded by
// flushTimeout, so a stalled datastore delays shutdown rather than blocking it.
// It is idempotent.
func (pm *ProviderManager) Close() error {
	pm.cancel()
	<-pm.closed
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.stopped {
		return nil
	}
	err := pm.flushLocked()
	pm.stopped = true
	return err
}

// AddProvider adds a provider for key k. The provider's addresses are recorded
// in the peerstore, and the (key, provider) pair is written to the cache (when
// the key is already cached) and to the pending buffer. The buffer flushes to
// the datastore at batchBufferSize; a flush that fails is logged and retried
// rather than reported here, because it covers other callers' writes too. The
// only error this returns is ErrClosed, after Close.
func (pm *ProviderManager) AddProvider(ctx context.Context, k []byte, provInfo peer.AddrInfo) error {
	_, span := internal.StartSpan(ctx, "ProviderManager.AddProvider")
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
	}
	pm.pending[mkProvKeyFor(k, provInfo.ID)] = now
	if len(pm.pending) >= batchBufferSize {
		// The flush covers every buffered write, not just this one, so its
		// failure is not this caller's to report: reporting it would fail one
		// arbitrary ADD_PROVIDER out of a batch whose other writes already
		// returned success. flushLocked logs it and retries instead.
		_ = pm.flushLocked()
	}
	return nil
}

// flushLocked commits pending writes as one datastore batch, bounded by
// flushTimeout. The caller must hold pm.mu.
//
// A failed commit keeps pending, so the next write retries it and a transient
// datastore fault costs nothing; a timeout counts as one such fault, so a slow
// store loses no records. Retrying is bounded: at maxPendingWrites the
// buffer is dropped, so a datastore that cannot write can neither grow it
// without limit nor make every later write re-stage an ever-larger map under
// mu. Records dropped that way self-heal on their providers' next reprovide.
func (pm *ProviderManager) flushLocked() error {
	if len(pm.pending) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	err := pm.commitPendingLocked(ctx)
	if err != nil && len(pm.pending) < maxPendingWrites {
		log.Warnw("provider record flush failed, keeping writes buffered for retry",
			"buffered", len(pm.pending), "error", err)
		return err
	}
	if err != nil {
		log.Errorw("dropping buffered provider records after repeated flush failures",
			"dropped", len(pm.pending), "error", err)
	}
	clear(pm.pending)
	return err
}

// commitPendingLocked writes every pending entry to the datastore as a single
// batch, leaving pending untouched. The caller must hold pm.mu.
func (pm *ProviderManager) commitPendingLocked(ctx context.Context) error {
	batch, err := pm.dstore.Batch(ctx)
	if err != nil {
		return err
	}
	for dsk, t := range pm.pending {
		if err := batch.Put(ctx, ds.NewKey(dsk), encodeProviderTime(t)); err != nil {
			return err
		}
	}
	return batch.Commit(ctx)
}

// encodeProviderTime returns the on-disk value for a provider record's write
// time.
func encodeProviderTime(t time.Time) []byte {
	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())
	return buf[:n]
}

func mkProvKeyFor(k []byte, p peer.ID) string {
	return mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))
}

func mkProvKey(k []byte) string {
	return ProvidersKeyPrefix + base32.RawStdEncoding.EncodeToString(k)
}

// decodeProvKeyPeer extracts and validates the peer ID encoded in the last
// path segment of a provider datastore key produced by mkProvKeyFor.
func decodeProvKeyPeer(dsk string) (peer.ID, error) {
	lix := strings.LastIndex(dsk, "/")
	decstr, err := base32.RawStdEncoding.DecodeString(dsk[lix+1:])
	if err != nil {
		return "", err
	}
	return peer.IDFromBytes(decstr)
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
// (dropping any entries that have since expired) or loaded from the datastore
// and overlaid with pending writes for k. The caller must hold pm.mu.
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
	pm.applyPending(k, pset)

	if len(pset.providers) > 0 {
		pm.cache.Add(string(k), pset)
	}

	return pset, nil
}

// applyPending overlays unflushed writes for k onto pset. The caller must hold
// pm.mu.
//
// A pending entry old enough to be expired is dropped from pset but left in
// pending, so a later flush still persists it and GC reclaims it from disk by
// the usual path. The expiry check is load-bearing rather than defensive:
// flushes fire on batchBufferSize or Close and nothing else, so a node that
// never reaches a full buffer can hold an entry well past provideValidity,
// and only this check keeps it from being served as a live provider.
func (pm *ProviderManager) applyPending(k []byte, pset *providerSet) {
	prefix := mkProvKey(k) + "/"
	now := time.Now()
	for dsk, t := range pm.pending {
		if !strings.HasPrefix(dsk, prefix) {
			continue
		}
		if now.Sub(t) > pm.provideValidity {
			continue
		}
		pid, err := decodeProvKeyPeer(dsk)
		if err != nil {
			log.Error("invalid peer ID in provider key: ", err)
			continue
		}
		pset.setVal(pid, t)
	}
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

		pid, err := decodeProvKeyPeer(e.Key)
		if err != nil {
			log.Error("invalid peer ID in provider key: ", err)
			err = dstore.Delete(ctx, ds.RawKey(e.Key))
			if err != nil && !errors.Is(err, ds.ErrNotFound) {
				log.Error("failed to remove provider record from disk: ", err)
			}
			continue
		}

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
// record older than provideValidity. Deletes are committed in batches of
// batchBufferSize so a large leftover set (for example after switching to
// client mode) costs one fsync per batch, not one per record. It never takes
// mu, so it runs fully in parallel with AddProvider and GetProviders.
//
// Deletes are chosen from the query snapshot but applied at commit, so a flush
// landing in between writes the fresh record and clears pending, leaving it on
// disk for this commit to remove. That provider is gone from this server until
// its next reprovide. The race is accepted rather than closed: closing it would
// put GC back on mu, re-reading every staged key under the write lock, and it
// only opens for a record already past provideValidity, on a store whose
// records are best-effort and republished well before they expire.
//
// Reads never serve an expired record either way: getProviderSetForKey drops
// entries past provideValidity, overlays pending on top of loadProviderSet,
// and unqueried entries age out of the LRU.
func (pm *ProviderManager) collectExpired(ctx context.Context) {
	now := time.Now()

	res, err := pm.dstore.Query(ctx, dsq.Query{Prefix: ProvidersKeyPrefix})
	if err != nil {
		log.Error("provider record GC query failed: ", err)
		return
	}
	defer func() { _ = res.Close() }()

	batch, err := pm.dstore.Batch(ctx)
	if err != nil {
		log.Error("provider record GC batch failed: ", err)
		return
	}
	n := 0
	commit := func() bool {
		if n == 0 {
			return true
		}
		if err := batch.Commit(ctx); err != nil {
			log.Error("failed to commit provider record GC batch: ", err)
			return false
		}
		n = 0
		batch, err = pm.dstore.Batch(ctx)
		if err != nil {
			log.Error("provider record GC batch failed: ", err)
			return false
		}
		return true
	}

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
			if err := batch.Delete(ctx, ds.RawKey(e.Key)); err != nil && !errors.Is(err, ds.ErrNotFound) {
				log.Error("failed to remove provider record from disk: ", err)
				continue
			}
			n++
			if n >= batchBufferSize && !commit() {
				return
			}
		}
	}
	commit()
}
