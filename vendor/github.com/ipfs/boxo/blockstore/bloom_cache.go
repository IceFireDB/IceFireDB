package blockstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	bloom "github.com/ipfs/bbloom"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	metrics "github.com/ipfs/go-metrics-interface"
)

// bloomCached returns a Blockstore that caches Has requests using a Bloom
// filter. bloomSize is size of bloom filter in bytes. hashCount specifies the
// number of hashing functions in the bloom filter (usually known as k).
func bloomCached(ctx context.Context, bs Blockstore, bloomSize, hashCount int) (*bloomcache, error) {
	bl, err := bloom.New(float64(bloomSize), float64(hashCount))
	if err != nil {
		return nil, err
	}
	bc := &bloomcache{
		blockstore: bs,
		bloomSize:  bloomSize,
		hashCount:  hashCount,
		hits: metrics.NewCtx(ctx, "bloom.hits_total",
			"Number of cache hits in bloom cache").Counter(),
		total: metrics.NewCtx(ctx, "bloom_total",
			"Total number of requests to bloom cache").Counter(),
		buildChan: make(chan struct{}),
	}
	bc.bloom.Store(bl)
	if v, ok := bs.(Viewer); ok {
		bc.viewer = v
	}
	go func() {
		err := bc.build(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				logger.Warn("Cache rebuild closed by context finishing: ", err)
			default:
				logger.Error(err)
			}
			return
		}
		if metrics.Active() {
			fill := metrics.NewCtx(ctx, "bloom_fill_ratio",
				"Ratio of bloom filter fullnes, (updated once a minute)").Gauge()

			t := time.NewTicker(1 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					fill.Set(bc.bloom.Load().FillRatioTS())
				}
			}
		}
	}()
	return bc, nil
}

type bloomcache struct {
	active atomic.Bool

	// bloom is the live filter. It is swapped atomically by Rebuild, so all
	// accesses go through Load.
	bloom     atomic.Pointer[bloom.Bloom]
	bloomSize int
	hashCount int

	// buildMu serializes the initial build and any Rebuild, so at most one
	// enumeration populates the filter at a time.
	buildMu  sync.Mutex
	buildErr error

	buildChan  chan struct{}
	blockstore Blockstore
	viewer     Viewer

	// Statistics
	hits  metrics.Counter
	total metrics.Counter
}

var (
	_ Blockstore           = (*bloomcache)(nil)
	_ Viewer               = (*bloomcache)(nil)
	_ AllKeysChanWithErrer = (*bloomcache)(nil)
	_ BloomCacheStatus     = (*bloomcache)(nil)
)

func (b *bloomcache) BloomActive() bool {
	return b.active.Load()
}

func (b *bloomcache) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.buildChan:
		return b.buildErr
	}
}

func (b *bloomcache) build(ctx context.Context) error {
	logger.Debug("begin building bloomcache")
	start := time.Now()
	defer func() {
		logger.Debugf("bloomcache build finished in %s", time.Since(start))
	}()
	defer close(b.buildChan)

	b.buildMu.Lock()
	defer b.buildMu.Unlock()

	if err := b.populate(ctx, b.bloom.Load()); err != nil {
		b.buildErr = err
		return err
	}
	b.active.Store(true)
	return nil
}

// Rebuild discards the current Bloom filter and rebuilds it from a full
// AllKeysChan enumeration. It is meant to retry after a failed initial build
// (observable via [BloomCacheStatus]).
//
// While the rebuild runs, the filter is inactive: lookups fall through to the
// underlying blockstore (correct, but without Bloom acceleration) and writes
// populate the new filter. On a complete enumeration the filter is activated;
// if enumeration is truncated or ctx is cancelled, the filter is left inactive
// and the error is returned.
//
// Rebuild serializes with the initial build and concurrent Rebuild calls: it
// waits for any in-progress build to finish before starting (that wait does
// not observe ctx), then honors ctx for the enumeration. It does not update
// Wait, which reports only the initial build; observe a rebuild via this
// method's return value and BloomActive. Reliable rebuild while the store is
// written concurrently assumes a snapshot-consistent datastore enumeration
// (see rebuildLocked).
func (b *bloomcache) Rebuild(ctx context.Context) error {
	b.buildMu.Lock()
	defer b.buildMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	fresh, err := bloom.New(float64(b.bloomSize), float64(b.hashCount))
	if err != nil {
		return err
	}
	// Deactivate and swap in the empty filter before enumerating: reads degrade
	// to correct pass-through and concurrent writes land in the new filter,
	// exactly as during the initial build.
	//
	// Correctness against a Put that races this swap relies on the underlying
	// datastore's enumeration reflecting every write that completed before the
	// Query below was issued. Snapshotting backends (the in-memory MapDatastore,
	// MutexWrap, LevelDB, Badger) provide this. A backend whose enumeration is
	// not a point-in-time snapshot (such as flatfs's lazy directory walk) may
	// instead leave a block written concurrently with a rebuild as a transient
	// false negative until the next rebuild: the bloom-pointer atomic orders
	// only the filter swap, not datastore visibility.
	b.active.Store(false)
	b.bloom.Store(fresh)

	if err := b.populate(ctx, fresh); err != nil {
		return err
	}
	b.active.Store(true)
	return nil
}

// populate enumerates every key in the underlying blockstore into target. It
// returns a non-nil error if the enumeration did not run to completion, in
// which case the caller must not treat target as authoritative.
func (b *bloomcache) populate(ctx context.Context, target *bloom.Bloom) error {
	ch, errFn, err := allKeysChanWithErrFor(ctx, b.blockstore)
	if err != nil {
		return fmt.Errorf("AllKeysChan failed in bloomcache build with: %w", err)
	}
	for {
		select {
		case key, ok := <-ch:
			if !ok {
				// A closed channel alone does not prove the enumeration was
				// complete: it could have been truncated by a mid-iteration
				// error or a cancelled context. Trust the filter only if every
				// key was delivered, otherwise the "not in bloom" answer would
				// be a false negative for blocks that exist but were never
				// indexed.
				//
				// If the wrapped store does not implement AllKeysChanWithErrer,
				// errFn is a no-op returning nil (see allKeysChanWithErrFor) and
				// the filter is treated as complete, preserving the pre-existing
				// best-effort behavior for such stores.
				if err := errFn(); err != nil {
					return fmt.Errorf("bloomcache build incomplete, not activating filter: %w", err)
				}
				return nil
			}
			target.AddTS(key.Hash()) // Use binary key, the more compact the better
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *bloomcache) DeleteBlock(ctx context.Context, k cid.Cid) error {
	if has, ok := b.hasCached(k); ok && !has {
		return nil
	}

	return b.blockstore.DeleteBlock(ctx, k)
}

// if ok == false has is inconclusive
// if ok == true then has respons to question: is it contained
func (b *bloomcache) hasCached(k cid.Cid) (has bool, ok bool) {
	b.total.Inc()
	if !k.Defined() {
		logger.Error("undefined in bloom cache")
		// Return cache invalid so call to blockstore
		// in case of invalid key is forwarded deeper
		return false, false
	}
	if b.BloomActive() {
		blr := b.bloom.Load().HasTS(k.Hash())
		if !blr { // not contained in bloom is only conclusive answer bloom gives
			b.hits.Inc()
			return false, true
		}
	}
	return false, false
}

func (b *bloomcache) Has(ctx context.Context, k cid.Cid) (bool, error) {
	if has, ok := b.hasCached(k); ok {
		return has, nil
	}

	return b.blockstore.Has(ctx, k)
}

func (b *bloomcache) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	if has, ok := b.hasCached(k); ok && !has {
		return -1, ipld.ErrNotFound{Cid: k}
	}

	return b.blockstore.GetSize(ctx, k)
}

func (b *bloomcache) View(ctx context.Context, k cid.Cid, callback func([]byte) error) error {
	if b.viewer == nil {
		blk, err := b.Get(ctx, k)
		if err != nil {
			return err
		}
		return callback(blk.RawData())
	}

	if has, ok := b.hasCached(k); ok && !has {
		return ipld.ErrNotFound{Cid: k}
	}
	return b.viewer.View(ctx, k, callback)
}

func (b *bloomcache) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if has, ok := b.hasCached(k); ok && !has {
		return nil, ipld.ErrNotFound{Cid: k}
	}

	return b.blockstore.Get(ctx, k)
}

func (b *bloomcache) Put(ctx context.Context, bl blocks.Block) error {
	// See comment in PutMany
	err := b.blockstore.Put(ctx, bl)
	if err == nil {
		b.bloom.Load().AddTS(bl.Cid().Hash())
	}
	return err
}

func (b *bloomcache) PutMany(ctx context.Context, bs []blocks.Block) error {
	// bloom cache gives only conclusive resulty if key is not contained
	// to reduce number of puts we need conclusive information if block is contained
	// this means that PutMany can't be improved with bloom cache so we just
	// just do a passthrough.
	err := b.blockstore.PutMany(ctx, bs)
	if err != nil {
		return err
	}
	for _, bl := range bs {
		b.bloom.Load().AddTS(bl.Cid().Hash())
	}
	return nil
}

func (b *bloomcache) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return b.blockstore.AllKeysChan(ctx)
}

func (b *bloomcache) AllKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	return allKeysChanWithErrFor(ctx, b.blockstore)
}

func (b *bloomcache) GCLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).GCLock(ctx)
}

func (b *bloomcache) PinLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).PinLock(ctx)
}

func (b *bloomcache) GCRequested(ctx context.Context) bool {
	return b.blockstore.(GCBlockstore).GCRequested(ctx)
}
