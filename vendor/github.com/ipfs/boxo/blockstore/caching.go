package blockstore

import (
	"context"
	"errors"

	metrics "github.com/ipfs/go-metrics-interface"
)

// CacheOpts wraps options for CachedBlockStore().
// Next to each option is it approximate memory usage per unit
type CacheOpts struct {
	HasBloomFilterSize   int // 1 byte
	HasBloomFilterHashes int // No size, 7 is usually best, consult bloom papers
	HasTwoQueueCacheSize int // 32 bytes
}

// DefaultCacheOpts returns a CacheOpts initialized with default values.
func DefaultCacheOpts() CacheOpts {
	return CacheOpts{
		HasBloomFilterSize:   512 << 10,
		HasBloomFilterHashes: 7,
		HasTwoQueueCacheSize: 64 << 10,
	}
}

// BloomCacheStatus may be implemented by the Blockstore returned from
// [CachedBlockstore] when a Bloom filter is configured
// (HasBloomFilterSize > 0). It lets callers observe the initial, asynchronous
// Bloom filter build:
//
//	cbs, err := CachedBlockstore(ctx, bs, opts)
//	if err != nil {
//		// handle err
//	}
//	if s, ok := cbs.(BloomCacheStatus); ok {
//		if err := s.Wait(ctx); err != nil {
//			// The filter is not active: the blockstore still answers
//			// correctly, but without Bloom-filter acceleration.
//		}
//	}
type BloomCacheStatus interface {
	// Wait blocks until the initial Bloom filter build finishes, or until ctx
	// is done, and returns that build's error, if any. It reflects only the
	// initial build: a later Rebuild does not update it, so after calling
	// Rebuild use that method's return value and BloomActive instead. A non-nil
	// initial-build error means the filter is not active; the blockstore still
	// returns correct answers, just without Bloom acceleration.
	Wait(ctx context.Context) error

	// BloomActive reports whether the Bloom filter finished building
	// successfully and is being used to answer negative lookups.
	BloomActive() bool

	// Rebuild discards the current Bloom filter and rebuilds it from a full
	// enumeration of the blockstore, returning the build error if any. It is
	// meant to retry after a failed initial build (Wait returned an error).
	// While it runs, the filter is inactive and lookups fall through to the
	// underlying blockstore, so results stay correct but unaccelerated; on a
	// failed rebuild the filter is left inactive.
	//
	// Rebuild serializes with the initial build and concurrent Rebuild calls;
	// it waits for any in-progress build to finish before starting (that wait
	// does not observe ctx), then honors ctx for the enumeration itself.
	//
	// Reliable rebuild while the store is written concurrently assumes the
	// datastore's enumeration reflects all writes that completed before it
	// began; a backend without that property (e.g. a lazy directory walk) may
	// leave a block written during the rebuild as a transient false negative
	// until the next rebuild.
	Rebuild(ctx context.Context) error
}

// CachedBlockstore returns a blockstore wrapped in an TwoQueueCache and
// then in a bloom filter cache, if the options indicate it.
//
// When a Bloom filter is configured, the returned Blockstore implements
// [BloomCacheStatus], which can be used to wait for and check the result of
// the initial Bloom filter build.
func CachedBlockstore(
	ctx context.Context,
	bs Blockstore,
	opts CacheOpts,
) (cbs Blockstore, err error) {
	cbs = bs

	if opts.HasBloomFilterSize < 0 || opts.HasBloomFilterHashes < 0 ||
		opts.HasTwoQueueCacheSize < 0 {
		return nil, errors.New("all options for cache need to be greater than zero")
	}

	if opts.HasBloomFilterSize != 0 && opts.HasBloomFilterHashes == 0 {
		return nil, errors.New("bloom filter hash count can't be 0 when there is size set")
	}

	ctx = metrics.CtxSubScope(ctx, "bs.cache")

	if opts.HasTwoQueueCacheSize > 0 {
		cbs, err = newTwoQueueCachedBS(ctx, cbs, opts.HasTwoQueueCacheSize)
	}
	if opts.HasBloomFilterSize != 0 {
		// *8 because of bytes to bits conversion
		cbs, err = bloomCached(ctx, cbs, opts.HasBloomFilterSize*8, opts.HasBloomFilterHashes)
	}

	return cbs, err
}
