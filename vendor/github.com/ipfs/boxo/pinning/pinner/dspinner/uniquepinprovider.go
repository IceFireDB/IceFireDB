package dspinner

import (
	"context"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/dag/walker"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// NewUniquePinnedProvider returns a [provider.KeyChanFunc] that emits
// all blocks reachable from pinned roots, with bloom filter cross-pin
// deduplication via the shared [walker.VisitedTracker].
//
// Processing order: recursive pin DAGs first (via [walker.WalkDAG]),
// then direct pins. This order ensures that by the time direct pins
// are processed, all recursive DAGs have been walked and their CIDs
// are in the tracker.
//
// The existing [NewPinnedProvider] is unchanged. This function is used
// only when the +unique strategy modifier is active.
func NewUniquePinnedProvider(
	pinning ipfspinner.Pinner,
	bs blockstore.Blockstore,
	tracker walker.VisitedTracker,
) provider.KeyChanFunc {
	fetch := walker.LinksFetcherFromBlockstore(bs)
	return newPinnedProvider(pinning, tracker, func(ctx context.Context, root cid.Cid, emit func(cid.Cid) bool) error {
		return walker.WalkDAG(ctx, root, fetch, emit, walker.WithVisitedTracker(tracker))
	}, "unique provide")
}

// NewPinnedEntityRootsProvider returns a [provider.KeyChanFunc] that
// emits entity roots (files, directories, HAMT shards) reachable from
// pinned roots, skipping internal file chunks. Uses
// [walker.WalkEntityRoots] with the shared [walker.VisitedTracker]
// for cross-pin deduplication.
//
// Same processing order as [NewUniquePinnedProvider]: recursive pins
// first, direct pins second.
func NewPinnedEntityRootsProvider(
	pinning ipfspinner.Pinner,
	bs blockstore.Blockstore,
	tracker walker.VisitedTracker,
) provider.KeyChanFunc {
	fetch := walker.NodeFetcherFromBlockstore(bs)
	return newPinnedProvider(pinning, tracker, func(ctx context.Context, root cid.Cid, emit func(cid.Cid) bool) error {
		return walker.WalkEntityRoots(ctx, root, fetch, emit, walker.WithVisitedTracker(tracker))
	}, "entity provide")
}

// newPinnedProvider is the shared implementation for
// [NewUniquePinnedProvider] and [NewPinnedEntityRootsProvider]. The
// walk callback performs the actual DAG traversal for each recursive
// pin root.
func newPinnedProvider(
	pinning ipfspinner.Pinner,
	tracker walker.VisitedTracker,
	walk func(ctx context.Context, root cid.Cid, emit func(cid.Cid) bool) error,
	logPrefix string,
) provider.KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)

			emit := func(c cid.Cid) bool {
				select {
				case outCh <- c:
					return true
				case <-ctx.Done():
					return false
				}
			}

			// 1. Walk recursive pin DAGs (bulk of dedup benefit).
			// A corrupted pin entry is logged and skipped so it does
			// not prevent remaining pins from being provided.
			for sc := range pinning.RecursiveKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("%s recursive pins: %s", logPrefix, sc.Err)
					continue
				}
				if err := walk(ctx, sc.Pin.Key, emit); err != nil {
					return // context cancelled
				}
			}

			// 2. Direct pins (emit if not already visited).
			// Same best-effort: skip corrupted entries.
			for sc := range pinning.DirectKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("%s direct pins: %s", logPrefix, sc.Err)
					continue
				}
				// skip identity CIDs: content is inline, no need to provide
				if sc.Pin.Key.Prefix().MhType == mh.IDENTITY {
					continue
				}
				// skip if already visited (by a recursive pin walk above)
				if !tracker.Visit(sc.Pin.Key) {
					continue
				}
				// emit returns false when context is cancelled
				// (consumer stopped reading from the channel)
				if !emit(sc.Pin.Key) {
					return
				}
			}
		}()

		return outCh, nil
	}
}
