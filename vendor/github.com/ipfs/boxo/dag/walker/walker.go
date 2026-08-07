package walker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"

	blockstore "github.com/ipfs/boxo/blockstore"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("dagwalker")

// LinksFetcher returns child link CIDs for a given CID.
// Used by [WalkDAG] which doesn't need entity type information.
type LinksFetcher func(ctx context.Context, c cid.Cid) ([]cid.Cid, error)

// Option configures [WalkDAG] and [WalkEntityRoots].
type Option func(*walkConfig)

type walkConfig struct {
	tracker  VisitedTracker
	locality func(context.Context, cid.Cid) (bool, error)
}

// WithVisitedTracker sets the tracker used for cross-walk
// deduplication. When set, CIDs already visited (by this walk or a
// previous walk sharing the same tracker) are skipped along with
// their entire subtree.
func WithVisitedTracker(t VisitedTracker) Option {
	return func(c *walkConfig) { c.tracker = t }
}

// WithLocality sets a check function that determines whether a CID is
// locally available. When set, the walker only emits and descends into
// CIDs for which check returns true. Used by MFS providers to skip
// blocks not in the local blockstore (pass blockstore.Has directly).
//
// The locality check runs after the [VisitedTracker] check (which is
// a cheap in-memory operation), so already-visited CIDs never pay the
// locality I/O cost.
func WithLocality(check func(context.Context, cid.Cid) (bool, error)) Option {
	return func(c *walkConfig) { c.locality = check }
}

// WalkDAG performs an iterative depth-first walk of the DAG rooted at
// root, calling emit for each visited CID. Returns when the DAG is
// fully walked, emit returns false, or ctx is cancelled.
//
// The walk uses an explicit stack (not recursion) to avoid stack
// overflow on deep DAGs. For each CID:
//
//  1. [VisitedTracker].Visit -- if already visited, skip entire subtree.
//     The CID is marked visited immediately (before fetch). If fetch
//     later fails, the CID stays in the tracker and won't be retried
//     this cycle, but is caught in the next reprovide cycle. This
//     avoids a double bloom scan per CID.
//  2. If [WithLocality] is set, check locality -- if not local, skip.
//  3. Fetch block via fetch -- on error, log and skip.
//  4. Push child link CIDs to stack (deduped when popped at step 1).
//  5. Call emit(c) -- return false to stop the walk.
//
// # Traversal order
//
// Pre-order DFS with left-to-right sibling visiting: the root CID is
// always emitted first, and children are visited in the order they
// appear in the block's link list. This matches the legacy
// fetcherhelpers.BlockAll selector traversal and the conventional DFS
// order described in IPIP-0412.
func WalkDAG(
	ctx context.Context,
	root cid.Cid,
	fetch LinksFetcher,
	emit func(cid.Cid) bool,
	opts ...Option,
) error {
	cfg := &walkConfig{}
	for _, o := range opts {
		o(cfg)
	}
	return walkLoop(ctx, root, func(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
		return fetch(ctx, c)
	}, emit, cfg)
}

// walkLoop is the shared iterative DFS implementation used by both
// [WalkDAG] and [WalkEntityRoots]. The fetch callback returns the
// children to push onto the stack; returning nil means "do not descend"
// (the CID is still emitted unless it is an identity CID).
func walkLoop(
	ctx context.Context,
	root cid.Cid,
	fetch func(context.Context, cid.Cid) ([]cid.Cid, error),
	emit func(cid.Cid) bool,
	cfg *walkConfig,
) error {
	stack := []cid.Cid{root}

	for len(stack) > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// pop
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// visit (mark + dedup in one call). If the CID was already
		// visited (by this walk or a prior walk sharing the tracker),
		// skip it and its entire subtree.
		if cfg.tracker != nil && !cfg.tracker.Visit(c) {
			continue
		}

		// locality check
		if cfg.locality != nil {
			local, err := cfg.locality(ctx, c)
			if err != nil {
				log.Errorf("walk: locality check %s: %s", c, err)
				continue
			}
			if !local {
				continue
			}
		}

		// fetch and get children to descend into
		children, err := fetch(ctx, c)
		if err != nil {
			log.Errorf("walk: fetch %s: %s", c, err)
			continue
		}

		// push children in reverse order so the first link is on top
		// of the stack and gets popped next (left-to-right sibling
		// visit order, matching IPIP-0412 pre-order DFS).
		slices.Reverse(children)
		stack = append(stack, children...)

		// skip identity CIDs: content is inline, no need to provide.
		// children are still pushed (above) so an inlined dag-pb
		// directory's normal children get walked.
		if c.Prefix().MhType == mh.IDENTITY {
			continue
		}
		if !emit(c) {
			return nil
		}
	}

	return nil
}

// linkSystemForBlockstore creates an ipld.LinkSystem backed by a
// blockstore, used by both [LinksFetcherFromBlockstore] and
// [NodeFetcherFromBlockstore]. The blockstore is wrapped with
// [blockstore.NewIdStore] so identity CIDs (multihash code 0x00,
// data inline in the CID) are decoded transparently without
// requiring a datastore lookup.
func linkSystemForBlockstore(bs blockstore.Blockstore) ipld.LinkSystem {
	idBS := blockstore.NewIdStore(bs)
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cl, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link type: %T", lnk)
		}
		blk, err := idBS.Get(lctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
	return ls
}

// LinksFetcherFromBlockstore creates a [LinksFetcher] backed by a
// local blockstore. Blocks are decoded using the codecs registered in
// the global multicodec registry (via ipld-prime's
// [cidlink.DefaultLinkSystem]). Identity CIDs are handled
// transparently via [blockstore.NewIdStore].
//
// For custom link extraction, pass your own [LinksFetcher] to
// [WalkDAG] directly.
func LinksFetcherFromBlockstore(bs blockstore.Blockstore) LinksFetcher {
	ls := linkSystemForBlockstore(bs)

	return func(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
		lnk := cidlink.Link{Cid: c}
		nd, err := ls.Load(ipld.LinkContext{Ctx: ctx}, lnk, basicnode.Prototype.Any)
		if err != nil {
			return nil, err
		}
		return collectLinks(c, nd), nil
	}
}

// collectLinks extracts all link CIDs from an ipld-prime node by
// recursively traversing maps, lists, and scalar link values. Only map
// values are inspected, not keys (no known IPLD codec uses link-typed
// map keys). parent is used for debug logging only.
func collectLinks(parent cid.Cid, nd ipld.Node) []cid.Cid {
	var links []cid.Cid
	collectLinksRecursive(parent, nd, &links)
	return links
}

func collectLinksRecursive(parent cid.Cid, nd ipld.Node, out *[]cid.Cid) {
	switch nd.Kind() {
	case ipld.Kind_Link:
		lnk, err := nd.AsLink()
		if err != nil {
			log.Debugw("walk: link extraction failed", "cid", parent, "error", err)
			return
		}
		if cl, ok := lnk.(cidlink.Link); ok {
			*out = append(*out, cl.Cid)
		}
	case ipld.Kind_Map:
		itr := nd.MapIterator()
		for !itr.Done() {
			_, v, err := itr.Next()
			if err != nil {
				log.Debugw("walk: map iteration failed", "cid", parent, "error", err)
				break
			}
			collectLinksRecursive(parent, v, out)
		}
	case ipld.Kind_List:
		itr := nd.ListIterator()
		for !itr.Done() {
			_, v, err := itr.Next()
			if err != nil {
				log.Debugw("walk: list iteration failed", "cid", parent, "error", err)
				break
			}
			collectLinksRecursive(parent, v, out)
		}
	}
}
