// Package walker provides memory-efficient DAG traversal with
// deduplication. Optimized for the IPFS provide system, but useful
// anywhere repeated DAG walks need to skip already-visited subtrees.
//
// The primary entry point is [WalkDAG], which walks a DAG rooted at a
// given CID, emitting each visited CID to a callback. When combined
// with a [VisitedTracker] (e.g. [BloomTracker]), entire subtrees
// already seen are skipped in O(1).
//
// For entity-aware traversal that only emits file/directory/HAMT roots
// instead of every block, see [WalkEntityRoots].
//
// Blocks are decoded using the codecs registered in the process via
// the global multicodec registry. In a standard kubo build this
// includes dag-pb, dag-cbor, dag-json, cbor, json, and raw.
//
// Use [LinksFetcherFromBlockstore] to create a fetcher backed by a
// local blockstore. For custom link extraction (e.g. a different codec
// registry or non-blockstore storage), pass your own [LinksFetcher]
// function directly to [WalkDAG].
package walker
