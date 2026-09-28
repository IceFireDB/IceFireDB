package mfs

import (
	"os"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
)

// Option configures MFS root and directory creation.
//
// Options follow the functional options pattern used by the underlying
// [uio.DirectoryOption] in ipld/unixfs/io. Most settings are inherited
// from the parent directory when not explicitly set.
//
// Chunker is the only setting that uses a different inheritance path:
// it is stored per-directory and accessed by files via the [parent]
// interface's getChunker method, rather than being copied through
// options. This is because chunker is consumed by [File.Open] (not
// by the unixfs directory layer), and a single interface method is a
// cleaner abstraction than explicit copying for a value that never
// changes after root creation.
type Option func(*options)

// options holds resolved configuration. Zero values mean "unset" and
// will be inherited from the parent directory or fall back to defaults.
type options struct {
	cidBuilder         cid.Builder
	chunker            chunker.SplitterGen
	maxLinks           int
	maxHAMTFanout      int
	hamtShardingSize   int
	sizeEstimationMode *uio.SizeEstimationMode
	mode               os.FileMode
	modTime            time.Time
	fetchTimeout       time.Duration
}

// DefaultFetchTimeout is the fetch timeout applied to an MFS root that does not
// set its own with WithFetchTimeout. It bounds a single under-lock DAG read (a
// child lookup or HAMT-shard walk) so a missing, unreachable block fails with a
// deadline error instead of blocking MFS forever.
//
// Five minutes is deliberately generous. These reads fetch small directory and
// shard nodes: one resolves in well under a second from a connected peer, and
// usually within tens of seconds even when its provider must be found through
// the DHT, so a live-but-slow fetch never reaches the bound. It only takes
// effect against genuinely unreachable data, trading a permanent wedge for a
// recoverable error. On teardown the root context cancels the read at once, so
// the bound never delays shutdown.
//
// Pass WithFetchTimeout(0) to disable it and restore unbounded reads.
const DefaultFetchTimeout = 5 * time.Minute

func resolveOpts(opts []Option) options {
	o := options{fetchTimeout: DefaultFetchTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// fillFrom fills unset values from the given directory's current settings.
//
// Chunker is deliberately excluded: it is inherited via the [parent]
// interface's getChunker method instead of explicit copying (see the
// [Option] type documentation for rationale).
func (o *options) fillFrom(d *Directory) {
	if o.cidBuilder == nil {
		o.cidBuilder = d.unixfsDir.GetCidBuilder()
	}
	if o.maxLinks == 0 {
		o.maxLinks = d.unixfsDir.GetMaxLinks()
	}
	if o.maxHAMTFanout == 0 {
		o.maxHAMTFanout = d.unixfsDir.GetMaxHAMTFanout()
	}
	if o.hamtShardingSize == 0 {
		o.hamtShardingSize = d.unixfsDir.GetHAMTShardingSize()
	}
	if o.sizeEstimationMode == nil {
		mode := d.unixfsDir.GetSizeEstimationMode()
		o.sizeEstimationMode = &mode
	}
}

// WithCidBuilder sets the CID builder (version, codec, hash function)
// for new directories. If not set, the parent directory's builder is used.
func WithCidBuilder(b cid.Builder) Option {
	return func(o *options) {
		o.cidBuilder = b
	}
}

// WithChunker sets the chunker factory for files created under this
// MFS root. If not set, [chunker.DefaultSplitter] is used.
//
// Unlike other options, the chunker is not propagated through [options]
// but through the [parent] interface (see [Option] for details).
// This option only takes effect when passed to [NewRoot] or [NewEmptyRoot].
func WithChunker(c chunker.SplitterGen) Option {
	return func(o *options) {
		o.chunker = c
	}
}

// WithMaxLinks sets the maximum number of directory entries before
// the directory is converted to a HAMT shard.
func WithMaxLinks(n int) Option {
	return func(o *options) {
		o.maxLinks = n
	}
}

// WithMaxHAMTFanout sets the maximum fanout (bucket width) for HAMT
// sharded directories. Must be a power of 2 and a multiple of 8.
func WithMaxHAMTFanout(n int) Option {
	return func(o *options) {
		o.maxHAMTFanout = n
	}
}

// WithHAMTShardingSize sets the per-directory serialized block size
// threshold in bytes for converting to a HAMT shard.
// If not set, the global [uio.HAMTShardingSize] is used.
func WithHAMTShardingSize(size int) Option {
	return func(o *options) {
		o.hamtShardingSize = size
	}
}

// WithSizeEstimationMode sets the method used to estimate directory
// size for HAMT sharding threshold decisions.
func WithSizeEstimationMode(mode uio.SizeEstimationMode) Option {
	return func(o *options) {
		o.sizeEstimationMode = &mode
	}
}

// WithFetchTimeout bounds each DAG read that MFS performs while holding a
// directory lock: resolving a child link, or walking a HAMT shard. When unset,
// DefaultFetchTimeout applies; pass zero to disable the bound and restore
// unbounded reads.
//
// MFS routinely holds a DAG that is only lazily linked: the root node is
// present locally while the rest of the tree is fetched from the network on
// demand, the first time a path is traversed. This is what backs a reference
// like the one "ipfs files cp /ipfs/<cid> /dst" creates, where only the
// referenced root is stored and its children are pulled in as they are
// accessed. Those reads must stay online, so this is a timeout, not a switch
// to local-only lookups: a reachable remote node is still fetched. Directory
// and shard nodes are small, so a present-or-reachable one resolves well
// within a generous bound.
//
// The bound only bites when a node is neither local nor reachable: a
// lazily-linked child whose providers are gone, a block removed out of band,
// or a store left inconsistent by a crash. Without a bound that read blocks on
// the network forever while the directory lock is held, wedging every
// operation queued behind it, and graceful shutdown (which flushes MFS) with
// it. With one the read fails with a deadline error and releases the lock,
// turning a permanent wedge into an ordinary, recoverable error. See
// DefaultFetchTimeout for the default and its rationale.
//
// This option only takes effect when passed to NewRoot or NewEmptyRoot; it is
// inherited by every directory under the root.
func WithFetchTimeout(d time.Duration) Option {
	return func(o *options) {
		o.fetchTimeout = d
	}
}

// WithMode sets the Unix permission bits on the created directory.
func WithMode(mode os.FileMode) Option {
	return func(o *options) {
		o.mode = mode
	}
}

// WithModTime sets the modification time on the created directory.
func WithModTime(t time.Time) Option {
	return func(o *options) {
		o.modTime = t
	}
}
