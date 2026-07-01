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
}

func resolveOpts(opts []Option) options {
	var o options
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
