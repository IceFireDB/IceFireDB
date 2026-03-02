// package mfs implements an in memory model of a mutable IPFS filesystem.
// TODO: Develop on this line (and move it to `doc.go`).

package mfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/provider"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
)

// TODO: Remove if not used.
var (
	ErrNotExist = errors.New("no such rootfs")
	ErrClosed   = errors.New("file closed")
)

var log = logging.Logger("mfs")

// TODO: Remove if not used.
var ErrIsDirectory = errors.New("error: is a directory")

// The information that an MFS `Directory` has about its children
// when updating one of its entries: when a child mutates it signals
// its parent directory to update its entry (under `Name`) with the
// new content (in `Node`).
type child struct {
	Name string
	Node ipld.Node
}

// This interface represents the basic property of MFS directories of updating
// children entries with modified content. Implemented by both the MFS
// `Directory` and `Root` (which is basically a `Directory` with republishing
// support).
//
// TODO: What is `fullsync`? (unnamed `bool` argument)
// TODO: There are two types of persistence/flush that need to be
// distinguished here, one at the DAG level (when I store the modified
// nodes in the DAG service) and one in the UnixFS/MFS level (when I modify
// the entry/link of the directory that pointed to the modified node).
type parent interface {
	// Method called by a child to its parent to signal to update the content
	// pointed to in the entry by that child's name. The child sends its own
	// information in the `child` structure. As modifying a directory entry
	// entails modifying its contents the parent will also call *its* parent's
	// `updateChildEntry` to update the entry pointing to the new directory,
	// this mechanism is in turn repeated until reaching the `Root`.
	updateChildEntry(c child) error

	// getChunker returns the chunker factory for files, or nil for default.
	getChunker() chunker.SplitterGen
}

type NodeType int

const (
	TFile NodeType = iota
	TDir
)

const (
	repubQuick = 300 * time.Millisecond
	repubLong  = 3 * time.Second
)

// FSNode abstracts the `Directory` and `File` structures, it represents
// any child node in the MFS (i.e., all the nodes besides the `Root`). It
// is the counterpart of the `parent` interface which represents any
// parent node in the MFS (`Root` and `Directory`).
// (Not to be confused with the `unixfs.FSNode`.)
type FSNode interface {
	GetNode() (ipld.Node, error)

	Flush() error
	Type() NodeType
	SetModTime(ts time.Time) error
	SetMode(mode os.FileMode) error
}

// IsDir checks whether the FSNode is dir type
func IsDir(fsn FSNode) bool {
	return fsn.Type() == TDir
}

// IsFile checks whether the FSNode is file type
func IsFile(fsn FSNode) bool {
	return fsn.Type() == TFile
}

// Root represents the root of a filesystem tree.
type Root struct {
	// Root directory of the MFS layout.
	dir *Directory

	repub   *Republisher
	prov    provider.MultihashProvider
	chunker chunker.SplitterGen // chunker factory for files, nil means default

	// Directory settings to propagate to child directories when loaded from disk.
	maxLinks           int
	maxHAMTFanout      int
	hamtShardingSize   int
	sizeEstimationMode *uio.SizeEstimationMode
}

// RootOption is a functional option for configuring a Root.
type RootOption func(*Root)

// WithChunker sets the chunker factory for files created in this MFS.
// If not set (or nil), chunker.DefaultSplitter is used.
func WithChunker(c chunker.SplitterGen) RootOption {
	return func(r *Root) {
		r.chunker = c
	}
}

// WithMaxLinks sets the max links for directories created in this MFS.
// This is used to determine when to switch between BasicDirectory and HAMTDirectory.
func WithMaxLinks(n int) RootOption {
	return func(r *Root) {
		r.maxLinks = n
	}
}

// WithSizeEstimationMode sets the size estimation mode for directories in this MFS.
// This controls how directory size is estimated for HAMT sharding decisions.
func WithSizeEstimationMode(mode uio.SizeEstimationMode) RootOption {
	return func(r *Root) {
		r.sizeEstimationMode = &mode
	}
}

// WithMaxHAMTFanout sets the max fanout (width) for HAMT directories in this MFS.
// This controls the maximum number of children per HAMT bucket.
// Must be a power of 2 and multiple of 8.
func WithMaxHAMTFanout(n int) RootOption {
	return func(r *Root) {
		r.maxHAMTFanout = n
	}
}

// WithHAMTShardingSize sets the per-directory size threshold for switching to HAMT.
// When a directory's estimated size exceeds this threshold, it converts to HAMT.
// If not set (0), the global uio.HAMTShardingSize is used.
func WithHAMTShardingSize(size int) RootOption {
	return func(r *Root) {
		r.hamtShardingSize = size
	}
}

// NewRoot creates a new Root and starts up a republisher routine for it.
func NewRoot(ctx context.Context, ds ipld.DAGService, node *dag.ProtoNode, pf PubFunc, prov provider.MultihashProvider, opts ...RootOption) (*Root, error) {
	var repub *Republisher
	if pf != nil {
		repub = NewRepublisher(pf, repubQuick, repubLong, node.Cid())
	}

	root := &Root{
		repub: repub,
	}

	for _, opt := range opts {
		opt(root)
	}

	fsn, err := ft.FSNodeFromBytes(node.Data())
	if err != nil {
		log.Error("IPNS pointer was not unixfs node")
		// TODO: IPNS pointer?
		return nil, err
	}

	switch fsn.Type() {
	case ft.TDirectory, ft.THAMTShard:
		newDir, err := NewDirectory(ctx, node.String(), node, root, ds, prov)
		if err != nil {
			return nil, err
		}

		// Apply root-level directory settings
		if root.maxLinks > 0 {
			newDir.unixfsDir.SetMaxLinks(root.maxLinks)
		}
		if root.maxHAMTFanout > 0 {
			newDir.unixfsDir.SetMaxHAMTFanout(root.maxHAMTFanout)
		}
		if root.hamtShardingSize > 0 {
			newDir.unixfsDir.SetHAMTShardingSize(root.hamtShardingSize)
		}
		if root.sizeEstimationMode != nil {
			newDir.unixfsDir.SetSizeEstimationMode(*root.sizeEstimationMode)
		}

		root.dir = newDir
	case ft.TFile, ft.TMetadata, ft.TRaw:
		return nil, fmt.Errorf("root can't be a file (unixfs type: %s)", fsn.Type())
		// TODO: This special error reporting case doesn't seem worth it, we either
		// have a UnixFS directory or we don't.
	default:
		return nil, fmt.Errorf("unrecognized unixfs type: %s", fsn.Type())
	}
	return root, nil
}

// NewEmptyRoot creates an empty Root directory with the given directory
// options. A republisher is created if PubFunc is not nil.
func NewEmptyRoot(ctx context.Context, ds ipld.DAGService, pf PubFunc, prov provider.MultihashProvider, mkdirOpts MkdirOpts, rootOpts ...RootOption) (*Root, error) {
	root := new(Root)

	for _, opt := range rootOpts {
		opt(root)
	}

	// Pass settings from root opts to mkdir opts if not already set
	if mkdirOpts.Chunker == nil {
		mkdirOpts.Chunker = root.chunker
	}
	if mkdirOpts.MaxHAMTFanout == 0 && root.maxHAMTFanout > 0 {
		mkdirOpts.MaxHAMTFanout = root.maxHAMTFanout
	}
	if mkdirOpts.HAMTShardingSize == 0 && root.hamtShardingSize > 0 {
		mkdirOpts.HAMTShardingSize = root.hamtShardingSize
	}

	dir, err := NewEmptyDirectory(ctx, "", root, ds, prov, mkdirOpts)
	if err != nil {
		return nil, err
	}

	// Rather than "dir.GetNode()" because it is cheaper and we have no
	// risks.
	nd, err := dir.unixfsDir.GetNode()
	if err != nil {
		return nil, err
	}

	var repub *Republisher
	if pf != nil {
		repub = NewRepublisher(pf, repubQuick, repubLong, nd.Cid())
	}

	root.repub = repub
	root.dir = dir
	return root, nil
}

// GetDirectory returns the root directory.
func (kr *Root) GetDirectory() *Directory {
	return kr.dir
}

// GetChunker returns the chunker factory, or nil if using default.
func (kr *Root) GetChunker() chunker.SplitterGen {
	return kr.chunker
}

// getChunker implements the parent interface.
func (kr *Root) getChunker() chunker.SplitterGen {
	return kr.chunker
}

// Flush signals that an update has occurred since the last publish,
// and updates the Root republisher.
// TODO: We are definitely abusing the "flush" terminology here.
func (kr *Root) Flush() error {
	nd, err := kr.GetDirectory().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
	}
	return nil
}

// FlushMemFree flushes the root directory and then uncaches all of its links.
// This has the effect of clearing out potentially stale references and allows
// them to be garbage collected.
// CAUTION: Take care not to ever call this while holding a reference to any
// child directories. Those directories will be bad references and using them
// may have unintended racy side effects.
// A better implemented mfs system (one that does smarter internal caching and
// refcounting) shouldnt need this method.
// TODO: Review the motivation behind this method once the cache system is
// refactored.
func (kr *Root) FlushMemFree(ctx context.Context) error {
	dir := kr.GetDirectory()

	return dir.Flush()
}

// updateChildEntry implements the `parent` interface, and signals
// to the publisher that there are changes ready to be published.
// This is the only thing that separates a `Root` from a `Directory`.
// TODO: Evaluate merging both.
// TODO: The `sync` argument isn't used here (we've already reached
// the top), document it and maybe make it an anonymous variable (if
// that's possible).
func (kr *Root) updateChildEntry(c child) error {
	err := kr.GetDirectory().dagService.Add(context.TODO(), c.Node)
	if err != nil {
		return err
	}

	// TODO: Why are we not using the inner directory lock nor
	// applying the same procedure as `Directory.updateChildEntry`?

	if kr.prov != nil {
		log.Debugf("mfs: provide: %s", c.Node.Cid())
		if err := kr.prov.StartProviding(false, c.Node.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", c.Node.Cid(), err)
		}
	}

	if kr.repub != nil {
		kr.repub.Update(c.Node.Cid())
	}
	return nil
}

func (kr *Root) Close() error {
	nd, err := kr.GetDirectory().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
		return kr.repub.Close()
	}

	return nil
}
