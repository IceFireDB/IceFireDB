package mfs

import (
	"sync/atomic"

	"github.com/ipfs/boxo/provider"
	ipld "github.com/ipfs/go-ipld-format"
)

// inode abstracts the common characteristics of the MFS `File`
// and `Directory`. All of its attributes are initialized at
// creation.
type inode struct {
	// name of this `inode` in the MFS path (the same value
	// is also stored as the name of the DAG link).
	name string

	// parent directory of this `inode` (which may be the `Root`).
	parent parent

	// dagService used to store modifications made to the contents
	// of the file or directory the `inode` belongs to.
	dagService ipld.DAGService

	// provider used to announce CIDs
	prov provider.MultihashProvider

	// unlinked prevents flushUp from re-adding this entry to the
	// parent directory after Unlink removed it.
	//
	// This is not a problem for `ipfs files mv` (Mv in ops.go)
	// because it adds the new name before removing the old one,
	// and both happen synchronously in the same goroutine.
	//
	// With FUSE, the kernel sends RELEASE (triggers Close/flushUp)
	// and RENAME (triggers Unlink) as separate concurrent requests.
	// We cannot control the order, and serializing them at the FUSE
	// layer would block all operations on the directory. This flag
	// is checked once per file close in flushUp to skip propagation
	// when the entry no longer exists in the parent.
	unlinked atomic.Bool
}
