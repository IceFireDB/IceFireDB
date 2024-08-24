package format

import (
	"context"
	"errors"

	cid "github.com/ipfs/go-cid"
)

// ErrNotFound is used to signal when a Node could not be found. The specific
// meaning will depend on the DAGService implementation, which may be trying
// to read nodes locally but also, trying to find them remotely.
//
// The Cid field can be filled in to provide additional context.
type ErrNotFound struct {
	Cid cid.Cid
}

// Error implements the error interface and returns a human-readable
// message for this error.
func (e ErrNotFound) Error() string {
	if e.Cid == cid.Undef {
		return "ipld: could not find node"
	}

	return "ipld: could not find " + e.Cid.String()
}

// Is allows to check whether any error is of this ErrNotFound type.
// Do not use this directly, but rather errors.Is(yourError, ErrNotFound).
func (e ErrNotFound) Is(err error) bool {
	switch err.(type) {
	case ErrNotFound:
		return true
	default:
		return false
	}
}

// NotFound returns true.
func (e ErrNotFound) NotFound() bool {
	return true
}

// IsNotFound returns if the given error is or wraps an ErrNotFound
// (equivalent to errors.Is(err, ErrNotFound{}))
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound{})
}

// Either a node or an error.
type NodeOption struct {
	Node Node
	Err  error
}

// The basic Node resolution service.
type NodeGetter interface {
	// Get retrieves nodes by CID. Depending on the NodeGetter
	// implementation, this may involve fetching the Node from a remote
	// machine; consider setting a deadline in the context.
	Get(context.Context, cid.Cid) (Node, error)

	// GetMany returns a channel of NodeOptions given a set of CIDs.
	GetMany(context.Context, []cid.Cid) <-chan *NodeOption
}

// NodeAdder adds nodes to a DAG.
type NodeAdder interface {
	// Add adds a node to this DAG.
	Add(context.Context, Node) error

	// AddMany adds many nodes to this DAG.
	//
	// Consider using the Batch NodeAdder (`NewBatch`) if you make
	// extensive use of this function.
	AddMany(context.Context, []Node) error
}

// NodeGetters can optionally implement this interface to make finding linked
// objects faster.
type LinkGetter interface {
	NodeGetter

	// TODO(ipfs/go-ipld-format#9): This should return []cid.Cid

	// GetLinks returns the children of the node refered to by the given
	// CID.
	GetLinks(ctx context.Context, nd cid.Cid) ([]*Link, error)
}

// DAGService is an IPFS Merkle DAG service.
type DAGService interface {
	NodeGetter
	NodeAdder

	// Remove removes a node from this DAG.
	//
	// Remove returns no error if the requested node is not present in this DAG.
	Remove(context.Context, cid.Cid) error

	// RemoveMany removes many nodes from this DAG.
	//
	// It returns success even if the nodes were not present in the DAG.
	RemoveMany(context.Context, []cid.Cid) error
}
