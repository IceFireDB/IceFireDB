package ipfs

import (
	"context"

	"github.com/ipfs-shipyard/nopfs"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
)

var _ namesys.NameSystem = (*NameSystem)(nil)

// NameSystem implements a blocking namesys.NameSystem implementation.
type NameSystem struct {
	blocker *nopfs.Blocker
	ns      namesys.NameSystem
}

// WrapNameSystem wraps the given NameSystem with a content-blocking layer
// for Resolve operations.
func WrapNameSystem(ns namesys.NameSystem, blocker *nopfs.Blocker) namesys.NameSystem {
	logger.Debug("NameSystem wrapped with content blocker")
	return &NameSystem{
		blocker: blocker,
		ns:      ns,
	}
}

// Resolve resolves an IPNS name unless it is blocked.
func (ns *NameSystem) Resolve(ctx context.Context, p path.Path, options ...namesys.ResolveOption) (namesys.Result, error) {
	if err := ns.blocker.IsPathBlocked(p).ToError(); err != nil {
		logger.Warn(err.Response)
		return namesys.Result{}, err
	}
	return ns.ns.Resolve(ctx, p, options...)
}

// ResolveAsync resolves an IPNS name asynchronously unless it is blocked.
func (ns *NameSystem) ResolveAsync(ctx context.Context, p path.Path, options ...namesys.ResolveOption) <-chan namesys.AsyncResult {
	status := ns.blocker.IsPathBlocked(p)
	if err := status.ToError(); err != nil {
		logger.Warn(err.Response)
		ch := make(chan namesys.AsyncResult, 1)
		ch <- namesys.AsyncResult{
			Path: status.Path,
			Err:  err,
		}
		close(ch)
		return ch
	}

	return ns.ns.ResolveAsync(ctx, p, options...)
}

// Publish publishes an IPNS record.
func (ns *NameSystem) Publish(ctx context.Context, name crypto.PrivKey, value path.Path, options ...namesys.PublishOption) error {
	return ns.ns.Publish(ctx, name, value, options...)
}
