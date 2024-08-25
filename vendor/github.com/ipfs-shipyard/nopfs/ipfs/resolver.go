package ipfs

import (
	"context"

	"github.com/ipfs-shipyard/nopfs"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

var _ resolver.Resolver = (*Resolver)(nil)

// Resolver implements a blocking path.Resolver.
type Resolver struct {
	blocker  *nopfs.Blocker
	resolver resolver.Resolver
}

// WrapResolver wraps the given path Resolver with a content-blocking layer
// for Resolve operations.
func WrapResolver(res resolver.Resolver, blocker *nopfs.Blocker) resolver.Resolver {
	logger.Debugf("Path resolved wrapped with content blocker")
	return &Resolver{
		blocker:  blocker,
		resolver: res,
	}
}

// ResolveToLastNode checks if the given path is blocked before resolving.
func (res *Resolver) ResolveToLastNode(ctx context.Context, fpath path.ImmutablePath) (cid.Cid, []string, error) {
	if err := res.blocker.IsPathBlocked(fpath).ToError(); err != nil {
		logger.Warn(err.Response)
		return cid.Undef, nil, err
	}
	return res.resolver.ResolveToLastNode(ctx, fpath)
}

// ResolvePath checks if the given path is blocked before resolving.
func (res *Resolver) ResolvePath(ctx context.Context, fpath path.ImmutablePath) (ipld.Node, ipld.Link, error) {
	if err := res.blocker.IsPathBlocked(fpath).ToError(); err != nil {
		logger.Warn(err.Response)
		return nil, nil, err
	}
	return res.resolver.ResolvePath(ctx, fpath)
}

// ResolvePathComponents checks if the given path is blocked before resolving.
func (res *Resolver) ResolvePathComponents(ctx context.Context, fpath path.ImmutablePath) ([]ipld.Node, error) {
	if err := res.blocker.IsPathBlocked(fpath).ToError(); err != nil {
		logger.Warn(err.Response)
		return nil, err
	}
	return res.resolver.ResolvePathComponents(ctx, fpath)
}
