// Package resolver implements utilities for resolving paths within ipfs.
package resolver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	"github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var log = logging.Logger("path/resolver")

// ErrNoLink is returned when a link is not found in a path.
type ErrNoLink struct {
	Name string
	Node cid.Cid
}

// Error implements the [errors.Error] interface.
func (e *ErrNoLink) Error() string {
	return fmt.Sprintf("no link named %q under %s", e.Name, e.Node.String())
}

// Is implements [errors.Is] interface.
func (e *ErrNoLink) Is(err error) bool {
	switch err.(type) {
	case *ErrNoLink:
		return true
	default:
		return false
	}
}

// Resolver provides path resolution to IPFS.
type Resolver interface {
	// ResolveToLastNode walks the given path and returns the CID of the last block
	// referenced by the path, as well as the remainder of the path segments to traverse
	// from the final block boundary to the final node within the block.
	ResolveToLastNode(context.Context, path.ImmutablePath) (cid.Cid, []string, error)

	// ResolvePath fetches the node for the given path. It returns the last item returned
	// by [Resolver.ResolvePathComponents] and the last link traversed which can be used
	// to recover the block.
	ResolvePath(context.Context, path.ImmutablePath) (ipld.Node, ipld.Link, error)

	// ResolvePathComponents fetches the nodes for each segment of the given path. It
	// uses the first path component as the CID of the first node, then resolves all
	// other components walking the links via a selector traversal.
	ResolvePathComponents(context.Context, path.ImmutablePath) ([]ipld.Node, error)
}

// basicResolver implements the [Resolver] interface. It requires a [fetcher.Factory],
// which is used to resolve the nodes.
type basicResolver struct {
	FetcherFactory fetcher.Factory
}

// NewBasicResolver constructs a new basic resolver using the given [fetcher.Factory].
func NewBasicResolver(factory fetcher.Factory) Resolver {
	return &basicResolver{
		FetcherFactory: factory,
	}
}

// ResolveToLastNode implements [Resolver.ResolveToLastNode].
func (r *basicResolver) ResolveToLastNode(ctx context.Context, fpath path.ImmutablePath) (cid.Cid, []string, error) {
	ctx, span := startSpan(ctx, "basicResolver.ResolveToLastNode", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	c, remainder := fpath.RootCid(), fpath.Segments()[2:]

	if len(remainder) == 0 {
		return c, nil, nil
	}

	// create a selector to traverse and match all path segments
	pathSelector := pathAllSelector(remainder[:len(remainder)-1])

	// create a new cancellable session
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// resolve node before last path segment
	nodes, lastCid, depth, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(nodes) < 1 {
		return cid.Cid{}, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	} else if len(nodes) < len(remainder) {
		return cid.Undef, nil, &ErrNoLink{Name: remainder[len(nodes)-1], Node: lastCid}
	}

	parent := nodes[len(nodes)-1]
	lastSegment := remainder[len(remainder)-1]

	// find final path segment within node
	nd, err := parent.LookupBySegment(ipld.ParsePathSegment(lastSegment))
	switch err.(type) {
	case nil:
	case schema.ErrNoSuchField:
		return cid.Undef, nil, &ErrNoLink{Name: lastSegment, Node: lastCid}
	default:
		return cid.Cid{}, nil, err
	}

	// if last node is not a link, just return it's cid, add path to remainder and return
	if nd.Kind() != ipld.Kind_Link {
		// return the cid and the remainder of the path
		return lastCid, remainder[len(remainder)-depth-1:], nil
	}

	lnk, err := nd.AsLink()
	if err != nil {
		return cid.Cid{}, nil, err
	}

	clnk, ok := lnk.(cidlink.Link)
	if !ok {
		return cid.Cid{}, nil, fmt.Errorf("path %v resolves to a link that is not a cid link: %v", fpath, lnk)
	}

	return clnk.Cid, []string{}, nil
}

// ResolvePath implements [Resolver.ResolvePath].
//
// Note: if/when the context is cancelled or expires then if a multi-block ADL
// node is returned then it may not be possible to load certain values.
func (r *basicResolver) ResolvePath(ctx context.Context, fpath path.ImmutablePath) (ipld.Node, ipld.Link, error) {
	ctx, span := startSpan(ctx, "basicResolver.ResolvePath", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	c, remainder := fpath.RootCid(), fpath.Segments()[2:]

	// create a selector to traverse all path segments but only match the last
	pathSelector := pathLeafSelector(remainder)

	nodes, c, _, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return nil, nil, err
	}
	if len(nodes) < 1 {
		return nil, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	}
	return nodes[len(nodes)-1], cidlink.Link{Cid: c}, nil
}

// ResolvePathComponents implements [Resolver.ResolvePathComponents].
//
// Note: if/when the context is cancelled or expires then if a multi-block ADL
// node is returned then it may not be possible to load certain values.
func (r *basicResolver) ResolvePathComponents(ctx context.Context, fpath path.ImmutablePath) (nodes []ipld.Node, err error) {
	ctx, span := startSpan(ctx, "basicResolver.ResolvePathComponents", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	defer log.Debugw("resolvePathComponents", "fpath", fpath, "error", err)

	c, remainder := fpath.RootCid(), fpath.Segments()[2:]

	// create a selector to traverse and match all path segments
	pathSelector := pathAllSelector(remainder)

	nodes, _, _, err = r.resolveNodes(ctx, c, pathSelector)
	return nodes, err
}

// Finds nodes matching the selector starting with a cid. Returns the matched nodes, the cid of the block containing
// the last node, and the depth of the last node within its block (root is depth 0).
func (r *basicResolver) resolveNodes(ctx context.Context, c cid.Cid, sel ipld.Node) ([]ipld.Node, cid.Cid, int, error) {
	ctx, span := startSpan(ctx, "basicResolver.resolveNodes", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()
	session := r.FetcherFactory.NewSession(ctx)

	// traverse selector
	lastLink := cid.Undef
	depth := 0
	nodes := []ipld.Node{}
	err := fetcherhelpers.BlockMatching(ctx, session, cidlink.Link{Cid: c}, sel, func(res fetcher.FetchResult) error {
		if res.LastBlockLink == nil {
			res.LastBlockLink = cidlink.Link{Cid: c}
		}
		cidLnk, ok := res.LastBlockLink.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link is not a cidlink: %v", cidLnk)
		}

		// if we hit a block boundary
		if !lastLink.Equals(cidLnk.Cid) {
			depth = 0
			lastLink = cidLnk.Cid
		} else {
			depth++
		}

		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, cid.Undef, 0, err
	}

	return nodes, lastLink, depth, nil
}

func pathLeafSelector(path []string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) })
	})
}

func pathAllSelector(path []string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreUnion(
			ssb.Matcher(),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) }),
		)
	})
}

func pathSelector(path []string, ssb builder.SelectorSpecBuilder, reduce func(string, builder.SelectorSpec) builder.SelectorSpec) ipld.Node {
	spec := ssb.Matcher()
	for i := len(path) - 1; i >= 0; i-- {
		spec = reduce(path[i], spec)
	}
	return spec.Node()
}

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("boxo/path/resolver").Start(ctx, "Path."+name, opts...)
}
