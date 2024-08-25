package namesys

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// IPNSResolver implements [Resolver] for IPNS Records. This resolver always returns
// a TTL if the record is still valid. It happens as follows:
//
//  1. Provisory TTL is chosen: record TTL if it exists, otherwise `ipns.DefaultRecordTTL`.
//  2. If provisory TTL expires before EOL, then returned TTL is duration between EOL and now.
//  3. If record is expired, 0 is returned as TTL.
type IPNSResolver struct {
	routing routing.ValueStore
}

var _ Resolver = &IPNSResolver{}

// NewIPNSResolver constructs a new [IPNSResolver] from a [routing.ValueStore].
func NewIPNSResolver(route routing.ValueStore) *IPNSResolver {
	if route == nil {
		panic("attempt to create resolver with nil routing system")
	}

	return &IPNSResolver{
		routing: route,
	}
}

func (r *IPNSResolver) Resolve(ctx context.Context, p path.Path, options ...ResolveOption) (Result, error) {
	ctx, span := startSpan(ctx, "IPNSResolver.Resolve", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolve(ctx, r, p, ProcessResolveOptions(options))
}

func (r *IPNSResolver) ResolveAsync(ctx context.Context, p path.Path, options ...ResolveOption) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "IPNSResolver.ResolveAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolveAsync(ctx, r, p, ProcessResolveOptions(options))
}

func (r *IPNSResolver) resolveOnceAsync(ctx context.Context, p path.Path, options ResolveOptions) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "IPNSResolver.ResolveOnceAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	out := make(chan AsyncResult, 1)
	if p.Namespace() != path.IPNSNamespace {
		out <- AsyncResult{Err: fmt.Errorf("unsupported namespace: %s", p.Namespace())}
		close(out)
		return out
	}

	cancel := func() {}
	if options.DhtTimeout != 0 {
		// Resolution must complete within the timeout
		ctx, cancel = context.WithTimeout(ctx, options.DhtTimeout)
	}

	name, err := ipns.NameFromString(p.Segments()[1])
	if err != nil {
		out <- AsyncResult{Err: err}
		close(out)
		cancel()
		return out
	}

	vals, err := r.routing.SearchValue(ctx, string(name.RoutingKey()), dht.Quorum(int(options.DhtRecordCount)))
	if err != nil {
		out <- AsyncResult{Err: err}
		close(out)
		cancel()
		return out
	}

	go func() {
		defer cancel()
		defer close(out)
		ctx, span := startSpan(ctx, "IPNSResolver.ResolveOnceAsync.Worker")
		defer span.End()

		for {
			select {
			case val, ok := <-vals:
				if !ok {
					return
				}

				rec, err := ipns.UnmarshalRecord(val)
				if err != nil {
					emitOnceResult(ctx, out, AsyncResult{Err: err})
					return
				}

				resolvedBase, err := rec.Value()
				if err != nil {
					emitOnceResult(ctx, out, AsyncResult{Err: err})
					return
				}

				resolvedBase, err = joinPaths(resolvedBase, p)
				if err != nil {
					emitOnceResult(ctx, out, AsyncResult{Err: err})
					return
				}

				ttl, err := calculateBestTTL(rec)
				if err != nil {
					emitOnceResult(ctx, out, AsyncResult{Err: err})
					return
				}

				// TODO: in the future it would be interesting to set the last modified date
				// as the date in which the record has been signed.
				emitOnceResult(ctx, out, AsyncResult{Path: resolvedBase, TTL: ttl, LastMod: time.Now()})
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func calculateBestTTL(rec *ipns.Record) (time.Duration, error) {
	ttl := DefaultResolverCacheTTL
	if recordTTL, err := rec.TTL(); err == nil {
		ttl = recordTTL
	}

	switch eol, err := rec.Validity(); err {
	case ipns.ErrUnrecognizedValidity:
		// No EOL.
	case nil:
		ttEol := time.Until(eol)
		if ttEol < 0 {
			// It *was* valid when we first resolved it.
			ttl = 0
		} else if ttEol < ttl {
			ttl = ttEol
		}
	default:
		return 0, err
	}

	return ttl, nil
}
