// tracing provides high level method tracing for the [routing.Routing] API.
// Each method of the API has a corresponding method on [Tracer] which return either a defered wrapping callback or just defered callback.
package tracing

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the librairy name that will be passed to [otel.Tracer].
type Tracer string

func (t Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(string(t)).Start(ctx, name, opts...)
}

const base = multibase.Base64url

func bytesAsMultibase(b []byte) string {
	r, err := multibase.Encode(base, b)
	if err != nil {
		panic(fmt.Errorf("unreachable: %w", err))
	}
	return r
}

// keysAsMultibase avoids returning non utf8 which otel does not like.
func keysAsMultibase(name string, keys []multihash.Multihash) attribute.KeyValue {
	keysStr := make([]string, len(keys))
	for i, k := range keys {
		keysStr[i] = bytesAsMultibase(k)
	}
	return attribute.StringSlice(name, keysStr)
}

func (t Tracer) Provide(routerName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return t.provide(routerName+".Provide", ctx, key, announce)
}

func (t Tracer) provide(traceName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Bool("announce", announce),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (t Tracer) ProvideMany(routerName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return t.provideMany(routerName+".ProvideMany", ctx, keys)
}

func (t Tracer) provideMany(traceName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(keysAsMultibase("keys", keys))

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func peerInfoToAttributes(p peer.AddrInfo) []attribute.KeyValue {
	strs := make([]string, len(p.Addrs))
	for i, v := range p.Addrs {
		strs[i] = v.String()
	}

	return []attribute.KeyValue{
		attribute.Stringer("id", p.ID),
		attribute.StringSlice("addrs", strs),
	}
}

func (t Tracer) FindProvidersAsync(routerName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	// outline so the concatenation can be folded at compile-time
	return t.findProvidersAsync(routerName+".FindProvidersAsync", ctx, key, count)
}

func (t Tracer) findProvidersAsync(traceName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan peer.AddrInfo, _ error) <-chan peer.AddrInfo { return c }
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Int("count", count),
	)

	return ctx, func(in <-chan peer.AddrInfo, err error) <-chan peer.AddrInfo {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in
		}

		span.AddEvent("started streaming")

		out := make(chan peer.AddrInfo)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("found provider", trace.WithAttributes(peerInfoToAttributes(v)...))
				select {
				case out <- v:
				case <-ctx.Done():
					span.SetStatus(codes.Error, ctx.Err().Error())
				}
			}
		}()

		return out
	}
}

func (t Tracer) FindPeer(routerName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	// outline so the concatenation can be folded at compile-time
	return t.findPeer(routerName+".FindPeer", ctx, id)
}

func (t Tracer) findPeer(traceName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(peer.AddrInfo, error) {}
	}

	span.SetAttributes(attribute.Stringer("key", id))

	return ctx, func(p peer.AddrInfo, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("found peer", trace.WithAttributes(peerInfoToAttributes(p)...))
	}
}

func (t Tracer) PutValue(routerName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return t.putValue(routerName+".PutValue", ctx, key, val, opts...)
}

func (t Tracer) putValue(traceName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.String("value", bytesAsMultibase(val)),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (t Tracer) GetValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	// outline so the concatenation can be folded at compile-time
	return t.getValue(routerName+".GetValue", ctx, key, opts...)
}

func (t Tracer) getValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func([]byte, error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(val []byte, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("found value", trace.WithAttributes(
			attribute.String("value", bytesAsMultibase(val))))
	}
}

func (t Tracer) SearchValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	// outline so the concatenation can be folded at compile-time
	return t.searchValue(routerName+".SearchValue", ctx, key, opts...)
}

func (t Tracer) searchValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan []byte, err error) (<-chan []byte, error) { return c, err }
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(in <-chan []byte, err error) (<-chan []byte, error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in, err
		}

		span.AddEvent("started streaming")

		out := make(chan []byte)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("found value", trace.WithAttributes(
					attribute.String("value", bytesAsMultibase(v))),
				)
				select {
				case out <- v:
				case <-ctx.Done():
					span.SetStatus(codes.Error, ctx.Err().Error())
				}
			}
		}()

		return out, nil
	}
}

func (t Tracer) Bootstrap(routerName string, ctx context.Context) (_ context.Context, end func(error)) {
	// outline so the concatenation can be folded at compile-time
	return t.bootstrap(routerName+".Bootstrap", ctx)
}

func (t Tracer) bootstrap(traceName string, ctx context.Context) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
