package routinghelpers

import (
	"context"

	"github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.uber.org/multierr"
)

// Compose composes the components into a single router. Not specifying a
// component (leaving it nil) is equivalent to specifying the Null router.
//
// It also implements Bootstrap. All *distinct* components implementing
// Bootstrap will be bootstrapped in parallel. Identical components will not be
// bootstrapped twice.
type Compose struct {
	ValueStore     routing.ValueStore
	PeerRouting    routing.PeerRouting
	ContentRouting routing.ContentRouting
}

const composeName = "Compose"

// note: we implement these methods explicitly to avoid having to manually
// specify the Null router everywhere we don't want to implement some
// functionality.

// PutValue adds value corresponding to given Key.
func (cr *Compose) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(composeName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if cr.ValueStore == nil {
		return routing.ErrNotSupported
	}
	return cr.ValueStore.PutValue(ctx, key, value, opts...)
}

// GetValue searches for the value corresponding to given Key.
func (cr *Compose) GetValue(ctx context.Context, key string, opts ...routing.Option) (value []byte, err error) {
	ctx, end := tracer.GetValue(composeName, ctx, key, opts...)
	defer func() { end(value, err) }()

	if cr.ValueStore == nil {
		return nil, routing.ErrNotFound
	}
	return cr.ValueStore.GetValue(ctx, key, opts...)
}

// SearchValue searches for the value corresponding to given Key.
func (cr *Compose) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, wrapper := tracer.SearchValue(composeName, ctx, key, opts...)
	defer func() { ch, err = wrapper(ch, err) }()

	if cr.ValueStore == nil {
		out := make(chan []byte)
		close(out)
		return out, nil
	}
	return cr.ValueStore.SearchValue(ctx, key, opts...)
}

// Provide adds the given cid to the content routing system. If 'true' is
// passed, it also announces it, otherwise it is just kept in the local
// accounting of which objects are being provided.
func (cr *Compose) Provide(ctx context.Context, c cid.Cid, local bool) (err error) {
	ctx, end := tracer.Provide(composeName, ctx, c, local)
	defer func() { end(err) }()

	if cr.ContentRouting == nil {
		return routing.ErrNotSupported
	}
	return cr.ContentRouting.Provide(ctx, c, local)
}

// FindProvidersAsync searches for peers who are able to provide a given key.
//
// If count > 0, it returns at most count providers. If count == 0, it returns
// an unbounded number of providers.
func (cr *Compose) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	ctx, wrapper := tracer.FindProvidersAsync(composeName, ctx, c, count)

	if cr.ContentRouting == nil {
		ch := make(chan peer.AddrInfo)
		close(ch)
		return wrapper(ch, routing.ErrNotFound)
	}
	return wrapper(cr.ContentRouting.FindProvidersAsync(ctx, c, count), nil)
}

// FindPeer searches for a peer with given ID, returns a peer.AddrInfo
// with relevant addresses.
func (cr *Compose) FindPeer(ctx context.Context, p peer.ID) (info peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(composeName, ctx, p)
	defer func() { end(info, err) }()

	if cr.PeerRouting == nil {
		return peer.AddrInfo{}, routing.ErrNotFound
	}
	return cr.PeerRouting.FindPeer(ctx, p)
}

// GetPublicKey returns the public key for the given peer.
func (cr *Compose) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	if cr.ValueStore == nil {
		return nil, routing.ErrNotFound
	}
	return routing.GetPublicKey(cr.ValueStore, ctx, p)
}

// Bootstrap the router.
func (cr *Compose) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(composeName, ctx)
	defer func() { end(err) }()

	// Deduplicate. Technically, calling bootstrap multiple times shouldn't
	// be an issue but using the same router for multiple fields of Compose
	// is common.
	routers := make(map[Bootstrap]struct{}, 3)
	for _, value := range [...]interface{}{
		cr.ValueStore,
		cr.ContentRouting,
		cr.PeerRouting,
	} {
		switch b := value.(type) {
		case nil:
		case Null:
		case Bootstrap:
			routers[b] = struct{}{}
		}
	}

	var errs error
	for b := range routers {
		if err := b.Bootstrap(ctx); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

var _ routing.Routing = (*Compose)(nil)
var _ routing.PubKeyFetcher = (*Compose)(nil)
