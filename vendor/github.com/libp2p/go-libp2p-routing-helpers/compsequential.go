package routinghelpers

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

var _ routing.Routing = (*composableSequential)(nil)
var _ ProvideManyRouter = (*composableSequential)(nil)
var _ ReadyAbleRouter = (*composableSequential)(nil)
var _ ComposableRouter = (*composableSequential)(nil)

const sequentialName = "ComposableSequential"

type composableSequential struct {
	routers []*SequentialRouter
}

func NewComposableSequential(routers []*SequentialRouter) *composableSequential {
	return &composableSequential{
		routers: routers,
	}
}

func (r *composableSequential) Routers() []routing.Routing {
	var routers []routing.Routing
	for _, sr := range r.routers {
		routers = append(routers, sr.Router)
	}

	return routers
}

// Provide calls Provide method per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) Provide(ctx context.Context, cid cid.Cid, provide bool) (err error) {
	ctx, end := tracer.Provide(sequentialName, ctx, cid, provide)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Provide(ctx, cid, provide)
		})
}

// ProvideMany will call all supported Routers sequentially, falling back to iterative
// single Provide call for routers which do not support [ProvideManyRouter].
func (r *composableSequential) ProvideMany(ctx context.Context, keys []multihash.Multihash) (err error) {
	ctx, end := tracer.ProvideMany(sequentialName, ctx, keys)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			if pm, ok := r.(ProvideManyRouter); ok {
				return pm.ProvideMany(ctx, keys)
			}

			for _, k := range keys {
				if err := r.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// Ready will call all supported [ReadyAbleRouter] sequentially.
// If some of them are not ready, this method will return false.
func (r *composableSequential) Ready() bool {
	for _, ro := range r.routers {
		pm, ok := ro.Router.(ReadyAbleRouter)
		if !ok {
			continue
		}

		if !pm.Ready() {
			return false
		}
	}

	return true
}

// FindProvidersAsync calls FindProvidersAsync per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
// If count is set, the channel will return up to count results, stopping routers iteration.
func (r *composableSequential) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	ctx, wrapper := tracer.FindProvidersAsync(sequentialName, ctx, cid, count)

	var totalCount int64
	return wrapper(getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan peer.AddrInfo, error) {
			return r.FindProvidersAsync(ctx, cid, count), nil
		},
		func() bool {
			return atomic.AddInt64(&totalCount, 1) > int64(count) && count != 0
		},
	), nil)
}

// FindPeer calls FindPeer per each router sequentially.
// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) FindPeer(ctx context.Context, pid peer.ID) (p peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(sequentialName, ctx, pid)
	defer func() { end(p, err) }()

	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (peer.AddrInfo, bool, error) {
			addr, err := r.FindPeer(ctx, pid)
			return addr, addr.ID == "", err
		},
	)
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(sequentialName, ctx, key, val, opts...)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.PutValue(ctx, key, val, opts...)
		})
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) ([]byte, bool, error) {
			val, err := r.GetValue(ctx, key, opts...)
			return val, len(val) == 0, err
		},
	)
}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	ctx, wrapper := tracer.SearchValue(sequentialName, ctx, key, opts...)

	return wrapper(getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan []byte, error) {
			return r.SearchValue(ctx, key, opts...)
		},
		func() bool { return false },
	), nil)

}

// If some router fails and the IgnoreError flag is true, we continue to the next router.
// Context timeout error will be also ignored if the flag is set.
func (r *composableSequential) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(sequentialName, ctx)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Bootstrap(ctx)
		},
	)
}

func getValueOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (T, bool, error),
) (value T, err error) {
	for _, router := range routers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return value, ctxErr
		}

		ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
		defer cancel()

		value, empty, err := f(ctx, router.Router)
		if err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			return value, err
		}

		if empty {
			continue
		}

		return value, nil
	}

	return value, routing.ErrNotFound
}

func executeSequential(
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing,
	) error) error {
	for _, router := range routers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
		defer cancel()

		if err := f(ctx, router.Router); err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			return err
		}
	}

	return nil
}

func getChannelOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (<-chan T, error),
	shouldStop func() bool,
) chan T {
	chanOut := make(chan T)

	go func() {
		for _, router := range routers {
			if ctxErr := ctx.Err(); ctxErr != nil {
				close(chanOut)
				return
			}
			ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
			defer cancel()
			rch, err := f(ctx, router.Router)
			if err != nil &&
				!errors.Is(err, routing.ErrNotFound) &&
				!router.IgnoreError {
				break
			}

		f:
			for {
				select {
				case <-ctx.Done():
					break f
				case v, ok := <-rch:
					if !ok {
						break f
					}
					select {
					case <-ctx.Done():
						break f
					case chanOut <- v:
					}

				}
			}
		}

		close(chanOut)
	}()

	return chanOut
}
