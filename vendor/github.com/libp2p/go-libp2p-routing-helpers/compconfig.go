package routinghelpers

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-routing-helpers/tracing"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

const tracer = tracing.Tracer("go-libp2p-routing-helpers")

type ParallelRouter struct {
	Timeout      time.Duration
	Router       routing.Routing
	ExecuteAfter time.Duration
	// DoNotWaitForSearchValue is experimental while we wait for a better solution.
	DoNotWaitForSearchValue bool
	IgnoreError             bool
}

type SequentialRouter struct {
	Timeout     time.Duration
	IgnoreError bool
	Router      routing.Routing
}

type ProvideManyRouter interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

type ReadyAbleRouter interface {
	Ready() bool
}

type ComposableRouter interface {
	Routers() []routing.Routing
}
