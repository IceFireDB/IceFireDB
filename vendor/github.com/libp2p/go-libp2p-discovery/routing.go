// Deprecated: This package has moved into go-libp2p, split into multiple sub-packages: github.com/libp2p/go-libp2p/p2p/discovery.
package discovery

import (
	"github.com/libp2p/go-libp2p-core/discovery"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"

	"github.com/libp2p/go-libp2p-core/routing"
)

// RoutingDiscovery is an implementation of discovery using ContentRouting.
// Namespaces are translated to Cids using the SHA256 hash.
// Deprecated: use go-libp2p/p2p/discovery/routing.RoutingDiscovery instead.
type RoutingDiscovery = drouting.RoutingDiscovery

// Deprecated: use go-libp2p/p2p/discovery/routing.NewRoutingDiscovery instead.
func NewRoutingDiscovery(router routing.ContentRouting) *RoutingDiscovery {
	return drouting.NewRoutingDiscovery(router)
}

// Deprecated: use go-libp2p/p2p/discovery/routing.NewDiscoveryRouting instead.
func NewDiscoveryRouting(disc discovery.Discovery, opts ...discovery.Option) *DiscoveryRouting {
	return drouting.NewDiscoveryRouting(disc, opts...)
}

// Deprecated: use go-libp2p/p2p/discovery/routing.RoutingDiscovery instead.
type DiscoveryRouting = drouting.DiscoveryRouting
