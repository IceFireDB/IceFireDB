package discovery

import (
	"context"

	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/peer"

	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

// FindPeers is a utility function that synchronously collects peers from a Discoverer.
// Deprecated: use go-libp2p/p2p/discovery/routing.FindPeers instead.
func FindPeers(ctx context.Context, d discovery.Discoverer, ns string, opts ...discovery.Option) ([]peer.AddrInfo, error) {
	return dutil.FindPeers(ctx, d, ns, opts...)
}

// Advertise is a utility function that persistently advertises a service through an Advertiser.
// Deprecated: use go-libp2p/p2p/discovery/routing.Advertise instead.
func Advertise(ctx context.Context, a discovery.Advertiser, ns string, opts ...discovery.Option) {
	dutil.Advertise(ctx, a, ns, opts...)
}
