package discovery

import (
	"github.com/libp2p/go-libp2p-core/discovery"
	dbackoff "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

// BackoffDiscovery is an implementation of discovery that caches peer data and attenuates repeated queries
// Deprecated: use go-libp2p/p2p/discovery/backoff.BackoffDiscovery instead.
type BackoffDiscovery = dbackoff.BackoffDiscovery

// Deprecated: use go-libp2p/p2p/discovery/backoff.BackoffDiscoveryOption instead.
type BackoffDiscoveryOption = dbackoff.BackoffDiscoveryOption

// Deprecated: use go-libp2p/p2p/discovery/backoff.NewBackoffDiscovery instead.
func NewBackoffDiscovery(disc discovery.Discovery, stratFactory BackoffFactory, opts ...BackoffDiscoveryOption) (discovery.Discovery, error) {
	return dbackoff.NewBackoffDiscovery(disc, stratFactory, opts...)
}

// WithBackoffDiscoverySimultaneousQueryBufferSize sets the buffer size for the channels between the main FindPeers query
// for a given namespace and all simultaneous FindPeers queries for the namespace
// Deprecated: use go-libp2p/p2p/discovery/backoff.WithBackoffDiscoverySimultaneousQueryBufferSize instead.
func WithBackoffDiscoverySimultaneousQueryBufferSize(size int) BackoffDiscoveryOption {
	return dbackoff.WithBackoffDiscoverySimultaneousQueryBufferSize(size)
}

// WithBackoffDiscoveryReturnedChannelSize sets the size of the buffer to be used during a FindPeer query.
// Note: This does not apply if the query occurs during the backoff time
// Deprecated: use go-libp2p/p2p/discovery/backoff.WithBackoffDiscoveryReturnedChannelSize instead.
func WithBackoffDiscoveryReturnedChannelSize(size int) BackoffDiscoveryOption {
	return dbackoff.WithBackoffDiscoveryReturnedChannelSize(size)
}
