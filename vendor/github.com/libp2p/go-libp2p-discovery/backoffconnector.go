package discovery

import (
	"time"

	"github.com/libp2p/go-libp2p-core/host"

	dbackoff "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

// BackoffConnector is a utility to connect to peers, but only if we have not recently tried connecting to them already
// Deprecated: use go-libp2p/p2p/discovery/backoff.BackoffConnector inste
type BackoffConnector = dbackoff.BackoffConnector

// NewBackoffConnector creates a utility to connect to peers, but only if we have not recently tried connecting to them already
// cacheSize is the size of a TwoQueueCache
// connectionTryDuration is how long we attempt to connect to a peer before giving up
// backoff describes the strategy used to decide how long to backoff after previously attempting to connect to a peer
// Deprecated: use go-libp2p/p2p/discovery/backoff.NewBackoffConnector instead.
func NewBackoffConnector(h host.Host, cacheSize int, connectionTryDuration time.Duration, backoff BackoffFactory) (*BackoffConnector, error) {
	return dbackoff.NewBackoffConnector(h, cacheSize, connectionTryDuration, backoff)
}
