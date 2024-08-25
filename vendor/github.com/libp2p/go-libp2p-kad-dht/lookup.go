package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/trace"
)

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a channel of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error along
// with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}

	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), func(*qpeerset.QueryPeerset) bool { return false })

	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// refresh the cpl for this key as the query was successful
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, nil
}

// pmGetClosestPeers is the protocol messenger version of the GetClosestPeer queryFn.
func (dht *IpfsDHT) pmGetClosestPeers(key string) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}
