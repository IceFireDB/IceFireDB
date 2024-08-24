package client

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

type ContentRoutingClient struct {
	client DelegatedRoutingClient
}

var _ routing.ContentRouting = (*ContentRoutingClient)(nil)

func NewContentRoutingClient(c DelegatedRoutingClient) *ContentRoutingClient {
	return &ContentRoutingClient{client: c}
}

func (c *ContentRoutingClient) Provide(context.Context, cid.Cid, bool) error {
	return routing.ErrNotSupported
}

func (c *ContentRoutingClient) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	addrInfoCh := make(chan peer.AddrInfo)
	resultCh, err := c.client.FindProvidersAsync(ctx, key)
	if err != nil {
		close(addrInfoCh)
		return addrInfoCh
	}
	go func() {
		numProcessed := 0
		closed := false
		for asyncResult := range resultCh {
			if asyncResult.Err != nil {
				logger.Infof("find providers async emitted a transient error (%v)", asyncResult.Err)
			} else {
				for _, peerAddr := range asyncResult.AddrInfo {
					if numResults <= 0 || numProcessed < numResults {
						addrInfoCh <- peerAddr
					}
					numProcessed++
					if numProcessed == numResults {
						close(addrInfoCh)
						closed = true
					}
				}
			}
		}
		if !closed {
			close(addrInfoCh)
		}
	}()
	return addrInfoCh
}
