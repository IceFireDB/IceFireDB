package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (fp *Client) PutIPNS(ctx context.Context, id []byte, record []byte) error {
	_, err := peer.IDFromBytes(id)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}

	_, err = fp.client.PutIPNS(ctx, &proto.PutIPNSRequest{ID: id, Record: record})
	if err != nil {
		return err
	}
	return nil
}

type PutIPNSAsyncResult struct {
	Err error
}

func (fp *Client) PutIPNSAsync(ctx context.Context, id []byte, record []byte) (<-chan PutIPNSAsyncResult, error) {
	_, err := peer.IDFromBytes(id)
	if err != nil {
		return nil, fmt.Errorf("invalid peer ID: %w", err)
	}

	ch0, err := fp.client.PutIPNS_Async(ctx, &proto.PutIPNSRequest{ID: id, Record: record})
	if err != nil {
		return nil, err
	}
	ch1 := make(chan PutIPNSAsyncResult, 1)
	go func() {
		defer close(ch1)
		for {
			select {
			case <-ctx.Done():
				return
			case r0, ok := <-ch0:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case ch1 <- PutIPNSAsyncResult{
					Err: r0.Err,
				}:
				}
			}
		}
	}()

	return ch1, nil
}
