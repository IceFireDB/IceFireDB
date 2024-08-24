package client

import (
	"context"

	"github.com/ipfs/go-delegated-routing/gen/proto"
	ipns "github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

func (fp *Client) GetIPNS(ctx context.Context, id []byte) ([]byte, error) {
	resps, err := fp.GetIPNSAsync(ctx, id)
	if err != nil {
		return nil, err
	}
	records := [][]byte{}
	for resp := range resps {
		if resp.Err == nil {
			records = append(records, resp.Record)
		}
	}
	if len(records) == 0 {
		return nil, routing.ErrNotFound
	}
	best, err := fp.validator.Select(ipns.RecordKey(peer.ID(id)), records)
	if err != nil {
		return nil, err
	}
	return records[best], nil
}

type GetIPNSAsyncResult struct {
	Record []byte
	Err    error
}

func (fp *Client) GetIPNSAsync(ctx context.Context, id []byte) (<-chan GetIPNSAsyncResult, error) {
	ch0, err := fp.client.GetIPNS_Async(ctx, &proto.GetIPNSRequest{ID: id})
	if err != nil {
		return nil, err
	}
	ch1 := make(chan GetIPNSAsyncResult, 1)
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

				var r1 GetIPNSAsyncResult

				if r0.Err != nil {
					r1.Err = r0.Err
					select {
					case <-ctx.Done():
						return
					case ch1 <- r1:
					}
					continue
				}

				if r0.Resp == nil {
					continue
				}

				if err = fp.validator.Validate(ipns.RecordKey(peer.ID(id)), r0.Resp.Record); err != nil {
					r1.Err = err
					select {
					case <-ctx.Done():
						return
					case ch1 <- r1:
					}

					continue
				}

				r1.Record = r0.Resp.Record

				select {
				case <-ctx.Done():
					return
				case ch1 <- r1:
				}
			}
		}
	}()
	return ch1, nil
}
