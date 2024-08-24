package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p-core/routing"
	record "github.com/libp2p/go-libp2p-record"
)

var _ routing.ValueStore = &Client{}

// PutValue adds value corresponding to given Key.
func (c *Client) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return ipns.ErrKeyFormat
	}

	return c.PutIPNS(ctx, []byte(path), val)
}

// GetValue searches for the value corresponding to given Key.
func (c *Client) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return nil, ipns.ErrKeyFormat
	}

	return c.GetIPNS(ctx, []byte(path))
}

// SearchValue searches for better and better values from this value
// store corresponding to the given Key. By default implementations must
// stop the search after a good value is found. A 'good' value is a value
// that would be returned from GetValue.
//
// Useful when you want a result *now* but still want to hear about
// better/newer results.
//
// Implementations of this methods won't return ErrNotFound. When a value
// couldn't be found, the channel will get closed without passing any results
func (c *Client) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return nil, ipns.ErrKeyFormat
	}

	resChan, err := c.GetIPNSAsync(ctx, []byte(path))
	if err != nil {
		return nil, err
	}

	outCh := make(chan []byte, 1)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-resChan:
				if !ok {
					return
				}

				if r.Err != nil {
					continue
				}

				outCh <- r.Record
			}
		}
	}()

	return outCh, nil
}
