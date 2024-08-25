// package offline implements an object that implements the exchange
// interface but returns nil values to every request.
package offline

import (
	"context"
	"fmt"

	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func Exchange(bs blockstore.Blockstore) exchange.Interface {
	return &offlineExchange{bs: bs}
}

// offlineExchange implements the Exchange interface but doesn't return blocks.
// For use in offline mode.
type offlineExchange struct {
	bs blockstore.Blockstore
}

// GetBlock returns nil to signal that a block could not be retrieved for the
// given key.
// NB: This function may return before the timeout expires.
func (e *offlineExchange) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	blk, err := e.bs.Get(ctx, k)
	if ipld.IsNotFound(err) {
		return nil, fmt.Errorf("block was not found locally (offline): %w", err)
	}
	return blk, err
}

// NotifyNewBlocks tells the exchange that new blocks are available and can be served.
func (e *offlineExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	// as an offline exchange we have nothing to do
	return nil
}

// Close always returns nil.
func (e *offlineExchange) Close() error {
	// NB: exchange doesn't own the blockstore's underlying datastore, so it is
	// not responsible for closing it.
	return nil
}

func (e *offlineExchange) GetBlocks(ctx context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		for _, k := range ks {
			hit, err := e.bs.Get(ctx, k)
			if err != nil {
				// a long line of misses should abort when context is cancelled.
				select {
				// TODO case send misses down channel
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			select {
			case out <- hit:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
