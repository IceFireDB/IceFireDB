// Package providing implements an exchange wrapper which
// does content providing for new blocks.
package providing

import (
	"context"

	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/provider"
	blocks "github.com/ipfs/go-block-format"
)

// Exchange is an exchange wrapper that calls Provide for blocks received
// over NotifyNewBlocks.
type Exchange struct {
	exchange.Interface
	provider provider.Provider
}

// New creates a new providing Exchange with the given exchange and provider.
// This is a light wrapper. We recommend that the provider supports the
// handling of many concurrent provides etc. as it is called directly for
// every new block.
func New(base exchange.Interface, provider provider.Provider) *Exchange {
	return &Exchange{
		Interface: base,
		provider:  provider,
	}
}

// NotifyNewBlocks calls NotifyNewBlocks on the underlying provider and
// provider.Provide for every block after that.
func (ex *Exchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	// Notify blocks on the underlying exchange.
	err := ex.Interface.NotifyNewBlocks(ctx, blocks...)
	if err != nil {
		return err
	}

	for _, b := range blocks {
		if err := ex.provider.Provide(ctx, b.Cid(), true); err != nil {
			return err
		}
	}
	return nil
}
