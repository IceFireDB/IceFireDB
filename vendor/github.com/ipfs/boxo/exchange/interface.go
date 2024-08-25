// Package exchange defines the IPFS exchange interface
package exchange

import (
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// Interface defines the functionality of the IPFS block exchange protocol.
type Interface interface { // type Exchanger interface
	Fetcher

	// NotifyNewBlocks tells the exchange that new blocks are available and can be served.
	NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error

	io.Closer
}

// Fetcher is an object that can be used to retrieve blocks
type Fetcher interface {
	// GetBlock returns the block associated with a given cid.
	GetBlock(context.Context, cid.Cid) (blocks.Block, error)
	// GetBlocks returns the blocks associated with the given cids.
	// If the requested blocks are not found immediately, this function should hang until
	// they are found. If they can't be found later, it's also acceptable to terminate.
	GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error)
}

// SessionExchange is an exchange.Interface which supports
// sessions.
type SessionExchange interface {
	Interface
	// NewSession generates a new exchange session. You should use this, rather
	// that calling GetBlocks, any time you intend to do several related calls
	// in a row. The exchange can leverage that to be more efficient.
	NewSession(context.Context) Fetcher
}
