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
	// GetBlock returns the block associated with a given key.
	GetBlock(context.Context, cid.Cid) (blocks.Block, error)
	GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error)
}

// SessionExchange is an exchange.Interface which supports
// sessions.
type SessionExchange interface {
	Interface
	NewSession(context.Context) Fetcher
}
