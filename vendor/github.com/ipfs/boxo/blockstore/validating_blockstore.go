package blockstore

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// ValidatingBlockstore validates blocks on get.
type ValidatingBlockstore struct {
	Blockstore
}

var _ AllKeysChanWithErrer = (*ValidatingBlockstore)(nil)

func (bs *ValidatingBlockstore) AllKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	return allKeysChanWithErrFor(ctx, bs.Blockstore)
}

func (bs *ValidatingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	block, err := bs.Blockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	rbcid, err := c.Prefix().Sum(block.RawData())
	if err != nil {
		return nil, err
	}
	if !rbcid.Equals(c) {
		return nil, ErrHashMismatch
	}
	return block, nil
}
