package ipfs

import (
	"context"

	"github.com/ipfs-shipyard/nopfs"
	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var _ blockservice.BlockService = (*BlockService)(nil)

// BlockService implements a blocking BlockService.
type BlockService struct {
	blocker *nopfs.Blocker
	bs      blockservice.BlockService
}

// WrapBlockService wraps the given BlockService with a content-blocking layer
// for Get and Add operations.
func WrapBlockService(bs blockservice.BlockService, blocker *nopfs.Blocker) blockservice.BlockService {
	logger.Debug("BlockService wrapped with content blocker")

	return &BlockService{
		blocker: blocker,
		bs:      bs,
	}
}

// Closes the BlockService and the Blocker.
func (nbs *BlockService) Close() error {
	nbs.blocker.Close()
	return nbs.bs.Close()
}

// Gets a block unless CID has been blocked.
func (nbs *BlockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := nbs.blocker.IsCidBlocked(c).ToError(); err != nil {
		logger.Warn(err.Response)
		return nil, err
	}
	return nbs.bs.GetBlock(ctx, c)
}

// GetsBlocks reads several blocks. Blocked CIDs are filtered out of ks.
func (nbs *BlockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	var filtered []cid.Cid
	for _, c := range ks {
		if err := nbs.blocker.IsCidBlocked(c).ToError(); err != nil {
			logger.Warn(err.Response)
			logger.Warnf("GetBlocks dropped blocked block: %s", err)
		} else {
			filtered = append(filtered, c)
		}
	}
	return nbs.bs.GetBlocks(ctx, filtered)
}

// Blockstore returns the underlying Blockstore.
func (nbs *BlockService) Blockstore() blockstore.Blockstore {
	return nbs.bs.Blockstore()
}

// Exchange returns the underlying Exchange.
func (nbs *BlockService) Exchange() exchange.Interface {
	return nbs.bs.Exchange()
}

// AddBlock adds a block unless the CID is blocked.
func (nbs *BlockService) AddBlock(ctx context.Context, o blocks.Block) error {
	if err := nbs.blocker.IsCidBlocked(o.Cid()).ToError(); err != nil {
		logger.Warn(err.Response)
		return err
	}
	return nbs.bs.AddBlock(ctx, o)
}

// AddBlocks adds multiple blocks. Blocks with blocked CIDs are dropped.
func (nbs *BlockService) AddBlocks(ctx context.Context, bs []blocks.Block) error {
	var filtered []blocks.Block
	for _, o := range bs {
		if err := nbs.blocker.IsCidBlocked(o.Cid()).ToError(); err != nil {
			logger.Warn(err.Response)
			logger.Warnf("AddBlocks dropped blocked block: %s", err)
		} else {
			filtered = append(filtered, o)
		}
	}
	return nbs.bs.AddBlocks(ctx, filtered)
}

// DeleteBlock deletes a block.
func (nbs *BlockService) DeleteBlock(ctx context.Context, o cid.Cid) error {
	return nbs.bs.DeleteBlock(ctx, o)
}
