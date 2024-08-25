package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

type getBlock func(ctx context.Context, cid cid.Cid) (blocks.Block, error)

var errNilBlock = ErrInvalidResponse{Message: "received a nil block with no error"}

func carToLinearBlockGetter(ctx context.Context, reader io.Reader, timeout time.Duration, metrics *CarBackendMetrics) (getBlock, error) {
	cr, err := car.NewCarReaderWithOptions(reader, car.WithErrorOnEmptyRoots(false))
	if err != nil {
		return nil, err
	}

	cbCtx, cncl := context.WithCancel(ctx)

	type blockRead struct {
		block blocks.Block
		err   error
	}

	blkCh := make(chan blockRead, 1)
	go func() {
		defer cncl()
		defer close(blkCh)
		for {
			blk, rdErr := cr.Next()
			select {
			case blkCh <- blockRead{blk, rdErr}:
				if rdErr != nil {
					cncl()
				}
			case <-cbCtx.Done():
				return
			}
		}
	}()

	isFirstBlock := true
	mx := sync.Mutex{}

	return func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
		mx.Lock()
		defer mx.Unlock()
		if err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c); err != nil {
			return nil, err
		}

		isId, bdata := extractIdentityMultihashCIDContents(c)
		if isId {
			return blocks.NewBlockWithCid(bdata, c)
		}

		// initially set a higher timeout here so that if there's an initial timeout error we get it from the car reader.
		var t *time.Timer
		if isFirstBlock {
			t = time.NewTimer(timeout * 2)
		} else {
			t = time.NewTimer(timeout)
		}
		var blkRead blockRead
		var ok bool
		select {
		case blkRead, ok = <-blkCh:
			if !t.Stop() {
				<-t.C
			}
			t.Reset(timeout)
		case <-t.C:
			return nil, ErrGatewayTimeout
		}
		if !ok || blkRead.err != nil {
			if !ok || errors.Is(blkRead.err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, blockstoreErrToGatewayErr(blkRead.err)
		}
		if blkRead.block != nil {
			metrics.carBlocksFetchedMetric.Inc()
			if !blkRead.block.Cid().Equals(c) {
				return nil, ErrInvalidResponse{Message: fmt.Sprintf("received block with cid %s, expected %s", blkRead.block.Cid(), c)}
			}
			return blkRead.block, nil
		}
		return nil, errNilBlock
	}, nil
}

// extractIdentityMultihashCIDContents will check if a given CID has an identity multihash and if so return true and
// the bytes encoded in the digest, otherwise will return false.
// Taken from https://github.com/ipfs/boxo/blob/b96767cc0971ca279feb36e7844e527a774309ab/blockstore/idstore.go#L30
func extractIdentityMultihashCIDContents(k cid.Cid) (bool, []byte) {
	// Pre-check by calling Prefix(), this much faster than extracting the hash.
	if k.Prefix().MhType != multihash.IDENTITY {
		return false, nil
	}

	dmh, err := multihash.Decode(k.Hash())
	if err != nil || dmh.Code != multihash.IDENTITY {
		return false, nil
	}
	return true, dmh.Digest
}

func getCarLinksystem(fn getBlock) *ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(linkContext linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		c := link.(cidlink.Link).Cid
		blk, err := fn(linkContext.Ctx, c)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
	lsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	return &lsys
}
