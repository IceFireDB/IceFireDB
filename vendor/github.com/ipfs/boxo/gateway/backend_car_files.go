package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	ufsData "github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/hamt"
	ufsiter "github.com/ipfs/go-unixfsnode/iter"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
)

type awaitCloser interface {
	AwaitClose() <-chan error
}

type backpressuredFile struct {
	size    int64
	f       io.ReadSeeker
	getLsys lsysGetter

	ctx       context.Context
	fileCid   cid.Cid
	byteRange DagByteRange
	retErr    error

	closed chan error
}

func (b *backpressuredFile) AwaitClose() <-chan error {
	return b.closed
}

func (b *backpressuredFile) Close() error {
	close(b.closed)
	return nil
}

func (b *backpressuredFile) Mode() os.FileMode {
	panic("not implemented")
}

func (b *backpressuredFile) ModTime() time.Time {
	panic("not implemented")
}

func (b *backpressuredFile) Size() (int64, error) {
	return b.size, nil
}

func (b *backpressuredFile) Read(p []byte) (n int, err error) {
	if b.retErr == nil {
		n, err = b.f.Read(p)
		if err == nil || err == io.EOF {
			return n, err
		}

		if n > 0 {
			b.retErr = err
			return n, nil
		}
	} else {
		err = b.retErr
	}

	from, seekErr := b.f.Seek(0, io.SeekCurrent)
	if seekErr != nil {
		// Return the seek error since by this point seeking failures like this should be impossible
		return 0, seekErr
	}

	// we had an error while reading so attempt to reset the underlying reader
	for {
		if b.ctx.Err() != nil {
			return 0, b.ctx.Err()
		}

		retry, processedErr := isRetryableError(err)
		if !retry {
			return 0, processedErr
		}

		var nd files.Node
		nd, err = loadTerminalUnixFSElementWithRecursiveDirectories(b.ctx, b.fileCid, nil, nil, CarParams{Scope: DagScopeEntity, Range: &DagByteRange{From: from, To: b.byteRange.To}}, b.getLsys)
		if err != nil {
			continue
		}

		f, ok := nd.(files.File)
		if !ok {
			return 0, fmt.Errorf("not a file, should be unreachable")
		}

		b.f = f
		break
	}

	// now that we've reset the reader try reading again
	return b.Read(p)
}

func (b *backpressuredFile) Seek(offset int64, whence int) (int64, error) {
	return b.f.Seek(offset, whence)
}

var (
	_ files.File  = (*backpressuredFile)(nil)
	_ awaitCloser = (*backpressuredFile)(nil)
)

type singleUseDirectory struct {
	dirIter files.DirIterator
	closed  chan error
}

func (b *singleUseDirectory) AwaitClose() <-chan error {
	return b.closed
}

func (b *singleUseDirectory) Close() error {
	close(b.closed)
	return nil
}

func (b *singleUseDirectory) Mode() os.FileMode {
	return 0
}

func (b *singleUseDirectory) ModTime() time.Time {
	return time.Time{}
}

func (b *singleUseDirectory) Size() (int64, error) {
	// TODO implement me
	panic("implement me")
}

func (b *singleUseDirectory) Entries() files.DirIterator {
	return b.dirIter
}

var (
	_ files.Directory = (*singleUseDirectory)(nil)
	_ awaitCloser     = (*singleUseDirectory)(nil)
)

type backpressuredFlatDirIter struct {
	linksItr *dagpb.PBLinks__Itr
	lsys     *ipld.LinkSystem
	getLsys  lsysGetter
	ctx      context.Context

	curName string
	curFile files.Node

	err error
}

func (it *backpressuredFlatDirIter) Name() string {
	return it.curName
}

func (it *backpressuredFlatDirIter) Node() files.Node {
	return it.curFile
}

func (it *backpressuredFlatDirIter) Next() bool {
	if it.err != nil {
		return false
	}

	iter := it.linksItr
	if iter.Done() {
		return false
	}

	_, v := iter.Next()
	c := v.Hash.Link().(cidlink.Link).Cid
	var name string
	if v.Name.Exists() {
		name = v.Name.Must().String()
	}

	var nd files.Node
	var err error
	params := CarParams{Scope: DagScopeAll}
	for {
		if it.ctx.Err() != nil {
			it.err = it.ctx.Err()
			return false
		}
		if err != nil {
			it.lsys, err = it.getLsys(it.ctx, c, params)
			continue
		}
		nd, err = loadTerminalUnixFSElementWithRecursiveDirectories(it.ctx, c, nil, it.lsys, params, it.getLsys)
		if err != nil {
			if ctxErr := it.ctx.Err(); ctxErr != nil {
				continue
			}
			retry, processedErr := isRetryableError(err)
			if retry {
				err = processedErr
				continue
			}
			it.err = processedErr
			return false
		}
		break
	}

	it.curName = name
	it.curFile = nd
	return true
}

func (it *backpressuredFlatDirIter) Err() error {
	return it.err
}

var _ files.DirIterator = (*backpressuredFlatDirIter)(nil)

type backpressuredHAMTDirIter struct {
	linksItr ipld.MapIterator
	dirCid   cid.Cid

	lsys    *ipld.LinkSystem
	getLsys lsysGetter
	ctx     context.Context

	curName      string
	curFile      files.Node
	curProcessed int

	err error
}

func (it *backpressuredHAMTDirIter) Name() string {
	return it.curName
}

func (it *backpressuredHAMTDirIter) Node() files.Node {
	return it.curFile
}

func (it *backpressuredHAMTDirIter) Next() bool {
	if it.err != nil {
		return false
	}

	iter := it.linksItr
	if iter.Done() {
		return false
	}

	/*
		Since there is no way to make a graph request for part of a HAMT during errors we can either fill in the HAMT with
		block requests, or we can re-request the HAMT and skip over the parts we already have.

		Here we choose the latter, however in the event of a re-request we request the entity rather than the entire DAG as
		a compromise between more requests and over-fetching data.
	*/

	var err error
	for {
		if it.ctx.Err() != nil {
			it.err = it.ctx.Err()
			return false
		}

		retry, processedErr := isRetryableError(err)
		if !retry {
			it.err = processedErr
			return false
		}

		var nd ipld.Node
		if err != nil {
			var lsys *ipld.LinkSystem
			lsys, err = it.getLsys(it.ctx, it.dirCid, CarParams{Scope: DagScopeEntity})
			if err != nil {
				continue
			}

			_, pbn, ufsFieldData, _, ufsBaseErr := loadUnixFSBase(it.ctx, it.dirCid, nil, lsys)
			if ufsBaseErr != nil {
				err = ufsBaseErr
				continue
			}

			nd, err = hamt.NewUnixFSHAMTShard(it.ctx, pbn, ufsFieldData, lsys)
			if err != nil {
				err = fmt.Errorf("could not reify sharded directory: %w", err)
				continue
			}

			iter = nd.MapIterator()
			for i := 0; i < it.curProcessed; i++ {
				_, _, err = iter.Next()
				if err != nil {
					continue
				}
			}

			it.linksItr = iter
		}

		var k, v ipld.Node
		k, v, err = iter.Next()
		if err != nil {
			retry, processedErr = isRetryableError(err)
			if retry {
				err = processedErr
				continue
			}
			it.err = processedErr
			return false
		}

		var name string
		name, err = k.AsString()
		if err != nil {
			it.err = err
			return false
		}
		var lnk ipld.Link
		lnk, err = v.AsLink()
		if err != nil {
			it.err = err
			return false
		}

		cl, ok := lnk.(cidlink.Link)
		if !ok {
			it.err = fmt.Errorf("link not a cidlink")
			return false
		}

		c := cl.Cid
		params := CarParams{Scope: DagScopeAll}
		var childNd files.Node
		for {
			if it.ctx.Err() != nil {
				it.err = it.ctx.Err()
				return false
			}

			if err != nil {
				retry, processedErr = isRetryableError(err)
				if !retry {
					it.err = processedErr
					return false
				}

				it.lsys, err = it.getLsys(it.ctx, c, params)
				continue
			}

			childNd, err = loadTerminalUnixFSElementWithRecursiveDirectories(it.ctx, c, nil, it.lsys, params, it.getLsys)
			if err != nil {
				continue
			}
			break
		}

		it.curName = name
		it.curFile = childNd
		it.curProcessed++
		break
	}

	return true
}

func (it *backpressuredHAMTDirIter) Err() error {
	return it.err
}

var _ files.DirIterator = (*backpressuredHAMTDirIter)(nil)

type backpressuredHAMTDirIterNoRecursion struct {
	dagSize  uint64
	linksItr ipld.MapIterator
	dirCid   cid.Cid

	lsys    *ipld.LinkSystem
	getLsys lsysGetter
	ctx     context.Context

	curLnk       unixfs.LinkResult
	curProcessed int

	closed    chan error
	hasClosed bool
	err       error
}

func (it *backpressuredHAMTDirIterNoRecursion) AwaitClose() <-chan error {
	return it.closed
}

func (it *backpressuredHAMTDirIterNoRecursion) Link() unixfs.LinkResult {
	return it.curLnk
}

func (it *backpressuredHAMTDirIterNoRecursion) Next() bool {
	defer func() {
		if it.linksItr.Done() || it.err != nil {
			if !it.hasClosed {
				it.hasClosed = true
				close(it.closed)
			}
		}
	}()

	if it.err != nil {
		return false
	}

	iter := it.linksItr
	if iter.Done() {
		return false
	}

	/*
		Since there is no way to make a graph request for part of a HAMT during errors we can either fill in the HAMT with
		block requests, or we can re-request the HAMT and skip over the parts we already have.

		Here we choose the latter, however in the event of a re-request we request the entity rather than the entire DAG as
		a compromise between more requests and over-fetching data.
	*/

	var err error
	for {
		if it.ctx.Err() != nil {
			it.err = it.ctx.Err()
			return false
		}

		retry, processedErr := isRetryableError(err)
		if !retry {
			it.err = processedErr
			return false
		}

		var nd ipld.Node
		if err != nil {
			var lsys *ipld.LinkSystem
			lsys, err = it.getLsys(it.ctx, it.dirCid, CarParams{Scope: DagScopeEntity})
			if err != nil {
				continue
			}

			_, pbn, ufsFieldData, _, ufsBaseErr := loadUnixFSBase(it.ctx, it.dirCid, nil, lsys)
			if ufsBaseErr != nil {
				err = ufsBaseErr
				continue
			}

			nd, err = hamt.NewUnixFSHAMTShard(it.ctx, pbn, ufsFieldData, lsys)
			if err != nil {
				err = fmt.Errorf("could not reify sharded directory: %w", err)
				continue
			}

			iter = nd.MapIterator()
			for i := 0; i < it.curProcessed; i++ {
				_, _, err = iter.Next()
				if err != nil {
					continue
				}
			}

			it.linksItr = iter
		}

		var k, v ipld.Node
		k, v, err = iter.Next()
		if err != nil {
			retry, processedErr = isRetryableError(err)
			if retry {
				err = processedErr
				continue
			}
			it.err = processedErr
			return false
		}

		var name string
		name, err = k.AsString()
		if err != nil {
			it.err = err
			return false
		}

		var lnk ipld.Link
		lnk, err = v.AsLink()
		if err != nil {
			it.err = err
			return false
		}

		cl, ok := lnk.(cidlink.Link)
		if !ok {
			it.err = fmt.Errorf("link not a cidlink")
			return false
		}

		c := cl.Cid

		pbLnk, ok := v.(*ufsiter.IterLink)
		if !ok {
			it.err = fmt.Errorf("HAMT value is not a dag-pb link")
			return false
		}

		cumulativeDagSize := uint64(0)
		if pbLnk.Substrate.Tsize.Exists() {
			cumulativeDagSize = uint64(pbLnk.Substrate.Tsize.Must().Int())
		}

		it.curLnk = unixfs.LinkResult{
			Link: &format.Link{
				Name: name,
				Size: cumulativeDagSize,
				Cid:  c,
			},
		}
		it.curProcessed++
		break
	}

	return true
}

func (it *backpressuredHAMTDirIterNoRecursion) Err() error {
	return it.err
}

var _ awaitCloser = (*backpressuredHAMTDirIterNoRecursion)(nil)

/*
1. Run traversal to get the top-level response
2. Response can do a callback for another response
*/

type lsysGetter = func(ctx context.Context, c cid.Cid, params CarParams) (*ipld.LinkSystem, error)

func loadUnixFSBase(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem) ([]byte, dagpb.PBNode, ufsData.UnixFSData, int64, error) {
	lctx := ipld.LinkContext{Ctx: ctx}
	pathTerminalCidLink := cidlink.Link{Cid: c}

	var blockData []byte
	var err error

	if blk != nil {
		blockData = blk.RawData()
	} else {
		blockData, err = lsys.LoadRaw(lctx, pathTerminalCidLink)
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}

	if c.Type() == uint64(multicodec.Raw) {
		return blockData, nil, nil, 0, nil
	}

	// decode the terminal block into a node
	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	np, err := pc(pathTerminalCidLink, lctx)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	decoder, err := lsys.DecoderChooser(pathTerminalCidLink)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	nb := np.NewBuilder()
	if err := decoder(nb, bytes.NewReader(blockData)); err != nil {
		return nil, nil, nil, 0, err
	}
	lastCidNode := nb.Build()

	if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
		// If it's not valid dag-pb then we're done
		return nil, nil, nil, 0, errNotUnixFS
	} else if !pbn.FieldData().Exists() {
		// If it's not valid UnixFS then we're done
		return nil, nil, nil, 0, errNotUnixFS
	} else if unixfsFieldData, decodeErr := ufsData.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
		return nil, nil, nil, 0, errNotUnixFS
	} else {
		switch fieldNum := unixfsFieldData.FieldDataType().Int(); fieldNum {
		case ufsData.Data_Symlink, ufsData.Data_Metadata, ufsData.Data_Raw, ufsData.Data_File, ufsData.Data_Directory, ufsData.Data_HAMTShard:
			return nil, pbn, unixfsFieldData, fieldNum, nil
		default:
			return nil, nil, nil, 0, errNotUnixFS
		}
	}
}

func loadTerminalUnixFSElementWithRecursiveDirectories(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem, params CarParams, getLsys lsysGetter) (files.Node, error) {
	var err error
	if lsys == nil {
		lsys, err = getLsys(ctx, c, params)
		if err != nil {
			return nil, err
		}
	}

	lctx := ipld.LinkContext{Ctx: ctx}
	blockData, pbn, ufsFieldData, fieldNum, err := loadUnixFSBase(ctx, c, blk, lsys)
	if err != nil {
		return nil, err
	}

	if c.Type() == uint64(multicodec.Raw) {
		return files.NewBytesFile(blockData), nil
	}

	switch fieldNum {
	case ufsData.Data_Symlink:
		if !ufsFieldData.FieldData().Exists() {
			return nil, fmt.Errorf("invalid UnixFS symlink object")
		}
		lnkTarget := string(ufsFieldData.FieldData().Must().Bytes())
		f := files.NewLinkFile(lnkTarget, nil)
		return f, nil
	case ufsData.Data_Metadata:
		return nil, fmt.Errorf("UnixFS Metadata unsupported")
	case ufsData.Data_HAMTShard, ufsData.Data_Directory:
		switch fieldNum {
		case ufsData.Data_Directory:
			d := &singleUseDirectory{&backpressuredFlatDirIter{
				ctx:      ctx,
				linksItr: pbn.Links.Iterator(),
				lsys:     lsys,
				getLsys:  getLsys,
			}, make(chan error)}
			return d, nil
		case ufsData.Data_HAMTShard:
			dirNd, err := unixfsnode.Reify(lctx, pbn, lsys)
			if err != nil {
				return nil, fmt.Errorf("could not reify sharded directory: %w", err)
			}

			d := &singleUseDirectory{
				&backpressuredHAMTDirIter{
					linksItr: dirNd.MapIterator(),
					dirCid:   c,
					lsys:     lsys,
					getLsys:  getLsys,
					ctx:      ctx,
				}, make(chan error),
			}
			return d, nil
		default:
			return nil, fmt.Errorf("not a basic or HAMT directory: should be unreachable")
		}
	case ufsData.Data_Raw, ufsData.Data_File:
		nd, err := unixfsnode.Reify(lctx, pbn, lsys)
		if err != nil {
			return nil, err
		}

		fnd, ok := nd.(datamodel.LargeBytesNode)
		if !ok {
			return nil, fmt.Errorf("could not process file since it did not present as large bytes")
		}
		f, err := fnd.AsLargeBytes()
		if err != nil {
			return nil, err
		}

		fileSize, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("unable to get UnixFS file size: %w", err)
		}

		from := int64(0)
		var byteRange DagByteRange
		if params.Range != nil {
			byteRange = *params.Range
			from = params.Range.From
		}
		_, err = f.Seek(from, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("unable to get reset UnixFS file reader: %w", err)
		}

		return &backpressuredFile{ctx: ctx, fileCid: c, byteRange: byteRange, size: fileSize, f: f, getLsys: getLsys, closed: make(chan error)}, nil
	default:
		return nil, fmt.Errorf("unknown UnixFS field type")
	}
}
