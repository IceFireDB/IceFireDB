package car

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"

	util "github.com/ipld/go-car/util"
)

func init() {
	cbor.RegisterCborType(CarHeader{})
}

type Store interface {
	Put(context.Context, blocks.Block) error
}

type ReadStore interface {
	Get(context.Context, cid.Cid) (blocks.Block, error)
}

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

type carWriter struct {
	ds   format.NodeGetter
	w    io.Writer
	walk WalkFunc
}

type WalkFunc func(format.Node) ([]*format.Link, error)

func WriteCar(ctx context.Context, ds format.NodeGetter, roots []cid.Cid, w io.Writer, options ...merkledag.WalkOption) error {
	return WriteCarWithWalker(ctx, ds, roots, w, DefaultWalkFunc, options...)
}

func WriteCarWithWalker(ctx context.Context, ds format.NodeGetter, roots []cid.Cid, w io.Writer, walk WalkFunc, options ...merkledag.WalkOption) error {

	h := &CarHeader{
		Roots:   roots,
		Version: 1,
	}

	if err := WriteHeader(h, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}

	cw := &carWriter{ds: ds, w: w, walk: walk}
	seen := cid.NewSet()
	for _, r := range roots {
		if err := merkledag.Walk(ctx, cw.enumGetLinks, r, seen.Visit, options...); err != nil {
			return err
		}
	}
	return nil
}

func DefaultWalkFunc(nd format.Node) ([]*format.Link, error) {
	return nd.Links(), nil
}

func ReadHeader(br *bufio.Reader) (*CarHeader, error) {
	hb, err := util.LdRead(br)
	if err != nil {
		return nil, err
	}

	var ch CarHeader
	if err := cbor.DecodeInto(hb, &ch); err != nil {
		return nil, fmt.Errorf("invalid header: %v", err)
	}

	return &ch, nil
}

func WriteHeader(h *CarHeader, w io.Writer) error {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return err
	}

	return util.LdWrite(w, hb)
}

func HeaderSize(h *CarHeader) (uint64, error) {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return 0, err
	}

	return util.LdSize(hb), nil
}

func (cw *carWriter) enumGetLinks(ctx context.Context, c cid.Cid) ([]*format.Link, error) {
	nd, err := cw.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := cw.writeNode(ctx, nd); err != nil {
		return nil, err
	}

	return cw.walk(nd)
}

func (cw *carWriter) writeNode(ctx context.Context, nd format.Node) error {
	return util.LdWrite(cw.w, nd.Cid().Bytes(), nd.RawData())
}

var bufioReaderPool = sync.Pool{
	New: func() any { return bufio.NewReader(nil) },
}

type CarReader struct {
	br                *bufio.Reader // set nil on EOF
	Header            *CarHeader
	errorOnEmptyRoots bool
}

type CarReaderOption func(*CarReader) error

// WithErrorOnEmptyRoots is an option that can be passed to NewCarReader to
// specify the behavior when reading a car file that does not have any root
// cids set in the CAR header.
// Setting this option to true will cause CarReader to error on CAR that has
// no root CIDs listed in the header.
func WithErrorOnEmptyRoots(flag bool) CarReaderOption {
	return func(cr *CarReader) error {
		cr.errorOnEmptyRoots = flag
		return nil
	}
}

func NewCarReader(r io.Reader) (*CarReader, error) {
	// BACKWARD COMPATIBILITY NOTE:
	// WithErrorOnEmptyRoots(true) here is the legacy behavior
	// which we need to keep for reasons described in
	// https://github.com/ipfs/specs/pull/402#issuecomment-1599428849
	// (mainly, we need to migrate Filecoin code to use explicit
	// WithErrorOnEmptyRoots(true) before we can change the default here).
	return NewCarReaderWithOptions(r, WithErrorOnEmptyRoots(true))
}

func NewCarReaderWithOptions(r io.Reader, opts ...CarReaderOption) (*CarReader, error) {
	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(r)
	ch, err := ReadHeader(br)
	if err != nil {
		bufioReaderPool.Put(br)
		return nil, err
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	carReader := &CarReader{
		br:     br,
		Header: ch,
	}

	for _, o := range opts {
		err := o(carReader)
		if err != nil {
			return nil, err
		}
	}

	if carReader.errorOnEmptyRoots && len(ch.Roots) == 0 {
		return nil, fmt.Errorf("empty car, no roots")
	}

	return carReader, nil
}

func (cr *CarReader) Next() (blocks.Block, error) {
	if cr.br == nil {
		return nil, io.EOF
	}
	c, data, err := util.ReadNode(cr.br)
	if err != nil {
		if err == io.EOF {
			// Common happy case: recycle the bufio.Reader.
			// In the other error paths leaking it is fine.
			bufioReaderPool.Put(cr.br)
			cr.br = nil
		}
		return nil, err
	}

	hashed, err := c.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	if !hashed.Equals(c) {
		return nil, fmt.Errorf("mismatch in content integrity, name: %s, data: %s", c, hashed)
	}

	return blocks.NewBlockWithCid(data, c)
}

type batchStore interface {
	PutMany(context.Context, []blocks.Block) error
}

func LoadCar(ctx context.Context, s Store, r io.Reader) (*CarHeader, error) {
	cr, err := NewCarReader(r)
	if err != nil {
		return nil, err
	}

	if bs, ok := s.(batchStore); ok {
		return loadCarFast(ctx, bs, cr)
	}

	return loadCarSlow(ctx, s, cr)
}

func loadCarFast(ctx context.Context, s batchStore, cr *CarReader) (*CarHeader, error) {
	var buf []blocks.Block
	for {
		blk, err := cr.Next()
		if err != nil {
			if err == io.EOF {
				if len(buf) > 0 {
					if err := s.PutMany(ctx, buf); err != nil {
						return nil, err
					}
				}
				return cr.Header, nil
			}
			return nil, err
		}

		buf = append(buf, blk)

		if len(buf) > 1000 {
			if err := s.PutMany(ctx, buf); err != nil {
				return nil, err
			}
			buf = buf[:0]
		}
	}
}

func loadCarSlow(ctx context.Context, s Store, cr *CarReader) (*CarHeader, error) {
	for {
		blk, err := cr.Next()
		if err != nil {
			if err == io.EOF {
				return cr.Header, nil
			}
			return nil, err
		}

		if err := s.Put(ctx, blk); err != nil {
			return nil, err
		}
	}
}
