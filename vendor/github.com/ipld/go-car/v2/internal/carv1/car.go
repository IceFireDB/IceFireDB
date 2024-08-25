package carv1

import (
	"context"
	"fmt"
	"io"

	"github.com/ipld/go-car/v2/internal/carv1/util"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	internalio "github.com/ipld/go-car/v2/internal/io"
)

const DefaultMaxAllowedHeaderSize uint64 = 32 << 20 // 32MiB
const DefaultMaxAllowedSectionSize uint64 = 8 << 20 // 8MiB

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

func ReadHeaderAt(at io.ReaderAt, maxReadBytes uint64) (*CarHeader, error) {
	var rr io.Reader
	switch r := at.(type) {
	case io.Reader:
		rr = r
	default:
		var err error
		rr, err = internalio.NewOffsetReadSeeker(r, 0)
		if err != nil {
			return nil, err
		}
	}
	return ReadHeader(rr, maxReadBytes)
}

func ReadHeader(r io.Reader, maxReadBytes uint64) (*CarHeader, error) {
	hb, err := util.LdRead(r, false, maxReadBytes)
	if err != nil {
		if err == util.ErrSectionTooLarge {
			err = util.ErrHeaderTooLarge
		}
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

type CarReader struct {
	r                     io.Reader
	Header                *CarHeader
	zeroLenAsEOF          bool
	maxAllowedSectionSize uint64
}

func NewCarReaderWithZeroLengthSectionAsEOF(r io.Reader) (*CarReader, error) {
	return NewCarReaderWithoutDefaults(r, true, DefaultMaxAllowedHeaderSize, DefaultMaxAllowedSectionSize)
}

func NewCarReader(r io.Reader) (*CarReader, error) {
	return NewCarReaderWithoutDefaults(r, false, DefaultMaxAllowedHeaderSize, DefaultMaxAllowedSectionSize)
}

func NewCarReaderWithoutDefaults(r io.Reader, zeroLenAsEOF bool, maxAllowedHeaderSize uint64, maxAllowedSectionSize uint64) (*CarReader, error) {
	ch, err := ReadHeader(r, maxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	if len(ch.Roots) == 0 {
		return nil, fmt.Errorf("empty car, no roots")
	}

	return &CarReader{
		r:                     r,
		Header:                ch,
		zeroLenAsEOF:          zeroLenAsEOF,
		maxAllowedSectionSize: maxAllowedSectionSize,
	}, nil
}

func (cr *CarReader) Next() (blocks.Block, error) {
	c, data, err := util.ReadNode(cr.r, cr.zeroLenAsEOF, cr.maxAllowedSectionSize)
	if err != nil {
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

func LoadCar(s Store, r io.Reader) (*CarHeader, error) {
	ctx := context.TODO()
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

// Matches checks whether two headers match.
// Two headers are considered matching if:
//  1. They have the same version number, and
//  2. They contain the same root CIDs in any order.
//
// Note, this function explicitly ignores the order of roots.
// If order of roots matter use reflect.DeepEqual instead.
func (h CarHeader) Matches(other CarHeader) bool {
	if h.Version != other.Version {
		return false
	}
	thisLen := len(h.Roots)
	if thisLen != len(other.Roots) {
		return false
	}
	// Headers with a single root are popular.
	// Implement a fast execution path for popular cases.
	if thisLen == 1 {
		return h.Roots[0].Equals(other.Roots[0])
	}

	// Check other contains all roots.
	// TODO: should this be optimised for cases where the number of roots are large since it has O(N^2) complexity?
	for _, r := range h.Roots {
		if !other.containsRoot(r) {
			return false
		}
	}
	return true
}

func (h *CarHeader) containsRoot(root cid.Cid) bool {
	for _, r := range h.Roots {
		if r.Equals(root) {
			return true
		}
	}
	return false
}
