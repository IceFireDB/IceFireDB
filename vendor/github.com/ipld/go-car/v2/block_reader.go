package car

import (
	"errors"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-varint"
)

// BlockReader facilitates iteration over CAR blocks for both CARv1 and CARv2.
// See NewBlockReader
type BlockReader struct {
	// The detected version of the CAR payload.
	Version uint64
	// The roots of the CAR payload. May be empty.
	Roots []cid.Cid

	// Used internally only, by BlockReader.Next during iteration over blocks.
	r          io.Reader
	offset     uint64
	v1offset   uint64
	readerSize int64
	opts       Options
}

// NewBlockReader instantiates a new BlockReader facilitating iteration over blocks in CARv1 or
// CARv2 payload. Upon instantiation, the version is automatically detected and exposed via
// BlockReader.Version. The root CIDs of the CAR payload are exposed via BlockReader.Roots
//
// See BlockReader.Next
func NewBlockReader(r io.Reader, opts ...Option) (*BlockReader, error) {
	options := ApplyOptions(opts...)

	// Read CARv1 header or CARv2 pragma.
	// Both are a valid CARv1 header, therefore are read as such.
	pragmaOrV1Header, err := carv1.ReadHeader(r, options.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}

	// Populate the block reader version and options.
	br := &BlockReader{
		Version: pragmaOrV1Header.Version,
		opts:    options,
	}

	// Expect either version 1 or 2.
	switch br.Version {
	case 1:
		// If version is 1, r represents a CARv1.
		// Simply populate br.Roots and br.r without modifying r.
		br.Roots = pragmaOrV1Header.Roots
		br.r = r
		br.readerSize = -1
		br.offset, _ = carv1.HeaderSize(pragmaOrV1Header)
	case 2:
		// If the version is 2:
		//  1. Read CARv2 specific header to locate the inner CARv1 data payload offset and size.
		//  2. Skip to the beginning of the inner CARv1 data payload.
		//  3. Re-initialize r as a LimitReader, limited to the size of the inner CARv1 payload.
		//  4. Read the header of inner CARv1 data payload via r to populate br.Roots.

		// Read CARv2-specific header.
		v2h := Header{}
		if _, err := v2h.ReadFrom(r); err != nil {
			return nil, err
		}

		// Skip to the beginning of inner CARv1 data payload.
		// Note, at this point the pragma and CARv1 header have been read.
		// An io.ReadSeeker is opportunistically constructed from the given io.Reader r.
		// The constructor does not take an initial offset, so we use Seek in io.SeekCurrent to
		// fast forward to the beginning of data payload by subtracting pragma and header size from
		// dataOffset.
		rs := internalio.ToByteReadSeeker(r)
		if _, err := rs.Seek(int64(v2h.DataOffset)-PragmaSize-HeaderSize, io.SeekCurrent); err != nil {
			return nil, err
		}
		br.v1offset = uint64(v2h.DataOffset)
		br.offset = br.v1offset
		br.readerSize = int64(v2h.DataOffset + v2h.DataSize)

		// Set br.r to a LimitReader reading from r limited to dataSize.
		br.r = io.LimitReader(r, int64(v2h.DataSize))

		// Populate br.Roots by reading the inner CARv1 data payload header.
		header, err := carv1.ReadHeader(br.r, options.MaxAllowedHeaderSize)
		if err != nil {
			return nil, err
		}
		// Assert that the data payload header is exactly 1, i.e. the header represents a CARv1.
		if header.Version != 1 {
			return nil, fmt.Errorf("invalid data payload header version; expected 1, got %v", header.Version)
		}
		br.Roots = header.Roots
		hs, _ := carv1.HeaderSize(header)
		br.offset += hs
	default:
		// Otherwise, error out with invalid version since only versions 1 or 2 are expected.
		return nil, fmt.Errorf("invalid car version: %d", br.Version)
	}
	return br, nil
}

// Next iterates over blocks in the underlying CAR payload with an io.EOF error indicating the end
// is reached. Note, this function is forward-only; once the end has been reached it will always
// return io.EOF.
//
// When the payload represents a CARv1 the BlockReader.Next simply iterates over blocks until it
// reaches the end of the underlying io.Reader stream.
//
// As for CARv2 payload, the underlying io.Reader is read only up to the end of the last block.
// Note, in a case where ZeroLengthSectionAsEOF Option is enabled, io.EOF is returned
// immediately upon encountering a zero-length section without reading any further bytes from the
// underlying io.Reader.
func (br *BlockReader) Next() (blocks.Block, error) {
	c, data, err := util.ReadNode(br.r, br.opts.ZeroLengthSectionAsEOF, br.opts.MaxAllowedSectionSize)
	if err != nil {
		return nil, err
	}

	if !br.opts.TrustedCAR {
		hashed, err := c.Prefix().Sum(data)
		if err != nil {
			return nil, err
		}

		if !hashed.Equals(c) {
			return nil, fmt.Errorf("mismatch in content integrity, expected: %s, got: %s", c, hashed)
		}
	}

	ss := uint64(c.ByteLen()) + uint64(len(data))
	br.offset += uint64(varint.UvarintSize(ss)) + ss
	return blocks.NewBlockWithCid(data, c)
}

// BlockMetadata contains metadata about a block's section in a CAR file/stream.
//
// There are two offsets for the block section which will be the same if the
// original CAR is a CARv1, but will differ if the original CAR is a CARv2.
//
// The block section offset is position where the CAR section begins; that is,
// the begining of the length prefix (varint) prior to the CID and the block
// data. Reading the varint at the offset will give the length of the rest of
// the section (CID+data).
//
// In the case of a CARv2, SourceOffset will be the offset from the beginning of
// the file/steam, and Offset will be the offset from the beginning of the CARv1
// payload container within the CARv2.
//
// Offset is useful for index generation which requires an offset from the CARv1
// payload; while SourceOffset is useful for direct section reads out of the
// source file/stream regardless of version.
type BlockMetadata struct {
	cid.Cid
	Offset       uint64 // Offset of the section data in the container CARv1
	SourceOffset uint64 // SourceOffset is the offset of section data in the source file/stream
	Size         uint64
}

// SkipNext jumps over the next block, returning metadata about what it is (the CID, offset, and size).
// Like Next it will return an io.EOF once it has reached the end.
//
// If the underlying reader used by the BlockReader is actually a ReadSeeker, this method will attempt to
// seek over the underlying data rather than reading it into memory.
func (br *BlockReader) SkipNext() (*BlockMetadata, error) {
	sectionSize, err := util.LdReadSize(br.r, br.opts.ZeroLengthSectionAsEOF, br.opts.MaxAllowedSectionSize)
	if err != nil {
		return nil, err
	}
	if sectionSize == 0 {
		_, _, err := cid.CidFromBytes([]byte{}) // generate zero-byte CID error
		if err == nil {
			panic("expected zero-byte CID error")
		}
		return nil, err
	}

	lenSize := uint64(varint.UvarintSize(sectionSize))

	cidSize, c, err := cid.CidFromReader(io.LimitReader(br.r, int64(sectionSize)))
	if err != nil {
		return nil, err
	}

	blockSize := sectionSize - uint64(cidSize)
	blockOffset := br.offset

	// move our reader forward; either by seeking or slurping

	if brs, ok := br.r.(io.ReadSeeker); ok {
		// carv1 and we don't know the size, so work it out and cache it so we
		// can use it to determine over-reads
		if br.readerSize == -1 {
			cur, err := brs.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			end, err := brs.Seek(0, io.SeekEnd)
			if err != nil {
				return nil, err
			}
			br.readerSize = end
			if _, err = brs.Seek(cur, io.SeekStart); err != nil {
				return nil, err
			}
		}

		// seek forward past the block data
		finalOffset, err := brs.Seek(int64(blockSize), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if finalOffset != int64(br.offset)+int64(lenSize)+int64(sectionSize) {
			return nil, errors.New("unexpected length")
		}
		if finalOffset > br.readerSize {
			return nil, io.ErrUnexpectedEOF
		}
	} else { // just a reader, we need to slurp the block bytes
		readCnt, err := io.CopyN(io.Discard, br.r, int64(blockSize))
		if err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if readCnt != int64(blockSize) {
			return nil, errors.New("unexpected length")
		}
	}

	br.offset = br.offset + lenSize + uint64(cidSize) + blockSize

	return &BlockMetadata{
		Cid:          c,
		Offset:       blockOffset - br.v1offset,
		SourceOffset: blockOffset,
		Size:         blockSize,
	}, nil
}
