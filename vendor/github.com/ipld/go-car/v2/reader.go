package car

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"golang.org/x/exp/mmap"
)

// Reader represents a reader of CARv2.
type Reader struct {
	Header  Header
	Version uint64
	r       io.ReaderAt
	roots   []cid.Cid
	opts    Options
	closer  io.Closer
}

// OpenReader is a wrapper for NewReader which opens the file at path.
func OpenReader(path string, opts ...Option) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewReader(f, opts...)
	if err != nil {
		return nil, err
	}

	r.closer = f
	return r, nil
}

// NewReader constructs a new reader that reads either CARv1 or CARv2 from the given r.
// Upon instantiation, the reader inspects the payload and provides appropriate read operations
// for both CARv1 and CARv2.
//
// Note that any other version other than 1 or 2 will result in an error. The caller may use
// Reader.Version to get the actual version r represents. In the case where r represents a CARv1
// Reader.Header will not be populated and is left as zero-valued.
func NewReader(r io.ReaderAt, opts ...Option) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	cr.opts = ApplyOptions(opts...)

	or, err := internalio.NewOffsetReadSeeker(r, 0)
	if err != nil {
		return nil, err
	}
	cr.Version, err = ReadVersion(or, opts...)
	if err != nil {
		return nil, err
	}

	if cr.Version != 1 && cr.Version != 2 {
		return nil, fmt.Errorf("invalid car version: %d", cr.Version)
	}

	if cr.Version == 2 {
		if err := cr.readV2Header(); err != nil {
			return nil, err
		}
	}

	return cr, nil
}

// Roots returns the root CIDs.
// The root CIDs are extracted lazily from the data payload header.
func (r *Reader) Roots() ([]cid.Cid, error) {
	if r.roots != nil {
		return r.roots, nil
	}
	dr, err := r.DataReader()
	if err != nil {
		return nil, err
	}
	header, err := carv1.ReadHeader(dr, r.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}
	r.roots = header.Roots
	return r.roots, nil
}

func (r *Reader) readV2Header() (err error) {
	headerSection := io.NewSectionReader(r.r, PragmaSize, HeaderSize)
	_, err = r.Header.ReadFrom(headerSection)
	return
}

// SectionReader implements both io.ReadSeeker and io.ReaderAt.
// It is the interface version of io.SectionReader, but note that the
// implementation is not guaranteed to be an io.SectionReader.
type SectionReader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// DataReader provides a reader containing the data payload in CARv1 format.
func (r *Reader) DataReader() (SectionReader, error) {
	if r.Version == 2 {
		return io.NewSectionReader(r.r, int64(r.Header.DataOffset), int64(r.Header.DataSize)), nil
	}
	return internalio.NewOffsetReadSeeker(r.r, 0)
}

// IndexReader provides an io.Reader containing the index for the data payload if the index is
// present. Otherwise, returns nil.
// Note, this function will always return nil if the backing payload represents a CARv1.
func (r *Reader) IndexReader() (io.Reader, error) {
	if r.Version == 1 || !r.Header.HasIndex() {
		return nil, nil
	}
	return internalio.NewOffsetReadSeeker(r.r, int64(r.Header.IndexOffset))
}

// Stats is returned by an Inspect() call
type Stats struct {
	Version        uint64
	Header         Header
	Roots          []cid.Cid
	RootsPresent   bool
	BlockCount     uint64
	CodecCounts    map[multicodec.Code]uint64
	MhTypeCounts   map[multicodec.Code]uint64
	AvgCidLength   uint64
	MaxCidLength   uint64
	MinCidLength   uint64
	AvgBlockLength uint64
	MaxBlockLength uint64
	MinBlockLength uint64
	IndexCodec     multicodec.Code
}

// Inspect does a quick scan of a CAR, performing basic validation of the format
// and returning a Stats object that provides a high-level description of the
// contents of the CAR.
// Inspect works for CARv1 and CARv2 contents. A CARv1 will return an
// uninitialized Header value.
//
// If validateBlockHash is true, all block data in the payload will be hashed
// and compared to the CID for that block and an error will return if there
// is a mismatch. If false, block data will be skipped over and not checked.
// Performing a full block hash validation is similar to using a BlockReader and
// calling Next over all blocks.
//
// Inspect will perform a basic check of a CARv2 index, where present, but this
// does not guarantee that the index is correct. Attempting to read index data
// from untrusted sources is not recommended. If required, further validation of
// an index can be performed by loading the index and performing a ForEach() and
// sanity checking that the offsets are within the data payload section of the
// CAR. However, re-generation of index data in this case is the recommended
// course of action.
//
// Beyond the checks performed by Inspect, a valid / good CAR is somewhat
// use-case dependent. Factors to consider include:
//
//   - Bad indexes, including incorrect offsets, duplicate entries, or other
//     faulty data. Indexes should be re-generated, regardless, if you need to use
//     them and have any reason to not trust the source.
//
//   - Blocks use codecs that your system doesn't have access to—which may mean
//     you can't traverse a DAG or use the contained data. Stats.CodecCounts
//     contains a list of codecs found in the CAR so this can be checked.
//
//   - CIDs use multihashes that your system doesn't have access to—which will
//     mean you can't validate block hashes are correct (using validateBlockHash
//     in this case will result in a failure). Stats.MhTypeCounts contains a
//     list of multihashes found in the CAR so this can be checked.
//
//   - The presence of IDENTITY CIDs, which may not be supported (or desired) by
//     the consumer of the CAR. Stats.CodecCounts can determine the presence
//     of IDENTITY CIDs.
//
//   - Roots: the number of roots, duplicates, and whether they are related to the
//     blocks contained within the CAR. Stats contains a list of Roots and a
//     RootsPresent bool so further checks can be performed.
//
//   - DAG completeness is not checked. Any properties relating to the DAG, or
//     DAGs contained within a CAR are the responsibility of the user to check.
func (r *Reader) Inspect(validateBlockHash bool) (Stats, error) {
	stats := Stats{
		Version:      r.Version,
		Header:       r.Header,
		CodecCounts:  make(map[multicodec.Code]uint64),
		MhTypeCounts: make(map[multicodec.Code]uint64),
	}

	var totalCidLength uint64
	var totalBlockLength uint64
	var minCidLength uint64 = math.MaxUint64
	var minBlockLength uint64 = math.MaxUint64

	dr, err := r.DataReader()
	if err != nil {
		return Stats{}, err
	}
	bdr := internalio.ToByteReader(dr)

	// read roots, not using Roots(), because we need the offset setup in the data trader
	header, err := carv1.ReadHeader(dr, r.opts.MaxAllowedHeaderSize)
	if err != nil {
		return Stats{}, err
	}
	stats.Roots = header.Roots
	var rootsPresentCount int
	rootsPresent := make([]bool, len(stats.Roots))

	// read block sections
	for {
		sectionLength, err := varint.ReadUvarint(bdr)
		if err != nil {
			if err == io.EOF {
				// if the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
				if sectionLength > 0 {
					return Stats{}, io.ErrUnexpectedEOF
				}
				// otherwise, this is a normal ending
				break
			}
			return Stats{}, err
		}
		if sectionLength == 0 && r.opts.ZeroLengthSectionAsEOF {
			// normal ending for this read mode
			break
		}
		if sectionLength > r.opts.MaxAllowedSectionSize {
			return Stats{}, util.ErrSectionTooLarge
		}

		// decode just the CID bytes
		cidLen, c, err := cid.CidFromReader(dr)
		if err != nil {
			return Stats{}, err
		}

		if sectionLength < uint64(cidLen) {
			// this case is handled different in the normal ReadNode() path since it
			// slurps in the whole section bytes and decodes CID from there - so an
			// error should come from a failing io.ReadFull
			return Stats{}, errors.New("section length shorter than CID length")
		}

		// is this a root block? (also account for duplicate root CIDs)
		if rootsPresentCount < len(stats.Roots) {
			for i, r := range stats.Roots {
				if !rootsPresent[i] && c == r {
					rootsPresent[i] = true
					rootsPresentCount++
				}
			}
		}

		cp := c.Prefix()
		codec := multicodec.Code(cp.Codec)
		count := stats.CodecCounts[codec]
		stats.CodecCounts[codec] = count + 1
		mhtype := multicodec.Code(cp.MhType)
		count = stats.MhTypeCounts[mhtype]
		stats.MhTypeCounts[mhtype] = count + 1

		blockLength := sectionLength - uint64(cidLen)

		if validateBlockHash {
			// Use multihash.SumStream to avoid having to copy the entire block content into memory.
			// The SumStream uses a buffered copy to write bytes into the hasher which will take
			// advantage of streaming hash calculation depending on the hash function.
			// TODO: introduce SumStream in go-cid to simplify the code here.
			blockReader := io.LimitReader(dr, int64(blockLength))
			mhl := cp.MhLength
			if mhtype == multicodec.Identity {
				mhl = -1
			}
			mh, err := multihash.SumStream(blockReader, cp.MhType, mhl)
			if err != nil {
				return Stats{}, err
			}
			var gotCid cid.Cid
			switch cp.Version {
			case 0:
				gotCid = cid.NewCidV0(mh)
			case 1:
				gotCid = cid.NewCidV1(cp.Codec, mh)
			default:
				return Stats{}, fmt.Errorf("invalid cid version: %d", cp.Version)
			}
			if !gotCid.Equals(c) {
				return Stats{}, fmt.Errorf("mismatch in content integrity, expected: %s, got: %s", c, gotCid)
			}
		} else {
			// otherwise, skip over it
			if _, err := dr.Seek(int64(blockLength), io.SeekCurrent); err != nil {
				return Stats{}, err
			}
		}

		stats.BlockCount++
		totalCidLength += uint64(cidLen)
		totalBlockLength += blockLength
		if uint64(cidLen) < minCidLength {
			minCidLength = uint64(cidLen)
		}
		if uint64(cidLen) > stats.MaxCidLength {
			stats.MaxCidLength = uint64(cidLen)
		}
		if blockLength < minBlockLength {
			minBlockLength = blockLength
		}
		if blockLength > stats.MaxBlockLength {
			stats.MaxBlockLength = blockLength
		}
	}

	stats.RootsPresent = len(stats.Roots) == rootsPresentCount
	if stats.BlockCount > 0 {
		stats.MinCidLength = minCidLength
		stats.MinBlockLength = minBlockLength
		stats.AvgCidLength = totalCidLength / stats.BlockCount
		stats.AvgBlockLength = totalBlockLength / stats.BlockCount
	}

	if stats.Version != 1 && stats.Header.HasIndex() {
		idxr, err := r.IndexReader()
		if err != nil {
			return Stats{}, err
		}
		stats.IndexCodec, err = index.ReadCodec(idxr)
		if err != nil {
			return Stats{}, err
		}
	}

	return stats, nil
}

// Close closes the underlying reader if it was opened by OpenReader.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// ReadVersion reads the version from the pragma.
// This function accepts both CARv1 and CARv2 payloads.
func ReadVersion(r io.Reader, opts ...Option) (uint64, error) {
	o := ApplyOptions(opts...)
	header, err := carv1.ReadHeader(r, o.MaxAllowedHeaderSize)
	if err != nil {
		return 0, err
	}
	return header.Version, nil
}
