package car

import (
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

// GenerateIndex generates index for the given car payload reader.
// The index can be stored in serialized format using index.WriteTo.
//
// Note, the index is re-generated every time even if the payload is in CARv2 format and already has
// an index. To read existing index when available see ReadOrGenerateIndex.
// See: LoadIndex.
func GenerateIndex(v1r io.Reader, opts ...Option) (index.Index, error) {
	wopts := ApplyOptions(opts...)
	idx, err := index.New(wopts.IndexCodec)
	if err != nil {
		return nil, err
	}
	if err := LoadIndex(idx, v1r, opts...); err != nil {
		return nil, err
	}
	return idx, nil
}

// LoadIndex populates idx with index records generated from r.
// The r may be in CARv1 or CARv2 format.
//
// If the StoreIdentityCIDs option is set when calling LoadIndex, identity
// CIDs will be included in the index. By default this option is off, and
// identity CIDs will not be included in the index.
//
// Note, the index is re-generated every time even if r is in CARv2 format and already has an index.
// To read existing index when available see ReadOrGenerateIndex.
func LoadIndex(idx index.Index, r io.Reader, opts ...Option) error {
	// Parse Options.
	o := ApplyOptions(opts...)

	reader := internalio.ToByteReadSeeker(r)
	pragma, err := carv1.ReadHeader(r, o.MaxAllowedHeaderSize)
	if err != nil {
		return fmt.Errorf("error reading car header: %w", err)
	}

	var dataSize, dataOffset int64
	switch pragma.Version {
	case 1:
		break
	case 2:
		// Read V2 header which should appear immediately after pragma according to CARv2 spec.
		var v2h Header
		_, err := v2h.ReadFrom(r)
		if err != nil {
			return err
		}

		// Sanity-check the CARv2 header
		if v2h.DataOffset < HeaderSize {
			return fmt.Errorf("malformed CARv2; data offset too small: %d", v2h.DataOffset)
		}
		if v2h.DataSize < 1 {
			return fmt.Errorf("malformed CARv2; data payload size too small: %d", v2h.DataSize)
		}

		// Seek to the beginning of the inner CARv1 payload
		_, err = reader.Seek(int64(v2h.DataOffset), io.SeekStart)
		if err != nil {
			return err
		}

		// Set dataSize and dataOffset which are then used during index loading logic to decide
		// where to stop and adjust section offset respectively.
		// Note that we could use a LimitReader here and re-define reader with it. However, it means
		// the internalio.ToByteReadSeeker will be less efficient since LimitReader does not
		// implement ByteReader nor ReadSeeker.
		dataSize = int64(v2h.DataSize)
		dataOffset = int64(v2h.DataOffset)

		// Read the inner CARv1 header to skip it and sanity check it.
		v1h, err := carv1.ReadHeader(reader, o.MaxAllowedHeaderSize)
		if err != nil {
			return err
		}
		if v1h.Version != 1 {
			return fmt.Errorf("expected data payload header version of 1; got %d", v1h.Version)
		}
	default:
		return fmt.Errorf("expected either version 1 or 2; got %d", pragma.Version)
	}

	// Record the start of each section, with first section starring from current position in the
	// reader, i.e. right after the header, since we have only read the header so far.
	var sectionOffset int64

	// The Seek call below is equivalent to getting the reader.offset directly.
	// We get it through Seek to only depend on APIs of a typical io.Seeker.
	// This would also reduce refactoring in case the utility reader is moved.
	if sectionOffset, err = reader.Seek(0, io.SeekCurrent); err != nil {
		return err
	}

	// Subtract the data offset; if CARv1 this would be zero otherwise the value will come from the
	// CARv2 header.
	sectionOffset -= dataOffset

	records := make([]index.Record, 0)
	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Null padding; by default it's an error.
		if sectionLen == 0 {
			if o.ZeroLengthSectionAsEOF {
				break
			} else {
				return fmt.Errorf("carv1 null padding not allowed by default; see ZeroLengthSectionAsEOF")
			}
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(reader)
		if err != nil {
			return err
		}

		if o.StoreIdentityCIDs || c.Prefix().MhType != multihash.IDENTITY {
			if uint64(cidLen) > o.MaxIndexCidSize {
				return &ErrCidTooLarge{MaxSize: o.MaxIndexCidSize, CurrentSize: uint64(cidLen)}
			}
			records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})
		}

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if sectionOffset, err = reader.Seek(remainingSectionLen, io.SeekCurrent); err != nil {
			return err
		}
		// Subtract the data offset which will be non-zero when reader represents a CARv2.
		sectionOffset -= dataOffset

		// Check if we have reached the end of data payload and if so treat it as an EOF.
		// Note, dataSize will be non-zero only if we are reading from a CARv2.
		if dataSize != 0 && sectionOffset >= dataSize {
			break
		}
	}

	if err := idx.Load(records); err != nil {
		return err
	}

	return nil
}

// GenerateIndexFromFile walks a CAR file at the give path and generates an index of cid->byte offset.
// The index can be stored using index.WriteTo. Both CARv1 and CARv2 formats are accepted.
//
// Note, the index is re-generated every time even if the given CAR file is in CARv2 format and
// already has an index. To read existing index when available see ReadOrGenerateIndex.
//
// See: GenerateIndex.
func GenerateIndexFromFile(path string, opts ...Option) (index.Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return GenerateIndex(f, opts...)
}

// ReadOrGenerateIndex accepts both CARv1 and CARv2 formats, and reads or generates an index for it.
// When the given reader is in CARv1 format an index is always generated.
// For a payload in CARv2 format, an index is only generated if Header.HasIndex returns false.
// An error is returned for all other formats, i.e. pragma with versions other than 1 or 2.
//
// Note, the returned index lives entirely in memory and will not depend on the
// given reader to fulfill index lookup.
func ReadOrGenerateIndex(rs io.ReadSeeker, opts ...Option) (index.Index, error) {
	// Read version.
	version, err := ReadVersion(rs, opts...)
	if err != nil {
		return nil, err
	}
	// Seek to the beginning, since reading the version changes the reader's offset.
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	switch version {
	case 1:
		// Simply generate the index, since there can't be a pre-existing one.
		return GenerateIndex(rs, opts...)
	case 2:
		// Read CARv2 format
		v2r, err := NewReader(internalio.ToReaderAt(rs), opts...)
		if err != nil {
			return nil, err
		}
		// If index is present, then no need to generate; decode and return it.
		if v2r.Header.HasIndex() {
			ir, err := v2r.IndexReader()
			if err != nil {
				return nil, err
			}
			return index.ReadFrom(ir)
		}
		// Otherwise, generate index from CARv1 payload wrapped within CARv2 format.
		dr, err := v2r.DataReader()
		if err != nil {
			return nil, err
		}
		return GenerateIndex(dr, opts...)
	default:
		return nil, fmt.Errorf("unknown version %v", version)
	}
}
