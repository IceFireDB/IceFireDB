package store

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-varint"
)

type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

// ResumableVersion performs two tasks - check if there is a valid header at the start of the,
// reader, then check whether the version of that header matches what we expect.
func ResumableVersion(reader io.Reader, writeAsV1 bool) error {
	version, err := carv2.ReadVersion(reader)
	if err != nil {
		// The file is not a valid CAR file and cannot resume from it.
		// Or the write must have failed before pragma was written.
		return err
	}

	switch {
	case version == 1 && writeAsV1:
	case version == 2 && !writeAsV1:
	default:
		// The file is not the expected version and we cannot resume from it.
		return fmt.Errorf("cannot resume on CAR file with version %v", version)
	}
	return nil
}

// Resume will attempt to resume a CARv2 or CARv1 file by checking that there exists an existing
// CAR and that the CAR header details match what is being requested for resumption.
// Resumption of a CARv2 involves "unfinalizing" the header by resetting it back to a bare state
// and then truncating the file to remove the index. Truncation is important because it allows a
// non-finalized CARv2 to be resumed from as the header won't contain the DataSize of the payload
// body and if the file also contains an index, we cannot determine the end of the payload.
// Therefore, when using a resumed, existing and finalized, CARv2, whose body may not extend
// beyond the index and then closing without finalization (e.g. due to a crash), the file will no
// longer be parseable because we won't have DataSize, and we won't be able to determine it by
// parsing the payload to EOF.
func Resume(
	rw ReaderWriterAt,
	dataReader io.ReaderAt,
	dataWriter *internalio.OffsetWriteSeeker,
	idx *index.InsertionIndex,
	roots []cid.Cid,
	dataOffset uint64,
	v1 bool,
	maxAllowedHeaderSize uint64,
	zeroLengthSectionAsEOF bool,
) error {

	var headerInFile carv2.Header
	var v1r internalio.ReadSeekerAt

	if !v1 {
		if _, ok := rw.(interface{ Truncate(size int64) error }); !ok {
			return fmt.Errorf("cannot resume a CARv2 without the ability to truncate (e.g. an io.File)")
		}

		// Check if file was finalized by trying to read the CARv2 header.
		// We check because if finalized the CARv1 reader behaviour needs to be adjusted since
		// EOF will not signify end of CARv1 payload. i.e. index is most likely present.
		r, err := internalio.NewOffsetReadSeeker(rw, carv2.PragmaSize)
		if err != nil {
			return err
		}
		_, err = headerInFile.ReadFrom(r)

		// If reading CARv2 header succeeded, and CARv1 offset in header is not zero then the file is
		// most-likely finalized. Check padding and truncate the file to remove index.
		// Otherwise, carry on reading the v1 payload at offset determined from b.header.
		if err == nil && headerInFile.DataOffset != 0 {
			if headerInFile.DataOffset != dataOffset {
				// Assert that the padding on file matches the given WithDataPadding option.
				wantPadding := headerInFile.DataOffset - carv2.PragmaSize - carv2.HeaderSize
				gotPadding := dataOffset - carv2.PragmaSize - carv2.HeaderSize
				return fmt.Errorf(
					"cannot resume from file with mismatched CARv1 offset; "+
						"`WithDataPadding` option must match the padding on file. "+
						"Expected padding value of %v but got %v", wantPadding, gotPadding,
				)
			} else if headerInFile.DataSize == 0 {
				// If CARv1 size is zero, since CARv1 offset wasn't, then the CARv2 header was
				// most-likely partially written. Since we write the header last in Finalize then the
				// file most-likely contains the index and we cannot know where it starts, therefore
				// can't resume.
				return errors.New("corrupt CARv2 header; cannot resume from file")
			}
		}

		v1r, err = internalio.NewOffsetReadSeeker(dataReader, 0)
		if err != nil {
			return err
		}
	} else {
		var err error
		v1r, err = internalio.NewOffsetReadSeeker(rw, 0)
		if err != nil {
			return err
		}
	}

	header, err := carv1.ReadHeader(v1r, maxAllowedHeaderSize)
	if err != nil {
		// Cannot read the CARv1 header; the file is most likely corrupt.
		return fmt.Errorf("error reading car header: %w", err)
	}
	if !header.Matches(carv1.CarHeader{Roots: roots, Version: 1}) {
		// Cannot resume if version and root does not match.
		return errors.New("cannot resume on file with mismatching data header")
	}

	if headerInFile.DataOffset != 0 {
		// If header in file contains the size of car v1, then the index is most likely present.
		// Since we will need to re-generate the index, as the one in file is flattened, truncate
		// the file so that the Readonly.backing has the right set of bytes to deal with.
		// This effectively means resuming from a finalized file will wipe its index even if there
		// are no blocks put unless the user calls finalize.
		if err := rw.(interface{ Truncate(size int64) error }).Truncate(int64(headerInFile.DataOffset + headerInFile.DataSize)); err != nil {
			return err
		}
	}

	if !v1 {
		// Now that CARv2 header is present on file, clear it to avoid incorrect size and offset in
		// header in case blocksotre is closed without finalization and is resumed from.
		wat, ok := rw.(io.WriterAt)
		if !ok { // how would we get this far??
			return errors.New("cannot resume from file without io.WriterAt")
		}
		if _, err := new(carv2.Header).WriteTo(internalio.NewOffsetWriter(wat, carv2.PragmaSize)); err != nil {
			return fmt.Errorf("could not un-finalize: %w", err)
		}
	}

	// TODO See how we can reduce duplicate code here.
	// The code here comes from car.GenerateIndex.
	// Copied because we need to populate an insertindex, not a sorted index.
	// Producing a sorted index via generate, then converting it to insertindex is not possible.
	// Because Index interface does not expose internal records.
	// This may be done as part of https://github.com/ipld/go-car/issues/95

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return err
	}
	sectionOffset := int64(0)
	if sectionOffset, err = v1r.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	for {
		// Grab the length of the section.
		// Note that ReadUvarint wants a ByteReader.
		length, err := varint.ReadUvarint(v1r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Null padding; by default it's an error.
		if length == 0 {
			if zeroLengthSectionAsEOF {
				break
			} else {
				return fmt.Errorf("carv1 null padding not allowed by default; see WithZeroLegthSectionAsEOF")
			}
		}

		// Grab the CID.
		n, c, err := cid.CidFromReader(v1r)
		if err != nil {
			return err
		}
		idx.InsertNoReplace(c, uint64(sectionOffset))

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		if sectionOffset, err = v1r.Seek(int64(length)-int64(n), io.SeekCurrent); err != nil {
			return err
		}
	}
	// Seek to the end of last skipped block where the writer should resume writing.
	_, err = dataWriter.Seek(sectionOffset, io.SeekStart)
	return err
}
