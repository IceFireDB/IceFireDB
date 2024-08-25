package store

import (
	"bytes"
	"io"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

// FindCid can be used to either up the existence, size and offset of a block
// if it exists in CAR as specified by the index; and optionally the data bytes
// of the block.
func FindCid(
	reader io.ReaderAt,
	idx index.Index,
	key cid.Cid,
	useWholeCids bool,
	zeroLenAsEOF bool,
	maxReadBytes uint64,
	readBytes bool,
) ([]byte, int64, int, error) {

	var fnData []byte
	var fnOffset int64
	var fnLen int = -1
	var fnErr error
	err := idx.GetAll(key, func(offset uint64) bool {
		reader, err := internalio.NewOffsetReadSeeker(reader, int64(offset))
		if err != nil {
			fnErr = err
			return false
		}
		var readCid cid.Cid
		if readBytes {
			readCid, fnData, err = util.ReadNode(reader, zeroLenAsEOF, maxReadBytes)
			if err != nil {
				fnErr = err
				return false
			}
			fnLen = len(fnData)
		} else {
			sectionLen, err := varint.ReadUvarint(reader)
			if err != nil {
				fnErr = err
				return false
			}
			var cidLen int
			cidLen, readCid, err = cid.CidFromReader(reader)
			if err != nil {
				fnErr = err
				return false
			}
			fnLen = int(sectionLen) - cidLen
			fnOffset = int64(offset) + reader.(interface{ Position() int64 }).Position()
		}
		if useWholeCids {
			if !readCid.Equals(key) {
				fnLen = -1
				return true // continue looking
			}
			return false
		} else {
			if !bytes.Equal(readCid.Hash(), key.Hash()) {
				// weird, bad index, continue looking
				fnLen = -1
				return true
			}
			return false
		}
	})
	if err != nil {
		return nil, -1, -1, err
	}
	if fnErr != nil {
		return nil, -1, -1, fnErr
	}
	if fnLen == -1 {
		return nil, -1, -1, index.ErrNotFound
	}
	return fnData, fnOffset, fnLen, nil
}

// Finalize will write the index to the writer at the offset specified in the header. It should only
// be used for a CARv2 and when the CAR interface is being closed.
func Finalize(writer io.WriterAt, header carv2.Header, idx *index.InsertionIndex, dataSize uint64, storeIdentityCIDs bool, indexCodec multicodec.Code) error {
	// TODO check if add index option is set and don't write the index then set index offset to zero.
	header = header.WithDataSize(dataSize)
	header.Characteristics.SetFullyIndexed(storeIdentityCIDs)

	// TODO if index not needed don't bother flattening it.
	fi, err := idx.Flatten(indexCodec)
	if err != nil {
		return err
	}
	if _, err := index.WriteTo(fi, internalio.NewOffsetWriter(writer, int64(header.IndexOffset))); err != nil {
		return err
	}
	if _, err := header.WriteTo(internalio.NewOffsetWriter(writer, carv2.PragmaSize)); err != nil {
		return err
	}
	return nil
}
