package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-car/v2/internal/store"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
)

var ErrClosed = errors.New("cannot use a CAR storage after closing")

type ReaderAtWriterAt interface {
	io.ReaderAt
	io.Writer
	io.WriterAt
}

type ReadableCar interface {
	ipldstorage.ReadableStorage
	ipldstorage.StreamingReadableStorage
	Roots() []cid.Cid
	Index() index.Index
}

// WritableCar is compatible with storage.WritableStorage but also returns
// the roots of the CAR. It does not implement ipld.StreamingWritableStorage
// as the CAR format does not support streaming data followed by its CID, so
// any streaming implementation would perform buffering and copy the
// existing storage.PutStream() implementation.
type WritableCar interface {
	ipldstorage.WritableStorage
	Roots() []cid.Cid
	Index() index.Index
	Finalize() error
}

var _ ReadableCar = (*StorageCar)(nil)
var _ WritableCar = (*StorageCar)(nil)

type StorageCar struct {
	idx        index.Index
	reader     io.ReaderAt
	writer     positionedWriter
	dataWriter *internalio.OffsetWriteSeeker
	header     carv2.Header
	roots      []cid.Cid
	opts       carv2.Options

	closed bool
	mu     sync.RWMutex
}

type positionedWriter interface {
	io.Writer
	Position() int64
}

// OpenReadable opens a CARv1 or CARv2 file for reading as a ReadableStorage
// and StreamingReadableStorage as defined by
// github.com/ipld/go-ipld-prime/storage.
//
// The returned ReadableStorage is compatible with a linksystem SetReadStorage
// method as defined by github.com/ipld/go-ipld-prime/linking
// to provide a block source backed by a CAR.
//
// When opening a CAR, an initial scan is performed to generate an index, or
// load an index from a CARv2 index where available. This index data is kept in
// memory while the CAR is being used in order to provide efficient random
// Get access to blocks and Has operations.
//
// The Readable supports StreamingReadableStorage, which allows for efficient
// GetStreaming operations straight out of the underlying CAR where the
// linksystem can make use of it.
func OpenReadable(reader io.ReaderAt, opts ...carv2.Option) (ReadableCar, error) {
	sc := &StorageCar{opts: carv2.ApplyOptions(opts...)}

	rr := internalio.ToReadSeeker(reader)
	header, err := carv1.ReadHeader(rr, sc.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}
	switch header.Version {
	case 1:
		sc.roots = header.Roots
		sc.reader = reader
		rr.Seek(0, io.SeekStart)
		sc.idx = index.NewInsertionIndex()
		if err := carv2.LoadIndex(sc.idx, rr, opts...); err != nil {
			return nil, err
		}
	case 2:
		v2r, err := carv2.NewReader(reader, opts...)
		if err != nil {
			return nil, err
		}
		sc.roots, err = v2r.Roots()
		if err != nil {
			return nil, err
		}
		if v2r.Header.HasIndex() {
			ir, err := v2r.IndexReader()
			if err != nil {
				return nil, err
			}
			sc.idx, err = index.ReadFrom(ir)
			if err != nil {
				return nil, err
			}
		} else {
			dr, err := v2r.DataReader()
			if err != nil {
				return nil, err
			}
			sc.idx = index.NewInsertionIndex()
			if err := carv2.LoadIndex(sc.idx, dr, opts...); err != nil {
				return nil, err
			}
		}
		if sc.reader, err = v2r.DataReader(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported CAR version: %v", header.Version)
	}

	return sc, nil
}

// NewWritable creates a new WritableStorage as defined by
// github.com/ipld/go-ipld-prime/storage that writes a CARv1 or CARv2 format to
// the given io.Writer.
//
// The returned WritableStorage is compatible with a linksystem SetWriteStorage
// method as defined by github.com/ipld/go-ipld-prime/linking
// to provide a block sink backed by a CAR.
//
// The WritableStorage supports Put operations, which will write
// blocks to the CAR in the order they are received.
//
// When writing a CARv2 format (the default), the provided writer must be
// compatible with io.WriterAt in order to provide random access as the CARv2
// header must be written after the blocks in order to indicate the size of the
// CARv2 data payload.
//
// A CARv1 (generated using the WriteAsCarV1 option) only requires an io.Writer
// and can therefore stream CAR contents as blocks are written while still
// providing Has operations and the ability to avoid writing duplicate blocks
// as required.
//
// When writing a CARv2 format, it is important to call the Finalize method on
// the returned WritableStorage in order to write the CARv2 header and index.
func NewWritable(writer io.Writer, roots []cid.Cid, opts ...carv2.Option) (WritableCar, error) {
	sc, err := newWritable(writer, roots, opts...)
	if err != nil {
		return nil, err
	}
	return sc.init()
}

func newWritable(writer io.Writer, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc := &StorageCar{
		writer: &positionTrackingWriter{w: writer},
		idx:    index.NewInsertionIndex(),
		header: carv2.NewHeader(0),
		opts:   carv2.ApplyOptions(opts...),
		roots:  roots,
	}

	if p := sc.opts.DataPadding; p > 0 {
		sc.header = sc.header.WithDataPadding(p)
	}
	if p := sc.opts.IndexPadding; p > 0 {
		sc.header = sc.header.WithIndexPadding(p)
	}

	offset := int64(sc.header.DataOffset)
	if sc.opts.WriteAsCarV1 {
		offset = 0
	}

	if writerAt, ok := writer.(io.WriterAt); ok {
		sc.dataWriter = internalio.NewOffsetWriter(writerAt, offset)
	} else {
		if !sc.opts.WriteAsCarV1 {
			return nil, fmt.Errorf("cannot write as CARv2 to a non-seekable writer")
		}
	}

	return sc, nil
}

func newReadableWritable(rw ReaderAtWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}

	sc.reader = rw
	if !sc.opts.WriteAsCarV1 {
		sc.reader, err = internalio.NewOffsetReadSeeker(rw, int64(sc.header.DataOffset))
		if err != nil {
			return nil, err
		}
	}

	return sc, nil
}

// NewReadableWritable creates a new StorageCar that is able to provide both
// StorageReader and StorageWriter functionality.
//
// The returned StorageCar is compatible with a linksystem SetReadStorage and
// SetWriteStorage methods as defined by github.com/ipld/go-ipld-prime/linking.
//
// When writing a CARv2 format, it is important to call the Finalize method on
// the returned WritableStorage in order to write the CARv2 header and index.
func NewReadableWritable(rw ReaderAtWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newReadableWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}
	if _, err := sc.init(); err != nil {
		return nil, err
	}
	return sc, nil
}

// OpenReadableWritable creates a new StorageCar that is able to provide both
// StorageReader and StorageWriter functionality.
//
// The returned StorageCar is compatible with a linksystem SetReadStorage and
// SetWriteStorage methods as defined by github.com/ipld/go-ipld-prime/linking.
//
// It attempts to resume a CARv2 file that was previously written to by
// NewWritable, or NewReadableWritable.
func OpenReadableWritable(rw ReaderAtWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newReadableWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}

	// attempt to resume
	rs, err := internalio.NewOffsetReadSeeker(rw, 0)
	if err != nil {
		return nil, err
	}
	if err := store.ResumableVersion(rs, sc.opts.WriteAsCarV1); err != nil {
		return nil, err
	}
	if err := store.Resume(
		rw,
		sc.reader,
		sc.dataWriter,
		sc.idx.(*index.InsertionIndex),
		roots,
		sc.header.DataOffset,
		sc.opts.WriteAsCarV1,
		sc.opts.MaxAllowedHeaderSize,
		sc.opts.ZeroLengthSectionAsEOF,
	); err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *StorageCar) init() (WritableCar, error) {
	if !sc.opts.WriteAsCarV1 {
		if _, err := sc.writer.Write(carv2.Pragma); err != nil {
			return nil, err
		}
	}
	var w io.Writer = sc.dataWriter
	if sc.dataWriter == nil {
		w = sc.writer
	}
	if err := carv1.WriteHeader(&carv1.CarHeader{Roots: sc.roots, Version: 1}, w); err != nil {
		return nil, err
	}
	return sc, nil
}

// Roots returns the roots of the CAR.
func (sc *StorageCar) Roots() []cid.Cid {
	return sc.roots
}

// Index gives direct access to the index. It should be used with care.
// Modifying the index may result corruption or invalid reads.
func (sc *StorageCar) Index() index.Index {
	return sc.idx
}

// Put adds a block to the CAR, where the block is identified by the given CID
// provided in string form. The keyStr value must be a valid CID binary string
// (not a multibase string representation), i.e. generated with CID#KeyString().
func (sc *StorageCar) Put(ctx context.Context, keyStr string, data []byte) error {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return fmt.Errorf("bad CID key: %w", err)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.closed {
		return ErrClosed
	}

	idx, ok := sc.idx.(*index.InsertionIndex)
	if !ok || sc.writer == nil {
		return fmt.Errorf("cannot put into a read-only CAR")
	}

	if should, err := store.ShouldPut(
		idx,
		keyCid,
		sc.opts.MaxIndexCidSize,
		sc.opts.StoreIdentityCIDs,
		sc.opts.BlockstoreAllowDuplicatePuts,
		sc.opts.BlockstoreUseWholeCIDs,
	); err != nil {
		return err
	} else if !should {
		return nil
	}

	w := sc.writer
	if sc.dataWriter != nil {
		w = sc.dataWriter
	}
	n := uint64(w.Position())
	if err := util.LdWrite(w, keyCid.Bytes(), data); err != nil {
		return err
	}
	idx.InsertNoReplace(keyCid, n)

	return nil
}

// Has returns true if the CAR contains a block identified by the given CID
// provided in string form. The keyStr value must be a valid CID binary string
// (not a multibase string representation), i.e. generated with CID#KeyString().
func (sc *StorageCar) Has(ctx context.Context, keyStr string) (bool, error) {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return false, fmt.Errorf("bad CID key: %w", err)
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return false, ErrClosed
	}

	if idx, ok := sc.idx.(*index.InsertionIndex); ok && sc.writer != nil {
		// writable CAR, fast path using InsertionIndex
		return store.Has(
			idx,
			keyCid,
			sc.opts.MaxIndexCidSize,
			sc.opts.StoreIdentityCIDs,
			sc.opts.BlockstoreAllowDuplicatePuts,
			sc.opts.BlockstoreUseWholeCIDs,
		)
	}

	if !sc.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if _, ok, err := store.IsIdentity(keyCid); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	_, _, size, err := store.FindCid(
		sc.reader,
		sc.idx,
		keyCid,
		sc.opts.BlockstoreUseWholeCIDs,
		sc.opts.ZeroLengthSectionAsEOF,
		sc.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return size > -1, nil
}

// Get returns the block bytes identified by the given CID provided in string
// form. The keyStr value must be a valid CID binary string (not a multibase
// string representation), i.e. generated with CID#KeyString().
func (sc *StorageCar) Get(ctx context.Context, keyStr string) ([]byte, error) {
	rdr, err := sc.GetStream(ctx, keyStr)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rdr)
}

// GetStream returns a stream of the block bytes identified by the given CID
// provided in string form. The keyStr value must be a valid CID binary string
// (not a multibase string representation), i.e. generated with CID#KeyString().
func (sc *StorageCar) GetStream(ctx context.Context, keyStr string) (io.ReadCloser, error) {
	if sc.reader == nil {
		return nil, fmt.Errorf("cannot read from a write-only CAR")
	}

	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return nil, fmt.Errorf("bad CID key: %w", err)
	}

	if !sc.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if digest, ok, err := store.IsIdentity(keyCid); err != nil {
			return nil, err
		} else if ok {
			return io.NopCloser(bytes.NewReader(digest)), nil
		}
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return nil, ErrClosed
	}

	_, offset, size, err := store.FindCid(
		sc.reader,
		sc.idx,
		keyCid,
		sc.opts.BlockstoreUseWholeCIDs,
		sc.opts.ZeroLengthSectionAsEOF,
		sc.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return nil, ErrNotFound{Cid: keyCid}
	} else if err != nil {
		return nil, err
	}
	return io.NopCloser(io.NewSectionReader(sc.reader, offset, int64(size))), nil
}

// Finalize writes the CAR index to the underlying writer if the CAR being
// written is a CARv2. It also writes a finalized CARv2 header which details
// payload location. This should be called on a writable StorageCar in order to
// avoid data loss.
func (sc *StorageCar) Finalize() error {
	idx, ok := sc.idx.(*index.InsertionIndex)
	if !ok || sc.writer == nil {
		// ignore this, it's not writable
		return nil
	}

	if sc.opts.WriteAsCarV1 {
		return nil
	}

	wat, ok := sc.writer.(*positionTrackingWriter).w.(io.WriterAt)
	if !ok { // should should already be checked at construction if this is a writable
		return fmt.Errorf("cannot finalize a CARv2 without an io.WriterAt")
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.closed {
		// Allow duplicate Finalize calls, just like Close.
		// Still error, just like ReadOnly.Close; it should be discarded.
		return fmt.Errorf("called Finalize on a closed storage CAR")
	}

	sc.closed = true

	return store.Finalize(wat, sc.header, idx, uint64(sc.dataWriter.Position()), sc.opts.StoreIdentityCIDs, sc.opts.IndexCodec)
}

type positionTrackingWriter struct {
	w      io.Writer
	offset int64
}

func (ptw *positionTrackingWriter) Write(p []byte) (int, error) {
	written, err := ptw.w.Write(p)
	ptw.offset += int64(written)
	return written, err
}

func (ptw *positionTrackingWriter) Position() int64 {
	return ptw.offset
}
