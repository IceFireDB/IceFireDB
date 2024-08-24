package filestore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	pb "github.com/ipfs/go-filestore/pb"

	proto "github.com/gogo/protobuf/proto"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	posinfo "github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// FilestorePrefix identifies the key prefix for FileManager blocks.
var FilestorePrefix = ds.NewKey("filestore")

// FileManager is a blockstore implementation which stores special
// blocks FilestoreNode type. These nodes only contain a reference
// to the actual location of the block data in the filesystem
// (a path and an offset).
type FileManager struct {
	AllowFiles bool
	AllowUrls  bool
	ds         ds.Batching
	root       string
}

// CorruptReferenceError implements the error interface.
// It is used to indicate that the block contents pointed
// by the referencing blocks cannot be retrieved (i.e. the
// file is not found, or the data changed as it was being read).
type CorruptReferenceError struct {
	Code Status
	Err  error
}

// Error() returns the error message in the CorruptReferenceError
// as a string.
func (c CorruptReferenceError) Error() string {
	return c.Err.Error()
}

// NewFileManager initializes a new file manager with the given
// datastore and root. All FilestoreNodes paths are relative to the
// root path given here, which is prepended for any operations.
func NewFileManager(ds ds.Batching, root string) *FileManager {
	return &FileManager{ds: dsns.Wrap(ds, FilestorePrefix), root: root}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the FileManager. If the given context is cancelled the channel will be
// closed.
//
// All CIDs returned are of type Raw.
func (f *FileManager) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	q := dsq.Query{KeysOnly: true}

	res, err := f.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	out := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer close(out)
		for {
			v, ok := res.NextSync()
			if !ok {
				return
			}

			k := ds.RawKey(v.Key)
			mhash, err := dshelp.DsKeyToMultihash(k)
			if err != nil {
				logger.Errorf("decoding cid from filestore: %s", err)
				continue
			}

			select {
			case out <- cid.NewCidV1(cid.Raw, mhash):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// DeleteBlock deletes the reference-block from the underlying
// datastore. It does not touch the referenced data.
func (f *FileManager) DeleteBlock(ctx context.Context, c cid.Cid) error {
	err := f.ds.Delete(ctx, dshelp.MultihashToDsKey(c.Hash()))
	if err == ds.ErrNotFound {
		return ipld.ErrNotFound{Cid: c}
	}
	return err
}

// Get reads a block from the datastore. Reading a block
// is done in two steps: the first step retrieves the reference
// block from the datastore. The second step uses the stored
// path and offsets to read the raw block data directly from disk.
func (f *FileManager) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return nil, err
	}
	out, err := f.readDataObj(ctx, c.Hash(), dobj)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(out, c)
}

// GetSize gets the size of the block from the datastore.
//
// This method may successfully return the size even if returning the block
// would fail because the associated file is no longer available.
func (f *FileManager) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	dobj, err := f.getDataObj(ctx, c.Hash())
	if err != nil {
		return -1, err
	}
	return int(dobj.GetSize_()), nil
}

func (f *FileManager) readDataObj(ctx context.Context, m mh.Multihash, d *pb.DataObj) ([]byte, error) {
	if IsURL(d.GetFilePath()) {
		return f.readURLDataObj(ctx, m, d)
	}
	return f.readFileDataObj(m, d)
}

func (f *FileManager) getDataObj(ctx context.Context, m mh.Multihash) (*pb.DataObj, error) {
	o, err := f.ds.Get(ctx, dshelp.MultihashToDsKey(m))
	switch err {
	case ds.ErrNotFound:
		return nil, ipld.ErrNotFound{Cid: cid.NewCidV1(cid.Raw, m)}
	case nil:
		//
	default:
		return nil, err
	}

	return unmarshalDataObj(o)
}

func unmarshalDataObj(data []byte) (*pb.DataObj, error) {
	var dobj pb.DataObj
	if err := proto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
}

func (f *FileManager) readFileDataObj(m mh.Multihash, d *pb.DataObj) ([]byte, error) {
	if !f.AllowFiles {
		return nil, ErrFilestoreNotEnabled
	}

	p := filepath.FromSlash(d.GetFilePath())
	abspath := filepath.Join(f.root, p)

	fi, err := os.Open(abspath)
	if os.IsNotExist(err) {
		return nil, &CorruptReferenceError{StatusFileNotFound, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}
	defer fi.Close()

	_, err = fi.Seek(int64(d.GetOffset()), io.SeekStart)
	if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}

	outbuf := make([]byte, d.GetSize_())
	_, err = io.ReadFull(fi, outbuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, &CorruptReferenceError{StatusFileChanged, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}

	// Work with CIDs for this, as they are a nice wrapper and things
	// will not break if multihashes underlying types change.
	origCid := cid.NewCidV1(cid.Raw, m)
	outcid, err := origCid.Prefix().Sum(outbuf)
	if err != nil {
		return nil, err
	}

	if !origCid.Equals(outcid) {
		return nil, &CorruptReferenceError{StatusFileChanged,
			fmt.Errorf("data in file did not match. %s offset %d", d.GetFilePath(), d.GetOffset())}
	}

	return outbuf, nil
}

// reads and verifies the block from URL
func (f *FileManager) readURLDataObj(ctx context.Context, m mh.Multihash, d *pb.DataObj) ([]byte, error) {
	if !f.AllowUrls {
		return nil, ErrUrlstoreNotEnabled
	}

	req, err := http.NewRequestWithContext(ctx, "GET", d.GetFilePath(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", d.GetOffset(), d.GetOffset()+d.GetSize_()-1))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
		return nil, &CorruptReferenceError{StatusFileError,
			fmt.Errorf("expected HTTP 200 or 206 got %d", res.StatusCode)}
	}

	outbuf := make([]byte, d.GetSize_())
	_, err = io.ReadFull(res.Body, outbuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, &CorruptReferenceError{StatusFileChanged, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}
	res.Body.Close()

	// Work with CIDs for this, as they are a nice wrapper and things
	// will not break if multihashes underlying types change.
	origCid := cid.NewCidV1(cid.Raw, m)
	outcid, err := origCid.Prefix().Sum(outbuf)
	if err != nil {
		return nil, err
	}

	if !origCid.Equals(outcid) {
		return nil, &CorruptReferenceError{StatusFileChanged,
			fmt.Errorf("data in file did not match. %s offset %d", d.GetFilePath(), d.GetOffset())}
	}

	return outbuf, nil
}

// Has returns if the FileManager is storing a block reference. It does not
// validate the data, nor checks if the reference is valid.
func (f *FileManager) Has(ctx context.Context, c cid.Cid) (bool, error) {
	// NOTE: interesting thing to consider. Has doesnt validate the data.
	// So the data on disk could be invalid, and we could think we have it.
	dsk := dshelp.MultihashToDsKey(c.Hash())
	return f.ds.Has(ctx, dsk)
}

type putter interface {
	Put(context.Context, ds.Key, []byte) error
}

// Put adds a new reference block to the FileManager. It does not check
// that the reference is valid.
func (f *FileManager) Put(ctx context.Context, b *posinfo.FilestoreNode) error {
	return f.putTo(ctx, b, f.ds)
}

func (f *FileManager) putTo(ctx context.Context, b *posinfo.FilestoreNode, to putter) error {
	var dobj pb.DataObj

	if IsURL(b.PosInfo.FullPath) {
		if !f.AllowUrls {
			return ErrUrlstoreNotEnabled
		}
		dobj.FilePath = b.PosInfo.FullPath
	} else {
		if !f.AllowFiles {
			return ErrFilestoreNotEnabled
		}
		//lint:ignore SA1019 // ignore staticcheck
		if !filepath.HasPrefix(b.PosInfo.FullPath, f.root) {
			return fmt.Errorf("cannot add filestore references outside ipfs root (%s)", f.root)
		}

		p, err := filepath.Rel(f.root, b.PosInfo.FullPath)
		if err != nil {
			return err
		}

		dobj.FilePath = filepath.ToSlash(p)
	}
	dobj.Offset = b.PosInfo.Offset
	dobj.Size_ = uint64(len(b.RawData()))

	data, err := proto.Marshal(&dobj)
	if err != nil {
		return err
	}

	return to.Put(ctx, dshelp.MultihashToDsKey(b.Cid().Hash()), data)
}

// PutMany is like Put() but takes a slice of blocks instead,
// allowing it to create a batch transaction.
func (f *FileManager) PutMany(ctx context.Context, bs []*posinfo.FilestoreNode) error {
	batch, err := f.ds.Batch(ctx)
	if err != nil {
		return err
	}

	for _, b := range bs {
		if err := f.putTo(ctx, b, batch); err != nil {
			return err
		}
	}

	return batch.Commit(ctx)
}

// IsURL returns true if the string represents a valid URL that the
// urlstore can handle.  More specifically it returns true if a string
// begins with 'http://' or 'https://'.
func IsURL(str string) bool {
	return (len(str) > 7 && str[0] == 'h' && str[1] == 't' && str[2] == 't' && str[3] == 'p') &&
		((len(str) > 8 && str[4] == 's' && str[5] == ':' && str[6] == '/' && str[7] == '/') ||
			(str[4] == ':' && str[5] == '/' && str[6] == '/'))
}
