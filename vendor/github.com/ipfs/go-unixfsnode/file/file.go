package file

import (
	"context"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
)

// NewUnixFSFile attempts to construct an ipld node from the base protobuf node representing the
// root of a unixfs File.
// It provides a `bytes` view over the file, along with access to io.Reader streaming access
// to file data.
func NewUnixFSFile(ctx context.Context, substrate ipld.Node, lsys *ipld.LinkSystem) (LargeBytesNode, error) {
	if substrate.Kind() == ipld.Kind_Bytes {
		// A raw / single-node file.
		return &singleNodeFile{substrate}, nil
	}
	// see if it's got children.
	links, err := substrate.LookupByString("Links")
	if err != nil {
		return nil, err
	}
	if links.Length() == 0 {
		// no children.
		return newWrappedNode(substrate)
	}

	return &shardNodeFile{
		ctx:       ctx,
		lsys:      lsys,
		substrate: substrate,
	}, nil
}

// NewUnixFSFileWithPreload is the same as NewUnixFSFile but it performs a full load of constituent
// blocks where the file spans multiple blocks. This is useful where a system needs to watch the
// LinkSystem for block loads to determine which blocks make up this file.
// NewUnixFSFileWithPreload is used by the "unixfs-preload" reifier.
func NewUnixFSFileWithPreload(ctx context.Context, substrate ipld.Node, lsys *ipld.LinkSystem) (LargeBytesNode, error) {
	f, err := NewUnixFSFile(ctx, substrate, lsys)
	if err != nil {
		return nil, err
	}
	r, err := f.AsLargeBytes()
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}
	return f, nil
}

// A LargeBytesNode is an ipld.Node that can be streamed over. It is guaranteed to have a Bytes type.
type LargeBytesNode interface {
	adl.ADL
	AsLargeBytes() (io.ReadSeeker, error)
}

type singleNodeFile struct {
	ipld.Node
}

func (f *singleNodeFile) AsLargeBytes() (io.ReadSeeker, error) {
	return &singleNodeReader{f, 0}, nil
}

func (f *singleNodeFile) Substrate() datamodel.Node {
	return f.Node
}

type singleNodeReader struct {
	ipld.Node
	offset int
}

func (f *singleNodeReader) Read(p []byte) (int, error) {
	buf, err := f.Node.AsBytes()
	if err != nil {
		return 0, err
	}
	if f.offset >= len(buf) {
		return 0, io.EOF
	}
	n := copy(p, buf[f.offset:])
	f.offset += n
	return n, nil
}

func (f *singleNodeReader) Seek(offset int64, whence int) (int64, error) {
	buf, err := f.Node.AsBytes()
	if err != nil {
		return 0, err
	}

	switch whence {
	case io.SeekStart:
		f.offset = int(offset)
	case io.SeekCurrent:
		f.offset += int(offset)
	case io.SeekEnd:
		f.offset = len(buf) + int(offset)
	}
	if f.offset < 0 {
		return 0, io.EOF
	}
	return int64(f.offset), nil
}
