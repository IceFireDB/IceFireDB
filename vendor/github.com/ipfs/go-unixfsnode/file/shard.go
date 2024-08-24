package file

import (
	"context"
	"io"

	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
)

type shardNodeFile struct {
	ctx       context.Context
	lsys      *ipld.LinkSystem
	substrate ipld.Node
}

var _ ipld.Node = (*shardNodeFile)(nil)

type shardNodeReader struct {
	*shardNodeFile
	rdr    io.Reader
	offset int64
}

func (s *shardNodeReader) makeReader() (io.Reader, error) {
	links, err := s.shardNodeFile.substrate.LookupByString("Links")
	if err != nil {
		return nil, err
	}
	readers := make([]io.Reader, 0)
	lnki := links.ListIterator()
	at := int64(0)
	for !lnki.Done() {
		_, lnk, err := lnki.Next()
		if err != nil {
			return nil, err
		}
		sz, err := lnk.LookupByString("Tsize")
		if err != nil {
			return nil, err
		}
		childSize, err := sz.AsInt()
		if err != nil {
			return nil, err
		}
		if s.offset > at+childSize {
			at += childSize
			continue
		}
		lnkhash, err := lnk.LookupByString("Hash")
		if err != nil {
			return nil, err
		}
		lnklnk, err := lnkhash.AsLink()
		if err != nil {
			return nil, err
		}
		target := newDeferredFileNode(s.ctx, s.lsys, lnklnk)
		tr, err := target.AsLargeBytes()
		if err != nil {
			return nil, err
		}
		// fastforward the first one if needed.
		if at < s.offset {
			_, err := tr.Seek(s.offset-at, io.SeekStart)
			if err != nil {
				return nil, err
			}
		}
		at += childSize
		readers = append(readers, tr)
	}
	if len(readers) == 0 {
		return nil, io.EOF
	}
	return io.MultiReader(readers...), nil
}

func (s *shardNodeReader) Read(p []byte) (int, error) {
	// build reader
	if s.rdr == nil {
		rdr, err := s.makeReader()
		if err != nil {
			return 0, err
		}
		s.rdr = rdr
	}
	n, err := s.rdr.Read(p)
	return n, err
}

func (s *shardNodeReader) Seek(offset int64, whence int) (int64, error) {
	if s.rdr != nil {
		s.rdr = nil
	}
	switch whence {
	case io.SeekStart:
		s.offset = offset
	case io.SeekCurrent:
		s.offset += offset
	case io.SeekEnd:
		s.offset = s.length() + offset
	}
	return s.offset, nil
}

func (s *shardNodeFile) length() int64 {
	// see if we have size specified in the unixfs data. errors fall back to length from links
	nodeData, err := s.substrate.LookupByString("Data")
	if err != nil {
		return s.lengthFromLinks()
	}
	nodeDataBytes, err := nodeData.AsBytes()
	if err != nil {
		return s.lengthFromLinks()
	}
	ud, err := data.DecodeUnixFSData(nodeDataBytes)
	if err != nil {
		return s.lengthFromLinks()
	}
	if ud.FileSize.Exists() {
		if fs, err := ud.FileSize.Must().AsInt(); err == nil {
			return int64(fs)
		}
	}
	return s.lengthFromLinks()
}

func (s *shardNodeFile) lengthFromLinks() int64 {
	links, err := s.substrate.LookupByString("Links")
	if err != nil {
		return 0
	}
	size := int64(0)
	li := links.ListIterator()
	for !li.Done() {
		_, l, err := li.Next()
		if err != nil {
			return 0
		}
		sn, err := l.LookupByString("Tsize")
		if err != nil {
			return 0
		}
		ll, err := sn.AsInt()
		if err != nil {
			return 0
		}
		size += ll
	}
	return size
}

func (s *shardNodeFile) AsLargeBytes() (io.ReadSeeker, error) {
	return &shardNodeReader{s, nil, 0}, nil
}

func protoFor(link ipld.Link) ipld.NodePrototype {
	if lc, ok := link.(cidlink.Link); ok {
		if lc.Cid.Prefix().Codec == uint64(multicodec.DagPb) {
			return dagpb.Type.PBNode
		}
	}
	return basicnode.Prototype.Any
}

func (s *shardNodeFile) Kind() ipld.Kind {
	return ipld.Kind_Bytes
}

func (s *shardNodeFile) AsBytes() ([]byte, error) {
	rdr, err := s.AsLargeBytes()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rdr)
}

func (s *shardNodeFile) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "bool", MethodName: "AsBool", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsInt() (int64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "int", MethodName: "AsInt", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "float", MethodName: "AsFloat", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "string", MethodName: "AsString", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "link", MethodName: "AsLink", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsNode() (ipld.Node, error) {
	return nil, nil
}

func (s *shardNodeFile) Size() int {
	return 0
}

func (s *shardNodeFile) IsAbsent() bool {
	return false
}

func (s *shardNodeFile) IsNull() bool {
	return s.substrate.IsNull()
}

func (s *shardNodeFile) Length() int64 {
	return 0
}

func (s *shardNodeFile) ListIterator() ipld.ListIterator {
	return nil
}

func (s *shardNodeFile) MapIterator() ipld.MapIterator {
	return nil
}

func (s *shardNodeFile) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByString(key string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByNode(key ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

// shardded files / nodes look like dagpb nodes.
func (s *shardNodeFile) Prototype() ipld.NodePrototype {
	return dagpb.Type.PBNode
}
