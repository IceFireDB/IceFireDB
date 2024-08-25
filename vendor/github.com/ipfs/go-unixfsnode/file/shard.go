package file

import (
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
)

type shardNodeFile struct {
	ctx       context.Context
	lsys      *ipld.LinkSystem
	substrate ipld.Node

	// unixfs data unpacked from the substrate. access via .unpack()
	metadata data.UnixFSData
	unpackLk sync.Once
}

var _ adl.ADL = (*shardNodeFile)(nil)

type shardNodeReader struct {
	*shardNodeFile
	rdr    io.Reader
	offset int64
	len    int64
}

func (s *shardNodeReader) makeReader() (io.Reader, error) {
	links, err := s.shardNodeFile.substrate.LookupByString("Links")
	if err != nil {
		return nil, err
	}
	readers := make([]io.Reader, 0)
	lnkIter := links.ListIterator()
	at := int64(0)
	for !lnkIter.Done() {
		lnkIdx, lnk, err := lnkIter.Next()
		if err != nil {
			return nil, err
		}
		childSize, tr, err := s.linkSize(lnk, int(lnkIdx))
		if err != nil {
			return nil, err
		}
		if s.offset >= at+childSize {
			at += childSize
			continue
		}
		if tr == nil {
			lnkhash, err := lnk.LookupByString("Hash")
			if err != nil {
				return nil, err
			}
			lnklnk, err := lnkhash.AsLink()
			if err != nil {
				return nil, err
			}
			target := newDeferredFileNode(s.ctx, s.lsys, lnklnk)
			tr, err = target.AsLargeBytes()
			if err != nil {
				return nil, err
			}
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
	s.len = at
	return io.MultiReader(readers...), nil
}

func (s *shardNodeFile) unpack() (data.UnixFSData, error) {
	var retErr error
	s.unpackLk.Do(func() {
		nodeData, err := s.substrate.LookupByString("Data")
		if err != nil {
			retErr = err
			return
		}
		nodeDataBytes, err := nodeData.AsBytes()
		if err != nil {
			retErr = err
			return
		}
		ud, err := data.DecodeUnixFSData(nodeDataBytes)
		if err != nil {
			retErr = err
			return
		}
		s.metadata = ud
	})
	return s.metadata, retErr
}

// returns the size of the n'th link from this shard.
// the io.ReadSeeker of the child will be return if it was loaded as part of the size calculation.
func (s *shardNodeFile) linkSize(lnk ipld.Node, position int) (int64, io.ReadSeeker, error) {
	lnkhash, err := lnk.LookupByString("Hash")
	if err != nil {
		return 0, nil, err
	}
	lnklnk, err := lnkhash.AsLink()
	if err != nil {
		return 0, nil, err
	}
	_, c, err := cid.CidFromBytes([]byte(lnklnk.Binary()))
	if err != nil {
		return 0, nil, err
	}

	// efficiency shortcut: for raw blocks, the size will match the bytes of content
	if c.Prefix().Codec == cid.Raw {
		size, err := lnk.LookupByString("Tsize")
		if err != nil {
			return 0, nil, err
		}
		sz, err := size.AsInt()
		return sz, nil, err
	}

	// check if there are blocksizes written, use them if there are.
	// both err and md can be nil if this was not the first time unpack()
	// was called but there was an error on the first call.
	md, err := s.unpack()
	if err == nil && md != nil {
		pn, err := md.BlockSizes.LookupByIndex(int64(position))
		if err == nil {
			innerNum, err := pn.AsInt()
			if err == nil {
				return innerNum, nil, nil
			}
		}
	}

	// open the link and get its size.
	target := newDeferredFileNode(s.ctx, s.lsys, lnklnk)
	tr, err := target.AsLargeBytes()
	if err != nil {
		return 0, nil, err
	}

	end, err := tr.Seek(0, io.SeekEnd)
	if err != nil {
		return end, nil, err
	}
	_, err = tr.Seek(0, io.SeekStart)
	return end, tr, err
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
	s.offset += int64(n)
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
	nodeData, err := s.unpack()
	if err != nil || nodeData == nil {
		return s.lengthFromLinks()
	}
	if nodeData.FileSize.Exists() {
		if fs, err := nodeData.FileSize.Must().AsInt(); err == nil {
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
		idx, l, err := li.Next()
		if err != nil {
			return 0
		}
		ll, _, err := s.linkSize(l, int(idx))
		if err != nil {
			return 0
		}
		size += ll
	}
	return size
}

func (s *shardNodeFile) AsLargeBytes() (io.ReadSeeker, error) {
	return &shardNodeReader{s, nil, 0, 0}, nil
}

func (s *shardNodeFile) Substrate() ipld.Node {
	return s.substrate
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
