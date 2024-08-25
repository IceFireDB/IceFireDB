package merkledag

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	pb "github.com/ipfs/boxo/ipld/merkledag/pb"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	dagpb "github.com/ipld/go-codec-dagpb"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// Make sure the user doesn't upgrade this file.
// We need to check *here* as well as inside the `pb` package *just* in case the
// user replaces *all* go files in that package.
const _ = pb.DoNotUpgradeFileEverItWillChangeYourHashes

// for now, we use a PBNode intermediate thing.
// because native go objects are nice.

// pbLinkSlice is a slice of pb.PBLink, similar to LinkSlice but for sorting the
// PB form
type pbLinkSlice []*pb.PBLink

func (pbls pbLinkSlice) Len() int           { return len(pbls) }
func (pbls pbLinkSlice) Swap(a, b int)      { pbls[a], pbls[b] = pbls[b], pbls[a] }
func (pbls pbLinkSlice) Less(a, b int) bool { return *pbls[a].Name < *pbls[b].Name }

// unmarshal decodes raw data into a *Node instance.
// The conversion uses an intermediate PBNode.
func unmarshal(encodedBytes []byte) (*ProtoNode, error) {
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, encodedBytes); err != nil {
		return nil, err
	}
	nd := nb.Build()
	return fromImmutableNode(&immutableProtoNode{encodedBytes, nd.(dagpb.PBNode)}), nil
}

func fromImmutableNode(encoded *immutableProtoNode) *ProtoNode {
	n := new(ProtoNode)
	n.encoded = encoded
	if n.encoded.PBNode.Data.Exists() {
		n.data = n.encoded.PBNode.Data.Must().Bytes()
	}
	numLinks := n.encoded.PBNode.Links.Length()
	// links may not be sorted after deserialization, but we don't change
	// them until we mutate this node since we're representing the current,
	// as-serialized state
	n.links = make([]*format.Link, numLinks)
	linkAllocs := make([]format.Link, numLinks)
	for i := int64(0); i < numLinks; i++ {
		next := n.encoded.PBNode.Links.Lookup(i)
		name := ""
		if next.FieldName().Exists() {
			name = next.FieldName().Must().String()
		}
		c := next.FieldHash().Link().(cidlink.Link).Cid
		size := uint64(0)
		if next.FieldTsize().Exists() {
			size = uint64(next.FieldTsize().Must().Int())
		}
		link := &linkAllocs[i]
		link.Name = name
		link.Size = size
		link.Cid = c
		n.links[i] = link
	}
	// we don't set n.linksDirty because the order of the links list from
	// serialized form needs to be stable, until we start mutating the ProtoNode
	return n
}

func (n *ProtoNode) marshalImmutable() (*immutableProtoNode, error) {
	links := n.Links()
	nd, err := qp.BuildMap(dagpb.Type.PBNode, 2, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(int64(len(links)), func(la ipld.ListAssembler) {
			for _, link := range links {
				// it shouldn't be possible to get here with an undefined CID, but in
				// case it is we're going to drop this link from the encoded form
				// entirely
				if link.Cid.Defined() {
					qp.ListEntry(la, qp.Map(3, func(ma ipld.MapAssembler) {
						qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: link.Cid}))
						qp.MapEntry(ma, "Name", qp.String(link.Name))
						sz := int64(link.Size)
						if sz < 0 { // overflow, >MaxInt64 is almost certainly an error
							sz = 0
						}
						qp.MapEntry(ma, "Tsize", qp.Int(sz))
					}))
				}
			}
		}))
		if n.data != nil {
			qp.MapEntry(ma, "Data", qp.Bytes(n.data))
		}
	})
	if err != nil {
		return nil, err
	}

	// 1KiB can be allocated on the stack, and covers most small nodes
	// without having to grow the buffer and cause allocations.
	enc := make([]byte, 0, 1024)

	enc, err = dagpb.AppendEncode(enc, nd)
	if err != nil {
		return nil, err
	}
	return &immutableProtoNode{enc, nd.(dagpb.PBNode)}, nil
}

// Marshal encodes a *Node instance into a new byte slice.
// The conversion uses an intermediate PBNode.
func (n *ProtoNode) Marshal() ([]byte, error) {
	enc, err := n.marshalImmutable()
	if err != nil {
		return nil, err
	}
	return enc.encoded, nil
}

// GetPBNode converts *ProtoNode into it's protocol buffer variant.
// If you plan on mutating the data of the original node, it is recommended
// that you call ProtoNode.Copy() before calling ProtoNode.GetPBNode()
func (n *ProtoNode) GetPBNode() *pb.PBNode {
	pbn := &pb.PBNode{}
	if len(n.links) > 0 {
		pbn.Links = make([]*pb.PBLink, len(n.links))
	}

	for i, l := range n.links {
		pbn.Links[i] = &pb.PBLink{}
		pbn.Links[i].Name = &l.Name
		pbn.Links[i].Tsize = &l.Size
		if l.Cid.Defined() {
			pbn.Links[i].Hash = l.Cid.Bytes()
		}
	}

	// Ensure links are sorted prior to encode, regardless of `linksDirty`. They
	// may not have come sorted if we deserialized a badly encoded form that
	// didn't have links already sorted.
	sort.Stable(pbLinkSlice(pbn.Links))

	if len(n.data) > 0 {
		pbn.Data = n.data
	}
	return pbn
}

// EncodeProtobuf returns the encoded raw data version of a Node instance.
// It may use a cached encoded version, unless the force flag is given.
func (n *ProtoNode) EncodeProtobuf(force bool) ([]byte, error) {
	if n.encoded == nil || n.linksDirty || force {
		if n.linksDirty {
			// there was a mutation involving links, make sure we sort before we build
			// and cache a `Node` form that captures the current state
			sort.Stable(LinkSlice(n.links))
			n.linksDirty = false
		}
		n.cached = cid.Undef
		var err error
		n.encoded, err = n.marshalImmutable()
		if err != nil {
			return nil, err
		}
	}

	if !n.cached.Defined() {
		c, err := n.CidBuilder().Sum(n.encoded.encoded)
		if err != nil {
			return nil, err
		}

		n.cached = c
	}

	return n.encoded.encoded, nil
}

// DecodeProtobuf decodes raw data and returns a new Node instance.
func DecodeProtobuf(encoded []byte) (*ProtoNode, error) {
	n, err := unmarshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("incorrectly formatted merkledag node: %s", err)
	}
	return n, nil
}

// DecodeProtobufBlock is a block decoder for protobuf IPLD nodes conforming to
// node.DecodeBlockFunc
func DecodeProtobufBlock(b blocks.Block) (format.Node, error) {
	c := b.Cid()
	if c.Type() != cid.DagProtobuf {
		return nil, errors.New("this function can only decode protobuf nodes")
	}

	decnd, err := DecodeProtobuf(b.RawData())
	if err != nil {
		if strings.Contains(err.Error(), "Unmarshal failed") {
			return nil, fmt.Errorf("the block referred to by '%s' was not a valid merkledag node", c)
		}
		return nil, fmt.Errorf("failed to decode Protocol Buffers: %v", err)
	}

	decnd.cached = c
	decnd.builder = c.Prefix()
	return decnd, nil
}

// Type assertion
var _ format.DecodeBlockFunc = DecodeProtobufBlock
