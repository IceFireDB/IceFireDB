package ipldlegacy

import (
	"bytes"
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// UniversalNode satisfies both go-ipld-prime interfaces and legacy interfaces
type UniversalNode interface {
	ipld.Node
	format.Node
}

// NodeConverter converts a go-ipld-prime node + block combination to a UniversalNode that satisfies both current and legacy ipld formats
type NodeConverter func(b blocks.Block, nd ipld.Node) (UniversalNode, error)

type codecConverter struct {
	prototype ipld.NodePrototype
	converter NodeConverter
}

type Decoder struct {
	codecTable     map[uint64]codecConverter
	linkSystemBase ipld.LinkSystem
}

func NewDecoder() *Decoder {
	lsb := cidlink.DefaultLinkSystem()
	lsb.TrustedStorage = true
	return &Decoder{
		codecTable:     map[uint64]codecConverter{},
		linkSystemBase: lsb,
	}
}

func NewDecoderWithLS(ls ipld.LinkSystem) *Decoder {
	return &Decoder{
		codecTable:     map[uint64]codecConverter{},
		linkSystemBase: ls,
	}
}

// RegisterCodec registers a specialized prototype & converter for a specific codec
func (d *Decoder) RegisterCodec(codec uint64, prototype ipld.NodePrototype, converter NodeConverter) {
	d.codecTable[codec] = codecConverter{prototype, converter}
}

// DecodeNode builds a UniversalNode from a block
func (d *Decoder) DecodeNode(ctx context.Context, b blocks.Block) (UniversalNode, error) {
	c := b.Cid()
	link := cidlink.Link{Cid: c}
	lsys := d.linkSystemBase
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		return bytes.NewBuffer(b.RawData()), nil
	}

	var prototype ipld.NodePrototype = basicnode.Prototype.Any
	converter, hasConverter := d.codecTable[c.Prefix().Codec]
	if hasConverter {
		prototype = converter.prototype
	}
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, link, prototype)
	if err != nil {
		return nil, err
	}

	if hasConverter {
		return converter.converter(b, nd)
	}
	return &LegacyNode{b, nd}, nil
}
