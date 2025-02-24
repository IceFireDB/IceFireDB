package mdutils

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// NewDAGGenerator returns an object capable of
// producing IPLD DAGs.
func NewDAGGenerator() *DAGGenerator {
	return &DAGGenerator{}
}

// DAGGenerator generates BasicBlocks on demand.
// For each instance of DAGGenerator, each new DAG is different from the
// previous, although two different instances will produce the same, given the
// same parameters.
type DAGGenerator struct {
	seq int
}

// MakeDagBlock generate a balanced DAG with the given fanout and depth, and add the blocks to the adder.
// This adder can be for example a blockstore.Put or a blockservice.AddBlock.
func (dg *DAGGenerator) MakeDagBlock(adder func(ctx context.Context, block blocks.Block) error, fanout uint, depth uint) (c cid.Cid, allCids []cid.Cid, err error) {
	return dg.MakeDagNode(func(ctx context.Context, node format.Node) error {
		return adder(ctx, node.(blocks.Block))
	}, fanout, depth)
}

// MakeDagNode generate a balanced DAG with the given fanout and depth, and add the blocks to the adder.
// This adder can be for example a DAGService.Add.
func (dg *DAGGenerator) MakeDagNode(adder func(ctx context.Context, node format.Node) error, fanout uint, depth uint) (c cid.Cid, allCids []cid.Cid, err error) {
	c, _, allCids, err = dg.generate(adder, fanout, depth)
	return c, allCids, err
}

func (dg *DAGGenerator) generate(adder func(ctx context.Context, node format.Node) error, fanout uint, depth uint) (c cid.Cid, size uint64, allCids []cid.Cid, err error) {
	if depth == 0 {
		panic("depth should be at least 1")
	}
	if depth == 1 {
		c, size, err = dg.encodeBlock(adder)
		if err != nil {
			return cid.Undef, 0, nil, err
		}
		return c, size, []cid.Cid{c}, nil
	}
	links := make([]*format.Link, fanout)
	for i := uint(0); i < fanout; i++ {
		root, size, children, err := dg.generate(adder, fanout, depth-1)
		if err != nil {
			return cid.Undef, 0, nil, err
		}
		links[i] = &format.Link{Cid: root, Size: size}
		allCids = append(allCids, children...)
	}
	c, size, err = dg.encodeBlock(adder, links...)
	if err != nil {
		return cid.Undef, 0, nil, err
	}
	return c, size, append([]cid.Cid{c}, allCids...), nil
}

func (dg *DAGGenerator) encodeBlock(adder func(ctx context.Context, node format.Node) error, links ...*format.Link) (cid.Cid, uint64, error) {
	dg.seq++
	nd := &merkledag.ProtoNode{}
	nd.SetData([]byte(strconv.Itoa(dg.seq)))
	for i, link := range links {
		err := nd.AddRawLink(fmt.Sprintf("link-%d", i), link)
		if err != nil {
			return cid.Undef, 0, err
		}
	}
	err := adder(context.Background(), nd)
	if err != nil {
		return cid.Undef, 0, err
	}
	size, err := nd.Size()
	return nd.Cid(), size, err
}
