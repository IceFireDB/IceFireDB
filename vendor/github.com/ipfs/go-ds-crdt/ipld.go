package crdt

import (
	"context"
	"errors"

	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	"google.golang.org/protobuf/proto"
)

// IPLD related things

var _ ipld.NodeGetter = (*crdtNodeGetter)(nil)

func init() {
	ipld.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
}

// crdtNodeGetter wraps an ipld.NodeGetter with some additional utility methods
type crdtNodeGetter struct {
	ipld.NodeGetter
}

func (ng *crdtNodeGetter) GetDelta(ctx context.Context, c cid.Cid) (ipld.Node, *pb.Delta, error) {
	nd, err := ng.Get(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	delta, err := extractDelta(nd)
	return nd, delta, err
}

// GetHeight returns the height of a block
func (ng *crdtNodeGetter) GetPriority(ctx context.Context, c cid.Cid) (uint64, error) {
	_, delta, err := ng.GetDelta(ctx, c)
	if err != nil {
		return 0, err
	}
	return delta.Priority, nil
}

type deltaOption struct {
	delta *pb.Delta
	node  ipld.Node
	err   error
}

// GetDeltas uses GetMany to obtain many deltas.
func (ng *crdtNodeGetter) GetDeltas(ctx context.Context, cids []cid.Cid) <-chan *deltaOption {
	deltaOpts := make(chan *deltaOption, 1)
	go func() {
		defer close(deltaOpts)
		nodeOpts := ng.GetMany(ctx, cids)
		for nodeOpt := range nodeOpts {
			if nodeOpt.Err != nil {
				deltaOpts <- &deltaOption{err: nodeOpt.Err}
				continue
			}
			delta, err := extractDelta(nodeOpt.Node)
			if err != nil {
				deltaOpts <- &deltaOption{err: err}
				continue
			}
			deltaOpts <- &deltaOption{
				delta: delta,
				node:  nodeOpt.Node,
			}
		}
	}()
	return deltaOpts
}

func extractDelta(nd ipld.Node) (*pb.Delta, error) {
	protonode, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, errors.New("node is not a ProtoNode")
	}
	d := pb.Delta{}
	err := proto.Unmarshal(protonode.Data(), &d)
	return &d, err
}

func makeNode(delta *pb.Delta, heads []cid.Cid) (ipld.Node, error) {
	var data []byte
	var err error
	if delta != nil {
		data, err = proto.Marshal(delta)
		if err != nil {
			return nil, err
		}
	}

	nd := dag.NodeWithData(data)
	for _, h := range heads {
		err = nd.AddRawLink("", &ipld.Link{Cid: h})
		if err != nil {
			return nil, err
		}
	}
	// Ensure we work with CIDv1
	nd.SetCidBuilder(dag.V1CidPrefix())
	return nd, nil
}
