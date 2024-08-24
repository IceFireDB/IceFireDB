package file

import (
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func newWrappedNode(substrate ipld.Node) (LargeBytesNode, error) {
	dataField, err := substrate.LookupByString("Data")
	if err != nil {
		return nil, err
	}
	// unpack as unixfs proto.
	dfb, err := dataField.AsBytes()
	if err != nil {
		return nil, err
	}
	ufd, err := data.DecodeUnixFSData(dfb)
	if err != nil {
		return nil, err
	}

	if ufd.Data.Exists() {
		return &singleNodeFile{
			Node: ufd.Data.Must(),
		}, nil
	}

	// an empty degenerate one.
	return &singleNodeFile{
		Node: basicnode.NewBytes(nil),
	}, nil
}
