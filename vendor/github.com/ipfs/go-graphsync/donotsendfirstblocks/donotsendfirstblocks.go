package donotsendfirstblocks

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// EncodeDoNotSendFirstBlocks returns encoded cbor data for the given number
// of blocks to skip
func EncodeDoNotSendFirstBlocks(skipBlockCount int64) datamodel.Node {
	return basicnode.NewInt(skipBlockCount)
}

// DecodeDoNotSendFirstBlocks returns the number of blocks to skip
func DecodeDoNotSendFirstBlocks(data datamodel.Node) (int64, error) {
	return data.AsInt()
}
