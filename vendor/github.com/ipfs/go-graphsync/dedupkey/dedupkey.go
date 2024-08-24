package dedupkey

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// EncodeDedupKey returns encoded cbor data for string key
func EncodeDedupKey(key string) (datamodel.Node, error) {
	nb := basicnode.Prototype.String.NewBuilder()
	err := nb.AssignString(key)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// DecodeDedupKey returns a string key decoded from cbor data
func DecodeDedupKey(data datamodel.Node) (string, error) {
	return data.AsString()
}
