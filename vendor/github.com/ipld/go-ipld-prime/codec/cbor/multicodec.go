package cbor

import (
	"io"

	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/multicodec"
)

var (
	_ codec.Decoder = Decode
	_ codec.Encoder = Encode
)

func init() {
	multicodec.RegisterEncoder(0x51, Encode)
	multicodec.RegisterDecoder(0x51, Decode)
}

// Decode deserializes data from the given io.Reader and feeds it into the given datamodel.NodeAssembler.
// Decode fits the codec.Decoder function interface.
//
// This is the function that will be registered in the default multicodec registry during package init time.
func Decode(na datamodel.NodeAssembler, r io.Reader) error {
	return dagcbor.DecodeOptions{
		AllowLinks: false,
	}.Decode(na, r)
}

// Encode walks the given datamodel.Node and serializes it to the given io.Writer.
// Encode fits the codec.Encoder function interface.
//
// This is the function that will be registered in the default multicodec registry during package init time.
func Encode(n datamodel.Node, w io.Writer) error {
	return dagcbor.EncodeOptions{
		AllowLinks: false,
	}.Encode(n, w)
}
