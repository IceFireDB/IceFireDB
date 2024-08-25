package json

import (
	"io"

	rfmtjson "github.com/polydawn/refmt/json"

	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/multicodec"
)

var (
	_ codec.Decoder = Decode
	_ codec.Encoder = Encode
)

func init() {
	multicodec.RegisterEncoder(0x0200, Encode)
	multicodec.RegisterDecoder(0x0200, Decode)
}

// Decode deserializes data from the given io.Reader and feeds it into the given datamodel.NodeAssembler.
// Decode fits the codec.Decoder function interface.
//
// This is the function that will be registered in the default multicodec registry during package init time.
func Decode(na datamodel.NodeAssembler, r io.Reader) error {
	return dagjson.DecodeOptions{
		ParseLinks: false,
		ParseBytes: false,
	}.Decode(na, r)
}

// Encode walks the given datamodel.Node and serializes it to the given io.Writer.
// Encode fits the codec.Encoder function interface.
//
// This is the function that will be registered in the default multicodec registry during package init time.
func Encode(n datamodel.Node, w io.Writer) error {
	// Shell out directly to generic inspection path.
	//  (There's not really any fastpaths of note for json.)
	// Write another function if you need to tune encoding options about whitespace.
	return dagjson.Marshal(n, rfmtjson.NewEncoder(w, rfmtjson.EncodeOptions{
		Line:   []byte{'\n'},
		Indent: []byte{'\t'},
	}), dagjson.EncodeOptions{
		EncodeLinks: false,
		EncodeBytes: false,
		MapSortMode: codec.MapSortMode_None,
	})
}
