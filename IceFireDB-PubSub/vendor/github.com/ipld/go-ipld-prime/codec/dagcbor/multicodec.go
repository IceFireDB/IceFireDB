package dagcbor

import (
	"io"

	"github.com/polydawn/refmt/cbor"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/multicodec"
)

var (
	_ ipld.Decoder = Decode
	_ ipld.Encoder = Encode
)

func init() {
	multicodec.RegisterEncoder(0x71, Encode)
	multicodec.RegisterDecoder(0x71, Decode)
}

func Decode(na ipld.NodeAssembler, r io.Reader) error {
	// Probe for a builtin fast path.  Shortcut to that if possible.
	type detectFastPath interface {
		DecodeDagCbor(io.Reader) error
	}
	if na2, ok := na.(detectFastPath); ok {
		return na2.DecodeDagCbor(r)
	}
	// Okay, generic builder path.
	return Unmarshal(na, cbor.NewDecoder(cbor.DecodeOptions{}, r))
}

func Encode(n ipld.Node, w io.Writer) error {
	// Probe for a builtin fast path.  Shortcut to that if possible.
	type detectFastPath interface {
		EncodeDagCbor(io.Writer) error
	}
	if n2, ok := n.(detectFastPath); ok {
		return n2.EncodeDagCbor(w)
	}
	// Okay, generic inspection path.
	return Marshal(n, cbor.NewEncoder(w))
}
