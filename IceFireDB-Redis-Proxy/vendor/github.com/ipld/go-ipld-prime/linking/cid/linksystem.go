package cidlink

import (
	"fmt"
	"hash"

	"github.com/multiformats/go-multihash/core"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/multicodec"
)

func DefaultLinkSystem() ipld.LinkSystem {
	return ipld.LinkSystem{
		EncoderChooser: func(lp ipld.LinkPrototype) (ipld.Encoder, error) {
			switch lp2 := lp.(type) {
			case LinkPrototype:
				fn, err := multicodec.LookupEncoder(lp2.GetCodec())
				if err != nil {
					return nil, err
				}
				return fn, nil
			default:
				return nil, fmt.Errorf("this encoderChooser can only handle cidlink.LinkPrototype; got %T", lp)
			}
		},
		DecoderChooser: func(lnk ipld.Link) (ipld.Decoder, error) {
			lp := lnk.Prototype()
			switch lp2 := lp.(type) {
			case LinkPrototype:
				fn, err := multicodec.LookupDecoder(lp2.GetCodec())
				if err != nil {
					return nil, err
				}
				return fn, nil
			default:
				return nil, fmt.Errorf("this decoderChooser can only handle cidlink.LinkPrototype; got %T", lp)
			}
		},
		HasherChooser: func(lp ipld.LinkPrototype) (hash.Hash, error) {
			switch lp2 := lp.(type) {
			case LinkPrototype:
				h, err := multihash.GetHasher(lp2.MhType)
				if err != nil {
					return nil, fmt.Errorf("no hasher registered for multihash indicator 0x%x: %w", lp2.MhType, err)
				}
				return h, nil
			default:
				return nil, fmt.Errorf("this hasherChooser can only handle cidlink.LinkPrototype; got %T", lp)
			}
		},
	}
}
