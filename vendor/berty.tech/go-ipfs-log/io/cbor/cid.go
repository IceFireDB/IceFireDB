package cbor

import (
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

var cidAtlasEntry = atlas.BuildEntry(cid.Cid{}).
	UseTag(cbornode.CBORTagLink).
	Transform().
	TransformMarshal(atlas.MakeMarshalTransformFunc(
		castCidToBytes,
	)).
	TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(
		castBytesToCid,
	)).
	Complete()

func castBytesToCid(x []byte) (cid.Cid, error) {
	if len(x) == 0 {
		return cid.Cid{}, cbornode.ErrEmptyLink
	}

	// TODO: manually doing multibase checking here since our deps don't
	// support binary multibase yet
	if x[0] != 0 {
		return cid.Cid{}, cbornode.ErrInvalidMultibase
	}

	c, err := cid.Cast(x[1:])
	if err != nil {
		return cid.Cid{}, cbornode.ErrInvalidLink
	}

	return c, nil
}

func castCidToBytes(link cid.Cid) ([]byte, error) {
	if !link.Defined() {
		return nil, cbornode.ErrEmptyLink
	}
	return append([]byte{0}, link.Bytes()...), nil
}
