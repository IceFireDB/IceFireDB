package cbor

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"

	"berty.tech/go-ipfs-log/enc"
	"github.com/ipfs/go-ipld-cbor/encoding"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/kubo/core/coreiface"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/polydawn/refmt/obj/atlas"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/jsonable"
)

type IOCbor struct {
	debug bool

	refClock iface.IPFSLogLamportClock
	refEntry iface.IPFSLogEntry

	constantIdentity *identityprovider.Identity
	linkKey          enc.SharedKey
	atlasEntries     []*atlas.AtlasEntry
	cborMarshaller   encoding.PooledMarshaller
	cborUnmarshaller encoding.PooledUnmarshaller
}

type Options struct {
	//ConstantIdentity *identityprovider.Identity
	LinkKey enc.SharedKey
}

func (i *IOCbor) DecodeRawJSONLog(node format.Node) (*iface.JSONLog, error) {
	jsonLog := &iface.JSONLog{}
	err := cbornode.DecodeInto(node.RawData(), jsonLog)

	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	return jsonLog, nil
}

func (i *IOCbor) DecodeRawEntry(node format.Node, hash cid.Cid, p identityprovider.Interface) (iface.IPFSLogEntry, error) {
	obj := &jsonable.EntryV2{}
	err := cbornode.DecodeInto(node.RawData(), obj)
	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	obj, err = i.DecryptLinks(obj)
	if err != nil {
		return nil, errmsg.ErrDecrypt.Wrap(err)
	}

	obj.Hash = hash

	e := i.refEntry.New()
	if err := obj.ToPlain(e, p, i.refClock.New); err != nil {
		return nil, errmsg.ErrEntryDeserializationFailed.Wrap(err)
	}

	e.SetHash(hash)

	if i.constantIdentity != nil {
		e.SetIdentity(i.constantIdentity)
		e.SetKey(i.constantIdentity.PublicKey)
	}

	return e, nil
}

var _io = (*IOCbor)(nil)

func IO(refEntry iface.IPFSLogEntry, refClock iface.IPFSLogLamportClock) (*IOCbor, error) {
	if _io != nil {
		return _io, nil
	}

	_io = &IOCbor{
		debug:    false,
		refClock: refClock,
		refEntry: refEntry,
	}

	_io.atlasEntries = []*atlas.AtlasEntry{
		atlas.BuildEntry(jsonable.Entry{}).
			StructMap().
			AddField("V", atlas.StructMapEntry{SerialName: "v"}).
			AddField("LogID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Key", atlas.StructMapEntry{SerialName: "key"}).
			AddField("Sig", atlas.StructMapEntry{SerialName: "sig"}).
			AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
			AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
			AddField("Refs", atlas.StructMapEntry{SerialName: "refs"}).
			AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
			AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
			AddField("Identity", atlas.StructMapEntry{SerialName: "identity"}).
			AddField("EncryptedLinks", atlas.StructMapEntry{SerialName: "enc_links", OmitEmpty: true}).
			AddField("EncryptedLinksNonce", atlas.StructMapEntry{SerialName: "enc_links_nonce", OmitEmpty: true}).
			Complete(),

		atlas.BuildEntry(jsonable.EntryV1{}).
			StructMap().
			AddField("V", atlas.StructMapEntry{SerialName: "v"}).
			AddField("LogID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Key", atlas.StructMapEntry{SerialName: "key"}).
			AddField("Sig", atlas.StructMapEntry{SerialName: "sig"}).
			AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
			AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
			AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
			AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
			AddField("Identity", atlas.StructMapEntry{SerialName: "identity"}).
			Complete(),

		atlas.BuildEntry(iface.Hashable{}).
			StructMap().
			AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
			AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
			AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
			AddField("Refs", atlas.StructMapEntry{SerialName: "refs"}).
			AddField("V", atlas.StructMapEntry{SerialName: "v"}).
			AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
			AddField("AdditionalData", atlas.StructMapEntry{SerialName: "additional_data", OmitEmpty: true}).
			Complete(),

		atlas.BuildEntry(jsonable.LamportClock{}).
			StructMap().
			AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
			Complete(),

		atlas.BuildEntry(jsonable.Identity{}).
			StructMap().
			AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
			AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
			AddField("Signatures", atlas.StructMapEntry{SerialName: "signatures"}).
			Complete(),

		atlas.BuildEntry(jsonable.IdentitySignature{}).
			StructMap().
			AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
			Complete(),

		atlas.BuildEntry(ic.Secp256k1PublicKey{}).
			Transform().
			TransformMarshal(atlas.MakeMarshalTransformFunc(
				func(x ic.Secp256k1PublicKey) (string, error) {
					keyBytes, err := x.Raw()
					if err != nil {
						return "", errmsg.ErrNotSecp256k1PubKey.Wrap(err)
					}

					return base64.StdEncoding.EncodeToString(keyBytes), nil
				})).
			TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(
				func(x string) (ic.Secp256k1PublicKey, error) {
					keyBytes, err := base64.StdEncoding.DecodeString(x)
					if err != nil {
						return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
					}

					key, err := ic.UnmarshalSecp256k1PublicKey(keyBytes)
					if err != nil {
						return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
					}
					secpKey, ok := key.(*ic.Secp256k1PublicKey)
					if !ok {
						return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey
					}

					return *secpKey, nil
				})).
			Complete(),

		atlas.BuildEntry(iface.JSONLog{}).
			StructMap().
			AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
			AddField("Heads", atlas.StructMapEntry{SerialName: "heads"}).
			Complete(),
	}

	for _, atlasEntry := range _io.atlasEntries {
		cbornode.RegisterCborType(atlasEntry)
	}

	_io.createCborMarshaller()

	return _io, nil
}

func (i *IOCbor) createCborMarshaller() {
	i.cborMarshaller = encoding.NewPooledMarshaller(atlas.MustBuild(append(_io.atlasEntries, cidAtlasEntry)...).
		WithMapMorphism(atlas.MapMorphism{KeySortMode: atlas.KeySortMode_RFC7049}))
	i.cborUnmarshaller = encoding.NewPooledUnmarshaller(atlas.MustBuild(append(_io.atlasEntries, cidAtlasEntry)...).
		WithMapMorphism(atlas.MapMorphism{KeySortMode: atlas.KeySortMode_RFC7049}))
}

func (i *IOCbor) ApplyOptions(options *Options) *IOCbor {
	out := &IOCbor{
		debug:        i.debug,
		refClock:     i.refClock,
		refEntry:     i.refEntry,
		atlasEntries: i.atlasEntries,
		//constantIdentity: options.ConstantIdentity,
		linkKey: options.LinkKey,
	}

	out.createCborMarshaller()

	return out
}

func (i *IOCbor) SetDebug(val bool) {
	i.debug = val
}

// WriteCBOR writes a CBOR representation of a given object in IPFS' DAG.
func (i *IOCbor) Write(ctx context.Context, ipfs coreiface.CoreAPI, obj interface{}, opts *iface.WriteOpts) (cid.Cid, error) {
	if opts == nil {
		opts = &iface.WriteOpts{}
	}

	switch o := obj.(type) {
	case iface.IPFSLogEntry:
		if i.constantIdentity != nil {
			o.SetIdentity(nil)
			o.SetKey(nil)
		}

		obj = jsonable.ToJsonableEntry(o)
		break
	}

	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Undef, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	if i.debug {
		fmt.Printf("\nStr of cbor: %x\n", cborNode.RawData())
	}

	err = ipfs.Dag().Add(ctx, cborNode)
	if err != nil {
		return cid.Undef, errmsg.ErrIPFSOperationFailed.Wrap(err)
	}

	if opts.Pin {
		if err = ipfs.Pin().Add(ctx, path.FromCid(cborNode.Cid())); err != nil {
			return cid.Undef, errmsg.ErrIPFSOperationFailed.Wrap(err)
		}
	}

	return cborNode.Cid(), nil
}

// Read reads a CBOR representation of a given object from IPFS' DAG.
func (i *IOCbor) Read(ctx context.Context, ipfs coreiface.CoreAPI, contentIdentifier cid.Cid) (format.Node, error) {
	return ipfs.Dag().Get(ctx, contentIdentifier)
}

func (i *IOCbor) PreSign(entry iface.IPFSLogEntry) (iface.IPFSLogEntry, error) {
	if i.linkKey == nil {
		return entry, nil
	}

	if len(entry.GetNext()) == 0 && len(entry.GetRefs()) == 0 {
		return entry, nil
	}

	entry = entry.Copy()

	links := &jsonable.EntryV2{}
	links.Next = entry.GetNext()
	links.Refs = entry.GetRefs()

	cborPayload, err := i.cborMarshaller.Marshal(links)
	if err != nil {
		return nil, errmsg.ErrEncrypt.Wrap(fmt.Errorf("unable to cbor entry: %w", err))
	}

	nonce, err := i.linkKey.DeriveNonce(NonceRefForEntry(entry))
	if err != nil {
		return nil, errmsg.ErrEncrypt.Wrap(err)
	}

	encryptedLinks, err := i.linkKey.SealWithNonce(cborPayload, nonce)
	if err != nil {
		return nil, errmsg.ErrEncrypt.Wrap(fmt.Errorf("unable to encrypt message"))
	}

	entry.SetAdditionalDataValue(iface.KeyEncryptedLinks, base64.StdEncoding.EncodeToString(encryptedLinks))
	entry.SetAdditionalDataValue(iface.KeyEncryptedLinksNonce, base64.StdEncoding.EncodeToString(nonce))

	return entry, nil
}

func (i *IOCbor) DecryptLinks(entry *jsonable.EntryV2) (*jsonable.EntryV2, error) {
	if i.linkKey == nil || len(entry.EncryptedLinks) == 0 || len(entry.EncryptedLinksNonce) == 0 {
		return entry, nil
	}

	encryptedLinks, err := base64.StdEncoding.DecodeString(entry.EncryptedLinks)
	if err != nil {
		return nil, errmsg.ErrEntryDeserializationFailed.Wrap(err)
	}

	encryptedLinksNonce, err := base64.StdEncoding.DecodeString(entry.EncryptedLinksNonce)
	if err != nil {
		return nil, errmsg.ErrEntryDeserializationFailed.Wrap(err)
	}

	dec, err := i.linkKey.OpenWithNonce(encryptedLinks, encryptedLinksNonce)
	if err != nil {
		return nil, errmsg.ErrDecrypt.Wrap(err)
	}

	links := &jsonable.EntryV2{}
	if err := i.cborUnmarshaller.Unmarshal(dec, links); err != nil {
		return nil, errmsg.ErrEncrypt.Wrap(fmt.Errorf("unable to unmarshal decrypted message"))
	}

	entry.Next = links.Next
	entry.Refs = links.Refs

	return entry, nil
}

func NonceRefForEntry(entry iface.IPFSLogEntry) []byte {
	next := ""

	for _, c := range entry.GetNext() {
		next += "-" + c.String()
	}

	return []byte(fmt.Sprintf("%s,%s,%s,%s,%d,%s,%d",
		next,
		entry.GetKey(),
		entry.GetPayload(),
		entry.GetClock().GetID(),
		entry.GetClock().GetTime(),
		entry.GetLogID(),
		entry.GetV(),
	))
}
