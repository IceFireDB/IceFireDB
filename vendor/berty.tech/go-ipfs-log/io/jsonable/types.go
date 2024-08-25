package jsonable

import (
	"encoding/hex"

	"github.com/ipfs/go-cid"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
)

// Entry CBOR representable version of Entry
type Entry struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Refs     []cid.Cid
	Clock    *LamportClock
	Payload  string
	Identity *Identity

	EncryptedLinks      string
	EncryptedLinksNonce string
}

// EntryV0 CBOR representable version of Entry v0
type EntryV0 struct {
	Hash    *string       `json:"hash"`
	ID      string        `json:"id"`
	Payload string        `json:"payload"`
	Next    []string      `json:"next"`
	V       uint64        `json:"v"`
	Clock   *LamportClock `json:"clock"`
	Key     string        `json:"key"`
	Sig     string        `json:"sig"`
}

// EntryV1 CBOR representable version of Entry v1
type EntryV1 struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Clock    *LamportClock
	Payload  string
	Identity *Identity
}

// EntryV2 CBOR representable version of Entry v2
type EntryV2 = Entry

// ToPlain converts a CBOR serializable identity signature to a plain IdentitySignature.
func (c *IdentitySignature) ToPlain() (*identityprovider.IdentitySignature, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, errmsg.ErrIdentitySigDeserialization.Wrap(err)
	}

	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, errmsg.ErrIdentitySigDeserialization.Wrap(err)
	}

	return &identityprovider.IdentitySignature{
		PublicKey: publicKey,
		ID:        id,
	}, nil
}

func (e *EntryV0) ToPlain(out iface.IPFSLogEntry, _ identityprovider.Interface, newClock func() iface.IPFSLogLamportClock) error {
	c := cid.Undef

	if e.Hash != nil {
		var err error

		if c, err = cid.Parse(*e.Hash); err != nil {
			return err
		}
	}

	clock := newClock()
	if err := e.Clock.ToPlain(clock); err != nil {
		return err
	}

	sig, err := hex.DecodeString(e.Sig)
	if err != nil {
		return err
	}

	key, err := hex.DecodeString(e.Key)
	if err != nil {
		return errmsg.ErrKeyDeserialization.Wrap(err)
	}

	nextValues := make([]cid.Cid, len(e.Next))
	for i, n := range e.Next {
		var err error

		nextValues[i], err = cid.Parse(n)
		if err != nil {
			return err
		}
	}

	if c.Defined() {
		out.SetHash(c)
	}
	out.SetClock(clock)
	out.SetSig(sig)

	out.SetV(e.V)
	out.SetLogID(e.ID)
	out.SetKey(key)
	out.SetSig(sig)
	out.SetNext(nextValues)
	out.SetClock(clock)
	out.SetPayload([]byte(e.Payload))

	return nil
}

type IdentitySignature struct {
	ID        string `json:"id"`
	PublicKey string `json:"public_key"`
}

type Identity struct {
	ID         string             `json:"id"`
	PublicKey  string             `json:"public_key"`
	Signatures *IdentitySignature `json:"signatures"`
	Type       string             `json:"type"`
}

type LamportClock struct {
	ID   string `json:"id"`
	Time int    `json:"time"`
}

// ToPlain converts a CBOR serializable to a plain Identity object.
func (c *Identity) ToPlain(provider identityprovider.Interface) (*identityprovider.Identity, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	idSignatures, err := c.Signatures.ToPlain()
	if err != nil {
		return nil, errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	return &identityprovider.Identity{
		Signatures: idSignatures,
		PublicKey:  publicKey,
		Type:       c.Type,
		ID:         c.ID,
		Provider:   provider,
	}, nil
}

// ToJsonableEntry creates a CBOR serializable version of an entry
func ToJsonableEntry(e iface.IPFSLogEntry) interface{} {
	identity := (*Identity)(nil)
	if e.GetIdentity() != nil {
		identity = ToJsonableIdentity(e.GetIdentity())
	}

	switch e.GetV() {
	case 0:
		h := (*string)(nil)
		if e.GetHash().Defined() {
			val := e.GetHash().String()
			h = &val
		}

		nextValues := make([]string, len(e.GetNext()))
		for i, n := range e.GetNext() {
			nextValues[i] = n.String()
		}

		return &EntryV0{
			V:       e.GetV(),
			ID:      e.GetLogID(),
			Key:     hex.EncodeToString(e.GetKey()),
			Sig:     hex.EncodeToString(e.GetSig()),
			Hash:    h,
			Next:    nextValues,
			Clock:   ToJsonableLamportClock(e.GetClock()),
			Payload: string(e.GetPayload()),
		}
	case 1:
		return &EntryV1{
			V:        e.GetV(),
			LogID:    e.GetLogID(),
			Key:      hex.EncodeToString(e.GetKey()),
			Sig:      hex.EncodeToString(e.GetSig()),
			Hash:     nil,
			Next:     e.GetNext(),
			Clock:    ToJsonableLamportClock(e.GetClock()),
			Payload:  string(e.GetPayload()),
			Identity: identity,
		}
	default:
		ret := &EntryV2{
			V:        e.GetV(),
			LogID:    e.GetLogID(),
			Key:      hex.EncodeToString(e.GetKey()),
			Sig:      hex.EncodeToString(e.GetSig()),
			Hash:     nil,
			Next:     e.GetNext(),
			Refs:     e.GetRefs(),
			Clock:    ToJsonableLamportClock(e.GetClock()),
			Payload:  string(e.GetPayload()),
			Identity: identity,
		}

		{
			add := e.GetAdditionalData()

			encryptedLinks, okEncrypted := add[iface.KeyEncryptedLinks]
			encryptedLinksNonce, okEncryptedNonce := add[iface.KeyEncryptedLinksNonce]

			if okEncrypted && okEncryptedNonce {
				ret.EncryptedLinks = encryptedLinks
				ret.EncryptedLinksNonce = encryptedLinksNonce

				ret.Next = []cid.Cid{}
				ret.Refs = []cid.Cid{}
			}
		}

		return ret
	}
}

func ToJsonableLamportClock(l iface.IPFSLogLamportClock) *LamportClock {
	return &LamportClock{
		ID:   hex.EncodeToString(l.GetID()),
		Time: l.GetTime(),
	}
}

// ToJsonableIdentity converts an identity to a CBOR serializable identity.
func ToJsonableIdentity(id *identityprovider.Identity) *Identity {
	return &Identity{
		ID:         id.ID,
		PublicKey:  hex.EncodeToString(id.PublicKey),
		Type:       id.Type,
		Signatures: ToJsonableIdentitySignature(id.Signatures),
	}
}

// ToJsonableIdentitySignature converts to a CBOR serialized identity signature a plain IdentitySignature.
func ToJsonableIdentitySignature(id *identityprovider.IdentitySignature) *IdentitySignature {
	return &IdentitySignature{
		ID:        hex.EncodeToString(id.ID),
		PublicKey: hex.EncodeToString(id.PublicKey),
	}
}

// ToPlain returns a plain Entry from a CBOR serialized version
func (c *Entry) ToPlain(out iface.IPFSLogEntry, provider identityprovider.Interface, newClock func() iface.IPFSLogLamportClock) error {
	key, err := hex.DecodeString(c.Key)
	if err != nil {
		return errmsg.ErrKeyDeserialization.Wrap(err)
	}

	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return errmsg.ErrSigDeserialization.Wrap(err)
	}

	clock := newClock()
	if err := c.Clock.ToPlain(clock); err != nil {
		return errmsg.ErrClockDeserialization.Wrap(err)
	}

	identity := (*identityprovider.Identity)(nil)
	if c.Identity != nil {
		var err error

		identity, err = c.Identity.ToPlain(provider)
		if err != nil {
			return errmsg.ErrIdentityDeserialization.Wrap(err)
		}
	}

	out.SetV(c.V)
	out.SetLogID(c.LogID)
	out.SetKey(key)
	out.SetSig(sig)
	out.SetNext(c.Next)
	out.SetRefs(c.Refs)
	out.SetClock(clock)
	out.SetPayload([]byte(c.Payload))
	out.SetIdentity(identity)

	return nil
}

func (c *LamportClock) ToPlain(out iface.IPFSLogLamportClock) error {
	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return errmsg.ErrClockDeserialization.Wrap(err)
	}

	out.SetID(id)
	out.SetTime(c.Time)

	return nil
}
