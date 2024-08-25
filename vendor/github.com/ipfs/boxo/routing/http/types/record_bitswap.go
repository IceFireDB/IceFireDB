package types

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
)

// Deprecated: use the more versatile [SchemaPeer] instead. For more information, read [IPIP-417].
//
// [IPIP-417]: https://github.com/ipfs/specs/pull/417
const SchemaBitswap = "bitswap"

var (
	_ Record = &BitswapRecord{}
)

// Deprecated: use the more versatile [PeerRecord] instead. For more information, read [IPIP-417].
//
// [IPIP-417]: https://github.com/ipfs/specs/pull/417
type BitswapRecord struct {
	Schema   string
	Protocol string
	ID       *peer.ID
	Addrs    []Multiaddr `json:",omitempty"`
}

func (br *BitswapRecord) GetSchema() string {
	return br.Schema
}

var _ Record = &WriteBitswapRecord{}

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type WriteBitswapRecord struct {
	Schema    string
	Protocol  string
	Signature string

	// this content must be untouched because it is signed and we need to verify it
	RawPayload json.RawMessage `json:"Payload"`
	Payload    BitswapPayload  `json:"-"`
}

type BitswapPayload struct {
	Keys        []CID
	Timestamp   *Time
	AdvisoryTTL *Duration
	ID          *peer.ID
	Addrs       []Multiaddr
}

func (wr *WriteBitswapRecord) GetSchema() string {
	return wr.Schema
}

type tmpBWPR WriteBitswapRecord

func (p *WriteBitswapRecord) UnmarshalJSON(b []byte) error {
	var bwp tmpBWPR
	err := json.Unmarshal(b, &bwp)
	if err != nil {
		return err
	}

	p.Protocol = bwp.Protocol
	p.Schema = bwp.Schema
	p.Signature = bwp.Signature
	p.RawPayload = bwp.RawPayload

	return json.Unmarshal(bwp.RawPayload, &p.Payload)
}

func (p *WriteBitswapRecord) IsSigned() bool {
	return p.Signature != ""
}

func (p *WriteBitswapRecord) setRawPayload() error {
	payloadBytes, err := drjson.MarshalJSONBytes(p.Payload)
	if err != nil {
		return fmt.Errorf("marshaling bitswap write provider payload: %w", err)
	}

	p.RawPayload = payloadBytes

	return nil
}

func (p *WriteBitswapRecord) Sign(peerID peer.ID, key crypto.PrivKey) error {
	if p.IsSigned() {
		return errors.New("already signed")
	}

	if key == nil {
		return errors.New("no key provided")
	}

	sid, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return err
	}
	if sid != peerID {
		return errors.New("not the correct signing key")
	}

	err = p.setRawPayload()
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(p.RawPayload))
	sig, err := key.Sign(hash[:])
	if err != nil {
		return err
	}

	sigStr, err := multibase.Encode(multibase.Base64, sig)
	if err != nil {
		return fmt.Errorf("multibase-encoding signature: %w", err)
	}

	p.Signature = sigStr
	return nil
}

func (p *WriteBitswapRecord) Verify() error {
	if !p.IsSigned() {
		return errors.New("not signed")
	}

	if p.Payload.ID == nil {
		return errors.New("peer ID must be specified")
	}

	// note that we only generate and set the payload if it hasn't already been set
	// to allow for passing through the payload untouched if it is already provided
	if p.RawPayload == nil {
		err := p.setRawPayload()
		if err != nil {
			return err
		}
	}

	pk, err := p.Payload.ID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extracing public key from peer ID: %w", err)
	}

	_, sigBytes, err := multibase.Decode(p.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	hash := sha256.Sum256([]byte(p.RawPayload))
	ok, err := pk.Verify(hash[:], sigBytes)
	if err != nil {
		return fmt.Errorf("verifying hash with signature: %w", err)
	}
	if !ok {
		return errors.New("signature failed to verify")
	}

	return nil
}

var _ Record = &WriteBitswapRecordResponse{}

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type WriteBitswapRecordResponse struct {
	Schema      string
	Protocol    string
	AdvisoryTTL *Duration
}

func (r *WriteBitswapRecordResponse) GetSchema() string {
	return r.Schema
}
