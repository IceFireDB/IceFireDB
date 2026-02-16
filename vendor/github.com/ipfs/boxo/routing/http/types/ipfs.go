package types

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

type CID struct{ cid.Cid }

func (c *CID) MarshalJSON() ([]byte, error) { return drjson.MarshalJSONBytes(c.String()) }
func (c *CID) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	decodedCID, err := cid.Decode(s)
	if err != nil {
		return err
	}
	c.Cid = decodedCID
	return nil
}

type Multiaddr struct{ multiaddr.Multiaddr }

// MarshalJSON returns null for nil Multiaddr as a defensive measure.
// This prevents panics if corrupted data contains nil addresses.
// See: https://github.com/ipfs/kubo/issues/11116
func (m Multiaddr) MarshalJSON() ([]byte, error) {
	if m.Multiaddr == nil {
		return []byte("null"), nil
	}
	return json.Marshal(m.Multiaddr.String())
}

func (m *Multiaddr) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	ma, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		return err
	}
	m.Multiaddr = ma
	return nil
}
