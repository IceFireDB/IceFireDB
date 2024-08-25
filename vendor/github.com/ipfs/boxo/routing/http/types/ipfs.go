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
