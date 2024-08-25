package types

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/libp2p/go-libp2p/core/peer"
)

const SchemaPeer = "peer"

var _ Record = &PeerRecord{}

type PeerRecord struct {
	Schema    string
	ID        *peer.ID
	Addrs     []Multiaddr
	Protocols []string

	// Extra contains extra fields that were included in the original JSON raw
	// message, except for the known ones represented by the remaining fields.
	Extra map[string]json.RawMessage
}

func (pr *PeerRecord) GetSchema() string {
	return pr.Schema
}

func (pr *PeerRecord) UnmarshalJSON(b []byte) error {
	// Unmarshal all known fields and assign them.
	v := struct {
		Schema    string
		ID        *peer.ID
		Addrs     []Multiaddr
		Protocols []string
	}{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	pr.Schema = v.Schema
	pr.ID = v.ID
	pr.Addrs = v.Addrs
	pr.Protocols = v.Protocols

	// Unmarshal everything into the Extra field and remove the
	// known fields to avoid conflictual usages of the struct.
	err = json.Unmarshal(b, &pr.Extra)
	if err != nil {
		return err
	}
	delete(pr.Extra, "Schema")
	delete(pr.Extra, "ID")
	delete(pr.Extra, "Addrs")
	delete(pr.Extra, "Protocols")

	return nil
}

func (pr PeerRecord) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	if pr.Extra != nil {
		for key, val := range pr.Extra {
			m[key] = val
		}
	}

	// Schema and ID must always be set.
	m["Schema"] = pr.Schema
	m["ID"] = pr.ID

	if pr.Addrs != nil {
		m["Addrs"] = pr.Addrs
	}

	if pr.Protocols != nil {
		m["Protocols"] = pr.Protocols
	}

	return drjson.MarshalJSONBytes(m)
}
