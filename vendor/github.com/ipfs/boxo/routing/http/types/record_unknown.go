package types

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
)

var _ Record = &UnknownRecord{}

type UnknownRecord struct {
	Schema string

	// Bytes contains the raw JSON bytes that were used to unmarshal this record.
	// This value can be used, for example, to unmarshal de record into a different
	// type if Schema is of a known value.
	Bytes []byte
}

func (ur *UnknownRecord) GetSchema() string {
	return ur.Schema
}

func (ur *UnknownRecord) UnmarshalJSON(b []byte) error {
	v := struct {
		Schema string
	}{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	ur.Schema = v.Schema
	ur.Bytes = b
	return nil
}

func (ur UnknownRecord) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	if ur.Bytes != nil {
		err := json.Unmarshal(ur.Bytes, &m)
		if err != nil {
			return nil, err
		}
	}
	m["Schema"] = ur.Schema
	return drjson.MarshalJSONBytes(m)
}
