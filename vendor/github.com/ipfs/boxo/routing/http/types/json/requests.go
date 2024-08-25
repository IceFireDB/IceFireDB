package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type WriteProvidersRequest struct {
	Providers []types.Record
}

func (r *WriteProvidersRequest) UnmarshalJSON(b []byte) error {
	type wpr struct{ Providers []json.RawMessage }
	var tempWPR wpr
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.Providers {
		var rawProv types.UnknownRecord
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Schema {
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			var prov types.WriteBitswapRecord
			err := json.Unmarshal(rawProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			var prov types.UnknownRecord
			err := json.Unmarshal(b, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		}
	}
	return nil
}
