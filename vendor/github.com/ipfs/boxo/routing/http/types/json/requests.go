package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// Deprecated: historic API from [IPIP-526], may be removed in a future version.
//
// [IPIP-526]: https://specs.ipfs.tech/ipips/ipip-0526/
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
		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//nolint:staticcheck
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
