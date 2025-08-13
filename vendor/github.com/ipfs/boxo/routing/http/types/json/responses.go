package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// ProvidersResponse is the result of a GET Providers request.
type ProvidersResponse struct {
	Providers RecordsArray
}

func (r ProvidersResponse) Length() int {
	return len(r.Providers)
}

// PeersResponse is the result of a GET Peers request.
type PeersResponse struct {
	Peers []*types.PeerRecord
}

func (r PeersResponse) Length() int {
	return len(r.Peers)
}

// RecordsArray is an array of [types.Record]
type RecordsArray []types.Record

func (r *RecordsArray) UnmarshalJSON(b []byte) error {
	var tempRecords []json.RawMessage
	err := json.Unmarshal(b, &tempRecords)
	if err != nil {
		return err
	}

	for _, provBytes := range tempRecords {
		var readProv types.UnknownRecord
		err := json.Unmarshal(provBytes, &readProv)
		if err != nil {
			return err
		}

		switch readProv.Schema {
		case types.SchemaPeer:
			var prov types.PeerRecord
			err := json.Unmarshal(provBytes, &prov)
			if err != nil {
				return err
			}
			*r = append(*r, &prov)
		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//nolint:staticcheck
			//lint:ignore SA1019 // ignore staticcheck
			var prov types.BitswapRecord
			err := json.Unmarshal(provBytes, &prov)
			if err != nil {
				return err
			}
			*r = append(*r, &prov)
		default:
			*r = append(*r, &readProv)
		}

	}
	return nil
}

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type WriteProvidersResponse struct {
	ProvideResults []types.Record
}

func (r WriteProvidersResponse) Length() int {
	return len(r.ProvideResults)
}

func (r *WriteProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempWPR struct{ ProvideResults []json.RawMessage }
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.ProvideResults {
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
			var prov types.WriteBitswapRecordResponse
			err := json.Unmarshal(rawProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.ProvideResults = append(r.ProvideResults, &prov)
		default:
			r.ProvideResults = append(r.ProvideResults, &rawProv)
		}
	}

	return nil
}
