package ndjson

import (
	"encoding/json"
	"io"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
)

// NewRecordsIter returns an iterator that reads [types.Record] from the given [io.Reader].
func NewRecordsIter(r io.Reader) iter.Iter[iter.Result[types.Record]] {
	jsonIter := iter.FromReaderJSON[types.UnknownRecord](r)
	mapFn := func(upr iter.Result[types.UnknownRecord]) iter.Result[types.Record] {
		var result iter.Result[types.Record]
		if upr.Err != nil {
			result.Err = upr.Err
			return result
		}
		switch upr.Val.Schema {
		case types.SchemaPeer:
			var prov types.PeerRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			var prov types.BitswapRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		default:
			result.Val = &upr.Val
		}
		return result
	}

	return iter.Map[iter.Result[types.UnknownRecord]](jsonIter, mapFn)
}

// NewPeerRecordsIter returns an iterator that reads [types.PeerRecord] from the given [io.Reader].
// Records with a different schema are safely ignored. If you want to read all records, use
// [NewRecordsIter] instead.
func NewPeerRecordsIter(r io.Reader) iter.Iter[iter.Result[*types.PeerRecord]] {
	jsonIter := iter.FromReaderJSON[types.UnknownRecord](r)
	mapFn := func(upr iter.Result[types.UnknownRecord]) iter.Result[*types.PeerRecord] {
		var result iter.Result[*types.PeerRecord]
		if upr.Err != nil {
			result.Err = upr.Err
			return result
		}
		switch upr.Val.Schema {
		case types.SchemaPeer:
			var prov types.PeerRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		}
		return result
	}

	return iter.Map[iter.Result[types.UnknownRecord]](jsonIter, mapFn)
}
