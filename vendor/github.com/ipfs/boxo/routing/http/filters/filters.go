package filters

import (
	"net/url"
	"reflect"
	"slices"
	"strings"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("routing/http/filters")

// Package filters implements IPIP-0484

func ParseFilter(param string) []string {
	if param == "" {
		return nil
	}
	return strings.Split(strings.ToLower(param), ",")
}

func AddFiltersToURL(baseURL string, protocolFilter, addrFilter []string) string {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}

	query := parsedURL.Query()

	if len(protocolFilter) > 0 {
		query.Set("filter-protocols", strings.Join(protocolFilter, ","))
	}

	if len(addrFilter) > 0 {
		query.Set("filter-addrs", strings.Join(addrFilter, ","))
	}

	// The comma is in the "sub-delims" set of characters that don't need to be
	// encoded in most parts of a URL, including query parameters. Golang
	// standard library percent-escapes it for consistency, but we prefer
	// human-readable /routing/v1 URLs, and real comma is restored here to
	// ensure human and machine requests hit the same HTTP cache keys.
	parsedURL.RawQuery = strings.ReplaceAll(query.Encode(), "%2C", ",")

	return parsedURL.String()
}

// ApplyFiltersToIter applies the filters to the given iterator and returns a new iterator.
//
// The function iterates over the input iterator, applying the specified filters to each record.
// It supports both positive and negative filters for both addresses and protocols.
//
// Parameters:
// - recordsIter: An iterator of types.Record to be filtered.
// - filterAddrs: A slice of strings representing the address filter criteria.
// - filterProtocols: A slice of strings representing the protocol filter criteria.
func ApplyFiltersToIter(recordsIter iter.ResultIter[types.Record], filterAddrs, filterProtocols []string) iter.ResultIter[types.Record] {
	mappedIter := iter.Map(recordsIter, func(v iter.Result[types.Record]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		switch v.Val.GetSchema() {
		case types.SchemaPeer:
			record, ok := v.Val.(*types.PeerRecord)
			if !ok {
				logger.Errorw("problem casting find providers record", "Schema", v.Val.GetSchema(), "Type", reflect.TypeOf(v).String())
				// drop failed type assertion
				return iter.Result[types.Record]{}
			}

			record = applyFilters(record, filterAddrs, filterProtocols)
			if record == nil {
				return iter.Result[types.Record]{}
			}
			v.Val = record

		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			record, ok := v.Val.(*types.BitswapRecord)
			if !ok {
				logger.Errorw("problem casting find providers record", "Schema", v.Val.GetSchema(), "Type", reflect.TypeOf(v).String())
				// drop failed type assertion
				return iter.Result[types.Record]{}
			}
			peerRecord := types.FromBitswapRecord(record)
			peerRecord = applyFilters(peerRecord, filterAddrs, filterProtocols)
			if peerRecord == nil {
				return iter.Result[types.Record]{}
			}
			v.Val = peerRecord
		}
		return v
	})

	// filter out nil results and errors
	filteredIter := iter.Filter(mappedIter, func(v iter.Result[types.Record]) bool {
		return v.Err == nil && v.Val != nil
	})

	return filteredIter
}

func ApplyFiltersToPeerRecordIter(peerRecordIter iter.ResultIter[*types.PeerRecord], filterAddrs, filterProtocols []string) iter.ResultIter[*types.PeerRecord] {
	// Convert PeerRecord to Record so that we can reuse the filtering logic from findProviders
	mappedIter := iter.Map(peerRecordIter, func(v iter.Result[*types.PeerRecord]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return iter.Result[types.Record]{Err: v.Err}
		}

		var record types.Record = v.Val
		return iter.Result[types.Record]{Val: record}
	})

	filteredIter := ApplyFiltersToIter(mappedIter, filterAddrs, filterProtocols)

	// Convert Record back to PeerRecord 🙃
	return iter.Map(filteredIter, func(v iter.Result[types.Record]) iter.Result[*types.PeerRecord] {
		if v.Err != nil || v.Val == nil {
			return iter.Result[*types.PeerRecord]{Err: v.Err}
		}

		var record *types.PeerRecord = v.Val.(*types.PeerRecord)
		return iter.Result[*types.PeerRecord]{Val: record}
	})
}

// Applies the filters. Returns nil if the provider does not pass the protocols filter
// The address filter is more complicated because it potentially modifies the Addrs slice.
func applyFilters(provider *types.PeerRecord, filterAddrs, filterProtocols []string) *types.PeerRecord {
	if len(filterAddrs) == 0 && len(filterProtocols) == 0 {
		return provider
	}

	if !protocolsAllowed(provider.Protocols, filterProtocols) {
		// If the provider doesn't match any of the passed protocols, the provider is omitted from the response.
		return nil
	}

	// return untouched if there's no filter or filterAddrsQuery contains "unknown" and provider has no addrs
	if len(filterAddrs) == 0 || (len(provider.Addrs) == 0 && slices.Contains(filterAddrs, "unknown")) {
		return provider
	}

	filteredAddrs := applyAddrFilter(provider.Addrs, filterAddrs)

	// If filtering resulted in no addrs, omit the provider
	if len(filteredAddrs) == 0 {
		return nil
	}

	provider.Addrs = filteredAddrs
	return provider
}

// applyAddrFilter filters a list of multiaddresses based on the provided filter query.
//
// Parameters:
// - addrs: A slice of types.Multiaddr to be filtered.
// - filterAddrsQuery: A slice of strings representing the filter criteria.
//
// The function supports both positive and negative filters:
// - Positive filters (e.g., "tcp", "udp") include addresses that match the specified protocols.
// - Negative filters (e.g., "!tcp", "!udp") exclude addresses that match the specified protocols.
//
// If no filters are provided, the original list of addresses is returned unchanged.
// If only negative filters are provided, addresses not matching any negative filter are included.
// If positive filters are provided, only addresses matching at least one positive filter (and no negative filters) are included.
// If both positive and negative filters are provided, the address must match at least one positive filter and no negative filters to be included.
//
// Returns:
// A new slice of types.Multiaddr containing only the addresses that pass the filter criteria.
func applyAddrFilter(addrs []types.Multiaddr, filterAddrsQuery []string) []types.Multiaddr {
	if len(filterAddrsQuery) == 0 {
		return addrs
	}

	var filteredAddrs []types.Multiaddr
	var positiveFilters, negativeFilters []multiaddr.Protocol

	// Separate positive and negative filters
	for _, filter := range filterAddrsQuery {
		if strings.HasPrefix(filter, "!") {
			negativeFilters = append(negativeFilters, multiaddr.ProtocolWithName(filter[1:]))
		} else {
			positiveFilters = append(positiveFilters, multiaddr.ProtocolWithName(filter))
		}
	}

	for _, addr := range addrs {
		protocols := addr.Protocols()

		// Check negative filters
		if containsAny(protocols, negativeFilters) {
			continue
		}

		// If no positive filters or matches a positive filter, include the address
		if len(positiveFilters) == 0 || containsAny(protocols, positiveFilters) {
			filteredAddrs = append(filteredAddrs, addr)
		}
	}

	return filteredAddrs
}

// Helper function to check if protocols contain any of the filters
func containsAny(protocols []multiaddr.Protocol, filters []multiaddr.Protocol) bool {
	for _, filter := range filters {
		if containsProtocol(protocols, filter) {
			return true
		}
	}
	return false
}

func containsProtocol(protos []multiaddr.Protocol, proto multiaddr.Protocol) bool {
	for _, p := range protos {
		if p.Code == proto.Code {
			return true
		}
	}
	return false
}

// protocolsAllowed returns true if the peerProtocols are allowed by the filter protocols.
func protocolsAllowed(peerProtocols []string, filterProtocols []string) bool {
	if len(filterProtocols) == 0 {
		// If no filter is passed, do not filter
		return true
	}

	for _, filterProtocol := range filterProtocols {
		if filterProtocol == "unknown" && len(peerProtocols) == 0 {
			return true
		}

		for _, peerProtocol := range peerProtocols {
			if strings.EqualFold(peerProtocol, filterProtocol) {
				return true
			}
		}
	}
	return false
}
