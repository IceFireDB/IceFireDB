package multicodec

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
)

var encoderRegistry = make(map[uint64]ipld.Encoder)
var decoderRegistry = make(map[uint64]ipld.Decoder)

// RegisterEncoder updates a simple map of multicodec indicator number to ipld.Encoder function.
// The encoder functions registered can be subsequently looked up using LookupEncoder.
//
// Packages which implement an IPLD codec and have a multicodec number reserved in
// https://github.com/multiformats/multicodec/blob/master/table.csv
// are encouraged to register themselves in this map at package init time.
// (Doing this at package init time ensures this map can be accessed without race conditions.)
//
// This registry map is only used for default behaviors.
// For example, linking/cid.DefaultLinkSystem will use LookupEncoder to access this registry map
// and select encoders to use when serializing data for linking and storage.
// LinkSystem itself is not hardcoded to use the global LookupEncoder feature;
// therefore, you don't want to rely on this mapping, you can always construct your own LinkSystem.
//
// No effort is made to detect conflicting registrations in this map.
// If your dependency tree is such that this becomes a problem,
// there are two ways to address this:
// If RegisterEncoder is called with the same indicator code more than once, the last call wins.
// In practice, this means that if an application has a strong opinion about what implementation for a certain codec,
// then this can be done by making a Register call with that effect at init time in the application's main package.
// This should have the desired effect because the root of the import tree has its init time effect last.
// Alternatively, one can just avoid use of this registry entirely:
// do this by making a LinkSystem that uses a custom EncoderChooser function.
func RegisterEncoder(indicator uint64, encodeFunc ipld.Encoder) {
	// This function could arguably be just a bare map access.
	// We introduced a function primarily for the interest of potential future changes.
	// E.g. one could introduce logging here to help detect unintended conflicting registrations.
	// (We probably won't do this, but you can do it yourself as a printf debug hack. :))

	if encodeFunc == nil {
		panic("not sensible to attempt to register a nil function")
	}
	encoderRegistry[indicator] = encodeFunc
}

// LookupEncoder yields an ipld.Encoder function matching a multicodec indicator code number.
//
// Multicodec indicator numbers are specified in
// https://github.com/multiformats/multicodec/blob/master/table.csv
//
// To be available from this lookup function, an encoder must have been registered
// for this indicator number by an earlier call to the RegisterEncoder function.
func LookupEncoder(indicator uint64) (ipld.Encoder, error) {
	encodeFunc, exists := encoderRegistry[indicator]
	if !exists {
		return nil, fmt.Errorf("no encoder registered for multicodec code %d (0x%x)", indicator, indicator)
	}
	return encodeFunc, nil
}

// RegisterDecoder updates a simple map of multicodec indicator number to ipld.Decoder function.
// The decoder functions registered can be subsequently looked up using LookupDecoder.
//
// Packages which implement an IPLD codec and have a multicodec number reserved in
// https://github.com/multiformats/multicodec/blob/master/table.csv
// are encouraged to register themselves in this map at package init time.
// (Doing this at package init time ensures this map can be accessed without race conditions.)
//
// This registry map is only used for default behaviors.
// For example, linking/cid.DefaultLinkSystem will use LookupDecoder to access this registry map
// and select decoders to use when serializing data for linking and storage.
// LinkSystem itself is not hardcoded to use the global LookupDecoder feature;
// therefore, you don't want to rely on this mapping, you can always construct your own LinkSystem.
//
// No effort is made to detect conflicting registrations in this map.
// If your dependency tree is such that this becomes a problem,
// there are two ways to address this:
// If RegisterDecoder is called with the same indicator code more than once, the last call wins.
// In practice, this means that if an application has a strong opinion about what implementation for a certain codec,
// then this can be done by making a Register call with that effect at init time in the application's main package.
// This should have the desired effect because the root of the import tree has its init time effect last.
// Alternatively, one can just avoid use of this registry entirely:
// do this by making a LinkSystem that uses a custom DecoderChooser function.
func RegisterDecoder(indicator uint64, decodeFunc ipld.Decoder) {
	// This function could arguably be just a bare map access.
	// We introduced a function primarily for the interest of potential future changes.
	// E.g. one could introduce logging here to help detect unintended conflicting registrations.
	// (We probably won't do this, but you can do it yourself as a printf debug hack. :))

	if decodeFunc == nil {
		panic("not sensible to attempt to register a nil function")
	}
	decoderRegistry[indicator] = decodeFunc
}

// LookupDecoder yields an ipld.Decoder function matching a multicodec indicator code number.
//
// Multicodec indicator numbers are specified in
// https://github.com/multiformats/multicodec/blob/master/table.csv
//
// To be available from this lookup function, an decoder must have been registered
// for this indicator number by an earlier call to the RegisterDecoder function.
func LookupDecoder(indicator uint64) (ipld.Decoder, error) {
	decodeFunc, exists := decoderRegistry[indicator]
	if !exists {
		return nil, fmt.Errorf("no decoder registered for multicodec code %d (0x%x)", indicator, indicator)
	}
	return decodeFunc, nil
}
