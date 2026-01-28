// Package httpsfv implements serializing and parsing
// of Structured Field Values for HTTP as defined in RFC 9651.
//
// Structured Field Values are either lists, dictionaries or items. Dedicated types are provided for all of them.
// Dedicated types are also used for tokens, parameters and inner lists.
// Other values are stored in native types:
//
//	int64, for integers
//	float64, for decimals
//	string, for strings
//	byte[], for byte sequences
//	bool, for booleans
//
// The specification is available at https://httpwg.org/specs/rfc9651.html.
package httpsfv

import (
	"strings"
)

// marshaler is the interface implemented by types that can marshal themselves into valid SFV.
type marshaler interface {
	marshalSFV(b *strings.Builder) error
}

// StructuredFieldValue represents a List, a Dictionary or an Item.
type StructuredFieldValue interface {
	marshaler
}

// Marshal returns the HTTP Structured Value serialization of v
// as defined in https://httpwg.org/specs/rfc9651.html#text-serialize.
//
// v must be a List, a Dictionary, an Item or an InnerList.
func Marshal(v StructuredFieldValue) (string, error) {
	var b strings.Builder
	if err := v.marshalSFV(&b); err != nil {
		return "", err
	}

	return b.String(), nil
}
