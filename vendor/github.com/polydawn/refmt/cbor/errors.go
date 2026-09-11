package cbor

import (
	"errors"
	"fmt"

	. "github.com/polydawn/refmt/tok"
)

// ErrIndefiniteLength is returned by the decoder when an indefinite-length
// encoding is encountered and DecodeOptions.RejectIndefinite is set.
var ErrIndefiniteLength = errors.New("cbor: indefinite-length encoding rejected")

// ErrIndefiniteSizeExceeded is returned by the decoder when the cumulative
// size of an indefinite-length bytes or string value would exceed
// DecodeOptions.MaxIndefiniteSize.
var ErrIndefiniteSizeExceeded = errors.New("cbor: indefinite-length string/bytes total size exceeds limit")

// ErrNonMinimalInteger is returned by the decoder when a CBOR head's integer
// argument is encoded in more bytes than necessary and
// DecodeOptions.RejectNonMinimalInteger is set.
var ErrNonMinimalInteger = errors.New("cbor: integer not minimally encoded")

// ErrFloatNaN is returned by the decoder when a NaN float value is
// encountered and DecodeOptions.RejectNaN is set.
var ErrFloatNaN = errors.New("cbor: NaN float value rejected")

// ErrFloatInfinity is returned by the decoder when an infinite float value
// is encountered and DecodeOptions.RejectInfinity is set.
var ErrFloatInfinity = errors.New("cbor: infinite float value rejected")

// ErrNarrowFloat is returned by the decoder when a 16-bit or 32-bit float
// encoding is encountered and DecodeOptions.RejectNarrowFloat is set.
var ErrNarrowFloat = errors.New("cbor: float narrower than 64 bits rejected")

// Error raised by Encoder when invalid tokens or invalid ordering, e.g. a MapClose with no matching open.
// Should never be seen by the user in practice unless generating their own token streams.
type ErrInvalidTokenStream struct {
	Got        Token
	Acceptable []TokenType
}

func (e *ErrInvalidTokenStream) Error() string {
	return fmt.Sprintf("ErrInvalidTokenStream: unexpected %v, expected %v", e.Got, e.Acceptable)
	// More comprehensible strings might include "start of value", "start of key or end of map", "start of value or end of array".
}

var tokenTypesForKey = []TokenType{TString, TInt, TUint}
var tokenTypesForValue = []TokenType{TMapOpen, TArrOpen, TNull, TString, TBytes, TInt, TUint, TFloat64}
