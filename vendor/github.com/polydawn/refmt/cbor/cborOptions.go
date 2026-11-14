package cbor

type EncodeOptions struct {
	// there aren't a ton of options for cbor, but we still need this
	// for use as a sigil for the top-level refmt methods to demux on.
}

// marker method -- you may use this type to instruct `refmt.Marshal`
// what kind of encoder to use.
func (EncodeOptions) IsEncodeOptions() {}

// DecodeOptions controls decoder behaviour. Rejection flags select stricter
// CBOR dialects; all are independent and off by default. Indefinite-length
// bytes and strings remain accepted by default, but their aggregate size is
// capped by MaxIndefiniteSize.
type DecodeOptions struct {
	CoerceUndefToNull bool

	// RejectIndefinite errors at the indefinite-length sigil byte (0x5f,
	// 0x7f, 0x9f, 0xbf) before any chunks are read or allocated.
	RejectIndefinite bool

	// MaxIndefiniteSize caps the cumulative size, in bytes, of an
	// indefinite-length bytes or string value during chunk aggregation.
	// Decoding errors when the running total would exceed this. When zero,
	// a default of 32 MiB is used (matching the per-chunk maximum, so
	// indefinite values can't grow larger in total than a single
	// definite-length value).
	MaxIndefiniteSize int

	// RejectNonMinimalInteger rejects CBOR heads whose integer argument is
	// encoded in more bytes than necessary. Applies to uints, negative
	// ints, length headers (bytes/strings/arrays/maps) and tag headers.
	RejectNonMinimalInteger bool

	// RejectNaN errors when a float value decodes to NaN, regardless of
	// which bit pattern was used to encode it.
	RejectNaN bool

	// RejectInfinity errors when a float value decodes to +Inf or -Inf.
	RejectInfinity bool

	// RejectNarrowFloat rejects 16-bit (0xf9) and 32-bit (0xfa) float
	// encodings at the sigil byte, before any payload is read.
	RejectNarrowFloat bool
}

// defaultMaxIndefiniteSize matches the existing per-chunk size cap in
// decodeBytesOrStringIndefinite (33554432 bytes / 32 MiB). It bounds the
// total accumulator so an indefinite-length value cannot grow larger than
// what a single definite-length value would have been allowed to.
const defaultMaxIndefiniteSize = 33554432

func (cfg DecodeOptions) maxIndefiniteSize() int {
	if cfg.MaxIndefiniteSize > 0 {
		return cfg.MaxIndefiniteSize
	}
	return defaultMaxIndefiniteSize
}

// marker method -- you may use this type to instruct `refmt.Marshal`
// what kind of encoder to use.
func (DecodeOptions) IsDecodeOptions() {}
