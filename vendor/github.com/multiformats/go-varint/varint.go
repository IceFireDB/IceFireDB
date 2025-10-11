package varint

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

var (
	ErrOverflow   = errors.New("varints larger than uint63 not supported")
	ErrUnderflow  = errors.New("varints malformed, could not reach the end")
	ErrNotMinimal = errors.New("varint not minimally encoded")
)

const (
	// MaxLenUvarint63 is the maximum number of bytes representing an uvarint in
	// this encoding, supporting a maximum value of 2^63 (uint63), aka
	// MaxValueUvarint63.
	MaxLenUvarint63 = 9

	// MaxValueUvarint63 is the maximum encodable uint63 value.
	MaxValueUvarint63 = (1 << 63) - 1
)

// UvarintSize returns the size (in bytes) of `num` encoded as a unsigned varint.
//
// This may return a size greater than MaxUvarintLen63, which would be an
// illegal value, and would be rejected by readers.
func UvarintSize(num uint64) int {
	// This implementation follows the optimised approach from Google's protobuf library
	// for better performance through LZCNT instruction usage on modern CPUs.

	// OR with 1 to guarantee num is never 0, avoiding extra instructions for LZCNT.
	// Because `0` is a special case with undefined behaviour, Go has special casing
	// for it that we can avoid entirely.
	// This doesn't change the result since varint(0) and varint(1) both encode to
	// 1 byte and we only care about byte length.
	num |= 1

	// Using XOR 63 instead of subtraction for better performance on modern CPUs.
	// This computes log2(num) for our [1..2^64-1] range.
	log2value := uint32(bits.LeadingZeros64(num)) ^ 63

	// This computes 1 + (bits-1)/7, using 9/64 approximation of 1/7
	// Formula: ceil(log2(num)/7) = floor((log2(num)*9 + 73) / 64)
	return int((log2value*9 + (64 + 9)) / 64)
}

// ToUvarint converts an unsigned integer to a varint-encoded []byte
func ToUvarint(num uint64) []byte {
	buf := make([]byte, UvarintSize(num))
	n := binary.PutUvarint(buf, uint64(num))
	return buf[:n]
}

// FromUvarint reads an unsigned varint from the beginning of buf, returns the
// varint, and the number of bytes read.
func FromUvarint(buf []byte) (uint64, int, error) {
	// Modified from the go standard library. Copyright the Go Authors and
	// released under the BSD License.
	var x uint64
	var s uint
	for i, b := range buf {
		if (i == 8 && b >= 0x80) || i >= MaxLenUvarint63 {
			// this is the 9th and last byte we're willing to read, but it
			// signals there's more (1 in MSB).
			// or this is the >= 10th byte, and for some reason we're still here.
			return 0, 0, ErrOverflow
		}
		if b < 0x80 {
			if b == 0 && s > 0 {
				return 0, 0, ErrNotMinimal
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0, ErrUnderflow
}

// ReadUvarint reads a unsigned varint from the given reader.
func ReadUvarint(r io.ByteReader) (uint64, error) {
	// Modified from the go standard library. Copyright the Go Authors and
	// released under the BSD License.
	var x uint64
	var s uint
	for s = 0; ; s += 7 {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && s != 0 {
				// "eof" will look like a success.
				// If we've read part of a value, this is not a
				// success.
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if (s == 56 && b >= 0x80) || s >= (7*MaxLenUvarint63) {
			// this is the 9th and last byte we're willing to read, but it
			// signals there's more (1 in MSB).
			// or this is the >= 10th byte, and for some reason we're still here.
			return 0, ErrOverflow
		}
		if b < 0x80 {
			if b == 0 && s > 0 {
				return 0, ErrNotMinimal
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
	}
}

// PutUvarint is an alias for binary.PutUvarint.
//
// This is provided for convenience so users of this library can avoid built-in
// varint functions and easily audit code for uses of those functions.
//
// Make sure that x is smaller or equal to MaxValueUvarint63, otherwise this
// function will produce values that may be rejected by readers.
func PutUvarint(buf []byte, x uint64) int {
	return binary.PutUvarint(buf, x)
}
