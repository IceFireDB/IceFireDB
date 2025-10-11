package key

import (
	"errors"
	"sort"
	"strings"

	"github.com/probe-lab/go-libdht/kad"
)

// ErrInvalidDataLength is the error returned when attempting to construct a key from binary data of the wrong length.
var ErrInvalidDataLength = errors.New("invalid data length")

const BitPanicMsg = "bit index out of range"

// Equal reports whether two keys have equal numeric values.
func Equal[K kad.Key[K]](a, b K) bool {
	if a.BitLen() != b.BitLen() {
		return false
	}
	return a.Compare(b) == 0
}

// CommonPrefixLength returns the length of the common prefix of two keys.
// Note that keys can be of different types, and different lengths.
func CommonPrefixLength[K0 kad.Key[K0], K1 kad.Key[K1]](a K0, b K1) int {
	minLen := a.BitLen()
	if b.BitLen() < minLen {
		minLen = b.BitLen()
	}

	for i := 0; i < minLen; i++ {
		if a.Bit(i) != b.Bit(i) {
			return i
		}
	}
	return minLen
}

// BitString returns a string containing the binary representation of a key.
func BitString[K kad.Key[K]](k K) string {
	if bs, ok := any(k).(interface{ BitString() string }); ok {
		return bs.BitString()
	}
	b := new(strings.Builder)
	b.Grow(k.BitLen())
	for i := 0; i < k.BitLen(); i++ {
		if k.Bit(i) == 0 {
			b.WriteByte('0')
		} else {
			b.WriteByte('1')
		}
	}
	return b.String()
}

// HexString returns a string containing the hexadecimal representation of a key.
func HexString[K kad.Key[K]](k K) string {
	if hs, ok := any(k).(interface{ HexString() string }); ok {
		return hs.HexString()
	}
	b := new(strings.Builder)
	b.Grow((k.BitLen() + 3) / 4)

	h := [...]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}

	prebits := k.BitLen() % 4
	if prebits > 0 {
		// deal with initial nibble for key lengths that aren't a multiple of 4
		var n byte
		n |= byte(k.Bit(0))
		for i := 1; i < prebits; i++ {
			n <<= 1
			n |= byte(k.Bit(i))
		}
		b.WriteByte(h[n])
	}

	for i := prebits; i < k.BitLen(); i += 4 {
		var n byte
		n |= byte(k.Bit(i)) << 3
		n |= byte(k.Bit(i+1)) << 2
		n |= byte(k.Bit(i+2)) << 1
		n |= byte(k.Bit(i + 3))
		b.WriteByte(h[n])
	}
	return b.String()
}

// IsSorted reports whether a list of keys is sorted in ascending numerical order.
func IsSorted[K kad.Key[K]](ks []K) bool {
	return sort.IsSorted(KeyList[K](ks))
}

// KeyList is a list of Kademlia keys. It implements sort.Interface.
type KeyList[K kad.Key[K]] []K

func (ks KeyList[K]) Len() int           { return len(ks) }
func (ks KeyList[K]) Swap(i, j int)      { ks[i], ks[j] = ks[j], ks[i] }
func (ks KeyList[K]) Less(i, j int) bool { return ks[i].Compare(ks[j]) < 0 }
