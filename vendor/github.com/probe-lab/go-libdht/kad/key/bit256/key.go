package bit256

import (
	"bytes"
	"encoding/hex"
	"math"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
)

// KeyLen is the length of a 256-bit key in bytes.
const KeyLen = 32

// Key is a 256-bit Kademlia key.
type Key struct {
	b *[KeyLen]byte // this is a pointer to keep the size of Key small since it is often passed as argument
}

var _ kad.Key[Key] = Key{}

// NewKey returns a 256-bit Kademlia key whose bits are set from the supplied bytes.
func NewKey(data []byte) Key {
	if len(data) != KeyLen {
		panic(key.ErrInvalidDataLength)
	}
	var b [KeyLen]byte
	copy(b[:], data)
	return Key{b: &b}
}

// ZeroKey returns a 256-bit Kademlia key with all bits zeroed.
func ZeroKey() Key {
	var b [KeyLen]byte
	return Key{b: &b}
}

// Bit returns the value of the i'th bit of the key from most significant to least.
func (k Key) Bit(i int) uint {
	if i < 0 || i > 255 {
		panic(key.BitPanicMsg)
	}
	if k.b == nil {
		return 0
	}
	if k.b[i/8]&(byte(1)<<(7-i%8)) == 0 {
		return 0
	} else {
		return 1
	}
}

// BitLen returns the length of the key in bits, which is always 256.
func (Key) BitLen() int {
	return 256
}

// Xor returns the result of the eXclusive OR operation between the key and another key of the same type.
func (k Key) Xor(o Key) Key {
	var xored [KeyLen]byte
	if k.b != nil && o.b != nil {
		for i := 0; i < KeyLen; i++ {
			xored[i] = k.b[i] ^ o.b[i]
		}
	} else if k.b != nil && o.b == nil {
		copy(xored[:], k.b[:])
	} else if k.b == nil && o.b != nil {
		copy(xored[:], o.b[:])
	}
	return Key{b: &xored}
}

// CommonPrefixLength returns the number of leading bits the key shares with another key of the same type.
func (k Key) CommonPrefixLength(o Key) int {
	if k.b == nil || o.b == nil {
		return 256
	}
	var x byte
	for i := 0; i < KeyLen; i++ {
		x = k.b[i] ^ o.b[i]
		if x != 0 {
			return i*8 + 7 - int(math.Log2(float64(x))) // TODO: make this more efficient
		}
	}
	return 256
}

// Compare compares the numeric value of the key with another key of the same type.
func (k Key) Compare(o Key) int {
	if k.b != nil && o.b != nil {
		return bytes.Compare(k.b[:], o.b[:])
	}

	var zero [KeyLen]byte
	if k.b == nil {
		return bytes.Compare(zero[:], o.b[:])
	}
	return bytes.Compare(zero[:], k.b[:])
}

// HexString returns a string containing the hexadecimal representation of the key.
func (k Key) HexString() string {
	if k.b == nil {
		return ""
	}
	return hex.EncodeToString(k.b[:])
}

// MarshalBinary marshals the key into a byte slice.
// The bytes may be passed to NewKey to construct a new key with the same value.
func (k Key) MarshalBinary() ([]byte, error) {
	buf := make([]byte, KeyLen)
	if k.b != nil {
		copy(buf, (*k.b)[:])
	}
	return buf, nil
}
