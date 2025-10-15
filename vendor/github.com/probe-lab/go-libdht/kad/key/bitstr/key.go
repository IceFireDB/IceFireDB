package bitstr

import (
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
)

// Key is a binary key represented by a string of 1's and 0's
type Key string

var _ kad.Key[Key] = Key("1010")

var lengthPanicMsg = "bitstr.Key: other key has different length"

func (k Key) BitLen() int {
	return len(k)
}

func (k Key) Bit(i int) uint {
	if i < 0 || i > len(k) {
		panic(key.BitPanicMsg)
	}
	if k[i] == '1' {
		return 1
	} else if k[i] == '0' {
		return 0
	}
	panic("bitstr.Key: not a binary string")
}

func (k Key) Xor(o Key) Key {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return Key(o)
		}
		if len(o) == 0 && k.isZero() {
			return Key(k)
		}
		panic(lengthPanicMsg)
	}
	buf := make([]byte, len(k))
	for i := range buf {
		if k[i] != o[i] {
			buf[i] = '1'
		} else {
			buf[i] = '0'
		}
	}
	return Key(string(buf))
}

func (k Key) CommonPrefixLength(o Key) int {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return len(o)
		}
		if len(o) == 0 && k.isZero() {
			return len(k)
		}
	}
	minLen := min(len(k), len(o))
	for i := range minLen {
		if k[i] != o[i] {
			return i
		}
	}
	return minLen
}

func (k Key) Compare(o Key) int {
	if len(k) != len(o) {
		if len(k) == 0 && o.isZero() {
			return 0
		}
		if len(o) == 0 && k.isZero() {
			return 0
		}
	}
	for i := range min(len(k), len(o)) {
		if k[i] != o[i] {
			if k[i] < o[i] {
				return -1
			}
			return 1
		}
	}
	if len(k) == len(o) {
		return 0
	}
	if len(k) > len(o) {
		return 1
	}
	return -1
}

func (k Key) isZero() bool {
	for i := 0; i < len(k); i++ {
		if k[i] != '0' {
			return false
		}
	}
	return true
}
