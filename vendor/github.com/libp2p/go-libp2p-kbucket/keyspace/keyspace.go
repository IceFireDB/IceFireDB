package keyspace

import (
	"math/big"
	"slices"
)

// Key represents an identifier in a KeySpace. It holds a reference to the
// associated KeySpace, as well references to both the Original identifier,
// as well as the new, KeySpace Bytes one.
type Key struct {
	// Space is the KeySpace this Key is related to.
	Space KeySpace
	// Original is the original value of the identifier
	Original []byte
	// Bytes is the new value of the identifier, in the KeySpace.
	Bytes []byte
}

// Compare returns an integer that compares the two Keys.
func (k1 Key) Compare(k2 Key) int {
	if k1.Space != k2.Space {
		panic("k1 and k2 not in same key space.")
	}
	return k1.Space.Compare(k1, k2)
}

// Equal returns whether this key is equal to another.
func (k1 Key) Equal(k2 Key) bool {
	if k1.Space != k2.Space {
		panic("k1 and k2 not in same key space.")
	}
	return k1.Space.Equal(k1, k2)
}

// Distance returns this key's distance to another
func (k1 Key) Distance(k2 Key) *big.Int {
	if k1.Space != k2.Space {
		panic("k1 and k2 not in same key space.")
	}
	return k1.Space.Distance(k1, k2)
}

// KeySpace is an object used to do math on identifiers. Each keyspace has its
// own properties and rules. See XorKeySpace.
type KeySpace interface {
	// Key converts an identifier into a Key in this space.
	Key([]byte) Key
	// Equal returns whether keys are equal in this key space
	Equal(Key, Key) bool
	// Distance returns the distance metric in this key space
	Distance(Key, Key) *big.Int
	// Compare returns an integer comparing two keys.
	Compare(Key, Key) int
}

// keyDistance holds a Key alongside its precomputed distance to a center Key.
type keyDistance struct {
	key      Key
	distance *big.Int
}

// SortByDistance takes a KeySpace, a center Key, and a list of Keys toSort.
// It returns a new list, where the Keys toSort have been sorted by their
// distance to the center Key.
func SortByDistance(sp KeySpace, center Key, toSort []Key) []Key {
	distances := make([]keyDistance, len(toSort))
	for i, k := range toSort {
		distances[i] = keyDistance{key: k, distance: center.Distance(k)}
	}
	slices.SortFunc(distances, func(a, b keyDistance) int {
		return a.distance.Cmp(b.distance)
	})
	sorted := make([]Key, len(distances))
	for i, d := range distances {
		sorted[i] = d.key
	}
	return sorted
}
