package keyspace

import (
	"cmp"
	"crypto/sha256"
	"slices"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

// KeyLen is the length of a 256-bit kademlia identifier in bits.
const KeyLen = bit256.KeyLen * 8 // 256

// MhToBit256 converts a multihash to a its 256-bit kademlia identifier by
// hashing it with SHA-256.
func MhToBit256(h mh.Multihash) bit256.Key {
	hash := sha256.Sum256(h)
	return bit256.NewKey(hash[:])
}

// PeerIDToBit256 converts a peer.ID to a its 256-bit kademlia identifier by
// hashing it with SHA-256.
func PeerIDToBit256(id peer.ID) bit256.Key {
	hash := sha256.Sum256([]byte(id))
	return bit256.NewKey(hash[:])
}

// FlipLastBit flips the last bit of the given key.
func FlipLastBit(k bitstr.Key) bitstr.Key {
	if len(k) == 0 {
		return k
	}
	flipped := byte('0' + '1' - k[len(k)-1])
	return k[:len(k)-1] + bitstr.Key(flipped)
}

// FirstFullKeyWithPrefix returns to closest 256-bit key to order, starting
// with the given k as a prefix.
func FirstFullKeyWithPrefix[K kad.Key[K]](k bitstr.Key, order K) bitstr.Key {
	kLen := k.BitLen()
	if kLen > KeyLen {
		return k[:KeyLen]
	}
	return k + bitstr.Key(key.BitString(order))[kLen:]
}

// IsBitstrPrefix returns true if k0 is a prefix of k1.
func IsBitstrPrefix(k0 bitstr.Key, k1 bitstr.Key) bool {
	return len(k0) <= len(k1) && k0 == k1[:len(k0)]
}

// IsPrefix returns true if k0 is a prefix of k1
func IsPrefix[K0 kad.Key[K0], K1 kad.Key[K1]](k0 K0, k1 K1) bool {
	if k0.BitLen() > k1.BitLen() {
		return false
	}
	for i := range k0.BitLen() {
		if k0.Bit(i) != k1.Bit(i) {
			return false
		}
	}
	return true
}

const initMask = (byte(1) << 7) // 0x80

// KeyToBytes converts a kad.Key to a byte slice. If the provided key has a
// size that isn't a multiple of 8, right pad the resulting byte with 0s.
func KeyToBytes[K kad.Key[K]](k K) []byte {
	bitLen := k.BitLen()
	byteLen := (bitLen + 7) / 8
	b := make([]byte, byteLen)

	byteIndex := 0
	mask := initMask
	by := byte(0)

	for i := range bitLen {
		if k.Bit(i) == 1 {
			by |= mask
		}
		mask >>= 1

		if mask == 0 {
			b[byteIndex] = by
			byteIndex++
			by = 0
			mask = initMask
		}
	}
	if mask != initMask {
		b[byteIndex] = by
	}
	return b
}

// ShortestCoveredPrefix takes as input the `target` key and the list of
// closest peers to this key. It returns a prefix of `requested` that is
// covered by these peers, along with the peers matching this prefix.
//
// We say that a set of peers fully "covers" a prefix of the global keyspace,
// if all the peers matching this prefix are included in the set.
//
// If every peer shares the same CPL to `target`, then no deeper zone is
// covered, we learn that the adjacent sibling branch is empty. In this case we
// return the prefix one bit deeper (`minCPL+1`) and an empty peer list.
func ShortestCoveredPrefix(target bitstr.Key, peers []peer.ID) (bitstr.Key, []peer.ID) {
	if len(peers) == 0 {
		return target, peers
	}
	// Sort the peers by their distance to the requested key.
	peers = kb.SortClosestPeers(peers, KeyToBytes(target))

	minCpl := target.BitLen() // key bitlen
	coveredCpl := 0
	lastCoveredPeerIndex := 0
	for i, p := range peers {
		cpl := key.CommonPrefixLength(target, PeerIDToBit256(p))
		if cpl < minCpl {
			coveredCpl = cpl + 1
			lastCoveredPeerIndex = i
			minCpl = cpl
		}
	}
	return target[:coveredCpl], peers[:lastCoveredPeerIndex]
}

// ExtendBinaryPrefix returns all bitstrings of length n that start with prefix.
// Example: prefix="1101", n=6 -> ["110100", "110101", "110110", "110111"].
func ExtendBinaryPrefix(prefix bitstr.Key, n int) []bitstr.Key {
	extraBits := n - len(prefix)
	if n < 0 || extraBits < 0 {
		return nil
	}

	extLen := 1 << extraBits // 2^extraBits
	rd := make([]bitstr.Key, 0, extLen)
	wr := make([]bitstr.Key, 1, extLen)
	wr[0] = prefix

	// Iteratively append bits until reaching length n.
	for range extraBits {
		rd, wr = wr, rd[:0]
		for _, s := range rd {
			wr = append(wr, s+"0", s+"1")
		}
	}
	return wr
}

// SiblingPrefixes returns the prefixes of the sibling subtrees along the path
// to key. Together with the subtree under `key` itself, these prefixes
// partition the keyspace.
//
// For key "1100" it returns: ["0", "10", "111", "1101"].
func SiblingPrefixes(key bitstr.Key) []bitstr.Key {
	complements := make([]bitstr.Key, len(key))
	for i := range key {
		complements[i] = FlipLastBit(key[:i+1])
	}
	return complements
}

// PrefixAndKeys is a struct that holds a prefix and the multihashes whose
// kademlia identifier share the same prefix.
type PrefixAndKeys struct {
	Prefix bitstr.Key
	Keys   []mh.Multihash
}

// SortPrefixesBySize sorts the prefixes by the number of keys they contain,
// largest first.
func SortPrefixesBySize(prefixes map[bitstr.Key][]mh.Multihash) []PrefixAndKeys {
	out := make([]PrefixAndKeys, 0, len(prefixes))
	for prefix, keys := range prefixes {
		if keys != nil {
			out = append(out, PrefixAndKeys{Prefix: prefix, Keys: keys})
		}
	}
	slices.SortFunc(out, func(a, b PrefixAndKeys) int {
		return cmp.Compare(len(b.Keys), len(a.Keys))
	})
	return out
}
