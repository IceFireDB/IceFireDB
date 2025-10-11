package keyspace

import (
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

// AllEntries returns all entries (key + value) stored in the trie `t` sorted
// by their keys in the supplied `order`.
func AllEntries[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []*trie.Entry[K0, D] {
	return allEntriesAtDepth(t, order, 0)
}

func allEntriesAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1, depth int) []*trie.Entry[K0, D] {
	if t == nil || t.IsEmptyLeaf() {
		return nil
	}
	if t.IsNonEmptyLeaf() {
		return []*trie.Entry[K0, D]{{Key: *t.Key(), Data: t.Data()}}
	}
	b := int(order.Bit(depth))
	return append(allEntriesAtDepth(t.Branch(b), order, depth+1),
		allEntriesAtDepth(t.Branch(1-b), order, depth+1)...)
}

// AllValues returns all values stored in the trie `t` sorted by their keys in
// the supplied `order`.
func AllValues[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []D {
	entries := AllEntries(t, order)
	out := make([]D, len(entries))
	for i, entry := range entries {
		out[i] = entry.Data
	}
	return out
}

// FindPrefixOfKey checks whether the trie contains a leave whose key is a
// prefix or exact match of `k`.
//
// If there is a match, the function returns the matching key and true.
// Otherwise it returns the zero key and false.
func FindPrefixOfKey[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (K0, bool) {
	return findPrefixOfKeyAtDepth(t, k, 0)
}

func findPrefixOfKeyAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) (K0, bool) {
	var zero K0
	if t.IsLeaf() {
		if !t.HasKey() {
			return zero, false
		}
		return *t.Key(), key.CommonPrefixLength(*t.Key(), k) == (*t.Key()).BitLen()
	}
	if depth == k.BitLen() {
		return zero, false
	}
	b := int(k.Bit(depth))
	return findPrefixOfKeyAtDepth(t.Branch(b), k, depth+1)
}

// FindSubtrie returns the potential subtrie of `t` that matches the prefix
// `k`, and true if there was a match and false otherwise.
func FindSubtrie[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) (*trie.Trie[K0, D], bool) {
	if t.IsEmptyLeaf() {
		return t, false
	}
	branch := t
	for i := range k.BitLen() {
		if branch.IsEmptyLeaf() {
			return t, false
		}
		if branch.IsNonEmptyLeaf() {
			return branch, key.CommonPrefixLength(*branch.Key(), k) == k.BitLen()
		}
		branch = branch.Branch(int(k.Bit(i)))
	}
	return branch, !branch.IsEmptyLeaf()
}

// NextNonEmptyLeaf returns the leaf following the provided key `k` in the trie
// according to the provided `order`.
func NextNonEmptyLeaf[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1) *trie.Entry[K0, D] {
	return nextNonEmptyLeafAtDepth(t, k, order, 0, false)
}

// nextNonEmptyLeafAtDepth is a recursive function that finds the non empty
// leaf right after the supplied key `k`.
//
// We first need to go down the trie until we find the supplied `k` (if it
// exists). Once we have found the key, or its closest neighbor (if it doesn't
// exist) we hit the bottom of the trie. Then we go back up in the trie until
// we are able to go deeper again following `order` until we fall on a
// non-empty leaf. This is the leaf we are looking for.
func nextNonEmptyLeafAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1, depth int, hitBottom bool) *trie.Entry[K0, D] {
	if hitBottom {
		if t.IsNonEmptyLeaf() {
			// Found the next non-empty leaf.
			return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
		}
		if t.IsEmptyLeaf() {
			return nil
		}
		// Going down the trie, looking for next non-empty leaf according to order.
		orderBit := int(order.Bit(depth))
		nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(orderBit), k, order, depth+1, true)
		if nextLeaf != nil {
			return nextLeaf
		}
		return nextNonEmptyLeafAtDepth(t.Branch(1-orderBit), k, order, depth+1, true)
	}

	if t.IsLeaf() {
		// We have reached the bottom of the trie at k or its closest leaf
		if t.HasKey() {
			if depth == 0 {
				// Depth is 0, meaning there is a single key in the trie.
				return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
			cpl := k.CommonPrefixLength(*t.Key())
			if cpl < k.BitLen() && cpl < order.BitLen() && order.Bit(cpl) == k.Bit(cpl) {
				// k is closer to order than t.Key, so t.Key AFTER k, return it
				return &trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
		}
		return nil
	}
	kBit := int(k.Bit(depth))
	// Recursive call until we hit the bottom of the trie.
	nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, false)
	if nextLeaf != nil {
		// Branch has found the next leaf, return it.
		return nextLeaf
	}
	orderBit := int(order.Bit(depth))
	if kBit == orderBit || depth == 0 {
		// Neighbor branch is up next, according to order.
		nextLeaf = nextNonEmptyLeafAtDepth(t.Branch(1-kBit), k, order, depth+1, true)
		if nextLeaf != nil {
			return nextLeaf
		}
		if depth == 0 {
			// We have reached the end of the trie, start again from the first leaf.
			return nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, true)
		}
	}
	// Next leaf not found, signal it to parent by returning an empty entry.
	return nil
}

// PruneSubtrie removes the subtrie at the given key `k` from the trie `t` if
// it exists.
//
// All keys starting with the prefix `k` are purged from the trie.
func PruneSubtrie[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) {
	pruneSubtrieAtDepth(t, k, 0)
}

func pruneSubtrieAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) bool {
	if t.IsLeaf() {
		if t.HasKey() && IsPrefix(k, *t.Key()) {
			*t = trie.Trie[K0, D]{}
			return true
		}
		return false
	}

	// Not a leaf, continue pruning branches.
	if depth == k.BitLen() {
		*t = trie.Trie[K0, D]{}
		return true
	}

	pruned := pruneSubtrieAtDepth(t.Branch(int(k.Bit(depth))), k, depth+1)
	if pruned && t.Branch(1-int(k.Bit(depth))).IsEmptyLeaf() {
		*t = trie.Trie[K0, D]{}
		return true
	}
	return false
}

// TrieGaps returns all prefixes that aren't covered by a key (prefix) in the
// trie, at the `target` location. Combining the prefixes included in the trie
// with the gap prefixes results in a full keyspace coverage of the `target`
// subtrie.
//
// Results are sorted according to the provided `order`.
//
// Example:
//   - Trie: ["0000", "0010", "100"]
//   - Target: "0"
//   - Order: "0000",
//   - GapsInTrie: ["0001", "0011", "01"]
func TrieGaps[K kad.Key[K], D any](t *trie.Trie[bitstr.Key, D], target bitstr.Key, order K) []bitstr.Key {
	if t.IsLeaf() {
		if k := t.Key(); k != nil {
			if IsBitstrPrefix(target, *k) {
				siblingPrefixes := SiblingPrefixes(*k)[len(target):]
				sortBitstrKeysByOrder(siblingPrefixes, order)
				return siblingPrefixes
			}
			if IsBitstrPrefix(*k, target) {
				// The only key in the trie is a prefix of target, meaning the whole
				// target is covered.
				return nil
			}
		}
		return []bitstr.Key{target}
	}
	return trieGapsAtDepth(t, 0, target, order)
}

func trieGapsAtDepth[K kad.Key[K], D any](t *trie.Trie[bitstr.Key, D], depth int, target bitstr.Key, order K) []bitstr.Key {
	var gaps []bitstr.Key
	insideTarget := depth >= target.BitLen()
	b := int(order.Bit(depth))
	for _, i := range []int{b, 1 - b} {
		if !insideTarget && i != int(target.Bit(depth)) {
			continue
		}
		bstr := bitstr.Key(byte('0' + i))
		if b := t.Branch(i); b == nil {
			gaps = append(gaps, bstr)
		} else if b.IsLeaf() {
			if b.HasKey() {
				k := *b.Key()
				if len(k) > depth+1 {
					siblingPrefixes := SiblingPrefixes(k)[depth+1:]
					sortBitstrKeysByOrder(siblingPrefixes, order)
					for _, siblingPrefix := range siblingPrefixes {
						gaps = append(gaps, siblingPrefix[depth:])
					}
				}
			} else {
				gaps = append(gaps, bstr)
			}
		} else {
			for _, gap := range trieGapsAtDepth(b, depth+1, target, order) {
				gaps = append(gaps, bstr+gap)
			}
		}
	}
	return gaps
}

// sortBitstrKeysByOrder sorts the provided bitstr keys according to the
// provided order.
func sortBitstrKeysByOrder[K kad.Key[K]](keys []bitstr.Key, order K) {
	slices.SortFunc(keys, func(a, b bitstr.Key) int {
		maxLen := min(len(a), len(b), order.BitLen())
		for i := range maxLen {
			if a[i] != b[i] {
				if a.Bit(i) == order.Bit(i) {
					return -1
				}
				return 1
			}
		}
		if len(a) == len(b) || maxLen == order.BitLen() {
			return 0
		}
		if len(a) < len(b) {
			return 1
		}
		return -1
	})
}

// mapMerge merges all key-value pairs from the source map into the destination
// map. Values from the source are appended to existing slices in the
// destination.
func mapMerge[K comparable, V any](dst, src map[K][]V) {
	for k, vs := range src {
		dst[k] = append(dst[k], vs...)
	}
}

// AllocateToKClosest distributes items from the items trie to the k closest
// destinations in the dests trie based on XOR distance between their keys.
//
// The algorithm uses the trie structure to efficiently compute proximity
// without explicit distance calculations. Items are allocated to destinations
// by traversing both tries simultaneously and selecting the k destinations
// with the smallest XOR distance to each item's key.
//
// Returns a map where each destination value is associated with all items
// allocated to it. If k is 0 or either trie is empty, returns an empty map.
func AllocateToKClosest[K kad.Key[K], V0 any, V1 comparable](items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k int) map[V1][]V0 {
	return allocateToKClosestAtDepth(items, dests, k, 0)
}

// allocateToKClosestAtDepth performs the recursive allocation algorithm at a specific
// trie depth. At each depth, it processes both branches (0 and 1) of the trie,
// determining which destinations are closest to the items based on matching bit
// patterns at the current depth.
//
// The algorithm prioritizes destinations in the same branch as items (smaller XOR
// distance) and recursively processes deeper levels when more granular distance
// calculations are needed to select exactly k destinations.
//
// Parameters:
//   - items: trie containing items to be allocated
//   - dests: trie containing destination candidates
//   - k: maximum number of destinations to allocate each item to
//   - depth: current bit depth in the trie traversal
//
// Returns a map of destination values to their allocated items.
func allocateToKClosestAtDepth[K kad.Key[K], V0 any, V1 comparable](items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k, depth int) map[V1][]V0 {
	m := make(map[V1][]V0)
	if k == 0 {
		return m
	}
	for i := range 2 {
		// Assign all items from branch i

		matchingItemsBranch := items.Branch(i)
		matchingItems := AllValues(matchingItemsBranch, bit256.ZeroKey())
		if len(matchingItems) == 0 {
			if !items.IsNonEmptyLeaf() || int((*items.Key()).Bit(depth)) != i {
				// items' current branch is empty, skip it
				continue
			}
			// items' current branch contains a single leaf
			matchingItems = []V0{items.Data()}
			matchingItemsBranch = items
		}

		matchingDestsBranch := dests.Branch(i)
		otherDestsBranch := dests.Branch(1 - i)
		matchingDests := AllValues(matchingDestsBranch, bit256.ZeroKey())
		otherDests := AllValues(otherDestsBranch, bit256.ZeroKey())
		if dests.IsLeaf() {
			// Single key (leaf) in dests
			if dests.IsNonEmptyLeaf() {
				if int((*dests.Key()).Bit(depth)) == i {
					// Leaf matches current branch
					matchingDests = []V1{dests.Data()}
					matchingDestsBranch = dests
				} else {
					// Leaf matches other branch
					otherDests = []V1{dests.Data()}
					otherDestsBranch = dests
				}
			} else {
				// Empty leaf, no dests to allocate items.
				return m
			}
		}

		if nMatchingDests := len(matchingDests); nMatchingDests <= k {
			// Allocate matching items to the matching dests branch
			for _, dest := range matchingDests {
				m[dest] = append(m[dest], matchingItems...)
			}
			if nMatchingDests == k || len(otherDests) == 0 {
				// Items were assigned to all k dests, or other branch is empty.
				continue
			}

			nMissingDests := k - nMatchingDests
			if len(otherDests) <= nMissingDests {
				// Other branch contains at most the missing number of dests to be
				// allocated to. Allocate matching items to the other dests branch.
				for _, dest := range otherDests {
					m[dest] = append(m[dest], matchingItems...)
				}
			} else {
				// Other branch contains more than the missing number of dests, go one
				// level deeper to assign matching items to the closest dests.
				allocs := allocateToKClosestAtDepth(matchingItemsBranch, otherDestsBranch, nMissingDests, depth+1)
				mapMerge(m, allocs)
			}
		} else {
			// Number of matching dests is larger than k, go one level deeper.
			allocs := allocateToKClosestAtDepth(matchingItemsBranch, matchingDestsBranch, k, depth+1)
			mapMerge(m, allocs)
		}
	}
	return m
}

// Region represents a subtrie of the complete DHT keyspace.
//
//   - Prefix is the identifier of the subtrie.
//   - Peers contains all the network peers matching this region.
//   - Keys contains all the keys provided by the local node matching this
//     region.
type Region struct {
	Prefix bitstr.Key
	Peers  *trie.Trie[bit256.Key, peer.ID]
	Keys   *trie.Trie[bit256.Key, mh.Multihash]
}

// RegionsFromPeers returns the keyspace regions of size at least `regionSize`
// from the given `peers` sorted according to `order` along with the Common
// Prefix shared by all peers.
func RegionsFromPeers(peers []peer.ID, regionSize int, order bit256.Key) ([]Region, bitstr.Key) {
	if len(peers) == 0 {
		return []Region{}, ""
	}
	peersTrie := trie.New[bit256.Key, peer.ID]()
	minCpl := KeyLen
	firstPeerKey := PeerIDToBit256(peers[0])
	for _, p := range peers {
		k := PeerIDToBit256(p)
		peersTrie.Add(k, p)
		minCpl = min(minCpl, firstPeerKey.CommonPrefixLength(k))
	}
	commonPrefix := bitstr.Key(key.BitString(firstPeerKey)[:minCpl])
	regions := extractMinimalRegions(peersTrie, commonPrefix, regionSize, order)
	return regions, commonPrefix
}

// extractMinimalRegions returns the list of all non-overlapping subtries of
// `t` having at least `size` elements, sorted according to `order`. Every
// element is included in exactly one region.
func extractMinimalRegions(t *trie.Trie[bit256.Key, peer.ID], path bitstr.Key, size int, order bit256.Key) []Region {
	if t.IsEmptyLeaf() {
		return nil
	}
	branch0, branch1 := t.Branch(0), t.Branch(1)
	if branch0 != nil && branch1 != nil && branch0.Size() >= size && branch1.Size() >= size {
		b := int(order.Bit(len(path)))
		return append(extractMinimalRegions(t.Branch(b), path+bitstr.Key(byte('0'+b)), size, order),
			extractMinimalRegions(t.Branch(1-b), path+bitstr.Key(byte('1'-b)), size, order)...)
	}
	return []Region{{Prefix: path, Peers: t}}
}

// AssignKeysToRegions assigns the provided keys to the regions based on their
// kademlia identifier key.
func AssignKeysToRegions(regions []Region, keys []mh.Multihash) []Region {
	for i := range regions {
		regions[i].Keys = trie.New[bit256.Key, mh.Multihash]()
	}
	for _, k := range keys {
		h := MhToBit256(k)
		for i, r := range regions {
			if IsPrefix(r.Prefix, h) {
				regions[i].Keys.Add(h, k)
				break
			}
		}
	}
	return regions
}
