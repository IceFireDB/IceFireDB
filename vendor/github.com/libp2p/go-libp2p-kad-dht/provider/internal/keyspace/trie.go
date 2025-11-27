package keyspace

import (
	"iter"
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

var zeroKey = bit256.ZeroKey()

// trieIter is a generic helper that iterates over the trie and yields values
// extracted by the provided extract function. This allows efficient iteration
// without constructing unnecessary intermediate structures.
func trieIter[K0 kad.Key[K0], K1 kad.Key[K1], D any, T any](
	t *trie.Trie[K0, D],
	order K1,
	extract func(*trie.Trie[K0, D]) T,
) iter.Seq[T] {
	return func(yield func(T) bool) {
		trieIterAtDepth(t, order, 0, extract, yield)
	}
}

func trieIterAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any, T any](
	t *trie.Trie[K0, D],
	order K1,
	depth int,
	extract func(*trie.Trie[K0, D]) T,
	yield func(T) bool,
) bool {
	if t == nil || t.IsEmptyLeaf() {
		return true
	}
	if t.IsNonEmptyLeaf() {
		return yield(extract(t))
	}
	b := int(order.Bit(depth))
	// First traverse the branch according to order
	if !trieIterAtDepth(t.Branch(b), order, depth+1, extract, yield) {
		return false
	}
	// Then traverse the other branch
	return trieIterAtDepth(t.Branch(1-b), order, depth+1, extract, yield)
}

// EntriesIter returns an iterator over all entries (key + value) stored in
// the trie `t` sorted by their keys in the supplied `order`.
// The iterator allows processing entries one at a time without loading all of
// them into memory.
func EntriesIter[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) iter.Seq[trie.Entry[K0, D]] {
	return trieIter(t, order, func(node *trie.Trie[K0, D]) trie.Entry[K0, D] {
		return trie.Entry[K0, D]{Key: *node.Key(), Data: node.Data()}
	})
}

// ValuesIter returns an iterator over all values stored in the trie `t`
// sorted by their keys in the supplied `order`.
// The iterator allows processing values one at a time without loading all of
// them into memory.
func ValuesIter[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) iter.Seq[D] {
	return trieIter(t, order, func(node *trie.Trie[K0, D]) D {
		return node.Data()
	})
}

// KeysIter returns an iterator over all keys stored in the trie `t`
// sorted by their keys in the supplied `order`.
// The iterator allows processing keys one at a time without loading all of
// them into memory.
func KeysIter[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) iter.Seq[K0] {
	return trieIter(t, order, func(node *trie.Trie[K0, D]) K0 {
		return *node.Key()
	})
}

// AllEntries returns all entries (key + value) stored in the trie `t` sorted
// by their keys in the supplied `order`.
func AllEntries[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []trie.Entry[K0, D] {
	if t == nil || t.IsEmptyLeaf() {
		return []trie.Entry[K0, D]{}
	}
	result := make([]trie.Entry[K0, D], 0, t.Size())
	for entry := range EntriesIter(t, order) {
		result = append(result, entry)
	}
	return result
}

// AllValues returns all values stored in the trie `t` sorted by their keys in
// the supplied `order`.
func AllValues[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []D {
	if t == nil || t.IsEmptyLeaf() {
		return []D{}
	}
	result := make([]D, 0, t.Size())
	for value := range ValuesIter(t, order) {
		result = append(result, value)
	}
	return result
}

// AllKeys returns all keys stored in the trie `t` sorted by their keys in
// the supplied `order`.
func AllKeys[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []K0 {
	if t == nil || t.IsEmptyLeaf() {
		return []K0{}
	}
	result := make([]K0, 0, t.Size())
	for key := range KeysIter(t, order) {
		result = append(result, key)
	}
	return result
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

// CoalesceTrie merges sibling keys into their parent prefix recursively. When
// two keys in the trie differ only in their last bit (e.g., "1100" and "1101"),
// they are replaced by their common parent prefix (e.g., "110"). This process
// continues recursively until no more sibling pairs exist, reducing the trie to
// its minimal prefix coverage.
// Data associated with coalesced keys is discarded.
func CoalesceTrie[D any](t *trie.Trie[bitstr.Key, D]) {
	if t == nil || t.IsLeaf() {
		return
	}
	branches := [2]*trie.Trie[bitstr.Key, D]{t.Branch(0), t.Branch(1)}
	for _, b := range branches {
		if b != nil && !b.IsLeaf() {
			CoalesceTrie(b)
		}
	}
	if branches[0].IsNonEmptyLeaf() && branches[1].IsNonEmptyLeaf() {
		keys := [2]bitstr.Key{*branches[0].Key(), *branches[1].Key()}
		l := keys[0].BitLen() - 1
		if keys[0].BitLen() == keys[1].BitLen() && keys[0].CommonPrefixLength(keys[1]) == l {
			// Remove siblings from self (parent), and set common prefix as key.
			*t = trie.Trie[bitstr.Key, D]{}
			parentKey := keys[0][:l]
			var d D
			t.Add(parentKey, d)
		}
	}
}

// SubtractTrie returns a new trie containing all entries from trie `t0` that
// are not covered by any prefix in trie `t1`. This performs a set subtraction
// operation (t0 - t1) where keys in `t0` are excluded if they match or are
// prefixed by any key in `t1`.
//
// For example:
//   - t0: ["0000", "0010", "0100"]
//   - t1: ["00"]
//   - Result: ["0100"]
//
// The subtraction is prefix-aware: if `t1` contains "00", all keys in `t0`
// starting with "00" (like "0000" and "0010") are excluded from the result.
func SubtractTrie[D0, D1 any](t0 *trie.Trie[bitstr.Key, D0], t1 *trie.Trie[bitstr.Key, D1]) *trie.Trie[bitstr.Key, D0] {
	res := trie.New[bitstr.Key, D0]()
	subtractTrieAtDepth(t0, t1, res, 0)
	return res
}

// subtractTrieAtDepth recursively performs trie subtraction (t0 - t1) at the
// specified depth, adding entries from `t0` to `res` that are not covered by
// prefixes in `t1`. The `depth` parameter tracks the current bit position being
// examined during the traversal.
func subtractTrieAtDepth[D0, D1 any](t0 *trie.Trie[bitstr.Key, D0],
	t1 *trie.Trie[bitstr.Key, D1], res *trie.Trie[bitstr.Key, D0], depth int,
) {
	if t0 == nil || t0.IsEmptyLeaf() {
		return
	}
	if t1 == nil || t1.IsEmptyLeaf() {
		// t1 is empty, nothing to subtract, add all t0 to result.
		res.AddMany(AllEntries(t0, zeroKey)...)
		return
	}

	if t0.HasKey() && t1.HasKey() {
		// Both t0 and t1 are leaves.
		k0, k1 := *t0.Key(), *t1.Key()
		if !IsBitstrPrefix(k1, k0) {
			res.Add(k0, t0.Data())
		}
		return
	}
	if t0.HasKey() && !t1.HasKey() {
		// t0 is a leaf, but t1 is not.
		k0 := t0.Key()
		if k0.BitLen() <= depth {
			res.Add(*k0, t0.Data())
			return
		}
		b := int(k0.Bit(depth))
		// Go deeper in t1's branch that could cover t0's key.
		subtractTrieAtDepth(t0, t1.Branch(b), res, depth+1)
		return
	}
	if t1.HasKey() && !t0.HasKey() {
		// t1 is a leaf, but t0 is not.
		k1 := t1.Key()
		if k1.BitLen() <= depth {
			// t1 covers all entries in t0, nothing to add.
			return
		}
		b := int(k1.Bit(depth))
		// Add all entries in t0's branch that is not covered by t1.
		res.AddMany(AllEntries(t0.Branch(1-b), zeroKey)...)
		// Go deeper in the branch covered by t1.
		subtractTrieAtDepth(t0.Branch(b), t1, res, depth+1)
		return
	}
	// Both t0 and t1 are not leaves.
	for i := range 2 {
		subtractTrieAtDepth(t0.Branch(i), t1.Branch(i), res, depth+1)
	}
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

// AllocateToKClosest distributes items from the items trie to the k closest
// destinations in batches based on XOR distance between their keys.
//
// The algorithm uses the trie structure to efficiently compute proximity without
// explicit distance calculations. Items are allocated to destinations by traversing
// both tries simultaneously and selecting the k destinations with the smallest XOR
// distance to each item's key.
//
// Memory optimization is achieved by sharing batches of items among
// destinations instead of duplicating them, reducing overhead when multiple
// destinations receive the same batches.
//
// Returns a map where each destination has a slice of batches. Each batch is a slice
// of items. To iterate: for each peer, for each batch, for each key.
func AllocateToKClosest[K kad.Key[K], V0 any, V1 comparable](items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k int) map[V1][][]V0 {
	if dests.IsEmptyLeaf() || items.IsEmptyLeaf() || k == 0 {
		return nil
	}
	// Expected number of batches per destination
	result := make(map[V1][][]V0, 0)
	allocateToKClosestAtDepth(items, dests, k, 0, result)
	return result
}

// allocateToKClosestAtDepth performs recursive allocation using batch sharing.
// Instead of copying individual items, it creates batches at recursion leaves and
// shares the batch slice across destinations.
//
// Parameters:
//   - items: trie containing items to be allocated
//   - dests: trie containing destination candidates
//   - k: maximum number of destinations to allocate each item to
//   - depth: current bit depth in the trie traversal
//   - result: output map to write batch allocations into (modified in-place)
func allocateToKClosestAtDepth[K kad.Key[K], V0 any, V1 comparable](
	items *trie.Trie[K, V0], dests *trie.Trie[K, V1], k, depth int, result map[V1][][]V0,
) {
	if k == 0 {
		return
	}
	if dests.IsLeaf() {
		if !dests.HasKey() {
			return
		}
		// Single destination: create batch and add to result
		dest := dests.Data()
		batch := AllValues(items, zeroKey)
		if len(batch) == 0 && items.IsNonEmptyLeaf() {
			batch = []V0{items.Data()}
		}
		result[dest] = append(result[dest], batch)
		return
	}

	destBranches := []*trie.Trie[K, V1]{dests.Branch(0), dests.Branch(1)}
	destBranchSize := [2]int{destBranches[0].Size(), destBranches[1].Size()}

	// Lazy evaluation of destination values
	var destValues [2][]V1
	getDestValues := func(i int) []V1 {
		if destValues[i] == nil && destBranches[i] != nil && !destBranches[i].IsEmptyLeaf() {
			destValues[i] = AllValues(destBranches[i], zeroKey)
		}
		return destValues[i]
	}

	// Process each branch
	for i := range 2 {
		sameBranch := destBranches[i]
		otherBranch := destBranches[1-i]
		sameCount := destBranchSize[i]
		otherCount := destBranchSize[1-i]

		matchingItemsBranch := items.Branch(i)
		if matchingItemsBranch == nil || matchingItemsBranch.IsEmptyLeaf() {
			if !items.IsNonEmptyLeaf() || int((*items.Key()).Bit(depth)) != i {
				continue
			}
			matchingItemsBranch = items
		}
		if sameCount <= k {
			// Create batch from matching items
			batch := AllValues(matchingItemsBranch, zeroKey)
			if len(batch) == 0 {
				batch = []V0{items.Data()}
			}
			// Share this batch across all same-branch destinations
			for _, dest := range getDestValues(i) {
				result[dest] = append(result[dest], batch)
			}

			if sameCount == k || otherCount == 0 {
				continue
			}

			nMissingDests := k - sameCount
			if otherCount <= nMissingDests {
				// Share batch with other branch destinations too
				for _, dest := range getDestValues(1 - i) {
					result[dest] = append(result[dest], batch)
				}
			} else {
				// Recurse into other branch
				allocateToKClosestAtDepth(matchingItemsBranch, otherBranch, nMissingDests, depth+1, result)
			}
		} else {
			// Recurse into same branch
			allocateToKClosestAtDepth(matchingItemsBranch, sameBranch, k, depth+1, result)
		}
	}
}

// KeyspaceCovered checks whether the trie covers the entire keyspace without
// gaps.
func KeyspaceCovered[D any](t *trie.Trie[bitstr.Key, D]) bool {
	if t.IsLeaf() {
		if t.HasKey() {
			return *t.Key() == ""
		}
		return false
	}

	stack := []bitstr.Key{"1", "0"}
outerLoop:
	for p := range KeysIter(t, zeroKey) {
		stackTop := stack[len(stack)-1]
		stackTopLen := len(stackTop)
		if len(p) < stackTopLen {
			return false
		}

		for len(p) == stackTopLen {
			if stackTopLen == 1 && stackTop == p {
				stack = stack[:len(stack)-1]
				continue outerLoop
			}
			// Match with stackTop, pop stack and continue
			p = p[:stackTopLen-1]
			stack = stack[:len(stack)-1]
			stackTop = stack[len(stack)-1]
			stackTopLen = len(stackTop)
		}

		stack = append(stack, FlipLastBit(p))
	}
	return len(stack) == 0
}

// Region represents a subtrie of the complete DHT keyspace.
//
//   - Prefix is the identifier of the subtrie.
//   - Peers contains all the network peers matching this region.
//   - Keys contains all the keys provided by the local node matching this region.
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
	peerEntries := make([]trie.Entry[bit256.Key, peer.ID], len(peers))
	for i, p := range peers {
		k := PeerIDToBit256(p)
		peerEntries[i] = trie.Entry[bit256.Key, peer.ID]{Key: k, Data: p}
		minCpl = min(minCpl, firstPeerKey.CommonPrefixLength(k))
	}
	peersTrie.AddMany(peerEntries...)
	commonPrefix := bitstr.Key(key.BitString(firstPeerKey)[:minCpl])

	// Navigate to the subtrie at the common prefix depth before extracting regions.
	// This ensures extractMinimalRegions checks branches at the correct depth.
	subtrie := peersTrie
	for i := range commonPrefix {
		if subtrie.IsLeaf() {
			break
		}
		subtrie = subtrie.Branch(int(commonPrefix.Bit(i)))
		if subtrie == nil || subtrie.IsEmptyLeaf() {
			return nil, commonPrefix
		}
	}

	regions := extractMinimalRegions(subtrie, commonPrefix, regionSize, order)
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
	keyEntriesByRegion := make(map[bitstr.Key][]trie.Entry[bit256.Key, mh.Multihash], len(regions))
	for _, k := range keys {
		h := MhToBit256(k)
		for _, r := range regions {
			if IsPrefix(r.Prefix, h) {
				keyEntriesByRegion[r.Prefix] = append(keyEntriesByRegion[r.Prefix], trie.Entry[bit256.Key, mh.Multihash]{Key: h, Data: k})
				break
			}
		}
	}
	for _, r := range regions {
		r.Keys.AddMany(keyEntriesByRegion[r.Prefix]...)
	}
	return regions
}
