// Package trie provides an implementation of a XOR Trie
package trie

import (
	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
)

// Trie is a trie for equal-length bit vectors, which stores values only in the leaves.
// A node may optionally hold data of type D
// Trie node invariants:
// (1) Either both branches are nil, or both are non-nil.
// (2) If branches are non-nil, key must be nil.
// (3) If both branches are leaves, then they are both non-empty (have keys).
type Trie[K kad.Key[K], D any] struct {
	branch [2]*Trie[K, D]
	key    *K
	data   D
}

func New[K kad.Key[K], D any]() *Trie[K, D] {
	return &Trie[K, D]{}
}

func (tr *Trie[K, D]) Key() *K {
	return tr.key
}

func (tr *Trie[K, D]) Data() D {
	return tr.data
}

func (tr *Trie[K, D]) Branch(dir int) *Trie[K, D] {
	return tr.branch[dir]
}

// Size returns the number of keys added to the trie.
func (tr *Trie[K, D]) Size() int {
	if tr.IsLeaf() {
		if !tr.HasKey() {
			return 0
		} else {
			return 1
		}
	} else {
		return tr.branch[0].Size() + tr.branch[1].Size()
	}
}

// HasKey reports whether the Trie node holds a key.
func (tr *Trie[K, D]) HasKey() bool {
	return tr.key != nil
}

// IsLeaf reports whether the Trie is a leaf node. A leaf node has no child branches but may hold a key and data.
func (tr *Trie[K, D]) IsLeaf() bool {
	return tr.branch[0] == nil && tr.branch[1] == nil
}

// IsEmptyLeaf reports whether the Trie is a leaf node without branches that also has no key.
func (tr *Trie[K, D]) IsEmptyLeaf() bool {
	return !tr.HasKey() && tr.IsLeaf()
}

// IsNonEmptyLeaf reports whether the Trie is a leaf node without branches but has a key.
func (tr *Trie[K, D]) IsNonEmptyLeaf() bool {
	return tr.HasKey() && tr.IsLeaf()
}

func (tr *Trie[K, D]) Copy() *Trie[K, D] {
	if tr.IsLeaf() {
		return &Trie[K, D]{key: tr.key, data: tr.data}
	}

	return &Trie[K, D]{branch: [2]*Trie[K, D]{
		tr.branch[0].Copy(),
		tr.branch[1].Copy(),
	}}
}

func (tr *Trie[K, D]) shrink() {
	b0, b1 := tr.branch[0], tr.branch[1]
	switch {
	case b0.IsEmptyLeaf() && b1.IsEmptyLeaf():
		tr.branch[0], tr.branch[1] = nil, nil
	case b0.IsEmptyLeaf() && b1.IsNonEmptyLeaf():
		tr.key = b1.key
		tr.data = b1.data
		tr.branch[0], tr.branch[1] = nil, nil
	case b0.IsNonEmptyLeaf() && b1.IsEmptyLeaf():
		tr.key = b0.key
		tr.data = b0.data
		tr.branch[0], tr.branch[1] = nil, nil
	}
}

// Add attempts to add a key to the trie, mutating the trie.
// Returns true if the key was added, false otherwise.
func (tr *Trie[K, D]) Add(kk K, data D) bool {
	return tr.addManyAtDepth(0, Entry[K, D]{Key: kk, Data: data}) == 1
}

// AddMany attempts to add multiple entries to the trie, mutating the trie.
// Returns the number of entries that were successfully added.
// Duplicate keys within entries or keys already in the trie are ignored.
func (tr *Trie[K, D]) AddMany(entries ...Entry[K, D]) int {
	return tr.addManyAtDepth(0, entries...)
}

func (tr *Trie[K, D]) addManyAtDepth(depth int, entries ...Entry[K, D]) int {
	if len(entries) == 0 {
		return 0
	}

	// Partition entries by direction
	sortedEntries := [2][]Entry[K, D]{}
	for i := range entries {
		if entries[i].Key.BitLen() <= depth {
			// Ignore keys that are too short, it means the node already exists in
			// trie, but isn't a leaf.
			continue
		}
		b := entries[i].Key.Bit(depth)
		sortedEntries[b] = append(sortedEntries[b], entries[i])
	}

	if tr.IsLeaf() {
		if tr.HasKey() {
			// Check if any entry matches existing key
			if len(entries) == 1 && key.Equal(*tr.key, entries[0].Key) {
				// Key already exists
				return 0
			}

			b := int((*tr.key).Bit(depth))
			// Split this leaf into branches
			p := tr.key
			d := tr.data
			tr.key = nil
			var v D
			tr.data = v
			tr.branch[0], tr.branch[1] = &Trie[K, D]{}, &Trie[K, D]{}
			tr.branch[b].key = p
			tr.branch[b].data = d
		} else {
			if len(entries) == 1 {
				tr.key = &entries[0].Key
				tr.data = entries[0].Data
				return 1
			}
			// Create branches to distribute entries
			tr.branch[0], tr.branch[1] = &Trie[K, D]{}, &Trie[K, D]{}
		}
	}
	added := 0
	for i, branchEntries := range sortedEntries {
		for range len(branchEntries) - 1 {
			// Lazily removes duplicates
			if !key.Equal(branchEntries[0].Key, branchEntries[len(branchEntries)-1].Key) {
				break
			}
			branchEntries = branchEntries[:len(branchEntries)-1]
		}
		if len(branchEntries) > 0 {
			added += tr.branch[i].addManyAtDepth(depth+1, branchEntries...)
		}
	}
	return added
}

// Add adds the key to trie, returning a new trie if the key was not already in the trie.
// Add is immutable/non-destructive: the original trie remains unchanged.
func Add[K kad.Key[K], D any](tr *Trie[K, D], kk K, data D) (*Trie[K, D], error) {
	return addAtDepth(0, tr, kk, data), nil
}

func addAtDepth[K kad.Key[K], D any](depth int, tr *Trie[K, D], kk K, data D) *Trie[K, D] {
	switch {
	case tr.IsEmptyLeaf():
		return &Trie[K, D]{key: &kk, data: data}
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if eq {
			return tr
		}
		return trieForTwo(depth, *tr.key, tr.data, kk, data)

	default:
		dir := kk.Bit(depth)
		b := addAtDepth(depth+1, tr.branch[dir], kk, data)
		if b == tr.branch[dir] {
			return tr
		}
		s := &Trie[K, D]{}
		s.branch[dir] = b
		s.branch[1-dir] = tr.branch[1-dir]
		return s
	}
}

func trieForTwo[K kad.Key[K], D any](depth int, p K, pdata D, q K, qdata D) *Trie[K, D] {
	pDir, qDir := p.Bit(depth), q.Bit(depth)
	if qDir == pDir {
		s := &Trie[K, D]{}
		s.branch[pDir] = trieForTwo(depth+1, p, pdata, q, qdata)
		s.branch[1-pDir] = &Trie[K, D]{}
		return s
	} else {
		s := &Trie[K, D]{}
		s.branch[pDir] = &Trie[K, D]{key: &p, data: pdata}
		s.branch[qDir] = &Trie[K, D]{key: &q, data: qdata}
		return s
	}
}

// Remove attempts to remove a key from the trie, mutating the trie.
// Returns true if the key was removed, false otherwise.
func (tr *Trie[K, D]) Remove(kk K) bool {
	return tr.removeAtDepth(0, kk)
}

func (tr *Trie[K, D]) removeAtDepth(depth int, kk K) bool {
	switch {
	case tr.IsEmptyLeaf():
		return false
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if !eq {
			return false
		}
		tr.key = nil
		var v D
		tr.data = v
		return true
	default:
		if tr.branch[kk.Bit(depth)].removeAtDepth(depth+1, kk) {
			tr.shrink()
			return true
		}
		return false
	}
}

// Remove removes the key from the trie.
// Remove is immutable/non-destructive: the original trie remains unchanged.
// If the key did not exist in the trie then the original trie is returned.
func Remove[K kad.Key[K], D any](tr *Trie[K, D], kk K) (*Trie[K, D], error) {
	return removeAtDepth(0, tr, kk), nil
}

func removeAtDepth[K kad.Key[K], D any](depth int, tr *Trie[K, D], kk K) *Trie[K, D] {
	switch {
	case tr.IsEmptyLeaf():
		return tr
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, kk)
		if !eq {
			return tr
		}
		return &Trie[K, D]{}

	default:
		dir := kk.Bit(depth)
		afterDelete := removeAtDepth(depth+1, tr.branch[dir], kk)
		if afterDelete == tr.branch[dir] {
			return tr
		}
		copy := &Trie[K, D]{}
		copy.branch[dir] = afterDelete
		copy.branch[1-dir] = tr.branch[1-dir]
		copy.shrink()
		return copy
	}
}

func Equal[K kad.Key[K], D any](a, b *Trie[K, D]) bool {
	switch {
	case a.IsEmptyLeaf() && b.IsEmptyLeaf():
		return true
	case a.IsNonEmptyLeaf() && b.IsNonEmptyLeaf():
		eq := key.Equal(*a.key, *b.key)
		if !eq {
			return false
		}
		return true
	case !a.IsLeaf() && !b.IsLeaf():
		return Equal(a.branch[0], b.branch[0]) && Equal(a.branch[1], b.branch[1])
	}
	return false
}

// Find looks for a key in the trie.
// It reports whether the key was found along with data value held with the key.
func Find[K kad.Key[K], D any](tr *Trie[K, D], kk K) (bool, D) {
	f, _ := findFromDepth(tr, 0, kk)
	if f == nil {
		var v D
		return false, v
	}
	return true, f.data
}

// Locate looks for the position of a key in the trie.
// It reports whether the key was found along with the depth of the leaf reached along the path
// of the key, regardless of whether the key was found in that leaf.
func Locate[K kad.Key[K], D any](tr *Trie[K, D], target K) (bool, int) {
	f, depth := findFromDepth(tr, 0, target)
	if f == nil {
		return false, depth
	}
	return true, depth
}

func findFromDepth[K kad.Key[K], D any](tr *Trie[K, D], depth int, target K) (*Trie[K, D], int) {
	switch {
	case tr.IsEmptyLeaf():
		return nil, depth
	case tr.IsNonEmptyLeaf():
		eq := key.Equal(*tr.key, target)
		if !eq {
			return nil, depth
		}
		return tr, depth
	default:
		return findFromDepth(tr.branch[target.Bit(depth)], depth+1, target)
	}
}

func Closest[K kad.Key[K], D any](tr *Trie[K, D], target K, n int) []Entry[K, D] {
	closestEntries := closestAtDepth(tr, target, n, 0)
	if len(closestEntries) == 0 {
		return []Entry[K, D]{}
	}
	return closestEntries
}

type Entry[K kad.Key[K], D any] struct {
	Key  K
	Data D
}

func closestAtDepth[K kad.Key[K], D any](t *Trie[K, D], target K, n int, depth int) []Entry[K, D] {
	if t.IsLeaf() {
		if t.HasKey() {
			// We've found a leaf
			return []Entry[K, D]{
				{Key: *t.Key(), Data: t.Data()},
			}
		}
		// We've found an empty node?
		return nil
	}

	if depth > target.BitLen() {
		// should not be possible
		return nil
	}

	// Find the closest direction.
	dir := int(target.Bit(depth))
	// Add peers from the closest direction first
	found := closestAtDepth(t.Branch(dir), target, n, depth+1)
	if len(found) == n {
		return found
	}
	// Didn't find enough peers in the closest direction, try the other direction.
	return append(found, closestAtDepth(t.Branch(1-dir), target, n-len(found), depth+1)...)
}
