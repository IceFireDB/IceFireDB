package timeseries

import (
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

var zeroKey = bit256.ZeroKey()

type entry struct {
	time time.Time
	val  int64
}

// CycleStats tracks statistics organized by keyspace prefixes with deadline-based cleanup.
// It maintains a trie structure where statistics are aggregated by prefix and
// automatically cleaned up after the retention period.
type CycleStats struct {
	trie *trie.Trie[bitstr.Key, entry]

	queue *trie.Trie[bitstr.Key, entry]

	maxDelay time.Duration
}

// NewCycleStats creates a new CycleStats with the specified maximum delay.
// The maxDelay is used to prevent duplicate queue entries within a short time window.
func NewCycleStats(maxDelay time.Duration) CycleStats {
	return CycleStats{
		trie:     trie.New[bitstr.Key, entry](),
		queue:    trie.New[bitstr.Key, entry](),
		maxDelay: maxDelay,
	}
}

// Cleanup removes entries that have exceeded the specified deadline duration.
func (s *CycleStats) Cleanup(deadline time.Duration) {
	now := time.Now()
	// Collect all entries first to avoid modifying trie while iterating
	for _, e := range keyspace.AllEntries(s.trie, zeroKey) {
		if e.Data.time.Add(deadline).Before(now) {
			s.trie.Remove(e.Key)
			if subtrie, ok := keyspace.FindSubtrie(s.queue, e.Key); ok {
				for qe := range keyspace.EntriesIter(subtrie, zeroKey) {
					s.trie.Add(qe.Key, qe.Data)
				}
			}
		}
	}
}

// Add records a value for the given prefix, handling prefix aggregation logic.
func (s *CycleStats) Add(prefix bitstr.Key, val int64) {
	e := entry{time: time.Now(), val: val}
	if _, ok := keyspace.FindSubtrie(s.trie, prefix); ok {
		// shorter prefix
		keyspace.PruneSubtrie(s.trie, prefix)
		s.trie.Add(prefix, e)
		return
	}
	// longer prefix, group with complements before replacing
	target, ok := keyspace.FindPrefixOfKey(s.trie, prefix)
	if !ok {
		// No keys in s.trie is a prefix of `prefix`
		s.trie.Add(prefix, e)
		return
	}

	if queuePrefix, ok := keyspace.FindPrefixOfKey(s.queue, prefix); ok {
		_, entry := trie.Find(s.queue, queuePrefix)
		if time.Since(entry.time) < s.maxDelay {
			// A recent entry is a superset of the current one, skip.
			return
		}
		// Remove old entry
		keyspace.PruneSubtrie(s.queue, queuePrefix)
	} else {
		// Remove (older) superstrings from queue
		keyspace.PruneSubtrie(s.queue, prefix)
	}
	// Add prefix to queue
	s.queue.Add(prefix, e)

	subtrie, ok := keyspace.FindSubtrie(s.queue, target)
	if !ok || !keyspace.KeyspaceCovered(subtrie) {
		// Subtrie not complete
		return
	}
	// Target keyspace is fully covered by queue entries. Replace target with
	// queue entries.
	keyspace.PruneSubtrie(s.trie, target)
	for e := range keyspace.EntriesIter(subtrie, zeroKey) {
		s.trie.Add(e.Key, e.Data)
	}
}

// Sum returns the sum of all values in the trie.
func (s *CycleStats) Sum() int64 {
	var sum int64
	for v := range keyspace.ValuesIter(s.trie, zeroKey) {
		sum += v.val
	}
	return sum
}

// Avg returns the average of all values in the trie.
func (s *CycleStats) Avg() float64 {
	if s.trie == nil || s.trie.IsEmptyLeaf() {
		return 0
	}
	var sum int64
	var count float64
	for v := range keyspace.ValuesIter(s.trie, zeroKey) {
		sum += v.val
		count++
	}
	return float64(sum) / count
}

// Count returns the number of entries in the trie.
func (s *CycleStats) Count() int {
	return s.trie.Size()
}

// FullyCovered returns true if the trie covers the complete keyspace.
func (s *CycleStats) FullyCovered() bool {
	return keyspace.KeyspaceCovered(s.trie)
}
