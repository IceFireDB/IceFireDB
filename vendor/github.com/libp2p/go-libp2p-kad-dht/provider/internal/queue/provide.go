package queue

import (
	"sync"

	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
)

// ProvideQueue is a thread-safe queue storing multihashes about to be provided
// to a Kademlia DHT, allowing smart batching.
//
// The queue groups keys by their kademlia identifier prefixes, so that keys
// that should be allocated to the same DHT peers are dequeued together from
// the queue, for efficient batch providing.
//
// The insertion order of prefixes is preserved, but not for keys. Inserting
// keys matching a prefix that is already in the queue inserts the keys at the
// position of the existing prefix.
//
// ProvideQueue allows dequeuing the first prefix of the queue, with all
// matching keys or dequeuing all keys matching a requested prefix.
type ProvideQueue struct {
	mu sync.Mutex

	queue prefixQueue
	keys  *trie.Trie[bit256.Key, mh.Multihash] // used to store keys in the queue
}

// NewProvideQueue creates a new ProvideQueue instance.
func NewProvideQueue() *ProvideQueue {
	return &ProvideQueue{
		queue: prefixQueue{prefixes: trie.New[bitstr.Key, struct{}]()},
		keys:  trie.New[bit256.Key, mh.Multihash](),
	}
}

// Enqueue adds the supplied keys to the queue under the given prefix.
//
// If the prefix already sits in the queue, supplied keys join the queue at the
// position of the existing prefix. If the queue contains prefixes that are
// superstrings of the supplied prefix, all keys matching the supplied prefix
// are consolidated at the position of the first matching superstring in the
// queue.
//
// If supplied prefix doesn't exist yet in the queue, add it at the end.
//
// Supplied keys MUST match the supplied prefix.
func (q *ProvideQueue) Enqueue(prefix bitstr.Key, keys ...mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	// Enqueue the prefix in the queue if required.
	q.queue.Push(prefix)

	// Add keys to the keys trie.
	entries := make([]trie.Entry[bit256.Key, mh.Multihash], len(keys))
	for i, h := range keys {
		entries[i] = trie.Entry[bit256.Key, mh.Multihash]{Key: keyspace.MhToBit256(h), Data: h}
	}
	q.keys.AddMany(entries...)
}

// Dequeue pops the first prefix of the queue along with all matching keys.
//
// The prefix and keys are removed from the queue. If the queue is empty,
// return false and the empty prefix.
func (q *ProvideQueue) Dequeue() (bitstr.Key, []mh.Multihash, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	prefix, ok := q.queue.Pop()
	if !ok {
		return prefix, nil, false
	}

	// Get all keys that match the prefix.
	subtrie, _ := keyspace.FindSubtrie(q.keys, prefix)
	keys := keyspace.AllValues(subtrie, bit256.ZeroKey())

	// Remove the keys from the keys trie.
	keyspace.PruneSubtrie(q.keys, prefix)

	return prefix, keys, true
}

// DequeueMatching returns keys matching the given prefix from the queue.
//
// The keys and prefix are removed from the queue. If the queue is empty, or
// supplied prefix doesn't match any keys, an empty slice is returned.
func (q *ProvideQueue) DequeueMatching(prefix bitstr.Key) []mh.Multihash {
	q.mu.Lock()
	defer q.mu.Unlock()

	subtrie, ok := keyspace.FindSubtrie(q.keys, prefix)
	if !ok {
		// No keys matching the prefix.
		return nil
	}
	keys := keyspace.AllValues(subtrie, bit256.ZeroKey())

	// Remove the keys from the keys trie.
	keyspace.PruneSubtrie(q.keys, prefix)

	// Remove prefix and its superstrings from queue if any.
	removed := q.queue.Remove(prefix)
	if !removed {
		// prefix and superstrings not in queue.
		if shorterPrefix, ok := keyspace.FindPrefixOfKey(q.queue.prefixes, prefix); ok {
			// prefix is a superstring of some other shorter prefix in the queue.
			// Leave it in the queue, unless the shorter prefix doesn't have any
			// matching keys left.
			if _, ok := keyspace.FindSubtrie(q.keys, shorterPrefix); !ok {
				// No keys matching shorterPrefix, remove shorterPrefix from queue.
				q.queue.Remove(shorterPrefix)
			}
		}
	}
	return keys
}

// Remove removes the supplied keys from the queue.
//
// If this operation removes the last keys for prefixes in the queue, remove
// the prefixes from the queue.
func (q *ProvideQueue) Remove(keys ...mh.Multihash) {
	q.mu.Lock()
	defer q.mu.Unlock()

	matchingPrefixes := make(map[bitstr.Key]struct{})

	// Remove keys from the keys trie.
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		q.keys.Remove(k)
		if prefix, ok := keyspace.FindPrefixOfKey(q.queue.prefixes, k); ok {
			// Get the trie leaf matching the key, if any.
			matchingPrefixes[prefix] = struct{}{}
		}
	}

	// For matching prefixes, if no more keys are matching, remove them from
	// queue.
	prefixesToRemove := make([]bitstr.Key, 0)
	for prefix := range matchingPrefixes {
		if _, ok := keyspace.FindSubtrie(q.keys, prefix); !ok {
			prefixesToRemove = append(prefixesToRemove, prefix)
		}
	}
	if len(prefixesToRemove) > 0 {
		q.queue.removePrefixesFromQueue(prefixesToRemove)
	}
}

// IsEmpty returns true if the queue is empty.
func (q *ProvideQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Size() == 0
}

// Size returns the number of regions containing at least one key in the queue.
func (q *ProvideQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.keys.Size()
}

// Clear removes all keys from the queue and returns the number of keys that
// were removed.
func (q *ProvideQueue) Clear() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	size := q.keys.Size()

	q.queue.Clear()
	*q.keys = trie.Trie[bit256.Key, mh.Multihash]{}

	return size
}
