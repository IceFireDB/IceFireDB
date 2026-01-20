// Package queue provides queues for batching DHT provide operations by
// Kademlia prefix, with optional datastore persistence.
package queue

import (
	"slices"

	"github.com/gammazero/deque"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

var zeroKey = bit256.ZeroKey()

// prefixQueue is a non-thread safe queue storing non overlapping, unique
// prefixes of kademlia keys, in the order they were enqueued.
type prefixQueue struct {
	queue    deque.Deque[bitstr.Key]          // used to preserve the queue order
	prefixes *trie.Trie[bitstr.Key, struct{}] // used to track prefixes in the queue
}

// Push adds a prefix to the queue.
//
// If prefix is already in the queue, this is a no-op.
//
// If the queue contains superstrings of the supplied prefix, insert the
// supplied prefix at the position of the first superstring in the queue, and
// remove all superstrings from the queue. The prefixes are consolidated around
// the shortest prefix.
func (q *prefixQueue) Push(prefixes ...bitstr.Key) {
	for _, prefix := range prefixes {
		if firstRemovedIndex := q.removeSuperstrings(prefix); firstRemovedIndex >= 0 {
			// `prefix` has superstrings in the queue. Remove them all and insert
			// `prefix` in the queue at the location of the first removed superstring.
			q.queue.Insert(firstRemovedIndex, prefix)
			// Add `prefix` to prefixes trie.
			q.prefixes.Add(prefix, struct{}{})
		} else if _, ok := keyspace.FindPrefixOfKey(q.prefixes, prefix); !ok {
			// No prefixes nor superstrings of `prefix` found in the queue.
			q.queue.PushBack(prefix)
			q.prefixes.Add(prefix, struct{}{})
		}
	}
}

// Pop removes and returns the first prefix from the queue.
func (q *prefixQueue) Pop() (bitstr.Key, bool) {
	if q.queue.Len() == 0 {
		return bitstr.Key(""), false
	}
	// Dequeue the first prefix from the queue.
	prefix := q.queue.PopFront()
	// Remove the prefix from the prefixes trie.
	q.prefixes.Remove(prefix)

	return prefix, true
}

// Remove removes a prefix or all its superstrings from the queue, if any.
func (q *prefixQueue) Remove(prefix bitstr.Key) bool {
	return q.removeSuperstrings(prefix) >= 0
}

// Returns the number of prefixes in the queue.
func (q *prefixQueue) Size() int {
	return q.queue.Len()
}

// Clear removes all keys from the queue and returns the number of keys that
// were removed.
func (q *prefixQueue) Clear() int {
	size := q.Size()

	q.queue.Clear()
	*q.prefixes = trie.Trie[bitstr.Key, struct{}]{}

	return size
}

// removeSuperstrings finds all superstrings of `prefix` in the trie, removes
// them from the queue, and returns the index at which the first removal
// occurred, or -1 if none.
func (q *prefixQueue) removeSuperstrings(prefix bitstr.Key) int {
	subtrie, ok := keyspace.FindSubtrie(q.prefixes, prefix)
	if !ok {
		return -1
	}
	return q.removePrefixesFromQueue(keyspace.AllKeys(subtrie, zeroKey))
}

// removeSubtrieFromQueue removes all keys in the provided subtrie from q.queue
// and q.prefixes. Returns the position of the first removed key in the queue.
func (q *prefixQueue) removePrefixesFromQueue(prefixes []bitstr.Key) int {
	indexes := make([]int, 0, len(prefixes))
	for _, prefix := range prefixes {
		// Remove elements from the queue that are superstrings of `prefix`.
		q.prefixes.Remove(prefix)
		// Find indexes of the superstrings in the queue.
		index := q.queue.Index(func(element bitstr.Key) bool { return element == prefix })
		if index >= 0 {
			indexes = append(indexes, index)
		}
	}
	// Sort indexes to remove in descending order so that we can remove them
	// without affecting the indexes of the remaining elements.
	slices.Sort(indexes)
	slices.Reverse(indexes)
	// Remove items in the queue that are prefixes of `prefix`
	for _, index := range indexes {
		q.queue.Remove(index)
	}
	return indexes[len(indexes)-1] // return the position of the first removed key
}
