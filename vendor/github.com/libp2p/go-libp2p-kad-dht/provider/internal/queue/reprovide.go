package queue

import (
	"sync"

	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

// ReprovideQueue is a thread-safe queue storing non-overlapping, unique
// kademlia keyspace prefixes in the order they were enqueued.
type ReprovideQueue struct {
	mu    sync.Mutex
	queue prefixQueue
}

// NewReprovideQueue creates a new ReprovideQueue instance.
func NewReprovideQueue() *ReprovideQueue {
	return &ReprovideQueue{queue: prefixQueue{prefixes: trie.New[bitstr.Key, struct{}]()}}
}

// Enqueue adds the supplied prefix to the queue.
//
// If the prefix is already in the queue, this is a no-op.
//
// If the queue contains superstrings of the supplied prefix, insert the
// supplied prefix at the position of the first superstring in the queue, and
// remove all superstrings from the queue. The prefixes are consolidated around
// the shortest prefix.
func (q *ReprovideQueue) Enqueue(prefixes ...bitstr.Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue.Push(prefixes...)
}

// Dequeue removes and returns the first prefix from the queue.
func (q *ReprovideQueue) Dequeue() (bitstr.Key, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Pop()
}

// Remove removes a prefix or all its superstrings from the queue, if any.
func (q *ReprovideQueue) Remove(prefix bitstr.Key) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Remove(prefix)
}

// IsEmpty returns true if the queue is empty.
func (q *ReprovideQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Size() == 0
}

// Size returns the number of prefixes currently in the queue.
func (q *ReprovideQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Size()
}

// Clear removes all prefixes from the queue and returns the number of removed
// prefixes.
func (q *ReprovideQueue) Clear() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Clear()
}
