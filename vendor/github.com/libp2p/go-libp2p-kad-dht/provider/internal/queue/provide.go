package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key/bit256"
	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/trie"

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
	keys  *trie.Trie[bit256.Key, mh.Multihash] // stores keys currently in the queue
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
	q.enqueueNoLock(prefix, keys)
}

// enqueueNoLock adds the supplied keys to the queue under the given prefix
// without acquiring the mutex. The caller must hold q.mu.
func (q *ProvideQueue) enqueueNoLock(prefix bitstr.Key, keys []mh.Multihash) {
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
	keys := keyspace.AllValues(subtrie, zeroKey)

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
	keys := keyspace.AllValues(subtrie, zeroKey)

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
	return q.keys.IsEmptyLeaf()
}

// Size returns the number of keys currently in the queue.
func (q *ProvideQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.keys.Size()
}

// NumRegions returns the number of regions containing at least one key
// currently in the queue.
func (q *ProvideQueue) NumRegions() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Size()
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

// Persist saves the current state of the queue to the provided datastore.
//
// The queue state includes the ordered list of prefixes and all associated
// multihashes. The data is stored under keys formatted as:
// * {queue-position:012x}/{prefix-bitstring}
//
// This operation does not modify the queue's in-memory state.
func (q *ProvideQueue) Persist(ctx context.Context, d ds.Batching, batchSize int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	commitBatchIfFull := func(b *ds.Batch, size int) error {
		if size%batchSize == 0 {
			// Max batch size reached, commit and start a new batch.
			if err := (*b).Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit batch: %w", err)
			}
			newBatch, err := d.Batch(ctx)
			if err != nil {
				return fmt.Errorf("failed to create batch: %w", err)
			}
			*b = newBatch
		}
		return nil
	}
	finalCommit := func(b ds.Batch, size int) error {
		if size%batchSize != 0 {
			if err := b.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit batch: %w", err)
			}
		}
		return nil
	}

	// Remove all existing persisted entries first.
	batch, err := d.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	res, err := d.Query(ctx, query.Query{KeysOnly: true})
	if err != nil {
		return fmt.Errorf("failed to query datastore: %w", err)
	}
	defer res.Close()
	i := 0
	for r := range res.Next() {
		if r.Error != nil {
			return fmt.Errorf("error reading query result: %w", r.Error)
		}
		batch.Delete(ctx, ds.NewKey(r.Key))
		i++
		if err := commitBatchIfFull(&batch, i); err != nil {
			return err
		}
	}
	if err := finalCommit(batch, i); err != nil {
		return err
	}

	// Batch writes
	batch, err = d.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}

	i = 0
	for prefix := range q.queue.queue.Iter() {
		// Find all keys matching this prefix
		if subtrie, ok := keyspace.FindSubtrie(q.keys, prefix); ok {
			// Concatenate all multihash bytes
			var buf []byte
			for h := range keyspace.ValuesIter(subtrie, zeroKey) {
				buf = append(buf, []byte(h)...)
			}

			// Store with queue position and prefix as key
			key := ds.NewKey(fmt.Sprintf("%012x/%s", i, string(prefix)))
			if err := batch.Put(ctx, key, buf); err != nil {
				return fmt.Errorf("failed to store prefix data: %w", err)
			}
		}
		i++
		if err := commitBatchIfFull(&batch, i); err != nil {
			return err
		}
	}

	if err := finalCommit(batch, i); err != nil {
		return err
	}
	return nil
}

// decodeMultihashes parses concatenated multihash bytes and returns individual multihashes.
func decodeMultihashes(data []byte) ([]mh.Multihash, error) {
	var multihashes []mh.Multihash
	offset := 0

	for offset < len(data) {
		// Parse the multihash at current offset
		// MHFromBytes returns (consumed, multihash, error)
		consumed, mhash, err := mh.MHFromBytes(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to decode multihash at offset %d: %w", offset, err)
		}

		multihashes = append(multihashes, mhash)
		offset += consumed
	}

	return multihashes, nil
}

// DrainDatastore loads persisted queue entries from the datastore, adds them to
// the current queue, and removes them from the datastore.
//
// This operation does NOT clear the existing queue state - it is additive.
func (q *ProvideQueue) DrainDatastore(ctx context.Context, d ds.Batching) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Query all persisted entries, ordered by key (which preserves queue order)
	dsQuery := query.Query{
		Orders: []query.Order{query.OrderByKey{}},
	}

	results, err := d.Query(ctx, dsQuery)
	if err != nil {
		return fmt.Errorf("failed to query datastore: %w", err)
	}
	defer results.Close()

	// Create a batch for deletes
	batch, err := d.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}

	for result := range results.Next() {
		if result.Error != nil {
			return fmt.Errorf("error reading query result: %w", result.Error)
		}

		// Key format: "/position/prefix"
		parts := strings.Split(strings.TrimPrefix(result.Key, "/"), "/")
		if len(parts) != 2 {
			continue // Skip invalid keys
		}
		prefix := bitstr.Key(parts[1])

		// Decode concatenated multihashes
		keys, err := decodeMultihashes(result.Value)
		if err != nil {
			return fmt.Errorf("failed to decode multihashes for prefix %s: %w", prefix, err)
		}
		if len(keys) == 0 {
			continue
		}

		// Add keys to provide queue
		q.enqueueNoLock(prefix, keys)

		// Delete key from datastore
		batch.Delete(ctx, ds.NewKey(result.Key))
	}

	// Commit deletions
	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}
