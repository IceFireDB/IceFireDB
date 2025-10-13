package keystore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

var ErrClosed = errors.New("keystore is closed")

// Keystore provides thread-safe storage and retrieval of multihashes, indexed
// by their kademlia 256-bit identifier.
type Keystore interface {
	Put(context.Context, ...mh.Multihash) ([]mh.Multihash, error)
	Get(context.Context, bitstr.Key) ([]mh.Multihash, error)
	ContainsPrefix(context.Context, bitstr.Key) (bool, error)
	Delete(context.Context, ...mh.Multihash) error
	Empty(context.Context) error
	Size(context.Context) (int, error)
	Close() error
}

// operation types for the worker goroutine
type opType uint8

const (
	opPut opType = iota
	opGet
	opContainsPrefix
	opDelete
	opEmpty
	opSize
	lastOp
)

// operation request sent to worker goroutine
type operation struct {
	op       opType
	ctx      context.Context
	keys     []mh.Multihash
	prefix   bitstr.Key
	response chan<- operationResponse
}

// response from worker goroutine
type operationResponse struct {
	multihashes []mh.Multihash
	found       bool
	size        int
	err         error
}

// keystore indexes multihashes by their kademlia identifier.
type keystore struct {
	ds         ds.Batching
	prefixBits int
	batchSize  int

	// worker goroutine communication
	requests chan operation
	close    chan struct{}
	done     chan struct{}
}

// NewKeystore creates a new Keystore backed by the provided datastore.
func NewKeystore(d ds.Batching, opts ...Option) (Keystore, error) {
	cfg, err := getOpts(opts)
	if err != nil {
		return nil, err
	}
	ks := &keystore{
		ds:         namespace.Wrap(d, ds.NewKey(cfg.path)),
		prefixBits: cfg.prefixBits,
		batchSize:  cfg.batchSize,
		requests:   make(chan operation),
		close:      make(chan struct{}),
		done:       make(chan struct{}),
	}
	go ks.worker()

	return ks, nil
}

// dsKey returns the datastore key for the provided binary key.
//
// The function creates a hierarchical datastore key by expanding bits into
// path components (`0` or `1`) separated by `/`, and optionally a
// base64URL-encoded suffix.
//
// Full keys (256-bit):
// The first `prefixBits` bits become individual path components, and the
// remaining bytes (after prefixBits/8) are base64URL encoded as the final
// component. Example: "/0/0/0/0/1/1/1/1/AAAA...A=="
//
// Prefix keys (<256-bit):
// If the key is shorter than 256-bits, only the available bits (up to
// `prefixBits`) become path components. No base64URL suffix is added. This
// creates a prefix that can be used in datastore queries to find all matching
// full keys.
//
// If the prefix is longer than `prefixBits`, only the first `prefixBits` bits
// are used, allowing the returned key to serve as a query prefix for the
// datastore.
func dsKey[K kad.Key[K]](k K, prefixBits int) ds.Key {
	b := strings.Builder{}
	l := k.BitLen()
	for i := range min(prefixBits, l) {
		b.WriteRune(rune('0' + k.Bit(i)))
		b.WriteRune('/')
	}
	if l == keyspace.KeyLen {
		b.WriteString(base64.URLEncoding.EncodeToString(keyspace.KeyToBytes(k)[prefixBits/8:]))
	}
	return ds.NewKey(b.String())
}

// decodeKey reconstructs a 256-bit binary key from a hierarchical datastore key string.
//
// This function reverses the process of dsKey, converting a datastore key back into
// its original binary representation by parsing the individual bit components and
// base64URL-encoded suffix.
//
// The input datastore key format is expected to be:
// "/bit0/bit1/.../bitN/base64url_suffix"
//
// Returns the reconstructed 256-bit key or an error if base64URL decoding fails.
func (s *keystore) decodeKey(dsk string) (bit256.Key, error) {
	bs := make([]byte, 32)
	// Extract individual bits from odd positions (skip '/' separators)
	for i := range s.prefixBits {
		if dsk[2*i+1] == '1' {
			bs[i/8] |= byte(1) << (7 - i%8)
		}
	}
	// Decode base64URL suffix and append to remaining bytes
	decoded, err := base64.URLEncoding.DecodeString(dsk[2*(s.prefixBits)+1:])
	if err != nil {
		return bit256.Key{}, err
	}
	copy(bs[s.prefixBits/8:], decoded)
	return bit256.NewKey(bs), nil
}

// worker processes operations sequentially in a single goroutine
func (s *keystore) worker() {
	defer close(s.done)

	for {
		select {
		case <-s.close:
			return
		case op := <-s.requests:
			switch op.op {
			case opPut:
				newKeys, err := s.put(op.ctx, op.keys)
				op.response <- operationResponse{multihashes: newKeys, err: err}

			case opGet:
				keys, err := s.get(op.ctx, op.prefix)
				op.response <- operationResponse{multihashes: keys, err: err}

			case opContainsPrefix:
				found, err := s.containsPrefix(op.ctx, op.prefix)
				op.response <- operationResponse{found: found, err: err}

			case opDelete:
				err := s.delete(op.ctx, op.keys)
				op.response <- operationResponse{err: err}

			case opEmpty:
				err := empty(op.ctx, s.ds, s.batchSize)
				op.response <- operationResponse{err: err}

			case opSize:
				size, err := s.size(op.ctx)
				op.response <- operationResponse{size: size, err: err}

			default:
				op.response <- operationResponse{err: fmt.Errorf("unknown operation %d", op.op)}
			}
		}
	}
}

// put stores the provided keys while assuming s.lk is already held, and
// returns the keys that weren't present already in the keystore.
func (s *keystore) put(ctx context.Context, keys []mh.Multihash) ([]mh.Multihash, error) {
	seen := make(map[bit256.Key]struct{}, len(keys))
	b, err := s.ds.Batch(ctx)
	if err != nil {
		return nil, err
	}
	newKeys := make([]mh.Multihash, 0, len(keys))

	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		dsk := dsKey(k, s.prefixBits)
		ok, err := s.ds.Has(ctx, dsk)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := b.Put(ctx, dsk, h); err != nil {
				return nil, err
			}
			newKeys = append(newKeys, h)
		}
	}
	if err := b.Commit(ctx); err != nil {
		return nil, err
	}
	return newKeys, nil
}

// get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *keystore) get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	out := make([]mh.Multihash, 0)
	longPrefix := prefix.BitLen() > s.prefixBits

	dsk := dsKey(prefix, s.prefixBits).String()
	q := query.Query{Prefix: dsk}
	for r, err := range ds.QueryIter(ctx, s.ds, q) {
		if err != nil {
			return nil, err
		}
		// Depending on prefix length, filter out non matching keys
		if longPrefix {
			k, err := s.decodeKey(r.Key)
			if err != nil {
				return nil, err
			}
			if !keyspace.IsPrefix(prefix, k) {
				continue
			}
		}
		out = append(out, mh.Multihash(r.Value))
	}

	return out, nil
}

// containsPrefix reports whether the Keystore currently holds at least one
// multihash whose kademlia identifier (bit256.Key) starts with the provided
// bit-prefix.
func (s *keystore) containsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	dsk := dsKey(prefix, s.prefixBits).String()
	q := query.Query{Prefix: dsk, KeysOnly: true}
	longPrefix := prefix.BitLen() > s.prefixBits
	if !longPrefix {
		// Exact match on hex character, only one possible match
		q.Limit = 1
	}
	for r, err := range ds.QueryIter(ctx, s.ds, q) {
		if err != nil {
			return false, err
		}
		if !longPrefix {
			return true, nil
		}
		k, err := s.decodeKey(r.Key)
		if err != nil {
			return false, err
		}
		if keyspace.IsPrefix(prefix, k) {
			return true, nil
		}
	}
	return false, nil
}

// empty deletes all entries under the datastore prefix, assuming s.lk is
// already held.
func empty(ctx context.Context, d ds.Batching, batchSize int) error {
	batch, err := d.Batch(ctx)
	if err != nil {
		return err
	}
	var writeCount int
	q := query.Query{KeysOnly: true}
	for res, err := range ds.QueryIter(ctx, d, q) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if writeCount >= batchSize {
			writeCount = 0
			if err = batch.Commit(ctx); err != nil {
				return fmt.Errorf("cannot commit keystore updates: %w", err)
			}
			// Create a new batch after committing the previous one
			batch, err = d.Batch(ctx)
			if err != nil {
				return err
			}
		}
		if err != nil {
			return fmt.Errorf("cannot read query result from keystore: %w", err)
		}
		if err = batch.Delete(ctx, ds.NewKey(res.Key)); err != nil {
			return fmt.Errorf("cannot delete key from keystore: %w", err)
		}
		writeCount++
	}
	if writeCount > 0 {
		if err = batch.Commit(ctx); err != nil {
			return fmt.Errorf("cannot commit keystore updates: %w", err)
		}
	}
	if err = d.Sync(ctx, ds.NewKey("")); err != nil {
		return fmt.Errorf("cannot sync datastore: %w", err)
	}
	return nil
}

// delete removes the given keys from datastore.
func (s *keystore) delete(ctx context.Context, keys []mh.Multihash) error {
	b, err := s.ds.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsk := dsKey(keyspace.MhToBit256(h), s.prefixBits)
		err := b.Delete(ctx, dsk)
		if err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// size returns the number of keys currently stored in the Keystore.
func (s *keystore) size(ctx context.Context) (size int, err error) {
	q := query.Query{KeysOnly: true}
	for _, err = range ds.QueryIter(ctx, s.ds, q) {
		if err != nil {
			return
		}
		size++
	}
	return
}

// executeOperation sends an operation request to the worker goroutine and
// waits for the response. It handles the communication protocol and returns
// the results based on the operation type.
func (s *keystore) executeOperation(op opType, ctx context.Context, keys []mh.Multihash, prefix bitstr.Key) ([]mh.Multihash, int, bool, error) {
	response := make(chan operationResponse, 1)
	select {
	case s.requests <- operation{
		op:       op,
		ctx:      ctx,
		keys:     keys,
		prefix:   prefix,
		response: response,
	}:
	case <-ctx.Done():
		return nil, 0, false, ctx.Err()
	case <-s.close:
		return nil, 0, false, ErrClosed
	}

	select {
	case resp := <-response:
		return resp.multihashes, resp.size, resp.found, resp.err
	case <-ctx.Done():
		return nil, 0, false, ctx.Err()
	}
}

// Put stores the provided keys in the underlying datastore, grouping them by
// the first prefixLen bits. It returns only the keys that were not previously
// persisted in the datastore (i.e., newly added keys).
func (s *keystore) Put(ctx context.Context, keys ...mh.Multihash) ([]mh.Multihash, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	newKeys, _, _, err := s.executeOperation(opPut, ctx, keys, "")
	return newKeys, err
}

// Get returns all keys whose bit256 representation matches the provided
// prefix.
func (s *keystore) Get(ctx context.Context, prefix bitstr.Key) ([]mh.Multihash, error) {
	keys, _, _, err := s.executeOperation(opGet, ctx, nil, prefix)
	return keys, err
}

// ContainsPrefix reports whether the Keystore currently holds at least one
// multihash whose kademlia identifier (bit256.Key) starts with the provided
// bit-prefix.
func (s *keystore) ContainsPrefix(ctx context.Context, prefix bitstr.Key) (bool, error) {
	_, _, found, err := s.executeOperation(opContainsPrefix, ctx, nil, prefix)
	return found, err
}

// Empty deletes all entries under the datastore prefix.
func (s *keystore) Empty(ctx context.Context) error {
	_, _, _, err := s.executeOperation(opEmpty, ctx, nil, "")
	return err
}

// Delete removes the given keys from datastore.
func (s *keystore) Delete(ctx context.Context, keys ...mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	_, _, _, err := s.executeOperation(opDelete, ctx, keys, "")
	return err
}

// Size returns the number of keys currently stored in the Keystore.
//
// The size is obtained by iterating over all keys in the underlying
// datastore, so it may be expensive for large stores.
func (s *keystore) Size(ctx context.Context) (int, error) {
	_, size, _, err := s.executeOperation(opSize, ctx, nil, "")
	return size, err
}

// Close shuts down the worker goroutine and releases resources.
func (s *keystore) Close() error {
	select {
	case <-s.close:
		// Already closed
		return nil
	default:
		close(s.close)
		<-s.done
	}
	return nil
}
