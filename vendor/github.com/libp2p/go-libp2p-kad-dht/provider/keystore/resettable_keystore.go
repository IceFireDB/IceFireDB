package keystore

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
)

var ErrResetInProgress = errors.New("reset already in progress")

const (
	opStart opType = iota
	opCleanup
)

type resetOp struct {
	op       opType
	success  bool
	response chan<- error
}

// ResettableKeystore is a Keystore implementation that supports atomic reset
// operations using a dual-datastore architecture. It maintains two separate
// datastores (primary and alternate) where only one is active at any time,
// enabling atomic replacement of all stored keys without interrupting
// concurrent operations.
//
// Architecture:
//   - Primary datastore: Currently active storage for all read/write operations
//   - Alternate datastore: Standby storage used during reset operations
//   - The datastores use "/0" and "/1" namespace suffixes and can be swapped
//
// Reset Operation Flow:
//  1. New keys from reset are written to the alternate (inactive) datastore
//  2. Concurrent Put operations are automatically duplicated to both datastores
//     to maintain consistency during the transition
//  3. Once all reset keys are written, the datastores are atomically swapped
//  4. The old datastore (now alternate) is cleaned up
//
// Thread Safety:
//   - All operations are processed sequentially by a single worker goroutine
//   - Reset operations are non-blocking for concurrent reads and writes
//   - Only one reset operation can be active at a time
//
// The reset operation allows complete replacement of stored multihashes
// without data loss or service interruption, making it suitable for
// scenarios requiring periodic full dataset updates.
type ResettableKeystore struct {
	keystore

	altDs           ds.Batching
	resetInProgress bool
	resetOps        chan resetOp // reset operations that must be run in main go routine
}

var _ Keystore = (*ResettableKeystore)(nil)

// NewResettableKeystore creates a new ResettableKeystore backed by the
// provided datastore. It automatically adds "/0" and "/1" suffixes to the
// configured datastore path to create two alternate storage locations for
// atomic reset operations.
func NewResettableKeystore(d ds.Batching, opts ...Option) (*ResettableKeystore, error) {
	cfg, err := getOpts(opts)
	if err != nil {
		return nil, err
	}

	rks := &ResettableKeystore{
		keystore: keystore{
			ds:         namespace.Wrap(d, ds.NewKey(cfg.path+"/0")),
			prefixBits: cfg.prefixBits,
			batchSize:  cfg.batchSize,
			requests:   make(chan operation),
			close:      make(chan struct{}),
			done:       make(chan struct{}),
		},
		altDs:    namespace.Wrap(d, ds.NewKey(cfg.path+"/1")),
		resetOps: make(chan resetOp),
	}

	// start worker goroutine
	go rks.worker()

	return rks, nil
}

// worker processes operations sequentially in a single goroutine for ResettableKeystore
func (s *ResettableKeystore) worker() {
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
			}
		case op := <-s.resetOps:
			s.handleResetOp(op)
		}
	}
}

// resettablePutLocked handles put operations for ResettableKeystore, with special
// handling during reset operations.
func (s *ResettableKeystore) put(ctx context.Context, keys []mh.Multihash) ([]mh.Multihash, error) {
	if s.resetInProgress {
		// Reset is in progress, write to alternate datastore in addition to
		// current datastore
		s.altPut(ctx, keys)
	}
	return s.keystore.put(ctx, keys)
}

// altPut writes the given multihashes to the alternate datastore.
func (s *ResettableKeystore) altPut(ctx context.Context, keys []mh.Multihash) error {
	b, err := s.altDs.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		dsk := dsKey(keyspace.MhToBit256(h), s.prefixBits)
		if err := b.Put(ctx, dsk, h); err != nil {
			return err
		}
	}
	return b.Commit(ctx)
}

// handleResetOp processes reset operations that need to happen synchronously.
func (s *ResettableKeystore) handleResetOp(op resetOp) {
	if op.op == opStart {
		if s.resetInProgress {
			op.response <- ErrResetInProgress
			return
		}
		if err := empty(context.Background(), s.altDs, s.batchSize); err != nil {
			op.response <- err
			return
		}
		s.resetInProgress = true
		op.response <- nil
		return
	}

	// Cleanup operation
	if op.success {
		// Swap the active datastore.
		oldDs := s.ds
		s.ds = s.altDs
		s.altDs = oldDs
	}
	// Empty the unused datastore.
	s.resetInProgress = false
	op.response <- empty(context.Background(), s.altDs, s.batchSize)
}

// ResetCids atomically replaces all stored keys with the CIDs received from
// keysChan. The operation is thread-safe and non-blocking for concurrent reads
// and writes.
//
// During the reset:
//   - New keys from keysChan are written to an alternate storage location
//   - Concurrent Put operations are duplicated to both current and alternate
//     locations
//   - Once all keys are processed, storage locations are atomically swapped
//   - The old storage location is cleaned up
//
// Returns ErrResetInProgress if another reset operation is already running.
// The operation can be cancelled via context, which will clean up partial
// state.
func (s *ResettableKeystore) ResetCids(ctx context.Context, keysChan <-chan cid.Cid) error {
	if keysChan == nil {
		return nil
	}

	opsChan := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	case s.resetOps <- resetOp{op: opStart, response: opsChan}:
		select {
		case err := <-opsChan:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var success bool

	defer func() {
		// Cleanup before returning on success and failure
		select {
		case s.resetOps <- resetOp{op: opCleanup, success: success, response: opsChan}:
			<-opsChan
		case <-s.done:
			// Safe not to go through the worker since we are done, and we need to
			// cleanup
			empty(context.Background(), s.altDs, s.batchSize)
		}
	}()

	keys := make([]mh.Multihash, 0)

	// Read all the keys from the channel and write them to the altDs
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrClosed
		case c, ok := <-keysChan:
			if !ok {
				break loop
			}
			keys = append(keys, c.Hash())
			if len(keys) >= s.batchSize {
				if err := s.altPut(ctx, keys); err != nil {
					return err
				}
				keys = keys[:0]
			}
		}
	}
	// Put final batch
	if err := s.altPut(ctx, keys); err != nil {
		return err
	}
	success = true

	return nil
}
