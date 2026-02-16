package keystore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key/bit256"
)

var ErrResetInProgress = errors.New("reset already in progress")

const (
	opStart opType = iota
	opCleanup
)

// activeNamespaceKey is the key used to persist which namespace (0 or 1) is currently active
var activeNamespaceKey = ds.NewKey("active")

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
//   - Active namespace is persisted to survive restarts
//
// Reset Operation Flow:
//  1. New keys from reset are written to the alternate (inactive) datastore
//  2. Concurrent Put operations are automatically duplicated to both datastores
//     to maintain consistency during the transition
//  3. Once all reset keys are written, the datastores are atomically swapped
//  4. The active namespace marker is updated to persist the swap
//  5. The old datastore (now alternate) is cleaned up
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

	baseDs          ds.Batching // Unwrapped datastore for storing active namespace marker
	altDs           ds.Batching
	altSize         atomic.Int64
	resetInProgress bool
	activeNamespace byte
	resetOps        chan resetOp  // reset operations that must be run in main go routine
	altPutSem       chan struct{} // binary semaphore: empty when altPut running, has token when idle
}

var _ Keystore = (*ResettableKeystore)(nil)

// NewResettableKeystore creates a new ResettableKeystore backed by the
// provided datastore. It automatically adds "/0" and "/1" suffixes to the
// configured datastore path to create two alternate storage locations for
// atomic reset operations.
//
// On initialization, it checks for a persisted active namespace marker to
// determine which namespace was active during the previous session. This
// ensures keystore data persists correctly across restarts.
func NewResettableKeystore(d ds.Batching, opts ...Option) (*ResettableKeystore, error) {
	cfg, err := getOpts(opts)
	if err != nil {
		return nil, err
	}

	// Create namespaced datastores
	baseDs := namespace.Wrap(d, ds.NewKey(cfg.path))
	datastores := []ds.Batching{
		namespace.Wrap(d, ds.NewKey(cfg.path+"/0")),
		namespace.Wrap(d, ds.NewKey(cfg.path+"/1")),
	}

	// Check if there's a persisted active namespace marker
	ctx := context.Background()
	activeDs, err := baseDs.Get(ctx, activeNamespaceKey)
	var primaryDs, altDs ds.Batching
	var activeIdx byte
	if err != nil {
		if err != ds.ErrNotFound {
			return nil, err
		}
		// No marker found, default to namespace 0 as primary
		primaryDs = datastores[0]
		altDs = datastores[1]
		activeIdx = 0
	} else {
		// Marker found, use it to determine active namespace
		if len(activeDs) != 1 {
			return nil, fmt.Errorf("invalid active namespace marker length: %d", len(activeDs))
		}
		activeIdx = activeDs[0]
		primaryDs = datastores[activeIdx]
		altDs = datastores[1-activeIdx]
	}

	rks := &ResettableKeystore{
		keystore: keystore{
			ds:         primaryDs,
			prefixBits: cfg.prefixBits,
			batchSize:  cfg.batchSize,
			requests:   make(chan operation),
			close:      make(chan struct{}),
			done:       make(chan struct{}),
			logger:     log.Logger(cfg.loggerName),
		},
		baseDs:          baseDs,
		altDs:           altDs,
		activeNamespace: activeIdx,
		resetOps:        make(chan resetOp),
		altPutSem:       make(chan struct{}, 1),
	}
	// Initialize semaphore with token (altPut not running)
	rks.altPutSem <- struct{}{}

	// start worker goroutine
	go rks.worker()

	return rks, nil
}

// worker processes operations sequentially in a single goroutine for ResettableKeystore
func (s *ResettableKeystore) worker() {
	defer close(s.done)
	s.loadSize()

	for {
		select {
		case <-s.close:
			return
		case op := <-s.requests:
			switch op.op {
			case opPut:
				newKeys, err := s.put(op.ctx, op.keys)
				op.response <- operationResponse{multihashes: newKeys, err: err}
				if err != nil {
					if size, err := refreshSize(op.ctx, s.ds); err == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after put: ", err)
					}
				}

			case opGet:
				keys, err := s.get(op.ctx, op.prefix)
				op.response <- operationResponse{multihashes: keys, err: err}

			case opContainsPrefix:
				found, err := s.containsPrefix(op.ctx, op.prefix)
				op.response <- operationResponse{found: found, err: err}

			case opDelete:
				err := s.delete(op.ctx, op.keys)
				op.response <- operationResponse{err: err}
				if err != nil {
					if size, err := refreshSize(op.ctx, s.ds); err == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after delete: ", err)
					}
				}

			case opEmpty:
				err := s.empty(op.ctx, s.ds)
				op.response <- operationResponse{err: err}
				if err == nil {
					s.size = 0
				} else {
					if size, err := refreshSize(op.ctx, s.ds); err == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after empty: ", err)
					}
				}

			case opSize:
				op.response <- operationResponse{size: s.size}
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
		<-s.altPutSem
		if err := s.altPut(ctx, keys); err != nil {
			if size, err := refreshSize(ctx, s.altDs); err == nil {
				s.altSize.Store(int64(size))
			} else {
				s.logger.Error("keystore: failed to refresh size after alt put: ", err)
			}
		}
		s.altPutSem <- struct{}{}
	}
	return s.keystore.put(ctx, keys)
}

// altPut writes the given multihashes to the alternate datastore.
// The caller must hold the altPutSem token.
func (s *ResettableKeystore) altPut(ctx context.Context, keys []mh.Multihash) error {
	b, err := s.altDs.Batch(ctx)
	if err != nil {
		return err
	}
	seen := make(map[bit256.Key]struct{}, len(keys))
	var added int64
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		// Skip duplicates within this batch
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}

		dsk := dsKey(k, s.prefixBits)
		ok, err := s.altDs.Has(ctx, dsk)
		if err != nil {
			return err
		}
		if !ok {
			if err := b.Put(ctx, dsk, h); err != nil {
				return err
			}
			added++
		}
	}
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit keystore updates: %w", err)
	}
	s.altSize.Add(added)
	if err = s.altDs.Sync(ctx, ds.NewKey("")); err != nil {
		s.logger.Warnf("keystore: failed to sync datastore after alt put: %v", err)
	}
	return nil
}

// tryAltPut is a concurrency-safe wrapper around altPut that coordinates with
// both the worker goroutine and Close() to prevent data races during reset
// operations.
//
// This method is called exclusively by ResetCids(), which runs outside the
// worker goroutine. The binary semaphore (altPutSem) provides mutual exclusion
// between this method, the worker's put() (which also acquires the semaphore
// during reset), and Close():
//   - tryAltPut blocks until the semaphore token is available or the worker
//     exits (s.done closed), in which case it returns ErrClosed
//   - Close() acquires the token after the worker exits, ensuring no
//     concurrent altPut during persistSize()
func (s *ResettableKeystore) tryAltPut(ctx context.Context, keys []mh.Multihash) error {
	select {
	case <-s.altPutSem:
	case <-s.done:
		return ErrClosed
	}
	err := s.altPut(ctx, keys)
	s.altPutSem <- struct{}{}
	return err
}

// handleResetOp processes reset operations that need to happen synchronously.
func (s *ResettableKeystore) handleResetOp(op resetOp) {
	ctx := context.Background()
	if op.op == opStart {
		if s.resetInProgress {
			op.response <- ErrResetInProgress
			return
		}
		s.altSize.Store(0)
		if err := s.empty(ctx, s.altDs); err != nil {
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
		s.size = int(s.altSize.Load())

		// Toggle the active namespace index
		s.activeNamespace = 1 - s.activeNamespace
		// Persist the new active namespace
		activeValue := []byte{s.activeNamespace}

		// Write the active namespace marker
		if err := s.baseDs.Put(ctx, activeNamespaceKey, activeValue); err != nil {
			s.logger.Errorf("keystore: failed to persist active namespace marker: %v", err)
		}
		// Sync to ensure marker is persisted
		if err := s.baseDs.Sync(ctx, activeNamespaceKey); err != nil {
			s.logger.Warnf("keystore: failed to sync active namespace marker: %v", err)
		}
	}
	// Empty the unused datastore.
	s.resetInProgress = false
	op.response <- s.empty(ctx, s.altDs)
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

	var success bool

	start := time.Now()
	s.logger.Info("keystore: ResetCids started")
	defer func() {
		if success {
			s.logger.Infof("keystore: ResetCids finished in %s", time.Since(start))
		} else {
			s.logger.Infof("keystore: ResetCids failed after %s", time.Since(start))
		}
	}()

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

	defer func() {
		// Cleanup before returning on success and failure
		select {
		case s.resetOps <- resetOp{op: opCleanup, success: success, response: opsChan}:
			<-opsChan
		case <-s.done:
			// Worker is done, which means the keystore is closing or has closed.
			// The underlying datastore may already be closed, so attempting to
			// empty it could cause a panic. Skip cleanup since the datastore
			// will be cleaned up during normal shutdown.
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
				if err := s.tryAltPut(ctx, keys); err != nil {
					return err
				}
				keys = keys[:0]
			}
		}
	}
	// Put final batch
	if err := s.tryAltPut(ctx, keys); err != nil {
		return err
	}
	success = true

	return nil
}

// Close shuts down the ResettableKeystore. It waits for any ongoing altPut
// operations to complete before persisting state to avoid race conditions.
func (s *ResettableKeystore) Close() error {
	var err error
	select {
	case <-s.close:
		// Already closed
	default:
		close(s.close)
		<-s.done // Wait for worker to exit
		// Wait for any ongoing altPut operations to complete by acquiring the
		// semaphore token. This blocks if altPut is running, succeeds immediately
		// if idle.
		<-s.altPutSem
		if err = s.persistSize(); err != nil {
			return fmt.Errorf("error persisting size on close: %w", err)
		}
		if err = s.ds.Sync(context.Background(), sizeKey); err != nil {
			return fmt.Errorf("error syncing size on close: %w", err)
		}
	}
	return err
}
