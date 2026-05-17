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
// operations. It uses two storage namespaces ("0" and "1") where only one is
// active at any time, enabling atomic replacement of all stored keys without
// interrupting concurrent operations.
//
// Two storage modes are supported:
//
// Shared-datastore mode (default): Both namespaces live inside the provided
// metaDs via namespace.Wrap. The alternate namespace is emptied via
// iterate-and-delete after each reset.
//
// Factory mode (WithDatastoreFactory): Each namespace is a physically separate
// datastore created by the factory. Only the active datastore exists between
// resets; the alternate is created on demand by ResetCids and destroyed after
// the swap completes. This enables full disk reclamation because the old
// datastore is deleted from disk rather than emptied key-by-key. The provided
// metaDs stores only the active-namespace marker and persisted size.
//
// Reset Operation Flow:
//  1. The alternate datastore is prepared (created or emptied)
//  2. New keys from the reset are written to the alternate datastore
//  3. Concurrent Put operations are duplicated to both datastores to maintain
//     consistency during the transition
//  4. Once all reset keys are written, the datastores are atomically swapped
//  5. The active namespace marker is updated and persisted
//  6. The old datastore (now alternate) is torn down (destroyed or emptied)
//
// Thread Safety:
//   - All operations are processed sequentially by a single worker goroutine
//   - Reset operations are non-blocking for concurrent reads and writes
//   - Only one reset operation can be active at a time
type ResettableKeystore struct {
	keystore

	metaDs          ds.Batching // stores the active namespace marker (size is persisted in the primary ds)
	altDs           ds.Batching // nil between resets in factory mode
	altSize         atomic.Int64
	resetInProgress bool
	activeNamespace byte
	resetOps        chan resetOp  // reset operations that must be run in main go routine
	altPutSem       chan struct{} // binary semaphore: empty when altPut running, has token when idle

	createDs  func(string) (ds.Batching, error) // nil in shared-datastore mode
	destroyDs func(string) error                // nil in shared-datastore mode
}

var _ Keystore = (*ResettableKeystore)(nil)

// NewResettableKeystore creates a new ResettableKeystore backed by the
// provided datastore d.
//
// In shared-datastore mode (default), "/0" and "/1" namespace suffixes are
// created inside d for atomic reset operations, and the active-namespace
// marker is stored directly in d.
//
// In factory mode (WithDatastoreFactory), independent datastores are created
// per namespace by the factory, enabling full disk reclamation after a reset.
// The provided datastore d is used as the meta store for the active-namespace
// marker.
//
// If the keystore is reset periodically (e.g. to GC keys that should no longer
// be advertised), and the underlying datastore does not reclaim disk space
// immediately upon key deletion (e.g. LevelDB, Pebble deferring reclamation to
// background compaction), it is strongly advised to use WithDatastoreFactory.
// In factory mode the old datastore is destroyed after each reset rather than
// emptied key-by-key, so disk space is reclaimed immediately without waiting
// for compaction.
//
// Base keystore options (WithPrefixBits, WithBatchSize, etc.) are passed
// via KeystoreOption.
//
// On initialization, it checks for a persisted active namespace marker to
// determine which namespace was active during the previous session. This
// ensures keystore data persists correctly across restarts.
func NewResettableKeystore(d ds.Batching, opts ...ResettableKeystoreOption) (*ResettableKeystore, error) {
	rcfg, err := getResettableOpts(opts)
	if err != nil {
		return nil, err
	}
	cfg := rcfg.config

	var createDs func(string) (ds.Batching, error)
	var destroyDs func(string) error

	if rcfg.createDs != nil {
		// Factory mode: d is the meta datastore. Determine active
		// namespace, then create only the active datastore. The alternate
		// datastore is created on demand when ResetCids is called and
		// destroyed after the reset completes, enabling full disk
		// reclamation.
		createDs = rcfg.createDs
		destroyDs = rcfg.destroyDs
	}

	logger := log.Logger(cfg.loggerName)

	// Determine which namespace ("0" or "1") was active in the previous
	// session. A missing marker means first run; a corrupted marker
	// (wrong length or value > 1) is treated as recoverable: we log a
	// warning and fall back to namespace 0 (clear slate).
	ctx := context.Background()
	activeVal, err := d.Get(ctx, activeNamespaceKey)
	var primaryDs, altDs ds.Batching
	var activeIdx byte
	var markerCorrupted bool
	if err != nil {
		if err != ds.ErrNotFound {
			return nil, err
		}
		// No marker found, default to namespace 0.
	} else if len(activeVal) != 1 || activeVal[0] > 1 {
		// Corrupted marker: fall back to namespace 0 (clean slate).
		// Both namespaces are purged below so no stale data is served.
		// The corrected marker is persisted so subsequent restarts don't
		// repeat the warning.
		logger.Errorf("keystore: corrupted active namespace marker (got %x, expected 0x00 or 0x01), falling back to empty namespace 0; the provide system will re-advertise all CIDs from scratch on the next reprovide cycle", activeVal)
		activeIdx = 0
		markerCorrupted = true
		if err := d.Put(ctx, activeNamespaceKey, []byte{0}); err != nil {
			return nil, fmt.Errorf("failed to correct corrupted namespace marker: %w", err)
		}
	} else {
		activeIdx = activeVal[0]
	}

	if createDs != nil {
		if markerCorrupted {
			// Purge both namespaces so no stale data survives.
			_ = destroyDs("0")
			_ = destroyDs("1")
		}
		// Factory mode: create only the active datastore.
		activeSuffix := fmt.Sprintf("%d", activeIdx)
		primaryDs, err = createDs(activeSuffix)
		if err != nil {
			return nil, fmt.Errorf("failed to create datastore for namespace %s: %w", activeSuffix, err)
		}
		logger.Infof("keystore: initialized with active namespace %s (factory mode)", activeSuffix)
		// altDs stays nil — created on demand by ResetCids
	} else {
		// Shared datastore mode: both namespaces always exist
		datastores := [2]ds.Batching{
			namespace.Wrap(d, ds.NewKey("0")),
			namespace.Wrap(d, ds.NewKey("1")),
		}
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
			logger:     logger,
		},
		metaDs:          d,
		altDs:           altDs,
		activeNamespace: activeIdx,
		resetOps:        make(chan resetOp),
		altPutSem:       make(chan struct{}, 1),
		createDs:        createDs,
		destroyDs:       destroyDs,
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

// put handles put operations for ResettableKeystore. During a reset, writes are
// duplicated to the alternate datastore to maintain consistency.
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

// prepareAltDs sets up an empty alternate datastore before a reset.
// In factory mode it creates a fresh datastore. In shared-datastore mode
// it empties the existing one.
func (s *ResettableKeystore) prepareAltDs() error {
	if s.createDs != nil {
		altSuffix := fmt.Sprintf("%d", 1-s.activeNamespace)
		// Remove any stale data left by a prior crash or incomplete teardown.
		_ = s.destroyDs(altSuffix)
		newDs, err := s.createDs(altSuffix)
		if err != nil {
			return fmt.Errorf("failed to create alt datastore %s: %w", altSuffix, err)
		}
		s.altDs = newDs
		s.logger.Infof("keystore: created new datastore %s", altSuffix)
		return nil
	}
	return s.empty(context.Background(), s.altDs)
}

// teardownAltDs cleans up the alternate datastore after a reset completes
// (or fails). In factory mode it closes and destroys the datastore, setting
// altDs to nil so no disk space is wasted between resets. In shared-datastore
// mode it empties the existing namespace.
func (s *ResettableKeystore) teardownAltDs() error {
	if s.createDs != nil {
		if s.altDs == nil {
			return nil
		}
		altSuffix := fmt.Sprintf("%d", 1-s.activeNamespace)
		var errs []error
		if err := s.altDs.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close alt datastore %s: %w", altSuffix, err))
		}
		s.altDs = nil
		s.logger.Infof("keystore: removing old datastore %s", altSuffix)
		if err := s.destroyDs(altSuffix); err != nil {
			errs = append(errs, fmt.Errorf("failed to destroy alt datastore %s: %w", altSuffix, err))
		}
		return errors.Join(errs...)
	}
	return s.empty(context.Background(), s.altDs)
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
		if err := s.prepareAltDs(); err != nil {
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
		s.logger.Infof("keystore: swapped active namespace to %d (size=%d)", s.activeNamespace, s.size)
		// Persist the new active namespace
		activeValue := []byte{s.activeNamespace}

		// Write the active namespace marker
		if err := s.metaDs.Put(ctx, activeNamespaceKey, activeValue); err != nil {
			s.logger.Errorf("keystore: failed to persist active namespace marker: %v", err)
		}
		// Sync to ensure marker is persisted
		if err := s.metaDs.Sync(ctx, activeNamespaceKey); err != nil {
			s.logger.Warnf("keystore: failed to sync active namespace marker: %v", err)
		}
	}
	// Tear down the unused datastore (old active after swap, or partial
	// alt on failure).
	s.resetInProgress = false
	op.response <- s.teardownAltDs()
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
// In factory mode, the primary and (if mid-reset) alternate datastores are
// closed since they are owned by the keystore. The metaDs is not closed
// because it is owned by the caller.
func (s *ResettableKeystore) Close() (err error) {
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

		// In factory mode, defer closing owned datastores (primary and alt)
		// so they are closed even if persistSize/Sync fails. The metaDs
		// is not closed here because it is owned by the caller.
		if s.createDs != nil {
			defer func() {
				if cerr := s.ds.Close(); cerr != nil {
					err = errors.Join(err, fmt.Errorf("error closing primary datastore: %w", cerr))
				}
				// altDs is nil between resets; only close if a reset was in
				// progress when Close was called.
				if s.altDs != nil {
					if cerr := s.altDs.Close(); cerr != nil {
						err = errors.Join(err, fmt.Errorf("error closing alt datastore: %w", cerr))
					}
				}
			}()
		}

		if err = s.persistSize(); err != nil {
			err = fmt.Errorf("error persisting size on close: %w", err)
			return
		}
		if err = s.ds.Sync(context.Background(), sizeKey); err != nil {
			err = fmt.Errorf("error syncing size on close: %w", err)
			return
		}
	}
	return
}
