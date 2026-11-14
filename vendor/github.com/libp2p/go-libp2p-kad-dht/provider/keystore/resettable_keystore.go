package keystore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"

	"github.com/ipfs/go-libdht/kad/key/bit256"
)

var ErrResetInProgress = errors.New("reset already in progress")

// phaseADrainInterval bounds how long the worker buffer can grow between
// drains during Phase A when keysChan delivers too slowly to trigger drains
// via batch flushes. Caps buf growth at concurrent_put_rate × interval,
// independent of keysChan throughput.
const phaseADrainInterval = 100 * time.Millisecond

const (
	opStart opType = iota
	opCleanup
)

// activeNamespaceKey is the key used to persist which namespace (0 or 1) is
// currently active.
var activeNamespaceKey = ds.NewKey("active")

// sharedSlotPrefix maps a reset slot (0 or 1) to the datastore prefix that
// holds its keys in shared-datastore mode. Keep the leading "k": it stops the
// slot prefix from colliding with the keystore's bit-path components.
//
// The keystore files each key under a path whose first components are the
// single characters '0' and '1', one per Kademlia bit (see dsKey), for example
// "/0/1/0/...". A bare "/0" or "/1" slot prefix would look like one of those
// bits, and go-datastore's PrefixTransform is asymmetric: on write it skips a
// prefix the key already starts with, but on read it always strips one
// component. A key whose first bit equals the slot digit would then lose that
// bit on read and decode to the wrong value. The "k" keeps the prefix out of
// the bit alphabet, so write and read stay symmetric. See
// https://github.com/libp2p/go-libp2p-kad-dht/issues/1260.
var sharedSlotPrefix = [2]ds.Key{ds.NewKey("k0"), ds.NewKey("k1")}

type resetOp struct {
	ctx      context.Context
	op       opType
	success  bool
	response chan<- error
}

// ResettableKeystore is a Keystore implementation that supports atomic reset
// operations. It uses two storage namespaces (0 and 1) where only one is
// active at any time, enabling atomic replacement of all stored keys without
// interrupting concurrent operations.
//
// Two storage modes are supported:
//
// Shared-datastore mode (default): both namespaces live inside the provided
// metaDs, wrapped under the "/k0" and "/k1" prefixes (see sharedSlotPrefix).
// The alternate namespace is emptied via iterate-and-delete after each reset.
//
// Factory mode (WithDatastoreFactory): Each namespace is a physically separate
// datastore created by the factory. Only the active datastore exists between
// resets; the alternate is created on demand by ResetCids and destroyed after
// the swap completes. This enables full disk reclamation because the old
// datastore is deleted from disk rather than emptied key-by-key. The provided
// metaDs stores only the active-namespace marker and persisted size.
//
// Reset Operation Flow:
//  1. opStart prepares an empty alternate datastore.
//  2. ResetCids runs in three phases, all altDs writes happening on its own
//     goroutine (the worker is never the one calling altDs):
//     a. bulk     — blindly Put keysChan batches into altDs, no Has check.
//     Periodically blind-drain the worker buffer.
//     b. refresh  — run refreshSize on the now-quiesced altDs to establish
//     the exact key count. The worker may continue buffering during this
//     phase but does not write to altDs.
//     c. catch-up — drain whatever the worker buffered during refresh, this
//     time with Has-before-Put so altSize is incremented once per unique
//     new key.
//  3. opCleanup drains any tail of the buffer that arrived after Phase C,
//     Syncs altDs (durability boundary), then atomically swaps primary and
//     altDs and persists the active namespace marker.
//  4. The old datastore (now alternate) is torn down (destroyed or emptied).
//
// Thread Safety:
//   - All keystore operations (Put/Get/...) are processed sequentially by a
//     single worker goroutine.
//   - During a reset, the worker's Put does not synchronously write to
//     altDs; it appends to an in-memory buffer. This is what keeps the
//     worker responsive when altDs is slow
//   - bufferKeys back-pressures the worker if the buffer is full. This can
//     only happen if a reset is unusually slow at draining; preferred over
//     silently dropping keys, which would desynchronise altDs from primary.
//   - Only one reset operation can be active at a time.
type ResettableKeystore struct {
	keystore

	metaDs          ds.Batching // stores the active namespace marker (size is persisted in the primary ds)
	altDs           ds.Batching // nil between resets in factory mode
	altSize         atomic.Int64
	resetInProgress bool
	activeNamespace byte
	resetOps        chan resetOp
	// resetBufCap bounds the in-memory buffer of concurrent Put keys
	// duplicated to altDs during a reset. ResetCids drains the buffer in
	// three phases (bulk, refresh, catch-up); if the worker fills the
	// buffer faster than the current phase can drain it, bufferKeys
	// back-pressures the worker instead of dropping keys. Configurable via
	// WithResetBufferCapacity.
	resetBufCap int

	// Worker-buffered keys staged for altDs during a reset. The worker
	// appends here (under bufMu) instead of writing to altDs synchronously;
	// ResetCids drains here in Phases A and C and opCleanup drains it one
	// final time before the swap.
	//
	// bufDrained is the channel-as-condition idiom: every drain replaces it
	// with a fresh empty channel, closing the previous one to wake any
	// goroutines blocked in bufferKeys waiting for the buffer to have room.
	//
	// resetDrainCanceled is closed by ResetCids' defer when no further drain
	// will arrive (success or failure) and the worker must be free to
	// receive opCleanup. bufferKeys observes it as an escape, appending the
	// remainder unconditionally so opCleanup can still drain (success path)
	// or discard (failure path) those keys. Allocated by opStart; only
	// meaningful while resetInProgress is true.
	bufMu              sync.Mutex
	buf                []mh.Multihash
	bufDrained         chan struct{}
	resetDrainCanceled chan struct{}

	// altDsBusy is a binary semaphore (1-deep channel, token present when
	// altDs is idle) that serialises Close with in-flight altDs writes from
	// ResetCids. Close acquires the token after the worker exits and never
	// releases it, so any later altDs caller falls through on <-s.done.
	altDsBusy chan struct{}

	createDs  func(string) (ds.Batching, error) // nil in shared-datastore mode
	destroyDs func(string) error                // nil in shared-datastore mode
}

var _ Keystore = (*ResettableKeystore)(nil)

// NewResettableKeystore creates a new ResettableKeystore backed by the
// provided datastore d.
//
// In shared-datastore mode (default), the "/k0" and "/k1" prefixes inside d
// hold the two reset slots, and the active-namespace marker is stored directly
// in d.
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
		// Shared-datastore mode: both slots always exist, each wrapped under
		// its own non-colliding prefix (see sharedSlotPrefix).
		datastores := [2]ds.Batching{
			namespace.Wrap(d, sharedSlotPrefix[0]),
			namespace.Wrap(d, sharedSlotPrefix[1]),
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
		resetBufCap:     rcfg.resetBufCap,
		bufDrained:      make(chan struct{}),
		altDsBusy:       make(chan struct{}, 1),
		createDs:        createDs,
		destroyDs:       destroyDs,
	}
	// Initialise the altDsBusy semaphore with a token (altDs is idle).
	rks.altDsBusy <- struct{}{}

	go rks.worker()

	return rks, nil
}

// worker processes operations sequentially in a single goroutine.
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
					if size, refreshErr := refreshSize(op.ctx, s.ds); refreshErr == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after put: ", refreshErr)
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
					if size, refreshErr := refreshSize(op.ctx, s.ds); refreshErr == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after delete: ", refreshErr)
					}
				}

			case opEmpty:
				err := s.empty(op.ctx, s.ds)
				op.response <- operationResponse{err: err}
				if err == nil {
					s.size = 0
				} else {
					if size, refreshErr := refreshSize(op.ctx, s.ds); refreshErr == nil {
						s.size = size
					} else {
						s.logger.Error("keystore: failed to refresh size after empty: ", refreshErr)
					}
				}

			case opSize:
				op.response <- operationResponse{size: s.size}

			case opCount:
				n, err := s.countUpTo(op.ctx, op.prefix, op.limit)
				op.response <- operationResponse{size: n, err: err}
			}
		case op := <-s.resetOps:
			s.handleResetOp(op)
		}
	}
}

// put handles put operations for ResettableKeystore. During a reset the keys
// are buffered for later duplication to altDs; the worker never calls altDs
// directly. If the buffer is at capacity, bufferKeys blocks until ResetCids
// drains it, applying back-pressure rather than dropping keys.
func (s *ResettableKeystore) put(ctx context.Context, keys []mh.Multihash) ([]mh.Multihash, error) {
	if s.resetInProgress {
		if err := s.bufferKeys(ctx, keys); err != nil {
			return nil, err
		}
	}
	return s.keystore.put(ctx, keys)
}

// bufferKeys appends keys to the reset buffer. When the buffer fills, it
// blocks until a drain creates room and then resumes. Inputs larger than
// resetBufCap are buffered across multiple drain cycles rather than waiting
// for room they could never obtain in a single pass. Returns early if the
// keystore is closed or the request's context is cancelled. If ResetCids
// signals resetDrainCanceled (no more drains coming, e.g. the reset's ctx
// was cancelled mid-Phase-A with no tail drain), bufferKeys appends the
// remainder unconditionally and returns nil; this prevents the worker from
// being wedged while opCleanup is queued. The temporary over-cap is bounded
// by one Put's worth and is cleared by opCleanup.
func (s *ResettableKeystore) bufferKeys(ctx context.Context, keys []mh.Multihash) error {
	canceled := s.resetDrainCanceled
	s.bufMu.Lock()
	for len(keys) > 0 {
		if room := s.resetBufCap - len(s.buf); room > 0 {
			n := min(room, len(keys))
			s.buf = append(s.buf, keys[:n]...)
			keys = keys[n:]
			continue
		}
		ch := s.bufDrained
		s.bufMu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.close:
			return ErrClosed
		case <-canceled:
			s.bufMu.Lock()
			s.buf = append(s.buf, keys...)
			s.bufMu.Unlock()
			return nil
		}
		s.bufMu.Lock()
	}
	s.bufMu.Unlock()
	return nil
}

// takeBuf atomically swaps the buffer with a fresh empty slice and returns
// the old contents. It wakes any goroutines blocked in bufferKeys.
func (s *ResettableKeystore) takeBuf() []mh.Multihash {
	s.bufMu.Lock()
	out := s.buf
	s.buf = nil
	drained := s.bufDrained
	s.bufDrained = make(chan struct{})
	s.bufMu.Unlock()
	close(drained)
	return out
}

// bufEmpty reports whether the worker-buffered keys staging area is
// currently empty. Used by Phase A's ticker to skip the drainBuf round-trip
// (takeBuf channel swap + altDsBusy semaphore op) when there's no work.
func (s *ResettableKeystore) bufEmpty() bool {
	s.bufMu.Lock()
	defer s.bufMu.Unlock()
	return len(s.buf) == 0
}

// altPutBlind writes keys to altDs without checking for prior existence.
// Used by Phase A (bulk). altSize is NOT updated here; Phase B's refreshSize
// establishes the authoritative count over the entire altDs. Duplicate Puts
// — within a single batch or across batches — are idempotent in pebble, so
// no per-call dedupe is performed. No per-call Sync; see ResetCids for the
// durability boundary.
func (s *ResettableKeystore) altPutBlind(ctx context.Context, keys []mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	b, err := s.altDs.Batch(ctx)
	if err != nil {
		return err
	}
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
		if err := b.Put(ctx, dsKey(k, s.prefixBits), h); err != nil {
			return err
		}
	}
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit keystore updates: %w", err)
	}
	return nil
}

// altPutChecked writes keys to altDs after a Has check on each, and
// increments altSize for each unique addition. Used by Phase C (catch-up)
// and by opCleanup's final drain. No per-call Sync; Has-before-Put only
// needs Commit visibility, not Sync.
func (s *ResettableKeystore) altPutChecked(ctx context.Context, keys []mh.Multihash) error {
	if len(keys) == 0 {
		return nil
	}
	b, err := s.altDs.Batch(ctx)
	if err != nil {
		return err
	}
	seen := make(map[bit256.Key]struct{}, len(keys))
	var added int64
	for _, h := range keys {
		k := keyspace.MhToBit256(h)
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
	return nil
}

// drainBuf takes the current buffer and writes it to altDs in batchSize chunks
// using the supplied put function.
func (s *ResettableKeystore) drainBuf(ctx context.Context, put func(context.Context, []mh.Multihash) error) error {
	keys := s.takeBuf()
	for i := 0; i < len(keys); i += s.batchSize {
		end := min(i+s.batchSize, len(keys))
		if err := put(ctx, keys[i:end]); err != nil {
			return err
		}
	}
	return nil
}

// withAltDs runs fn while holding the altDsBusy token. Returns ErrClosed
// if the worker has exited or ctx.Err() if the caller's context is done.
// The token is released even when fn returns an error.
func (s *ResettableKeystore) withAltDs(ctx context.Context, fn func() error) error {
	select {
	case <-s.altDsBusy:
	case <-s.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { s.altDsBusy <- struct{}{} }()
	return fn()
}

// prepareAltDs sets up an empty alternate datastore before a reset.
// In factory mode it creates a fresh datastore. In shared-datastore mode
// it empties the existing one.
func (s *ResettableKeystore) prepareAltDs(ctx context.Context) error {
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
	return s.emptySharedAltDs(ctx)
}

// emptySharedAltDs deletes every key in the alternate slot in shared-datastore
// mode. It iterates the underlying datastore directly under the slot's prefix
// ("/k0" or "/k1") and deletes the raw keys, instead of going through the
// prefix-wrapped view. Working on raw keys empties the whole slot subtree in
// one pass and keeps the teardown independent of the wrapper's key transform.
//
// Only valid in shared-datastore mode, where metaDs is the un-wrapped
// underlying datastore that altDs is namespace.Wrap'd over. In factory mode
// altDs is a physically separate datastore and metaDs holds only the
// active-namespace marker, so this function must not be called; callers gate
// on s.createDs == nil.
func (s *ResettableKeystore) emptySharedAltDs(ctx context.Context) error {
	altPrefix := sharedSlotPrefix[1-s.activeNamespace].String()
	q := query.Query{Prefix: altPrefix, KeysOnly: true}
	b, err := s.metaDs.Batch(ctx)
	if err != nil {
		return err
	}
	var n int
	for res, err := range ds.QueryIter(ctx, s.metaDs, q) {
		if err != nil {
			return err
		}
		if n >= s.batchSize {
			if err := b.Commit(ctx); err != nil {
				return fmt.Errorf("cannot commit keystore updates: %w", err)
			}
			b, err = s.metaDs.Batch(ctx)
			if err != nil {
				return err
			}
			n = 0
		}
		if err := b.Delete(ctx, ds.RawKey(res.Key)); err != nil {
			return err
		}
		n++
	}
	if n > 0 {
		if err := b.Commit(ctx); err != nil {
			return fmt.Errorf("cannot commit keystore updates: %w", err)
		}
	}
	if err := s.metaDs.Sync(ctx, ds.NewKey(altPrefix)); err != nil {
		s.logger.Warnf("keystore: failed to sync after emptying alt namespace: %v", err)
	}
	return nil
}

// teardownAltDs cleans up the alternate datastore after a reset completes
// (or fails). In factory mode it closes and destroys the datastore, setting
// altDs to nil so no disk space is wasted between resets. In shared-datastore
// mode it empties the existing namespace.
func (s *ResettableKeystore) teardownAltDs(ctx context.Context) error {
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
	return s.emptySharedAltDs(ctx)
}

// handleResetOp processes reset operations on the worker goroutine.
func (s *ResettableKeystore) handleResetOp(op resetOp) {
	ctx := op.ctx
	if op.op == opStart {
		if s.resetInProgress {
			op.response <- ErrResetInProgress
			return
		}
		s.altSize.Store(0)
		s.buf = nil
		if err := s.prepareAltDs(ctx); err != nil {
			op.response <- err
			return
		}
		s.resetDrainCanceled = make(chan struct{})
		s.resetInProgress = true
		op.response <- nil
		return
	}

	// Cleanup. The worker is the sole goroutine that writes to buf, and we
	// are running on the worker now, so no new keys can arrive while this
	// drain runs. We drain anything ResetCids' Phase C drain missed
	// (writes that arrived between Phase C's takeBuf and opCleanup).
	// withAltDs keeps the altDs serialisation invariant uniform: every
	// altDs write goes through the altDsBusy token.
	if op.success {
		if err := s.withAltDs(ctx, func() error { return s.drainBuf(ctx, s.altPutChecked) }); err != nil {
			s.logger.Errorf("keystore: aborting swap, final buf drain failed: %v", err)
			op.success = false
		}
	}
	if op.success {
		// Durability boundary: altDs must be on disk before the marker flips.
		if err := s.withAltDs(ctx, func() error { return s.altDs.Sync(ctx, ds.NewKey("")) }); err != nil {
			s.logger.Errorf("keystore: aborting swap, altDs sync failed: %v", err)
			op.success = false
		}
	}

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
	s.buf = nil
	op.response <- s.teardownAltDs(ctx)
}

// ResetCids atomically replaces all stored keys with the CIDs received from
// keysChan. The operation is thread-safe and non-blocking for concurrent reads
// and writes against the primary namespace.
//
// During the reset:
//   - New keys from keysChan are written to an alternate storage location
//     without per-key Has checks (Phase A).
//   - Concurrent Puts go to an in-memory buffer; bufferKeys back-pressures
//     the caller if the buffer is full.
//   - refreshSize establishes the exact key count on the now-quiesced
//     alternate datastore (Phase B).
//   - Buffered keys are drained into the alternate datastore with
//     Has-before-Put so altSize is incremented exactly once per unique
//     addition (Phase C).
//   - opCleanup performs a final small drain (anything that arrived after
//     Phase C) and atomically swaps the datastores.
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

	// Cancel altDs operations when the keystore is closed (s.done), so
	// pebble calls that honor ctx return promptly and Close can acquire
	// altDsBusy. The goroutine exits on the first of {s.done closed,
	// user ctx done, ResetCids' defer running cancelCtx}; if it observes
	// user cancellation before s.done it stops watching s.done — which is
	// fine because the user-cancelled ctx will already unblock any altDs
	// caller below.
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	go func() {
		select {
		case <-s.done:
			cancelCtx()
		case <-ctx.Done():
		}
	}()

	opsChan := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	case s.resetOps <- resetOp{ctx: ctx, op: opStart, response: opsChan}:
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
		// Wake any worker parked in bufferKeys: no further drain will
		// arrive before opCleanup, and the worker must be free to receive
		// it. Without this, a Phase-A ctx cancel with the buffer at
		// capacity deadlocks (worker blocked, opCleanup send blocked).
		close(s.resetDrainCanceled)
		// Cleanup before returning on success and failure.
		select {
		case s.resetOps <- resetOp{ctx: ctx, op: opCleanup, success: success, response: opsChan}:
			<-opsChan
		case <-s.done:
			// Worker is done; underlying datastore may already be closed,
			// so we cannot run the swap. Close() handles altDs teardown.
		}
	}()

	// Phase A — bulk. Blind-write keysChan batches; periodically drain the
	// worker buffer with blind writes too. No per-key Has check.
	// A phaseADrainInterval ticker bounds buf growth when keysChan delivers
	// too slowly to trigger drains via batch flushes.
	batch := make([]mh.Multihash, 0, s.batchSize)
	ticker := time.NewTicker(phaseADrainInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrClosed
		case <-ticker.C:
			if s.bufEmpty() {
				continue
			}
			if err := s.withAltDs(ctx, func() error { return s.drainBuf(ctx, s.altPutBlind) }); err != nil {
				return err
			}
		case c, ok := <-keysChan:
			if !ok {
				break loop
			}
			batch = append(batch, c.Hash())
			if len(batch) >= s.batchSize {
				if err := s.withAltDs(ctx, func() error { return s.altPutBlind(ctx, batch) }); err != nil {
					return err
				}
				batch = batch[:0]
				if err := s.withAltDs(ctx, func() error { return s.drainBuf(ctx, s.altPutBlind) }); err != nil {
					return err
				}
			}
		}
	}
	if len(batch) > 0 {
		if err := s.withAltDs(ctx, func() error { return s.altPutBlind(ctx, batch) }); err != nil {
			return err
		}
	}
	if err := s.withAltDs(ctx, func() error { return s.drainBuf(ctx, s.altPutBlind) }); err != nil {
		return err
	}
	// Off-worker fsync of the bulk write, so opCleanup's Sync (which runs
	// on the worker) only has to flush Phase C's small drain.
	if err := s.withAltDs(ctx, func() error { return s.altDs.Sync(ctx, ds.NewKey("")) }); err != nil {
		return fmt.Errorf("phase A altDs sync: %w", err)
	}

	// Phase B — refresh. altDs is naturally quiesced: the worker buffers,
	// it doesn't write to altDs, and ResetCids is the only altDs writer.
	var size int
	if err := s.withAltDs(ctx, func() (e error) { size, e = refreshSize(ctx, s.altDs); return }); err != nil {
		return err
	}
	s.altSize.Store(int64(size))

	// Phase C — catch-up. One Has-checked drain of whatever the worker
	// buffered during Phase B (or between Phase A's final drain and now).
	// opCleanup will drain any tail that arrives after this.
	if err := s.withAltDs(ctx, func() error { return s.drainBuf(ctx, s.altPutChecked) }); err != nil {
		return err
	}

	success = true
	return nil
}

// Close shuts down the ResettableKeystore. It waits for the worker to exit
// and for any in-flight altDs write from ResetCids to finish before
// persisting state. In factory mode, the primary and (if mid-reset)
// alternate datastores are closed since they are owned by the keystore.
// The metaDs is not closed because it is owned by the caller.
func (s *ResettableKeystore) Close() (err error) {
	select {
	case <-s.close:
		// Already closed
	default:
		close(s.close)
		<-s.done // Wait for worker to exit (no new buffer appends after this).
		// Wait for any in-flight altDs write from ResetCids to finish.
		// We never release the token, so subsequent ResetCids callers fall
		// through on <-s.done.
		<-s.altDsBusy

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
