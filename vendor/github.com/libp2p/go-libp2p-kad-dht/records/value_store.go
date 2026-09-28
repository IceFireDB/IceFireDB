package records

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-base32"
	"google.golang.org/protobuf/proto"
)

// ErrOldRecord is returned by ValueStore.Put when the stored record is at least
// as desirable as the one being written, per the Validator's Select.
var ErrOldRecord = errors.New("old record")

// ValueStore persists DHT value records (such as /pk and /ipns) in a
// datastore. It owns the validate / select / time-stamping policy that used to
// be inlined across the DHT value handlers: records are validated and selected
// against on write, age-checked on read, and stamped with the time they were
// received. Corrupt, misfiled, or expired records are discarded on access.
//
// The validator runs on write, not on read. A record's value is verified once,
// when Put stores it, so Get trusts the stored value and only re-checks its
// age. This keeps reads that serve a remote GET_VALUE cheap: a signature-
// verifying validator (such as ipns) would otherwise cost a signature
// verification per served record, and requesters validate every record they
// receive regardless.
//
// A ValueStore is safe for concurrent use. Writes to the same key are
// serialised so that a concurrent Put cannot overwrite a better record.
type ValueStore struct {
	ds           ds.Datastore
	validator    record.Validator
	maxRecordAge time.Duration
	putLocks     [256]sync.Mutex

	// namespaces are the value record namespaces the background GC sweeps. It is
	// derived from validator at construction so the sweep only scans keys the
	// store owns; it is empty for a non-namespaced validator, which makes the
	// sweep fall back to a whole-datastore scan. Namespaces registered on the
	// validator after construction are not swept by the background GC (they are
	// still expired lazily on read).
	namespaces []string

	// GC lifecycle: gcMu guards the fields below, set up by StartGC and torn down
	// by Close, so the two are safe to call from different goroutines.
	gcMu      sync.Mutex
	gcStarted bool
	gcCancel  context.CancelFunc
	gcClosed  chan struct{}
}

// NewValueStore returns a ValueStore backed by dstore. It uses validator to
// validate and select records, and treats records older than maxRecordAge as
// absent. A non-positive maxRecordAge disables age expiry.
func NewValueStore(dstore ds.Datastore, validator record.Validator, maxRecordAge time.Duration) *ValueStore {
	return &ValueStore{
		ds:           dstore,
		validator:    validator,
		maxRecordAge: maxRecordAge,
		namespaces:   valueNamespaces(validator),
	}
}

// valueNamespaces returns the record namespaces a namespaced validator knows
// about (e.g. "pk", "ipns"), sorted for a deterministic sweep order. It returns
// nil for any other validator, since the set of value namespaces is then
// unknown.
func valueNamespaces(validator record.Validator) []string {
	nsval, ok := validator.(record.NamespacedValidator)
	if !ok {
		return nil
	}
	return slices.Sorted(maps.Keys(nsval))
}

// Get returns the record stored under key. It returns (nil, nil) when there is
// no record, or when the stored record is corrupt, is filed under the wrong
// key, or is older than maxRecordAge; such records are deleted as a side
// effect. A non-nil error indicates a datastore failure.
//
// Get does not run the validator. A record's value is verified once, when Put
// stores it, so Get trusts the stored value and only re-checks its age. This
// keeps serving a remote GET_VALUE cheap (no signature verification per served
// record), and requesters validate every record they receive regardless.
func (v *ValueStore) Get(ctx context.Context, key string) (*recpb.Record, error) {
	dskey := valueDsKey(key)
	buf, err := v.ds.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rec := new(recpb.Record)
	if err := proto.Unmarshal(buf, rec); err != nil {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	// The stored record must belong to the key it is filed under.
	if string(rec.GetKey()) != key {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	if v.expired(rec) {
		v.discardIfUnchanged(ctx, key, dskey, buf)
		return nil, nil
	}

	return rec, nil
}

// Put validates rec, selects it against any record already stored under key,
// stamps it with the current receive time, and stores it. It returns
// ErrOldRecord when the existing record is at least as desirable as rec.
// Concurrent Puts for the same key are serialised. rec is not modified.
func (v *ValueStore) Put(ctx context.Context, key string, rec *recpb.Record) error {
	if err := v.validator.Validate(key, rec.GetValue()); err != nil {
		return fmt.Errorf("validating record: %w", err)
	}

	lock := &v.putLocks[lockIndex(key)]
	lock.Lock()
	defer lock.Unlock()

	dskey := valueDsKey(key)
	existing, err := v.existingForSelect(ctx, dskey)
	if err != nil {
		return err
	}
	if existing != nil {
		best, err := v.validator.Select(key, [][]byte{rec.GetValue(), existing.GetValue()})
		if err != nil {
			return fmt.Errorf("selecting record: %w", err)
		}
		if best != 0 {
			return ErrOldRecord
		}
	}

	// Stamp a clone so we never mutate the caller's record, which may be shared
	// across goroutines (e.g. a record being written locally and sent to peers).
	stored := proto.Clone(rec).(*recpb.Record)
	stored.TimeReceived = internal.FormatRFC3339(time.Now())

	data, err := proto.Marshal(stored)
	if err != nil {
		return err
	}
	return v.ds.Put(ctx, dskey, data)
}

// existingForSelect reads the record currently stored at dskey for comparison
// during Put. Unlike Get it does not age-check or delete: a missing, corrupt,
// or invalid record is reported as absent so the incoming record replaces it.
// This means a stored record blocks a worse incoming one via Select regardless
// of its age; once it passes maxRecordAge, GC (like Get) removes it, so an
// expired record offers no rollback guarantee against an older incoming record.
func (v *ValueStore) existingForSelect(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := v.ds.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec := new(recpb.Record)
	if err := proto.Unmarshal(buf, rec); err != nil {
		return nil, nil
	}
	if err := v.validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		return nil, nil
	}
	return rec, nil
}

// expired reports whether rec is older than maxRecordAge, or carries no valid
// receive time. A non-positive maxRecordAge disables age expiry entirely.
func (v *ValueStore) expired(rec *recpb.Record) bool {
	if v.maxRecordAge <= 0 {
		return false
	}
	recvtime, err := internal.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		return true
	}
	return time.Since(recvtime) > v.maxRecordAge
}

// discardIfUnchanged best-effort deletes a bad record, but only while it still
// holds the exact bytes the reader saw. It takes the key's Put lock and
// re-reads, so it never clobbers a good record written by a concurrent Put
// between the read and the delete. Failures are logged, not returned.
func (v *ValueStore) discardIfUnchanged(ctx context.Context, key string, dskey ds.Key, seen []byte) {
	lock := &v.putLocks[lockIndex(key)]
	lock.Lock()
	defer lock.Unlock()

	current, err := v.ds.Get(ctx, dskey)
	if err != nil {
		return // already gone, or a datastore error we cannot act on
	}
	if !bytes.Equal(current, seen) {
		return // replaced by a concurrent Put; keep the newer record
	}
	if err := v.ds.Delete(ctx, dskey); err != nil {
		log.Errorw("failed to delete bad record from datastore", "key", dskey, "error", err)
	}
}

// StartGC launches a background sweep that periodically deletes value records
// older than maxRecordAge, bounded by ctx. It is a no-op when age expiry is
// disabled (maxRecordAge <= 0) or interval <= 0, and starts at most one
// sweeper. Stop the sweeper with Close.
func (v *ValueStore) StartGC(ctx context.Context, interval time.Duration) {
	if v.maxRecordAge <= 0 || interval <= 0 {
		return
	}
	v.gcMu.Lock()
	defer v.gcMu.Unlock()
	if v.gcStarted {
		return
	}
	v.gcStarted = true
	ctx, v.gcCancel = context.WithCancel(ctx)
	v.gcClosed = make(chan struct{})
	go v.gcLoop(ctx, interval, v.gcClosed)
}

// Close stops the background sweep started by StartGC and waits for it to exit.
// It is safe to call when StartGC was never called or was a no-op, and is
// idempotent and safe to call concurrently with StartGC.
//
// Close does not fence reads and writes: Get and Put issued after (or racing)
// Close still reach the backing datastore. Stop issuing them before closing
// the datastore itself.
func (v *ValueStore) Close() error {
	v.gcMu.Lock()
	cancel, closed := v.gcCancel, v.gcClosed
	v.gcMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	<-closed
	return nil
}

func (v *ValueStore) gcLoop(ctx context.Context, interval time.Duration, closed chan struct{}) {
	defer close(closed)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			v.collectExpired(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// collectExpired deletes every expired value record in one pass. When the value
// namespaces are known it queries each one's subtree; otherwise it scans the
// whole datastore while skipping the reserved provider subtree.
func (v *ValueStore) collectExpired(ctx context.Context) {
	if len(v.namespaces) == 0 {
		v.sweep(ctx, "")
		return
	}
	for _, ns := range v.namespaces {
		v.sweep(ctx, valueNsPrefix(ns))
	}
}

// sweep deletes expired value records whose datastore key starts with prefix.
// An empty prefix scans the whole datastore. Provider records and entries that
// are not this store's own value records are left untouched, so a datastore may
// be shared with the provider store and other data.
func (v *ValueStore) sweep(ctx context.Context, prefix string) {
	res, err := v.ds.Query(ctx, dsq.Query{Prefix: prefix})
	if err != nil {
		log.Errorw("value GC query failed", "prefix", prefix, "error", err)
		return
	}
	defer func() { _ = res.Close() }()

	for e := range res.Next() {
		if ctx.Err() != nil {
			return
		}
		if e.Error != nil {
			log.Errorw("value GC query result error", "error", e.Error)
			continue
		}
		if strings.HasPrefix(e.Key, ProvidersKeyPrefix) {
			continue // reserved for provider records
		}
		rec := new(recpb.Record)
		if proto.Unmarshal(e.Value, rec) != nil {
			continue // not a well-formed record; Get discards it on read
		}
		// Only touch entries this store itself filed here, and only once expired.
		key := string(rec.GetKey())
		dskey := valueDsKey(key)
		if dskey.String() != e.Key || !v.expired(rec) {
			continue
		}
		v.discardIfUnchanged(ctx, key, dskey, e.Value)
	}
}

// valueDsKey encodes a record key as its datastore key:
// "/" + <namespace> + "/" + base32(fullkey). Value records are stored under
// their record namespace verbatim ("/pk/…", "/ipns/…"), which is readable and,
// because record namespaces are distinct, keeps a shared datastore
// collision-free. base32-encoding the full key keeps the record key recoverable
// and the ValueStore.Get key check valid. A malformed key (no namespace) lands
// at root.
func valueDsKey(key string) ds.Key {
	ns, _, _ := record.SplitKey(key)
	return ds.NewKey(valueNsPrefix(ns) + "/" + base32.RawStdEncoding.EncodeToString([]byte(key)))
}

// valueNsPrefix returns the datastore key prefix under which value records of
// namespace ns are stored, so the GC can sweep a namespace's subtree. The
// reserved provider namespace is base32-encoded to keep such a (non-standard)
// value record out of the "/providers/" subtree; every other namespace is used
// verbatim.
func valueNsPrefix(ns string) string {
	if ns == providerNamespace {
		ns = base32.RawStdEncoding.EncodeToString([]byte(ns))
	}
	return "/" + ns
}

// lockIndex maps a key to one of the striped Put locks by its last byte.
func lockIndex(key string) byte {
	if len(key) == 0 {
		return 0
	}
	return key[len(key)-1]
}
