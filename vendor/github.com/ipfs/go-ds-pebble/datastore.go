package pebbleds

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-log/v2"
)

var logger = log.Logger("pebble")

// Datastore is a pebble-backed github.com/ipfs/go-datastore.Datastore.
//
// It supports batching. It does not support TTL or transactions, because pebble
// doesn't have those features.
type Datastore struct {
	db *pebble.DB

	cache      *pebble.Cache
	closing    chan struct{}
	disableWAL bool
	status     int32
	wg         sync.WaitGroup
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)

// NewDatastore creates a pebble-backed datastore.
//
// Users can provide pebble options using WithPebbleOpts or rely on Pebble's
// defaults. Any pebble options that are not assigned a value are assigned
// pebble's default value for the option.
func NewDatastore(path string, options ...Option) (*Datastore, error) {
	opts := getOpts(options)

	// Use the provided database or create a new one.
	db := opts.db
	var disableWAL bool
	var cache *pebble.Cache
	if db == nil {
		pebbleOpts := opts.pebbleOpts.EnsureDefaults()
		pebbleOpts.Logger = logger
		disableWAL = pebbleOpts.DisableWAL
		// Use the provided cache, create a custom-sized cache, or use default.
		if pebbleOpts.Cache == nil && opts.cacheSize != 0 {
			cache = pebble.NewCache(opts.cacheSize)
			// Keep ref to cache if it is created here.
			pebbleOpts.Cache = cache
		}
		var err error
		db, err = pebble.Open(path, pebbleOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to open pebble database: %w", err)
		}
	}

	return &Datastore{
		db:         db,
		disableWAL: disableWAL,
		cache:      cache,
		closing:    make(chan struct{}),
	}, nil
}

// get performs a get on the database, copying the value to a new slice and
// returning it if retval is true. If retval is false, no copy will be
// performed, and the returned value will always be nil, whether or not the key
// exists. If the key doesn't exist, ds.ErrNotFound will be returned. When no
// error occurs, the size of the value is also returned.
func (d *Datastore) get(key []byte, retval bool) ([]byte, int, error) {
	val, closer, err := d.db.Get(key)
	switch err {
	case nil:
		// do nothing
	case pebble.ErrNotFound:
		return nil, 0, ds.ErrNotFound
	default:
		return nil, -1, fmt.Errorf("pebble error during get: %w", err)
	}

	var cpy []byte
	if retval {
		cpy = make([]byte, len(val))
		copy(cpy, val)
	}
	size := len(val)
	_ = closer.Close()
	return cpy, size, nil
}

// Get reads a key from the datastore.
func (d *Datastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	val, _, err := d.get(key.Bytes(), true)
	return val, err
}

// Has can be used to check whether a key is stored in the datastore. Has()
// calls are not cheaper than Get() though. In Pebble, lookups for existing
// keys will also read the values. Avoid using Has() if you later expect to
// read the key anyways.
func (d *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, _ error) {
	_, _, err := d.get(key.Bytes(), false)
	switch err {
	case ds.ErrNotFound:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

func (d *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, _ error) {
	_, size, err := d.get(key.Bytes(), false)
	if err != nil {
		return -1, err
	}
	return size, nil
}

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var (
		prefix      = ds.NewKey(q.Prefix).String()
		limit       = q.Limit
		offset      = q.Offset
		orders      = q.Orders
		filters     = q.Filters
		keysOnly    = q.KeysOnly
		_           = q.ReturnExpirations // pebble doesn't support TTL; noop
		returnSizes = q.ReturnsSizes
	)

	if prefix != "/" {
		prefix = prefix + "/"
	}

	opts := &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: func() []byte {
			// if the prefix is 0x01..., we want 0x02 as an upper bound.
			// if the prefix is 0x0000ff..., we want 0x0001 as an upper bound.
			// if the prefix is 0x0000ff01..., we want 0x0000ff02 as an upper bound.
			// if the prefix is 0xffffff..., we don't want an upper bound.
			// if the prefix is 0xff..., we don't want an upper bound.
			// if the prefix is empty, we don't want an upper bound.
			// basically, we want to find the last byte that can be lexicographically incremented.
			var upper []byte
			for i := len(prefix) - 1; i >= 0; i-- {
				b := prefix[i]
				if b == 0xff {
					continue
				}
				upper = make([]byte, i+1)
				copy(upper, prefix)
				upper[i] = b + 1
				break
			}
			return upper
		}(),
	}

	iter, err := d.db.NewIter(opts)
	if err != nil {
		return nil, err
	}

	var move func() bool
	switch l := len(orders); l {
	case 0:
		iter.First()
		move = iter.Next
	case 1:
		switch o := orders[0]; o.(type) {
		case query.OrderByKey, *query.OrderByKey:
			iter.First()
			move = iter.Next
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			iter.Last()
			move = iter.Prev
		default:
			defer iter.Close()
			return d.inefficientOrderQuery(ctx, q, nil)
		}
	default:
		var baseOrder query.Order
		for _, o := range orders {
			if baseOrder != nil {
				return nil, fmt.Errorf("incompatible orders passed: %+v", orders)
			}
			switch o.(type) {
			case query.OrderByKey, query.OrderByKeyDescending, *query.OrderByKey, *query.OrderByKeyDescending:
				baseOrder = o
			}
		}
		defer iter.Close()
		return d.inefficientOrderQuery(ctx, q, baseOrder)
	}

	if !iter.Valid() {
		_ = iter.Close()
		// there are no valid results.
		return query.ResultsWithEntries(q, []query.Entry{}), nil
	}

	// filterFn takes an Entry and tells us if we should return it.
	filterFn := func(entry query.Entry) bool {
		for _, f := range filters {
			if !f.Filter(entry) {
				return false
			}
		}
		return true
	}
	doFilter := false
	if len(filters) > 0 {
		doFilter = true
	}

	createEntry := func() query.Entry {
		// iter.Key and iter.Value may change on the next call to iter.Next.
		// string conversion takes a copy
		entry := query.Entry{Key: string(iter.Key())}
		if !keysOnly {
			// take a copy.
			cpy := make([]byte, len(iter.Value()))
			copy(cpy, iter.Value())
			entry.Value = cpy
		}
		if returnSizes {
			entry.Size = len(iter.Value())
		}
		return entry
	}

	d.wg.Add(1)
	results := query.ResultsWithContext(q, func(ctx context.Context, outCh chan<- query.Result) {
		defer d.wg.Done()
		defer iter.Close()

		const interrupted = "interrupted"

		defer func() {
			switch r := recover(); r {
			case nil, interrupted:
				// nothing, or flow interrupted; all ok.
			default:
				panic(r) // a genuine panic, propagate.
			}
		}()

		sendOrInterrupt := func(r query.Result) {
			select {
			case outCh <- r:
				return
			case <-d.closing:
			case <-ctx.Done():
			}

			// we are closing; try to send a closure error to the client.
			// but do not halt because they might have stopped receiving.
			select {
			case outCh <- query.Result{Error: fmt.Errorf("close requested")}:
			default:
			}
			panic(interrupted)
		}

		// skip over 'offset' entries; if a filter is provided, only entries
		// that match the filter will be counted as a skipped entry.
		for skipped := 0; skipped < offset && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			if doFilter && !filterFn(createEntry()) {
				// if we have a filter, and this entry doesn't match it,
				// don't count it.
				continue
			}
			skipped++
		}

		// start sending results, capped at limit (if > 0)
		for sent := 0; (limit <= 0 || sent < limit) && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			entry := createEntry()
			if doFilter && !filterFn(entry) {
				// if we have a filter, and this entry doesn't match it,
				// do not sendOrInterrupt it.
				continue
			}
			sendOrInterrupt(query.Result{Entry: entry})
			sent++
		}
	})
	return results, nil
}

func (d *Datastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	err := d.db.Set(key.Bytes(), value, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during set: %w", err)
	}
	return nil
}

// DiskUsage implements the PersistentDatastore interface and returns current
// size on disk.
func (d *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	m := d.db.Metrics()
	// since we requested metrics, print them up on debug
	logger.Debugf("\n\n%s\n\n", m)
	return m.DiskSpaceUsage(), nil
}

func (d *Datastore) Delete(ctx context.Context, key ds.Key) error {
	err := d.db.Delete(key.Bytes(), pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during delete: %w", err)
	}
	return nil
}

func (d *Datastore) Sync(ctx context.Context, _ ds.Key) error {
	// pebble provides a Flush operation, but it writes the memtables to stable
	// storage. That's not what Sync is supposed to do. Sync is supposed to
	// guarantee that previously performed write operations will survive a machine
	// crash. In pebble this is done by fsyncing the WAL, which can be requested when
	// performing write operations. But there is no separate operation to fsync
	// only. The closest is LogData, which actually writes a log entry on the WAL.
	if d.disableWAL { // otherwise this errors
		return nil
	}
	err := d.db.LogData(nil, pebble.Sync)
	if err != nil {
		return fmt.Errorf("pebble error during sync: %w", err)
	}
	return nil
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return &Batch{d.db.NewBatch()}, nil
}

func (d *Datastore) Close() error {
	if !atomic.CompareAndSwapInt32(&d.status, 0, 1) {
		// already closed, or closing.
		d.wg.Wait()
		return nil
	}
	if d.cache != nil {
		defer d.cache.Unref()
	}
	close(d.closing)
	d.wg.Wait()
	_ = d.db.Flush()
	return d.db.Close()
}

func (d *Datastore) inefficientOrderQuery(ctx context.Context, q query.Query, baseOrder query.Order) (query.Results, error) {
	// Ok, we have a weird order we can't handle. Let's
	// perform the _base_ query (prefix, filter, etc.), then
	// handle sort/offset/limit later.

	// Skip the stuff we can't apply.
	baseQuery := q
	baseQuery.Limit = 0
	baseQuery.Offset = 0
	baseQuery.Orders = nil
	if baseOrder != nil {
		baseQuery.Orders = []query.Order{baseOrder}
	}

	// perform the base query.
	res, err := d.Query(ctx, baseQuery)
	if err != nil {
		return nil, err
	}

	// fix the query
	res = query.ResultsReplaceQuery(res, q)

	// Remove the parts we've already applied.
	naiveQuery := q
	naiveQuery.Prefix = ""
	naiveQuery.Filters = nil

	// Apply the rest of the query
	return query.NaiveQueryApply(naiveQuery, res), nil
}

type Batch struct {
	batch *pebble.Batch
}

var _ ds.Batch = (*Batch)(nil)

func (b *Batch) Put(ctx context.Context, key ds.Key, value []byte) error {
	err := b.batch.Set(key.Bytes(), value, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during set within batch: %w", err)
	}
	return nil
}

func (b *Batch) Delete(ctx context.Context, key ds.Key) error {
	err := b.batch.Delete(key.Bytes(), pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during delete within batch: %w", err)
	}
	return nil
}

func (b *Batch) Commit(ctx context.Context) error {
	return b.batch.Commit(pebble.NoSync)
}
