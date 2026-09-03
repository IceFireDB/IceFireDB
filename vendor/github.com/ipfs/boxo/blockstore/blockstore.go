// Package blockstore implements a thin wrapper over a datastore, giving a
// clean interface for Getting and Putting block objects.
package blockstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/provider"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("blockstore")

// BlockPrefix namespaces blockstore datastores
var BlockPrefix = ds.NewKey("blocks")

// ErrHashMismatch is an error returned when the hash of a block
// is different than expected.
var ErrHashMismatch = errors.New("block in storage has different hash than requested")

// Blockstore wraps a Datastore block-centered methods and provides a layer
// of abstraction which allows to add different caching strategies.
type Blockstore interface {
	DeleteBlock(context.Context, cid.Cid) error
	Has(context.Context, cid.Cid) (bool, error)
	Get(context.Context, cid.Cid) (blocks.Block, error)

	// GetSize returns the CIDs mapped BlockSize
	GetSize(context.Context, cid.Cid) (int, error)

	// Put puts a given block to the underlying datastore
	Put(context.Context, blocks.Block) error

	// PutMany puts a slice of blocks at the same time using batching
	// capabilities of the underlying datastore whenever possible.
	PutMany(context.Context, []blocks.Block) error

	// AllKeysChan returns a channel from which
	// the CIDs in the Blockstore can be read. It should respect
	// the given context, closing the channel if it becomes Done.
	//
	// AllKeysChan treats the underlying blockstore as a set, and returns that
	// set in full. The only guarantee is that the consumer of AKC will
	// encounter every CID in the underlying set, at least once. If the
	// underlying blockstore supports duplicate CIDs it is up to the
	// implementation to elect to return such duplicates or not. Similarly no
	// guarantees are made regarding CID ordering.
	//
	// When underlying blockstore is operating on Multihash and codec information
	// is not preserved, returned CIDs will use Raw (0x55) codec.
	//
	// If enumeration fails partway (for example an I/O error mid-iteration or a
	// cancelled context), the channel may be closed early without warning.
	// Callers MUST NOT assume the returned error reflects enumeration
	// completeness: it is only guaranteed to cover query setup, not
	// mid-iteration failures. Consumers that require a complete enumeration
	// (such as building a Bloom filter) should check whether the blockstore
	// implements [AllKeysChanWithErrer].
	AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
}

// AllKeysChanWithErrer is an optional capability a Blockstore may implement
// alongside AllKeysChan. AllKeysChanWithErr behaves like
// [Blockstore.AllKeysChan], but additionally returns a function that, once the
// returned channel has been fully drained, reports any error that terminated
// enumeration early.
//
// The reported error is nil if enumeration ran to completion without a
// mid-iteration error or cancellation; a non-nil error means enumeration was
// truncated and the delivered keys must not be treated as the complete set.
// Datastore keys that cannot be parsed as a block key are skipped without being
// treated as an error: such a key cannot correspond to a retrievable block, so
// omitting it cannot cause a Bloom-filter false negative.
//
// The function blocks until enumeration finishes, so it must be called only
// after the channel has been drained; calling it earlier deadlocks the caller
// once the producer fills the channel buffer.
type AllKeysChanWithErrer interface {
	AllKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error)
}

// Viewer can be implemented by blockstores that offer zero-copy access to
// values.
//
// Callers of View must not mutate or retain the byte slice, as it could be
// an mmapped memory region, or a pooled byte buffer.
//
// View is especially suitable for deserialising in place.
//
// The callback will only be called iff the query operation is successful (and
// the block is found); otherwise, the error will be propagated. Errors returned
// by the callback will be propagated as well.
type Viewer interface {
	View(ctx context.Context, cid cid.Cid, callback func([]byte) error) error
}

// GCLocker abstract functionality to lock a blockstore when performing
// garbage-collection operations.
type GCLocker interface {
	// GCLock locks the blockstore for garbage collection. No operations
	// that expect to finish with a pin should occur simultaneously.
	// Reading during GC is safe, and requires no lock.
	GCLock(context.Context) Unlocker

	// PinLock locks the blockstore for sequences of puts expected to finish
	// with a pin (before GC). Multiple put->pin sequences can write through
	// at the same time, but no GC should happen simultaneously.
	// Reading during Pinning is safe, and requires no lock.
	PinLock(context.Context) Unlocker

	// GcRequested returns true if GCLock has been called and is waiting to
	// take the lock
	GCRequested(context.Context) bool
}

// GCBlockstore is a blockstore that can safely run garbage-collection
// operations.
type GCBlockstore interface {
	Blockstore
	GCLocker
}

// NewGCBlockstore returns a default implementation of GCBlockstore
// using the given Blockstore and GCLocker.
func NewGCBlockstore(bs Blockstore, gcl GCLocker) GCBlockstore {
	return gcBlockstore{bs, gcl}
}

type gcBlockstore struct {
	Blockstore
	GCLocker
}

// Option is a default implementation Blockstore option
type Option struct {
	f func(bs *blockstore)
}

// WriteThrough skips checking if the blockstore already has a block before
// writing it, when enabled.
func WriteThrough(enabled bool) Option {
	return Option{
		func(bs *blockstore) {
			bs.writeThrough = enabled
		},
	}
}

// NoPrefix avoids wrapping the blockstore into the BlockPrefix namespace
// ("/blocks"), so keys will not be modified in any way.
func NoPrefix() Option {
	return Option{
		func(bs *blockstore) {
			bs.noPrefix = true
		},
	}
}

// Provider allows performing a StartProvide operation for every block written.
func Provider(provider provider.MultihashProvider) Option {
	return Option{
		func(bs *blockstore) {
			logger.Debug("providing-blockstore configured")
			bs.provider = provider
		},
	}
}

// NewBlockstore returns a default Blockstore implementation
// using the provided datastore.Batching backend.
func NewBlockstore(d ds.Batching, opts ...Option) Blockstore {
	bs := &blockstore{
		datastore: d,
	}

	for _, o := range opts {
		o.f(bs)
	}

	if !bs.noPrefix {
		bs.datastore = dsns.Wrap(bs.datastore, BlockPrefix)
	}
	return bs
}

// NewBlockstoreNoPrefix returns a default Blockstore implementation
// using the provided datastore.Batching backend.
// This constructor does not modify input keys in any way
//
// Deprecated: Use NewBlockstore with the NoPrefix option instead.
func NewBlockstoreNoPrefix(d ds.Batching) Blockstore {
	return NewBlockstore(d, NoPrefix())
}

type blockstore struct {
	datastore ds.Batching

	writeThrough bool
	noPrefix     bool
	provider     provider.MultihashProvider
}

func (bs *blockstore) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		logger.Error("undefined cid in blockstore")
		return nil, ipld.ErrNotFound{Cid: k}
	}
	bdata, err := bs.datastore.Get(ctx, dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return nil, ipld.ErrNotFound{Cid: k}
	}
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(bdata, k)
}

func (bs *blockstore) Put(ctx context.Context, block blocks.Block) error {
	k := dshelp.MultihashToDsKey(block.Cid().Hash())

	// Has is cheaper than Put, so see if we already have it
	if !bs.writeThrough {
		exists, err := bs.datastore.Has(ctx, k)
		if err == nil && exists {
			return nil // already stored.
		}
	}
	if err := bs.datastore.Put(ctx, k, block.RawData()); err != nil {
		return err
	}

	if bs.provider != nil {
		logger.Debugf("blockstore: provide %s", block.Cid())
		if err := bs.provider.StartProviding(false, block.Cid().Hash()); err != nil {
			logger.Warnf("blockstore: error while providing %s: %s", block.Cid(), err)
		}
	}
	return nil
}

func (bs *blockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	if len(blocks) == 1 {
		// performance fast-path
		return bs.Put(ctx, blocks[0])
	}

	t, err := bs.datastore.Batch(ctx)
	if err != nil {
		return err
	}
	for _, b := range blocks {
		k := dshelp.MultihashToDsKey(b.Cid().Hash())

		if !bs.writeThrough {
			exists, err := bs.datastore.Has(ctx, k)
			if err == nil && exists {
				continue
			}
		}

		err = t.Put(ctx, k, b.RawData())
		if err != nil {
			return err
		}
	}
	if err := t.Commit(ctx); err != nil {
		return err
	}

	if bs.provider != nil {
		hashes := make([]multihash.Multihash, 0, len(blocks))
		for _, block := range blocks {
			hashes = append(hashes, block.Cid().Hash())
		}
		logger.Debugf("blockstore: provide %d hashes", len(hashes))
		if err := bs.provider.StartProviding(false, hashes...); err != nil {
			logger.Warnf("blockstore: error while providing blocks: %s", err)
		}
	}
	return nil
}

func (bs *blockstore) Has(ctx context.Context, k cid.Cid) (bool, error) {
	return bs.datastore.Has(ctx, dshelp.MultihashToDsKey(k.Hash()))
}

func (bs *blockstore) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	size, err := bs.datastore.GetSize(ctx, dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return -1, ipld.ErrNotFound{Cid: k}
	}
	return size, err
}

func (bs *blockstore) DeleteBlock(ctx context.Context, k cid.Cid) error {
	return bs.datastore.Delete(ctx, dshelp.MultihashToDsKey(k.Hash()))
}

// AllKeysChan runs a query for keys from the blockstore.
// this is very simplistic, in the future, take dsq.Query as a param?
//
// AllKeysChan respects context.
func (bs *blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch, _, err := bs.AllKeysChanWithErr(ctx)
	return ch, err
}

var _ AllKeysChanWithErrer = (*blockstore)(nil)

// AllKeysChanWithErr implements [AllKeysChanWithErrer]. The returned function
// reports any error that ended key enumeration early (nil if every key was
// delivered) and must be called only after the channel has been drained.
func (bs *blockstore) AllKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	// KeysOnly, because that would be _a lot_ of data.
	q := dsq.Query{KeysOnly: true}
	res, err := bs.datastore.Query(ctx, q)
	if err != nil {
		return nil, nil, err
	}

	output := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	var iterErr error
	done := make(chan struct{})
	go func() {
		defer func() {
			res.Close() // ensure exit (signals early exit, too)
			close(output)
			// done is closed after output so a reader that drains output and
			// then calls the returned func observes the fully-written iterErr.
			close(done)
		}()

		for {
			e, ok := res.NextSync()
			if !ok {
				return
			}
			if e.Error != nil {
				iterErr = fmt.Errorf("blockstore.AllKeysChan iteration error: %w", e.Error)
				logger.Error(iterErr)
				return
			}

			// need to convert to key.Key using key.KeyFromDsKey.
			bk, err := dshelp.BinaryFromDsKey(ds.RawKey(e.Key))
			if err != nil {
				// A key that cannot be parsed as a block key cannot correspond
				// to a retrievable block, so skipping it (rather than treating
				// it as a truncating error) cannot cause a Bloom false negative.
				logger.Warnf("error parsing key from binary: %s", err)
				continue
			}
			k := cid.NewCidV1(cid.Raw, bk)
			select {
			case <-ctx.Done():
				iterErr = ctx.Err()
				return
			case output <- k:
			}
		}
	}()

	return output, func() error { <-done; return iterErr }, nil
}

// allKeysChanWithErrFor returns bs.AllKeysChanWithErr when bs implements
// [AllKeysChanWithErrer]. Otherwise it falls back to [Blockstore.AllKeysChan]
// with a no-op error function, preserving best-effort behavior for blockstores
// that cannot report enumeration errors.
func allKeysChanWithErrFor(ctx context.Context, bs Blockstore) (<-chan cid.Cid, func() error, error) {
	if e, ok := bs.(AllKeysChanWithErrer); ok {
		return e.AllKeysChanWithErr(ctx)
	}
	ch, err := bs.AllKeysChan(ctx)
	return ch, func() error { return nil }, err
}

// NewGCLocker returns a default implementation of
// GCLocker using standard [RW] mutexes.
func NewGCLocker() GCLocker {
	return &gclocker{}
}

type gclocker struct {
	lk    sync.RWMutex
	gcreq int32
}

// Unlocker represents an object which can Unlock
// something.
type Unlocker interface {
	Unlock(context.Context)
}

type unlocker struct {
	unlock func()
}

func (u *unlocker) Unlock(_ context.Context) {
	u.unlock()
	u.unlock = nil // ensure its not called twice
}

func (bs *gclocker) GCLock(_ context.Context) Unlocker {
	atomic.AddInt32(&bs.gcreq, 1)
	bs.lk.Lock()
	atomic.AddInt32(&bs.gcreq, -1)
	return &unlocker{bs.lk.Unlock}
}

func (bs *gclocker) PinLock(_ context.Context) Unlocker {
	bs.lk.RLock()
	return &unlocker{bs.lk.RUnlock}
}

func (bs *gclocker) GCRequested(_ context.Context) bool {
	return atomic.LoadInt32(&bs.gcreq) > 0
}
