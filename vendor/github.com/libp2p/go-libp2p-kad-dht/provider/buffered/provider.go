package buffered

import (
	"errors"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-dsqueue"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
	mh "github.com/multiformats/go-multihash"
)

var logger = logging.Logger(provider.LoggerName)

const (
	// provideOnceOp represents a one-time provide operation.
	provideOnceOp byte = iota
	// startProvidingOp represents starting continuous providing.
	startProvidingOp
	// forceStartProvidingOp represents forcefully starting providing (overrides existing).
	forceStartProvidingOp
	// stopProvidingOp represents stopping providing.
	stopProvidingOp
	// lastOp is used for array sizing.
	lastOp
)

var _ internal.Provider = (*SweepingProvider)(nil)

// SweepingProvider (buffered) is a wrapper around a SweepingProvider buffering
// requests, to allow core operations to return instantly. Operations are
// queued and processed asynchronously in batches for improved performance.
type SweepingProvider struct {
	closeOnce sync.Once
	done      chan struct{}
	closed    chan struct{}

	newItems  chan struct{}
	provider  internal.Provider
	queue     *dsqueue.DSQueue
	batchSize int
}

// New creates a new SweepingProvider that wraps the given provider with
// buffering capabilities. Operations are queued and processed asynchronously
// in batches for improved performance.
func New(prov internal.Provider, ds datastore.Batching, opts ...Option) *SweepingProvider {
	cfg := getOpts(opts)
	s := &SweepingProvider{
		done:   make(chan struct{}),
		closed: make(chan struct{}),

		newItems: make(chan struct{}, 1),
		provider: prov,
		queue: dsqueue.New(ds, cfg.dsName,
			dsqueue.WithDedupCacheSize(0), // disable deduplication
			dsqueue.WithIdleWriteTime(cfg.idleWriteTime),
		),
		batchSize: cfg.batchSize,
	}
	go s.worker()
	return s
}

// Close stops the provider and releases all resources.
//
// It waits for the worker goroutine to finish processing current operations
// and closes the underneath provider. The queue current state is persisted on
// the datastore.
func (s *SweepingProvider) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = errors.Join(s.queue.Close(), s.provider.Close())
		<-s.done
	})
	return err
}

// toBytes serializes an operation and multihash into a byte slice for storage.
func toBytes(op byte, key mh.Multihash) []byte {
	return append([]byte{op}, key...)
}

// fromBytes deserializes a byte slice back into an operation and multihash.
func fromBytes(data []byte) (byte, mh.Multihash, error) {
	op := data[0]
	h, err := mh.Cast(data[1:])
	return op, h, err
}

// getOperations processes a batch of dequeued operations and groups them by
// type.
//
// It discards multihashes from the `StopProviding` operation if
// `StartProviding` was called after `StopProviding` for the same multihash.
func getOperations(dequeued [][]byte) ([][]mh.Multihash, error) {
	stopProv := make(map[string]struct{})
	ops := [lastOp - 1][]mh.Multihash{} // don't store stop ops

	for _, bs := range dequeued {
		op, h, err := fromBytes(bs)
		if err != nil {
			return nil, err
		}
		switch op {
		case provideOnceOp:
			ops[provideOnceOp] = append(ops[provideOnceOp], h)
		case startProvidingOp, forceStartProvidingOp:
			delete(stopProv, string(h))
			ops[op] = append(ops[op], h)
		case stopProvidingOp:
			stopProv[string(h)] = struct{}{}
		}
	}
	stopOps := make([]mh.Multihash, 0, len(stopProv))
	for hstr := range stopProv {
		stopOps = append(stopOps, mh.Multihash(hstr))
	}
	return append(ops[:], stopOps), nil
}

// executeOperation executes a provider operation on the underlying provider
// with the given multihashes, logging any errors encountered.
func executeOperation(f func(...mh.Multihash) error, keys []mh.Multihash) {
	if len(keys) == 0 {
		return
	}
	if err := f(keys...); err != nil {
		logger.Warn(err)
	}
}

// worker processes operations from the queue in batches.
// It runs in a separate goroutine and continues until the provider is closed.
func (s *SweepingProvider) worker() {
	defer close(s.done)
	var emptyQueue bool
	for {
		if emptyQueue {
			select {
			case <-s.closed:
				return
			case <-s.newItems:
			}
			emptyQueue = false
		} else {
			select {
			case <-s.closed:
				return
			case <-s.newItems:
			default:
			}
		}

		res, err := s.queue.GetN(s.batchSize)
		if err != nil {
			logger.Warnf("BufferedSweepingProvider unable to dequeue: %v", err)
			continue
		}
		if len(res) < s.batchSize {
			// Queue was fully drained.
			emptyQueue = true
		}
		ops, err := getOperations(res)
		if err != nil {
			logger.Warnf("BufferedSweepingProvider unable to parse dequeued item: %v", err)
			continue
		}
		// Execute the 4 kinds of queued provider operations on the underlying
		// provider.

		// Process `StartProviding` (force=true) ops first, so that if
		// `StartProviding` (force=false) is called after, there is no need to
		// enqueue the multihash a second time to the provide queue.
		executeOperation(func(keys ...mh.Multihash) error { return s.provider.StartProviding(true, keys...) }, ops[forceStartProvidingOp])
		executeOperation(func(keys ...mh.Multihash) error { return s.provider.StartProviding(false, keys...) }, ops[startProvidingOp])
		executeOperation(s.provider.ProvideOnce, ops[provideOnceOp])
		// Process `StopProviding` last, so that multihashes that should have been
		// provided, and then stopped provided in the same batch are provided only
		// once. Don't `StopProviding` multihashes, for which `StartProviding` has
		// been called after `StopProviding`.
		executeOperation(s.provider.StopProviding, ops[stopProvidingOp])
	}
}

// enqueue adds operations to the queue for asynchronous processing.
func (s *SweepingProvider) enqueue(op byte, keys ...mh.Multihash) error {
	for _, h := range keys {
		if err := s.queue.Put(toBytes(op, h)); err != nil {
			return err
		}
	}
	select {
	case s.newItems <- struct{}{}:
	default:
	}
	return nil
}

// ProvideOnce enqueues multihashes for which the provider will send provider
// records out only once to the DHT swarm. It does NOT take the responsibility
// to reprovide these keys.
//
// Returns immediately after enqueuing the keys, the actual provide operation
// happens asynchronously. Returns an error if the multihashes couldn't be
// enqueued.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) error {
	return s.enqueue(provideOnceOp, keys...)
}

// StartProviding adds the supplied keys to the queue of keys that will be
// provided to the DHT swarm unless they were already provided in the past. The
// keys will be periodically reprovided until StopProviding is called for the
// same keys or the keys are removed from the Keystore.
//
// If force is true, the keys are provided to the DHT swarm regardless of
// whether they were already being reprovided in the past.
//
// Returns immediately after enqueuing the keys, the actual provide operation
// happens asynchronously. Returns an error if the multihashes couldn't be
// enqueued.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	op := startProvidingOp
	if force {
		op = forceStartProvidingOp
	}
	return s.enqueue(op, keys...)
}

// StopProviding adds the supplied multihashes to the BufferedSweepingProvider
// queue, to stop reproviding the given keys to the DHT swarm.
//
// The node stops being referred as a provider when the provider records in the
// DHT swarm expire.
//
// Returns immediately after enqueuing the keys, the actual provide operation
// happens asynchronously. Returns an error if the multihashes couldn't be
// enqueued.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) error {
	return s.enqueue(stopProvidingOp, keys...)
}

// Clear clears the all the keys from the provide queue and returns the number
// of keys that were cleared.
//
// The keys are not deleted from the keystore, so they will continue to be
// reprovided as scheduled.
func (s *SweepingProvider) Clear() int {
	return s.provider.Clear()
}

// RefreshSchedule scans the KeyStore for any keys that are not currently
// scheduled for reproviding. If such keys are found, it schedules their
// associated keyspace region to be reprovided.
//
// This function doesn't remove prefixes that have no keys from the schedule.
// This is done automatically during the reprovide operation if a region has no
// keys.
//
// Returns an error if the provider is closed or if the node is currently
// Offline (either never bootstrapped, or disconnected since more than
// `OfflineDelay`). The schedule depends on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) RefreshSchedule() error {
	return s.provider.RefreshSchedule()
}
