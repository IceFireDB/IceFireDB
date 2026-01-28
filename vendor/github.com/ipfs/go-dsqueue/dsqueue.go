package dsqueue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/deque"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dsqueue")

// DSQueue provides a FIFO interface to the datastore for storing items.
//
// Items in the process of being provided when a crash or shutdown occurs may
// be in the queue when the node is brought back online depending on whether
// they were fully written to the underlying datastore.
//
// Input to the queue is buffered in memory. The contents of the buffer are
// written to the datastore when the input buffer is full (see
// [WithBufferSize]), or when the queue has been idle for some time (see
// [WithIdleWriteTime]) since the previous batch write or dequeue. Items to
// dequeue are read, in order, from the input buffer if there are none in the
// datastore. Otherwise they are read from the datastore.
//
// If queued items are read from the input buffer before it reaches its limit,
// then queued items can remain in memory. When the queue is closed, any
// remaining items in memory are written to the datastore.
type DSQueue struct {
	cancel       context.CancelFunc
	closed       chan error
	closeOnce    sync.Once
	dequeue      chan []byte
	ds           datastore.Batching
	enqueue      chan []byte
	clear        chan chan<- int
	closeTimeout time.Duration
	getn         chan getRequest
	name         string
}

// New creates a queue for strings.
func New(ds datastore.Batching, name string, options ...Option) *DSQueue {
	cfg := getOpts(options)

	ctx, cancel := context.WithCancel(context.Background())

	q := &DSQueue{
		cancel:       cancel,
		closed:       make(chan error, 1),
		dequeue:      make(chan []byte),
		ds:           namespace.Wrap(ds, datastore.NewKey("/dsq-"+name)),
		enqueue:      make(chan []byte),
		clear:        make(chan chan<- int),
		closeTimeout: cfg.closeTimeout,
		getn:         make(chan getRequest),
		name:         name,
	}

	go q.worker(ctx, cfg.bufferSize, cfg.dedupCacheSize, cfg.idleWriteTime)

	return q
}

// Close stops the queue.
func (q *DSQueue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		// Close input queue and wait for worker to finish reading it.
		close(q.enqueue)
		var timeoutCh <-chan time.Time
		if q.closeTimeout != 0 {
			timeout := time.NewTimer(q.closeTimeout)
			defer timeout.Stop()
			timeoutCh = timeout.C
		}
		select {
		case <-q.closed:
		case <-timeoutCh:
			q.cancel() // force immediate shutdown
			err = <-q.closed
		}
		close(q.dequeue) // no more output from this queue
	})
	return err
}

// Put puts an item into the queue.
func (q *DSQueue) Put(item []byte) (err error) {
	if len(item) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s queue closed", q.name)
		}
	}()
	q.enqueue <- item
	return
}

type getRequest struct {
	n   int
	rsp chan getResponse
}

type getResponse struct {
	items [][]byte
	err   error
}

// GetN retrieves up to n items that are currently available in the queue. If
// there are no items currently available, then none are returned and GetN does
// not wait for any.
//
// GetN is used to poll the DSQueue for items and return batches of those
// items. This is the most efficient way of fetching currently available items.
//
// GetN and Out can both be used to read items from the DSQueue, but they
// should not be used concurrently as items will be returned by one or the
// other indeterminately.
func (q *DSQueue) GetN(n int) ([][]byte, error) {
	if n == 0 {
		return nil, nil
	}
	// Buffer response channel so write can happen even if reader gone.
	rsp := make(chan getResponse, 1)
	req := getRequest{
		n:   n,
		rsp: rsp,
	}

	select {
	case q.getn <- req:
	case <-q.closed:
		return nil, fmt.Errorf("%s queue closed", q.name)
	}

	getRsp := <-rsp
	return getRsp.items, getRsp.err
}

// Out returns a channel that for reading entries from the queue,
func (q *DSQueue) Out() <-chan []byte {
	return q.dequeue
}

// Clear clears all queued records from memory and the datastore. Returns the
// number of items removed from the queue.
func (q *DSQueue) Clear() int {
	rsp := make(chan int)
	q.clear <- rsp
	return <-rsp
}

// Name returns the name of this DSQueue instance.
func (q *DSQueue) Name() string {
	return q.name
}

func makeKey(item []byte, counter uint64) datastore.Key {
	b64Item := base64.RawURLEncoding.EncodeToString(item)
	return datastore.NewKey(fmt.Sprintf("%016x/%s", counter, b64Item))
}

// worker run dequeues and enqueues when available.
func (q *DSQueue) worker(ctx context.Context, bufferSize, dedupCacheSize int, idleWriteTime time.Duration) {
	defer close(q.closed)

	var (
		item    []byte
		counter uint64
		inBuf   deque.Deque[[]byte]
	)

	const baseCap = 1024
	inBuf.SetBaseCap(baseCap)
	k := datastore.Key{}
	var dedupCache *lru.Cache[string, struct{}]
	if dedupCacheSize != 0 {
		dedupCache, _ = lru.New[string, struct{}](dedupCacheSize)
	}

	defer func() {
		if item != nil {
			// Write the item directly, instead of pushing it to the front of
			// inbuf, in order to retain it's original kay, and therefore the
			// order in the datastore, which may not be empty.
			if err := q.ds.Put(ctx, k, nil); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("failed to write item to datastore", "err", err, "qname", q.name)
				}
				q.closed <- fmt.Errorf("%d items not written to datastore", 1+inBuf.Len())
				return
			}
		}
		if inBuf.Len() != 0 {
			err := q.commitInput(ctx, counter, &inBuf)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("error writing items to datastore", "err", err, "qname", q.name)
				}
				if inBuf.Len() != 0 {
					q.closed <- fmt.Errorf("%d items not written to datastore", inBuf.Len())
				}
			}
		}
		if err := q.ds.Sync(ctx, datastore.NewKey("")); err != nil {
			log.Errorw("failed to sync datastore", "err", err, "qname", q.name)
		}
	}()

	var (
		commit  bool
		dsEmpty bool
		err     error
		idle    bool
	)

	readInBuf := q.enqueue

	batchTimer := time.NewTimer(idleWriteTime)
	if idleWriteTime == 0 {
		batchTimer.Stop()
	} else {
		defer batchTimer.Stop()
	}

	for {
		if item == nil {
			if !dsEmpty {
				head, err := q.getQueueHead(ctx)
				if err != nil {
					log.Errorw("error querying for head of queue, stopping dsqueue", "err", err, "qname", q.name)
					return
				}
				if head != nil {
					k = datastore.NewKey(head.Key)
					if err = q.ds.Delete(ctx, k); err != nil {
						log.Errorw("error deleting queue entry, stopping dsqueue", "err", err, "key", head.Key, "qname", q.name)
						return
					}
					item, err = decodeItem(head.Key)
					if err != nil {
						log.Errorw(err.Error(), "qname", q.name)
						continue
					}
				} else {
					dsEmpty = true
				}
			}
			if dsEmpty && inBuf.Len() != 0 {
				// There were no queued CIDs in the datastore, so read one from
				// the input buffer.
				item = inBuf.PopFront()
				k = makeKey(item, counter)
				counter++
			}
		}

		// If c != cid.Undef set dequeue and attempt write.
		var dequeue chan []byte
		if item != nil {
			dequeue = q.dequeue
		}

		select {
		case toQueue, ok := <-readInBuf:
			if !ok {
				return
			}
			if dedupCache != nil {
				cacheItem := string(toQueue)
				if found, _ := dedupCache.ContainsOrAdd(cacheItem, struct{}{}); found {
					// update recentness in LRU cache
					dedupCache.Add(cacheItem, struct{}{})
					continue
				}
			}
			idle = false

			if item == nil {
				// Use this CID as the next output since there was nothing in
				// the datastore or buffer previously.
				item = toQueue
				k = makeKey(item, counter)
				counter++
				continue
			}

			inBuf.PushBack(toQueue)
			if bufferSize != 0 && inBuf.Len() >= bufferSize {
				commit = true
			}
		case getRequest := <-q.getn:
			n := getRequest.n
			rspChan := getRequest.rsp
			var outItems [][]byte

			if item != nil {
				outItems = append(outItems, item)

				if !dsEmpty {
					outItems, err = q.readDatastore(ctx, n-len(outItems), outItems)
					if err != nil {
						rspChan <- getResponse{
							err: err,
						}
						continue
					}
				}

				item = nil
				idle = false
			}
			if len(outItems) < n {
				for itm := range inBuf.IterPopFront() {
					outItems = append(outItems, itm)
					if len(outItems) == n {
						break
					}
				}
			}
			rspChan <- getResponse{
				items: outItems,
			}

		case dequeue <- item:
			item = nil
			idle = false

		case <-batchTimer.C:
			if idle {
				if inBuf.Len() != 0 {
					commit = true
				} else {
					if inBuf.Cap() > baseCap {
						inBuf = deque.Deque[[]byte]{}
						inBuf.SetBaseCap(baseCap)
					}
				}
			}
			idle = true
			batchTimer.Reset(idleWriteTime)

		case rsp := <-q.clear:
			var rmMemCount int
			if item != nil {
				rmMemCount = 1
			}
			item = nil
			k = datastore.Key{}
			idle = false
			rmMemCount += inBuf.Len()
			inBuf.Clear()
			if dedupCache != nil {
				dedupCache.Purge()
			}
			rmDSCount, err := q.clearDatastore(ctx)
			if err != nil {
				log.Errorw("cannot clear datastore", "err", err, "qname", q.name)
			} else {
				dsEmpty = true
			}
			log.Infow("cleared dsqueue", "fromMemory", rmMemCount, "fromDatastore", rmDSCount, "qname", q.name)
			rsp <- rmMemCount + rmDSCount
		}

		if commit {
			commit = false
			n := inBuf.Len()
			err = q.commitInput(ctx, counter, &inBuf)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("error writing items to datastore, stopping dsqueue", "err", err, "qname", q.name)
				}
				return
			}
			counter += uint64(n)
			dsEmpty = false
		}
	}
}

func (q *DSQueue) clearDatastore(ctx context.Context) (int, error) {
	qry := query.Query{
		KeysOnly: true,
	}
	results, err := q.ds.Query(ctx, qry)
	if err != nil {
		return 0, fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	batch, err := q.ds.Batch(ctx)
	if err != nil {
		return 0, fmt.Errorf("cannot create datastore batch: %w", err)
	}

	var rmCount int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		if err = batch.Delete(ctx, datastore.NewKey(result.Key)); err != nil {
			return 0, fmt.Errorf("cannot delete key from datastore: %w", err)
		}
		rmCount++
	}

	if err = batch.Commit(ctx); err != nil {
		return 0, fmt.Errorf("cannot commit datastore updated: %w", err)
	}
	if err = q.ds.Sync(ctx, datastore.NewKey("")); err != nil {
		return 0, fmt.Errorf("cannot sync datastore: %w", err)
	}

	return rmCount, nil
}

func (q *DSQueue) getQueueHead(ctx context.Context) (*query.Entry, error) {
	qry := query.Query{
		KeysOnly: true,
		Orders:   []query.Order{query.OrderByKey{}},
		Limit:    1,
	}
	results, err := q.ds.Query(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer results.Close()
	r, ok := results.NextSync()
	if !ok {
		return nil, nil
	}

	return &r.Entry, r.Error
}

func (q *DSQueue) commitInput(ctx context.Context, counter uint64, items *deque.Deque[[]byte]) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	b, err := q.ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}

	for item := range items.Iter() {
		key := makeKey(item, counter)
		if err = b.Put(ctx, key, nil); err != nil {
			log.Errorw("failed to add item to batch", "err", err, "qname", q.name)
			continue
		}
		counter++
	}

	items.Clear()

	if err = b.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch to datastore: %w", err)
	}

	return nil
}

// readDatastore reads at most n items from the data store queue, in order, and
// appends them to items slice. Items are batch-deleted from the datastore as
// they are read. The modified items slice is returned.
func (q *DSQueue) readDatastore(ctx context.Context, n int, items [][]byte) ([][]byte, error) {
	qry := query.Query{
		KeysOnly: true,
		Orders:   []query.Order{query.OrderByKey{}},
		Limit:    n,
	}
	results, err := q.ds.Query(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	batch, err := q.ds.Batch(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot create datastore batch: %w", err)
	}

	for result := range results.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if result.Error != nil {
			return nil, result.Error
		}

		if err = batch.Delete(ctx, datastore.NewKey(result.Key)); err != nil {
			return nil, fmt.Errorf("error deleting queue item: %w", err)
		}

		item, err := decodeItem(result.Key)
		if err != nil {
			log.Errorw(err.Error(), "qname", q.name)
			continue
		}
		items = append(items, item)
	}

	if err = batch.Commit(ctx); err != nil {
		return nil, fmt.Errorf("cannot commit datastore updated: %w", err)
	}
	if err = q.ds.Sync(ctx, datastore.NewKey("")); err != nil {
		return nil, fmt.Errorf("cannot sync datastore: %w", err)
	}

	return items, nil
}

func decodeItem(key string) ([]byte, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, "/"), "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("malformed queued item %q", key)
	}
	item, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("cannot decode queued item %q: %s", key, err)
	}
	return item, nil
}
