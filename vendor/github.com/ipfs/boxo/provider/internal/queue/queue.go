package queue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gammazero/deque"
	lru "github.com/hashicorp/golang-lru/v2"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	namespace "github.com/ipfs/go-datastore/namespace"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("provider")

const (
	// batchSize is the limit on number of CIDs kept in memory at which there
	// are all written to the datastore.
	batchSize = 16 * 1024
	// dedupCacheSize is the size of the LRU cache used to deduplicate CIDs in
	// the queue.
	dedupCacheSize = 2 * 1024
	// idleWriteTime is the amout of time to check if the queue has been idle
	// (no input or output). If the queue has been idle since the last check,
	// then write all buffered CIDs to the datastore.
	idleWriteTime = time.Minute
	// shutdownTimeout is the duration that Close waits to finish writing CIDs
	// to the datastore.
	shutdownTimeout = 10 * time.Second
)

// Queue provides a FIFO interface to the datastore for storing cids.
//
// CIDs in the process of being provided when a crash or shutdown occurs may be
// in the queue when the node is brought back online depending on whether they
// were fully written to the underlying datastore.
//
// Input to the queue is buffered in memory. The contents of the buffer are
// written to the datastore when the input buffer contains batchSize items, or
// when idleWriteTime has elapsed since the previous batch write or dequeue. CIDs to
// dequeue are read, in order, from the input buffer if there are none in the
// datastore. Otherwise they are read from the datastore.
//
// If queued items are read from the input buffer before it reaches its limit,
// then queued items can remain in memory. When the queue is closed, any
// remaining items in memory are written to the datastore.
type Queue struct {
	close     context.CancelFunc
	closed    chan error
	closeOnce sync.Once
	dequeue   chan cid.Cid
	ds        datastore.Batching
	enqueue   chan cid.Cid
}

// New creates a queue for cids.
func New(ds datastore.Batching) *Queue {
	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		close:   cancel,
		closed:  make(chan error, 1),
		dequeue: make(chan cid.Cid),
		ds:      namespace.Wrap(ds, datastore.NewKey("/queue")),
		enqueue: make(chan cid.Cid),
	}

	go q.worker(ctx)

	return q
}

// Close stops the queue.
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		// Close input queue and wait for worker to finish reading it.
		close(q.enqueue)
		select {
		case <-q.closed:
		case <-time.After(shutdownTimeout):
			q.close() // force immediate shutdown
			err = <-q.closed
		}
		close(q.dequeue) // no more output from this queue
	})
	return err
}

// Enqueue puts a cid in the queue.
func (q *Queue) Enqueue(c cid.Cid) (err error) {
	if c == cid.Undef {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("failed to enqueue CID: shutting down")
		}
	}()
	q.enqueue <- c
	return
}

// Dequeue returns a channel that for reading entries from the queue,
func (q *Queue) Dequeue() <-chan cid.Cid {
	return q.dequeue
}

func makeCidString(c cid.Cid) string {
	data := c.Bytes()
	if len(data) > 4 {
		data = data[len(data)-4:]
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func makeKey(c cid.Cid, counter uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("%020d/%s", counter, makeCidString(c)))
}

// worker run dequeues and enqueues when available.
func (q *Queue) worker(ctx context.Context) {
	defer close(q.closed)

	var (
		c       cid.Cid
		counter uint64
		inBuf   deque.Deque[cid.Cid]
	)

	const baseCap = 1024
	inBuf.SetBaseCap(baseCap)
	k := datastore.Key{}
	dedupCache, _ := lru.New[cid.Cid, struct{}](dedupCacheSize)

	defer func() {
		if c != cid.Undef {
			if err := q.ds.Put(ctx, k, c.Bytes()); err != nil {
				log.Errorw("provider queue: failed to write cid to datastore", "err", err)
			}
			counter++
		}
		if inBuf.Len() != 0 {
			err := q.commitInput(ctx, counter, &inBuf)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error(err)
				if inBuf.Len() != 0 {
					q.closed <- fmt.Errorf("provider queue: %d cids not written to datastore", inBuf.Len())
				}
			}
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
	defer batchTimer.Stop()

	for {
		if c == cid.Undef {
			if !dsEmpty {
				head, err := q.getQueueHead(ctx)
				if err != nil {
					log.Errorw("provider queue: error querying for head of queue, stopping provider", "err", err)
					return
				}
				if head != nil {
					k = datastore.NewKey(head.Key)
					if err = q.ds.Delete(ctx, k); err != nil {
						log.Errorw("provider queue: error deleting queue entry, stopping provider", "err", err, "key", head.Key)
						return
					}
					c, err = cid.Parse(head.Value)
					if err != nil {
						log.Warnw("provider queue: error parsing queue entry cid, removing it from queue", "err", err, "key", head.Key)
						continue
					}
				} else {
					dsEmpty = true
				}
			}
			if dsEmpty && inBuf.Len() != 0 {
				// There were no queued CIDs in the datastore, so read one from
				// the input buffer.
				c = inBuf.PopFront()
				k = makeKey(c, counter)
			}
		}

		// If c != cid.Undef set dequeue and attempt write.
		var dequeue chan cid.Cid
		if c != cid.Undef {
			dequeue = q.dequeue
		}

		select {
		case toQueue, ok := <-readInBuf:
			if !ok {
				return
			}
			if found, _ := dedupCache.ContainsOrAdd(toQueue, struct{}{}); found {
				// update recentness in LRU cache
				dedupCache.Add(toQueue, struct{}{})
				continue
			}
			idle = false

			if c == cid.Undef {
				// Use this CID as the next output since there was nothing in
				// the datastore or buffer previously.
				c = toQueue
				k = makeKey(c, counter)
				continue
			}

			inBuf.PushBack(toQueue)
			if inBuf.Len() >= batchSize {
				commit = true
			}
		case dequeue <- c:
			c = cid.Undef
			idle = false
		case <-batchTimer.C:
			if idle {
				if inBuf.Len() != 0 {
					commit = true
				} else {
					if inBuf.Cap() > baseCap {
						inBuf = deque.Deque[cid.Cid]{}
						inBuf.SetBaseCap(baseCap)
					}
				}
			}
			idle = true
			batchTimer.Reset(idleWriteTime)

		case <-ctx.Done():
			return
		}

		if commit {
			commit = false
			n := inBuf.Len()
			err = q.commitInput(ctx, counter, &inBuf)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("provider queue: error writing CIDs to datastore, stopping provider", "err", err)
				}
				return
			}
			counter += uint64(n)
			dsEmpty = false
		}
	}
}

func (q *Queue) getQueueHead(ctx context.Context) (*query.Entry, error) {
	qry := query.Query{
		Orders: []query.Order{query.OrderByKey{}},
		Limit:  1,
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

func (q *Queue) commitInput(ctx context.Context, counter uint64, cids *deque.Deque[cid.Cid]) error {
	b, err := q.ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}

	cstr := makeCidString(cids.Front())
	for i := range cids.Len() {
		c := cids.At(i)
		key := datastore.NewKey(fmt.Sprintf("%020d/%s", counter, cstr))
		if err = b.Put(ctx, key, c.Bytes()); err != nil {
			log.Errorw("provider queue: failed to add cid to batch", "err", err)
			continue
		}
		counter++
	}
	cids.Clear()

	if err = b.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch to datastore: %w", err)
	}

	return nil
}
