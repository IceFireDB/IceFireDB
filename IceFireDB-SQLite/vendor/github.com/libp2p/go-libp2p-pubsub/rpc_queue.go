package pubsub

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrQueueCancelled    = errors.New("rpc queue operation cancelled")
	ErrQueueClosed       = errors.New("rpc queue closed")
	ErrQueueFull         = errors.New("rpc queue full")
	ErrQueuePushOnClosed = errors.New("push on closed rpc queue")
)

type priorityQueue struct {
	normal   []*RPC
	priority []*RPC
}

func (q *priorityQueue) Len() int {
	return len(q.normal) + len(q.priority)
}

func (q *priorityQueue) NormalPush(rpc *RPC) {
	q.normal = append(q.normal, rpc)
}

func (q *priorityQueue) PriorityPush(rpc *RPC) {
	q.priority = append(q.priority, rpc)
}

func (q *priorityQueue) Pop() *RPC {
	var rpc *RPC

	if len(q.priority) > 0 {
		rpc = q.priority[0]
		q.priority[0] = nil
		q.priority = q.priority[1:]
	} else if len(q.normal) > 0 {
		rpc = q.normal[0]
		q.normal[0] = nil
		q.normal = q.normal[1:]
	}

	return rpc
}

type rpcQueue struct {
	dataAvailable  sync.Cond
	spaceAvailable sync.Cond
	// Mutex used to access queue
	queueMu sync.Mutex
	queue   priorityQueue

	closed  bool
	maxSize int
}

func newRpcQueue(maxSize int) *rpcQueue {
	q := &rpcQueue{maxSize: maxSize}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	return q
}

func (q *rpcQueue) Push(rpc *RPC, block bool) error {
	return q.push(rpc, false, block)
}

func (q *rpcQueue) UrgentPush(rpc *RPC, block bool) error {
	return q.push(rpc, true, block)
}

func (q *rpcQueue) push(rpc *RPC, urgent bool, block bool) error {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		panic(ErrQueuePushOnClosed)
	}

	for q.queue.Len() == q.maxSize {
		if block {
			q.spaceAvailable.Wait()
			// It can receive a signal because the queue is closed.
			if q.closed {
				panic(ErrQueuePushOnClosed)
			}
		} else {
			return ErrQueueFull
		}
	}
	if urgent {
		q.queue.PriorityPush(rpc)
	} else {
		q.queue.NormalPush(rpc)
	}

	q.dataAvailable.Signal()
	return nil
}

// Note that, when the queue is empty and there are two blocked Pop calls, it
// doesn't mean that the first Pop will get the item from the next Push. The
// second Pop will probably get it instead.
func (q *rpcQueue) Pop(ctx context.Context) (*RPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	unregisterAfterFunc := context.AfterFunc(ctx, func() {
		// Wake up all the waiting routines. The only routine that correponds
		// to this Pop call will return from the function. Note that this can
		// be expensive, if there are too many waiting routines.
		q.dataAvailable.Broadcast()
	})
	defer unregisterAfterFunc()

	for q.queue.Len() == 0 {
		select {
		case <-ctx.Done():
			return nil, ErrQueueCancelled
		default:
		}
		q.dataAvailable.Wait()
		// It can receive a signal because the queue is closed.
		if q.closed {
			return nil, ErrQueueClosed
		}
	}
	rpc := q.queue.Pop()
	q.spaceAvailable.Signal()
	return rpc, nil
}

func (q *rpcQueue) Close() {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	q.closed = true
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}
