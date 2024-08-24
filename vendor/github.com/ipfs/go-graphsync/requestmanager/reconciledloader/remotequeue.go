package reconciledloader

import (
	"sync"

	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
)

var linkedRemoteItemPool = sync.Pool{
	New: func() interface{} {
		return new(remotedLinkedItem)
	},
}

type remoteItem struct {
	link      cid.Cid
	action    graphsync.LinkAction
	block     []byte
	traceLink trace.Link
}

type remotedLinkedItem struct {
	remoteItem
	next *remotedLinkedItem
}

func newRemote() *remotedLinkedItem {
	newItem := linkedRemoteItemPool.Get().(*remotedLinkedItem)
	// need to reset next value to nil we're pulling out of a pool of potentially
	// old objects
	newItem.next = nil
	return newItem
}

func freeList(remoteItems []*remotedLinkedItem) {
	for _, ri := range remoteItems {
		ri.block = nil
		linkedRemoteItemPool.Put(ri)
	}
}

type remoteQueue struct {
	head     *remotedLinkedItem
	tail     *remotedLinkedItem
	dataSize uint64
	// we hold a reference to the last consumed item in order to
	// allow us to retry while online
	lastConsumed *remotedLinkedItem
}

func (rq *remoteQueue) empty() bool {
	return rq.head == nil
}

func (rq *remoteQueue) first() remoteItem {
	if rq.head == nil {
		return remoteItem{}
	}

	return rq.head.remoteItem
}

// retry last will put the last consumed item back in the queue at the front
func (rq *remoteQueue) retryLast() {
	if rq.lastConsumed != nil {
		rq.head = rq.lastConsumed
	}
}

func (rq *remoteQueue) consume() uint64 {
	// release and clear the previous last consumed item
	if rq.lastConsumed != nil {
		linkedRemoteItemPool.Put(rq.lastConsumed)
		rq.lastConsumed = nil
	}
	// update our total data size buffered
	rq.dataSize -= uint64(len(rq.head.block))
	// wipe the block reference -- if its been consumed, its saved
	// to local store, and we don't need it - let the memory get freed
	rq.head.block = nil

	// we hold the last consumed, minus the block, around so we can retry
	rq.lastConsumed = rq.head

	// advance the queue
	rq.head = rq.head.next
	return rq.dataSize
}

func (rq *remoteQueue) clear() {
	for rq.head != nil {
		rq.consume()
	}
	// clear any last consumed reference left over
	if rq.lastConsumed != nil {
		linkedRemoteItemPool.Put(rq.lastConsumed)
		rq.lastConsumed = nil
	}
}

func (rq *remoteQueue) queue(newItems []*remotedLinkedItem) uint64 {
	for _, newItem := range newItems {
		// update total size buffered

		// TODO: this is a good place to hold off on accepting data
		// to let the local traversal catch up
		// a second enqueue/dequeue signal would allow us
		// to make this call block until datasize dropped below a certain amount
		rq.dataSize += uint64(len(newItem.block))
		if rq.head == nil {
			rq.tail = newItem
			rq.head = rq.tail
		} else {
			rq.tail.next = newItem
			rq.tail = rq.tail.next
		}
	}
	return rq.dataSize
}
