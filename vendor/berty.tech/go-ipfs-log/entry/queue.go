package entry

import (
	"container/heap"

	cid "github.com/ipfs/go-cid"
)

type processQueue interface {
	Add(index int, hash cid.Cid)
	Next() cid.Cid
	Len() int
}

func newProcessQueue() processQueue {
	var items priorityQueue = []*item{}
	heap.Init(&items)
	return &items
}

type item struct {
	hash  cid.Cid
	index int
}

// processHashQueue is not thread safe
type priorityQueue []*item

func (pq *priorityQueue) Add(index int, hash cid.Cid) {
	heap.Push(pq, &item{
		hash:  hash,
		index: index,
	})
	// *pq = append(*pq, hash...)
}

func (pq *priorityQueue) Next() (hash cid.Cid) {
	item := heap.Pop(pq).(*item)
	return item.hash
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*item)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].index < pq[j].index
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// func (pq processHashQueue) GetQueue() []processItem {
// 	return pq
// }
