package session

import (
	"github.com/gammazero/deque"
	cid "github.com/ipfs/go-cid"
)

type cidQueue struct {
	elems deque.Deque[cid.Cid]
	eset  *cid.Set
}

func newCidQueue() *cidQueue {
	return &cidQueue{eset: cid.NewSet()}
}

func (cq *cidQueue) pop() cid.Cid {
	for {
		if cq.elems.Len() == 0 {
			return cid.Cid{}
		}

		out := cq.elems.PopFront()

		if cq.eset.Has(out) {
			cq.eset.Remove(out)
			return out
		}
	}
}

func (cq *cidQueue) push(c cid.Cid) {
	if cq.eset.Visit(c) {
		cq.elems.PushBack(c)
	}
}

func (cq *cidQueue) remove(c cid.Cid) {
	cq.eset.Remove(c)
}

func (cq *cidQueue) has(c cid.Cid) bool {
	return cq.eset.Has(c)
}

func (cq *cidQueue) len() int {
	return cq.eset.Len()
}

func (cq *cidQueue) gc() {
	if cq.elems.Len() > cq.eset.Len() {
		for i := 0; i < cq.elems.Len(); i++ {
			c := cq.elems.PopFront()
			if cq.eset.Has(c) {
				cq.elems.PushBack(c)
			}
		}
	}
}
