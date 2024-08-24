package reconciledloader

import (
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
)

// IngestResponse ingests new remote items into the reconciled loader
func (rl *ReconciledLoader) IngestResponse(md graphsync.LinkMetadata, traceLink trace.Link, blocks map[cid.Cid][]byte) {
	if md.Length() == 0 {
		return
	}
	duplicates := make(map[cid.Cid]struct{}, md.Length())
	items := make([]*remotedLinkedItem, 0, md.Length())
	md.Iterate(func(link cid.Cid, action graphsync.LinkAction) {
		newItem := newRemote()
		newItem.link = link
		newItem.action = action
		if action == graphsync.LinkActionPresent {
			if _, isDuplicate := duplicates[link]; !isDuplicate {
				duplicates[link] = struct{}{}
				newItem.block = blocks[link]
			}
		}
		newItem.traceLink = traceLink
		items = append(items, newItem)
	})
	rl.lock.Lock()

	// refuse to queue items when the request is ofline
	if !rl.open {
		// don't hold block memory if we're dropping these
		freeList(items)
		rl.lock.Unlock()
		return
	}

	buffered := rl.remoteQueue.queue(items)
	rl.signal.Signal()
	rl.lock.Unlock()

	log.Debugw("injested blocks for new response", "request_id", rl.requestID, "total_queued_bytes", buffered)
}
