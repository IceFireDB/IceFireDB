package reconciledloader

import (
	"context"
	"io/ioutil"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

// BlockReadOpener synchronously loads the next block result
// as long as the request is online, it will wait for more remote items until it can load this link definitively
// once the request is offline
func (rl *ReconciledLoader) BlockReadOpener(lctx linking.LinkContext, link datamodel.Link) types.AsyncLoadResult {
	if !rl.mostRecentLoadAttempt.empty() {
		// since we aren't retrying the most recent load, it's time to record it in the traversal record
		rl.traversalRecord.RecordNextStep(
			rl.mostRecentLoadAttempt.linkContext.LinkPath.Segments(),
			rl.mostRecentLoadAttempt.link.(cidlink.Link).Cid,
			rl.mostRecentLoadAttempt.successful,
		)
		rl.mostRecentLoadAttempt = loadAttempt{}
	}

	// the private method does the actual loading, while this wrapper simply does the record keeping
	usedRemote, result := rl.blockReadOpener(lctx, link)

	// now, we cache to allow a retry if we're offline
	rl.mostRecentLoadAttempt.link = link
	rl.mostRecentLoadAttempt.linkContext = lctx
	rl.mostRecentLoadAttempt.successful = result.Err == nil
	rl.mostRecentLoadAttempt.usedRemote = usedRemote
	return result
}

func (rl *ReconciledLoader) blockReadOpener(lctx linking.LinkContext, link datamodel.Link) (usedRemote bool, result types.AsyncLoadResult) {

	// catch up the remore or determine that we are offline
	hasRemoteData, err := rl.waitRemote()
	if err != nil {
		return false, types.AsyncLoadResult{Err: err, Local: !hasRemoteData}
	}

	// if we're offline just load local
	if !hasRemoteData {
		return false, rl.loadLocal(lctx, link)
	}

	// only attempt remote load if after reconciliation we're not on a missing path
	if !rl.pathTracker.stillOnUnfollowedRemotePath(lctx.LinkPath) {
		data, err := rl.loadRemote(lctx, link)
		if data != nil {
			return true, types.AsyncLoadResult{Data: data, Local: false}
		}
		if err != nil {
			return true, types.AsyncLoadResult{Err: err, Local: false}
		}
	}
	// remote had missing or duplicate block, attempt load local
	return true, rl.loadLocal(lctx, link)
}

func (rl *ReconciledLoader) loadLocal(lctx linking.LinkContext, link datamodel.Link) types.AsyncLoadResult {
	stream, err := rl.lsys.StorageReadOpener(lctx, link)
	if err != nil {
		return types.AsyncLoadResult{Err: graphsync.RemoteMissingBlockErr{Link: link, Path: lctx.LinkPath}, Local: true}
	}
	// skip a stream copy if it's not needed
	if br, ok := stream.(byteReader); ok {
		return types.AsyncLoadResult{Data: br.Bytes(), Local: true}
	}
	localData, err := ioutil.ReadAll(stream)
	if err != nil {
		return types.AsyncLoadResult{Err: graphsync.RemoteMissingBlockErr{Link: link, Path: lctx.LinkPath}, Local: true}
	}
	return types.AsyncLoadResult{Data: localData, Local: true}
}

func (rl *ReconciledLoader) loadRemote(lctx linking.LinkContext, link datamodel.Link) ([]byte, error) {
	rl.lock.Lock()
	head := rl.remoteQueue.first()
	buffered := rl.remoteQueue.consume()
	rl.lock.Unlock()

	// verify it matches the expected next load
	if !head.link.Equals(link.(cidlink.Link).Cid) {
		return nil, graphsync.RemoteIncorrectResponseError{
			LocalLink:  link,
			RemoteLink: cidlink.Link{Cid: head.link},
			Path:       lctx.LinkPath,
		}
	}

	// update path tracking
	rl.pathTracker.recordRemoteLoadAttempt(lctx.LinkPath, head.action)

	// if block == nil,
	// it can mean:
	// - metadata had a Missing Action (Block is missing)
	// - metadata had a Present Action but no block data in message
	// - block appeared twice in metadata for a single message. During
	//   InjestResponse we decided to hold on to block data only for the
	//   first metadata instance
	// Regardless, when block == nil, we need to simply try to load form local
	// datastore
	if head.block == nil {
		return nil, nil
	}

	// get a context
	ctx := lctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// start a span
	_, span := otel.Tracer("graphsync").Start(
		ctx,
		"verifyBlock",
		trace.WithLinks(head.traceLink),
		trace.WithAttributes(attribute.String("cid", link.String())))
	defer span.End()

	log.Debugw("verified block", "request_id", rl.requestID, "total_queued_bytes", buffered)

	// save the block
	buffer, committer, err := rl.lsys.StorageWriteOpener(lctx)
	if err != nil {
		return nil, err
	}
	if settable, ok := buffer.(settableWriter); ok {
		err = settable.SetBytes(head.block)
	} else {
		_, err = buffer.Write(head.block)
	}
	if err != nil {
		return nil, err
	}
	err = committer(link)
	if err != nil {
		return nil, err
	}

	// return the block
	return head.block, nil
}

func (rl *ReconciledLoader) waitRemote() (bool, error) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	for {
		// Case 1 item is  waiting
		if !rl.remoteQueue.empty() {
			if rl.verifier == nil || rl.verifier.Done() {
				rl.verifier = nil
				return true, nil
			}
			path := rl.verifier.CurrentPath()
			head := rl.remoteQueue.first()
			rl.remoteQueue.consume()
			err := rl.verifier.VerifyNext(head.link, head.action.DidFollowLink())
			if err != nil {
				return true, err
			}
			rl.pathTracker.recordRemoteLoadAttempt(path, head.action)
			continue

		}

		// Case 2 no available item and channel is closed
		if !rl.open {
			return false, nil
		}

		// Case 3 nothing available, wait for more items
		rl.signal.Wait()
	}
}
