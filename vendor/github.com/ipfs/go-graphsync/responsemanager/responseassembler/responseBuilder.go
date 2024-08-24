package responseassembler

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/messagequeue"
)

var log = logging.Logger("graphsync")

type responseOperation interface {
	build(builder *messagequeue.Builder)
	size() uint64
}

type responseBuilder struct {
	ctx         context.Context
	requestID   graphsync.RequestID
	operations  []responseOperation
	linkTracker *peerLinkTracker
}

func (rb *responseBuilder) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	op := rb.setupBlockOperation(link, data)
	rb.operations = append(rb.operations, op)
	return op.Block()
}

func (rb *responseBuilder) SendExtensionData(extension graphsync.ExtensionData) {
	rb.operations = append(rb.operations, extensionOperation{rb.requestID, extension})
}

func (rb *responseBuilder) FinishRequest() graphsync.ResponseStatusCode {
	op := rb.setupFinishOperation()
	rb.operations = append(rb.operations, op)
	return op.status
}

func (rb *responseBuilder) FinishWithError(status graphsync.ResponseStatusCode) {
	rb.operations = append(rb.operations, rb.setupFinishWithErrOperation(status))
}

func (rb *responseBuilder) PauseRequest() {
	rb.operations = append(rb.operations, statusOperation{rb.requestID, graphsync.RequestPaused})
}

// SendUpdates sets up a PartialResponse with just the extension data provided
func (rb *responseBuilder) SendUpdates(extensions []graphsync.ExtensionData) {
	for _, extension := range extensions {
		rb.SendExtensionData(extension)
	}
	rb.operations = append(rb.operations, statusOperation{rb.requestID, graphsync.PartialResponse})
}

func (rb *responseBuilder) Context() context.Context {
	return rb.ctx
}

func (rb *responseBuilder) setupBlockOperation(
	link ipld.Link, data []byte) blockOperation {
	hasBlock := data != nil
	send, index := rb.linkTracker.RecordLinkTraversal(rb.requestID, link, hasBlock)
	return blockOperation{
		data, send, link, rb.requestID, index,
	}
}

func (rb *responseBuilder) setupFinishOperation() statusOperation {
	isComplete := rb.linkTracker.FinishTracking(rb.requestID)
	var status graphsync.ResponseStatusCode
	if isComplete {
		status = graphsync.RequestCompletedFull
	} else {
		status = graphsync.RequestCompletedPartial
	}
	return statusOperation{rb.requestID, status}
}

func (rb *responseBuilder) setupFinishWithErrOperation(status graphsync.ResponseStatusCode) statusOperation {
	rb.linkTracker.FinishTracking(rb.requestID)
	return statusOperation{rb.requestID, status}
}

type statusOperation struct {
	requestID graphsync.RequestID
	status    graphsync.ResponseStatusCode
}

func (fo statusOperation) build(builder *messagequeue.Builder) {
	builder.AddResponseCode(fo.requestID, fo.status)
}

func (fo statusOperation) size() uint64 {
	return 0
}

type extensionOperation struct {
	requestID graphsync.RequestID
	extension graphsync.ExtensionData
}

func (eo extensionOperation) build(builder *messagequeue.Builder) {
	builder.AddExtensionData(eo.requestID, eo.extension)
}

func (eo extensionOperation) size() uint64 {
	if eo.extension.Data == nil {
		return 0
	}
	// any erorr produced by this call will be picked up during actual encode, so
	// we can defer handling till then and let it be zero for now
	len, _ := dagcbor.EncodedLength(eo.extension.Data)
	return uint64(len)
}

type blockOperation struct {
	data      []byte
	sendBlock bool
	link      ipld.Link
	requestID graphsync.RequestID
	index     int64
}

func (bo blockOperation) build(builder *messagequeue.Builder) {
	if bo.sendBlock {
		cidLink := bo.link.(cidlink.Link)
		block, err := blocks.NewBlockWithCid(bo.data, cidLink.Cid)
		if err != nil {
			log.Errorf("Data did not match cid when sending link for %s", cidLink.String())
		}
		builder.AddBlock(block)
	}
	action := graphsync.LinkActionPresent
	if bo.data == nil {
		action = graphsync.LinkActionMissing
	}
	builder.AddLink(bo.requestID, bo.link, action)
	builder.AddBlockData(bo.requestID, bo.Block())
}

func (bo blockOperation) size() uint64 {
	if !bo.sendBlock {
		return 0
	}
	return uint64(len(bo.data))
}

func (bo blockOperation) Block() blockQueued {
	return blockQueued{
		sendBlock: bo.sendBlock,
		link:      bo.link,
		index:     bo.index,
		size:      uint64(len(bo.data)),
	}
}

type blockQueued struct {
	sendBlock bool
	link      ipld.Link
	index     int64
	size      uint64
}

func (bo blockQueued) Link() ipld.Link {
	return bo.link
}

func (bo blockQueued) BlockSize() uint64 {
	return bo.size
}

func (bo blockQueued) BlockSizeOnWire() uint64 {
	if !bo.sendBlock {
		return 0
	}
	return bo.size
}

func (bo blockQueued) Index() int64 {
	return bo.index
}
