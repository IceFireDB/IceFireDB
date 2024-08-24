/*
Package responseassembler assembles responses that are queued for sending in outgoing messages

The response assembler's Transaction method allows a caller to specify response actions that will go into a single
libp2p2 message. The response assembler will also deduplicate blocks that have already been sent over the network in
a previous message
*/
package responseassembler

import (
	"context"
	"sync"

	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/peermanager"
)

// Transaction is a series of operations that should be send together in a single response
type Transaction func(ResponseBuilder) error

// ResponseBuilder is a limited interface for assembling responses inside a transaction, so that they are included
// in the same message on the protocol
type ResponseBuilder interface {
	// SendResponse adds a response to this transaction.
	SendResponse(
		link ipld.Link,
		data []byte,
	) graphsync.BlockData

	// SendExtensionData adds extension data to the transaction.
	SendExtensionData(graphsync.ExtensionData)

	// SendUpdates sets up a PartialResponse with just the extension data provided
	SendUpdates([]graphsync.ExtensionData)

	// FinishRequest completes the response to a request.
	FinishRequest() graphsync.ResponseStatusCode

	// FinishWithError end the response due to an error
	FinishWithError(status graphsync.ResponseStatusCode)

	// PauseRequest temporarily halts responding to the request
	PauseRequest()

	// Context returns the execution context for this transaction
	Context() context.Context
}

// PeerMessageHandler is an interface that can queue a response for a given peer to go out over the network
// If blkSize > 0, message building may block until enough memory has been freed from the queues to allocate the message.
type PeerMessageHandler interface {
	AllocateAndBuildMessage(p peer.ID, blkSize uint64, buildResponseFn func(*messagequeue.Builder))
}

// ResponseAssembler manages assembling responses to go out over the network
// in libp2p messages
type ResponseAssembler struct {
	*peermanager.PeerManager
	peerHandler PeerMessageHandler
}

// New generates a new ResponseAssembler for sending responses
func New(ctx context.Context, peerHandler PeerMessageHandler) *ResponseAssembler {
	return &ResponseAssembler{
		PeerManager: peermanager.New(ctx, func(ctx context.Context, p peer.ID) peermanager.PeerHandler {
			return newTracker()
		}),
		peerHandler: peerHandler,
	}
}

func (ra *ResponseAssembler) NewStream(ctx context.Context, p peer.ID, requestID graphsync.RequestID, subscriber notifications.Subscriber) ResponseStream {
	return &responseStream{
		ctx:            ctx,
		requestID:      requestID,
		p:              p,
		messageSenders: ra.peerHandler,
		linkTrackers:   ra.PeerManager,
		subscriber:     subscriber,
	}
}

type responseStream struct {
	ctx            context.Context
	requestID      graphsync.RequestID
	p              peer.ID
	closed         bool
	closedLk       sync.RWMutex
	messageSenders PeerMessageHandler
	linkTrackers   *peermanager.PeerManager
	subscriber     notifications.Subscriber
}

func (r *responseStream) Close() error {
	r.closedLk.Lock()
	r.closed = true
	r.closedLk.Unlock()
	return nil
}

func (r *responseStream) isClosed() bool {
	r.closedLk.RLock()
	defer r.closedLk.RUnlock()
	return r.closed
}

type ResponseStream interface {
	Transaction(transaction Transaction) error
	DedupKey(key string)
	IgnoreBlocks(links []ipld.Link)
	SkipFirstBlocks(skipFirstBlocks int64)
	// ClearRequest removes all tracking for this request.
	ClearRequest()
}

// DedupKey indicates that outgoing blocks should be deduplicated in a seperate bucket (only with requests that share
// supplied key string)
func (rs *responseStream) DedupKey(key string) {
	rs.linkTrackers.GetProcess(rs.p).(*peerLinkTracker).DedupKey(rs.requestID, key)
}

// IgnoreBlocks indicates that a list of keys should be ignored when sending blocks
func (rs *responseStream) IgnoreBlocks(links []ipld.Link) {
	rs.linkTrackers.GetProcess(rs.p).(*peerLinkTracker).IgnoreBlocks(rs.requestID, links)
}

// SkipFirstBlocks tells the assembler for the given request to not send the first N blocks
func (rs *responseStream) SkipFirstBlocks(skipFirstBlocks int64) {
	rs.linkTrackers.GetProcess(rs.p).(*peerLinkTracker).SkipFirstBlocks(rs.requestID, skipFirstBlocks)
}

// ClearRequest removes all tracking for this request.
func (rs *responseStream) ClearRequest() {
	_ = rs.linkTrackers.GetProcess(rs.p).(*peerLinkTracker).FinishTracking(rs.requestID)
}

func (rs *responseStream) Transaction(transaction Transaction) error {
	ctx, span := otel.Tracer("graphsync").Start(rs.ctx, "transaction")
	defer span.End()
	rb := &responseBuilder{
		ctx:         ctx,
		requestID:   rs.requestID,
		linkTracker: rs.linkTrackers.GetProcess(rs.p).(*peerLinkTracker),
	}
	err := transaction(rb)
	rs.execute(ctx, rb.operations)
	return err
}

func (rs *responseStream) execute(ctx context.Context, operations []responseOperation) {
	ctx, span := otel.Tracer("graphsync").Start(ctx, "execute")
	defer span.End()

	if rs.isClosed() {
		return
	}
	size := uint64(0)
	for _, op := range operations {
		size += op.size()
	}
	rs.messageSenders.AllocateAndBuildMessage(rs.p, size, func(builder *messagequeue.Builder) {
		_, span = otel.Tracer("graphsync").Start(ctx, "buildMessage", trace.WithLinks(trace.LinkFromContext(builder.Context())))
		defer span.End()

		if rs.isClosed() {
			return
		}
		for _, op := range operations {
			op.build(builder)
		}
		builder.SetResponseStream(rs.requestID, rs)
		builder.SetSubscriber(rs.requestID, rs.subscriber)
	})
}
