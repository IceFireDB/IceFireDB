package responsemanager

import (
	"context"
	"errors"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/peerstate"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/queryexecutor"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/taskqueue"
)

// The code in this file implements the public interface of the response manager.
// Functions in this file operate outside the internal thread and should
// NOT modify the internal state of the ResponseManager.

var log = logging.Logger("graphsync")

type inProgressResponseStatus struct {
	ctx            context.Context
	span           trace.Span
	cancelFn       func()
	peer           peer.ID
	request        gsmsg.GraphSyncRequest
	loader         ipld.BlockReadOpener
	traverser      ipldutil.Traverser
	signals        queryexecutor.ResponseSignals
	updates        []gsmsg.GraphSyncRequest
	state          graphsync.RequestState
	startTime      time.Time
	responseStream responseassembler.ResponseStream
}

// RequestHooks is an interface for processing request hooks
type RequestHooks interface {
	ProcessRequestHooks(p peer.ID, request graphsync.RequestData) hooks.RequestResult
}

// RequestQueuedHooks is an interface for processing request queued hooks
type RequestQueuedHooks interface {
	ProcessRequestQueuedHooks(p peer.ID, request graphsync.RequestData, reqCtx context.Context) context.Context
}

// UpdateHooks is an interface for processing update hooks
type UpdateHooks interface {
	ProcessUpdateHooks(p peer.ID, request graphsync.RequestData, update graphsync.RequestData) hooks.UpdateResult
}

// CompletedListeners is an interface for notifying listeners that responses are complete
type CompletedListeners interface {
	NotifyCompletedListeners(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode)
}

// CancelledListeners is an interface for notifying listeners that requestor cancelled
type CancelledListeners interface {
	NotifyCancelledListeners(p peer.ID, request graphsync.RequestData)
}

// BlockSentListeners is an interface for notifying listeners that of a block send occuring over the wire
type BlockSentListeners interface {
	NotifyBlockSentListeners(p peer.ID, request graphsync.RequestData, block graphsync.BlockData)
}

// NetworkErrorListeners is an interface for notifying listeners that an error occurred sending a data on the wire
type NetworkErrorListeners interface {
	NotifyNetworkErrorListeners(p peer.ID, request graphsync.RequestData, err error)
}

// ResponseAssembler is an interface that returns sender interfaces for peer responses.
type ResponseAssembler interface {
	NewStream(ctx context.Context, p peer.ID, requestID graphsync.RequestID, subscriber notifications.Subscriber) responseassembler.ResponseStream
}

type responseManagerMessage interface {
	handle(rm *ResponseManager)
}

// ResponseManager handles incoming requests from the network, initiates selector
// traversals, and transmits responses
type ResponseManager struct {
	ctx                   context.Context
	cancelFn              context.CancelFunc
	responseAssembler     ResponseAssembler
	requestHooks          RequestHooks
	linkSystem            ipld.LinkSystem
	requestQueuedHooks    RequestQueuedHooks
	updateHooks           UpdateHooks
	cancelledListeners    CancelledListeners
	completedListeners    CompletedListeners
	blockSentListeners    BlockSentListeners
	networkErrorListeners NetworkErrorListeners
	messages              chan responseManagerMessage
	inProgressResponses   map[graphsync.RequestID]*inProgressResponseStatus
	connManager           network.ConnManager
	// maximum number of links to traverse per request. A value of zero = infinity, or no limit
	maxLinksPerRequest uint64
	responseQueue      taskqueue.TaskQueue
}

// New creates a new response manager for responding to requests
func New(ctx context.Context,
	linkSystem ipld.LinkSystem,
	responseAssembler ResponseAssembler,
	requestQueuedHooks RequestQueuedHooks,
	requestHooks RequestHooks,
	updateHooks UpdateHooks,
	completedListeners CompletedListeners,
	cancelledListeners CancelledListeners,
	blockSentListeners BlockSentListeners,
	networkErrorListeners NetworkErrorListeners,
	connManager network.ConnManager,
	maxLinksPerRequest uint64,
	responseQueue taskqueue.TaskQueue,
) *ResponseManager {
	ctx, cancelFn := context.WithCancel(ctx)
	messages := make(chan responseManagerMessage, 16)
	rm := &ResponseManager{
		ctx:                   ctx,
		cancelFn:              cancelFn,
		requestHooks:          requestHooks,
		linkSystem:            linkSystem,
		responseAssembler:     responseAssembler,
		requestQueuedHooks:    requestQueuedHooks,
		updateHooks:           updateHooks,
		cancelledListeners:    cancelledListeners,
		completedListeners:    completedListeners,
		blockSentListeners:    blockSentListeners,
		networkErrorListeners: networkErrorListeners,
		messages:              messages,
		inProgressResponses:   make(map[graphsync.RequestID]*inProgressResponseStatus),
		connManager:           connManager,
		maxLinksPerRequest:    maxLinksPerRequest,
		responseQueue:         responseQueue,
	}
	return rm
}

// ProcessRequests processes incoming requests for the given peer
func (rm *ResponseManager) ProcessRequests(ctx context.Context, p peer.ID, requests []gsmsg.GraphSyncRequest) {
	rm.send(&processRequestsMessage{p, requests}, ctx.Done())
}

// UnpauseResponse unpauses a response that was previously paused
func (rm *ResponseManager) UnpauseResponse(ctx context.Context, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	response := make(chan error, 1)
	rm.send(&unpauseRequestMessage{requestID, response, extensions}, ctx.Done())
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

// PauseResponse pauses an in progress response (may take 1 or more blocks to process)
func (rm *ResponseManager) PauseResponse(ctx context.Context, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	rm.send(&pauseRequestMessage{requestID, response}, ctx.Done())
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

// CancelResponse cancels an in progress response
func (rm *ResponseManager) CancelResponse(ctx context.Context, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	rm.send(&errorRequestMessage{requestID, queryexecutor.ErrCancelledByCommand, response}, ctx.Done())
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

// UpdateRequest updates an in progress response
func (rm *ResponseManager) UpdateResponse(ctx context.Context, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	response := make(chan error, 1)
	rm.send(&updateRequestMessage{requestID, extensions, response}, ctx.Done())
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

// Synchronize is a utility method that blocks until all current messages are processed
func (rm *ResponseManager) synchronize() {
	sync := make(chan error)
	rm.send(&synchronizeMessage{sync}, nil)
	select {
	case <-rm.ctx.Done():
	case <-sync:
	}
}

// StartTask starts the given task from the peer task queue
func (rm *ResponseManager) StartTask(task *peertask.Task, p peer.ID, responseTaskChan chan<- queryexecutor.ResponseTask) {
	rm.send(&startTaskRequest{task, p, responseTaskChan}, nil)
}

// GetUpdates is called to read pending updates for a task and clear them
func (rm *ResponseManager) GetUpdates(requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest) {
	rm.send(&responseUpdateRequest{requestID, updatesChan}, nil)
}

// FinishTask marks a task from the task queue as done
func (rm *ResponseManager) FinishTask(task *peertask.Task, p peer.ID, err error) {
	done := make(chan struct{}, 1)
	rm.send(&finishTaskRequest{task, p, err, done}, nil)
	select {
	case <-rm.ctx.Done():
	case <-done:
	}
}

// CloseWithNetworkError closes a request due to a network error
func (rm *ResponseManager) CloseWithNetworkError(requestID graphsync.RequestID) {
	done := make(chan error, 1)
	rm.send(&errorRequestMessage{requestID, queryexecutor.ErrNetworkError, done}, nil)
	select {
	case <-rm.ctx.Done():
	case <-done:
	}
}

// TerminateRequest indicates a request has finished sending data and should no longer be tracked
func (rm *ResponseManager) TerminateRequest(requestID graphsync.RequestID) {
	done := make(chan struct{}, 1)
	rm.send(&terminateRequestMessage{requestID, done}, nil)
	select {
	case <-rm.ctx.Done():
	case <-done:
	}
}

// PeerState gets current state of the outgoing responses for a given peer
func (rm *ResponseManager) PeerState(p peer.ID) peerstate.PeerState {
	response := make(chan peerstate.PeerState)
	rm.send(&peerStateMessage{p, response}, nil)
	select {
	case <-rm.ctx.Done():
		return peerstate.PeerState{}
	case peerState := <-response:
		return peerState
	}
}

func (rm *ResponseManager) send(message responseManagerMessage, done <-chan struct{}) {
	select {
	case <-rm.ctx.Done():
	case <-done:
	case rm.messages <- message:
	}
}

// Startup starts processing for the WantManager.
func (rm *ResponseManager) Startup() {
	go rm.run()
}

// Shutdown ends processing for the want manager.
func (rm *ResponseManager) Shutdown() {
	rm.cancelFn()
}
