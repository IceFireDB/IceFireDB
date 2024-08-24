package graphsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
)

// RequestID is a unique identifier for a GraphSync request.
type RequestID struct{ string }

// Tag returns an easy way to identify this request id as a graphsync request (for libp2p connections)
func (r RequestID) Tag() string {
	return r.String()
}

// String form of a RequestID (should be a well-formed UUIDv4 string)
func (r RequestID) String() string {
	return uuid.Must(uuid.FromBytes([]byte(r.string))).String()
}

// Byte form of a RequestID
func (r RequestID) Bytes() []byte {
	return []byte(r.string)
}

// Create a new, random RequestID (should be a UUIDv4)
func NewRequestID() RequestID {
	u := uuid.New()
	return RequestID{string(u[:])}
}

// Create a RequestID from a byte slice
func ParseRequestID(b []byte) (RequestID, error) {
	_, err := uuid.FromBytes(b)
	if err != nil {
		return RequestID{}, err
	}
	return RequestID{string(b)}, nil
}

// Priority a priority for a GraphSync request.
type Priority int32

// ExtensionName is a name for a GraphSync extension
type ExtensionName string

// ExtensionData is a name/data pair for a graphsync extension
type ExtensionData struct {
	Name ExtensionName
	Data datamodel.Node
}

const (

	// Known Graphsync Extensions

	// ExtensionDoNotSendCIDs tells the responding peer not to send certain blocks if they
	// are encountered in a traversal and is documented at
	// https://github.com/ipld/specs/blob/master/block-layer/graphsync/known_extensions.md
	ExtensionDoNotSendCIDs = ExtensionName("graphsync/do-not-send-cids")

	// ExtensionsDoNotSendFirstBlocks tells the responding peer not to wait till the given
	// number of blocks have been traversed before it begins to send blocks over the wire
	ExtensionsDoNotSendFirstBlocks = ExtensionName("graphsync/do-not-send-first-blocks")

	// ExtensionDeDupByKey tells the responding peer to only deduplicate block sending
	// for requests that have the same key. The data for the extension is a string key
	ExtensionDeDupByKey = ExtensionName("graphsync/dedup-by-key")
)

// RequestClientCancelledErr is an error message received on the error channel when the request is cancelled on by the client code,
// either by closing the passed request context or calling CancelRequest
type RequestClientCancelledErr struct{}

func (e RequestClientCancelledErr) Error() string {
	return "request cancelled by client"
}

// RequestFailedBusyErr is an error message received on the error channel when the peer is busy
type RequestFailedBusyErr struct{}

func (e RequestFailedBusyErr) Error() string {
	return "request failed - peer is busy"
}

// RequestFailedContentNotFoundErr is an error message received on the error channel when the content is not found
type RequestFailedContentNotFoundErr struct{}

func (e RequestFailedContentNotFoundErr) Error() string {
	return "request failed - content not found"
}

// RequestFailedLegalErr is an error message received on the error channel when the request fails for legal reasons
type RequestFailedLegalErr struct{}

func (e RequestFailedLegalErr) Error() string {
	return "request failed - for legal reasons"
}

// RequestFailedUnknownErr is an error message received on the error channel when the request fails for unknown reasons
type RequestFailedUnknownErr struct{}

func (e RequestFailedUnknownErr) Error() string {
	return "request failed - unknown reason"
}

// RequestCancelledErr is an error message received on the error channel that indicates the responder cancelled a request
type RequestCancelledErr struct{}

func (e RequestCancelledErr) Error() string {
	return "request failed - responder cancelled"
}

// RequestNotFoundErr indicates that a request with a particular request ID was not found
type RequestNotFoundErr struct{}

func (e RequestNotFoundErr) Error() string {
	return "request not found"
}

// RemoteMissingBlockErr indicates that the remote peer was missing a block
// in the selector requested, and we also don't have it locally.
// It is a  -terminal error in the error stream
// for a request and does NOT cause a request to fail completely
type RemoteMissingBlockErr struct {
	Link ipld.Link
	Path ipld.Path
}

func (e RemoteMissingBlockErr) Error() string {
	return fmt.Sprintf("remote peer is missing block (%s) at path %s", e.Link.String(), e.Path)
}

// RemoteIncorrectResponseError indicates that the remote peer sent a response
// to a traversal that did not correspond with the expected next link
// in the selector traversal based on verification of data up to this point
type RemoteIncorrectResponseError struct {
	LocalLink  ipld.Link
	RemoteLink ipld.Link
	Path       ipld.Path
}

func (e RemoteIncorrectResponseError) Error() string {
	return fmt.Sprintf("expected link (%s) at path %s does not match link sent by remote (%s), possible malicious responder", e.LocalLink, e.Path, e.RemoteLink)
}

var (
	// ErrExtensionAlreadyRegistered means a user extension can be registered only once
	ErrExtensionAlreadyRegistered = errors.New("extension already registered")
)

// ResponseProgress is the fundamental unit of responses making progress in Graphsync.
type ResponseProgress struct {
	Node      ipld.Node // a node which matched the graphsync query
	Path      ipld.Path // the path of that node relative to the traversal start
	LastBlock struct {  // LastBlock stores the Path and Link of the last block edge we had to load.
		Path ipld.Path
		Link ipld.Link
	}
}

// RequestData describes a received graphsync request.
type RequestData interface {
	// ID Returns the request ID for this Request
	ID() RequestID

	// Root returns the CID to the root block of this request
	Root() cid.Cid

	// Selector returns the byte representation of the selector for this request
	Selector() ipld.Node

	// Priority returns the priority of this request
	Priority() Priority

	// Extension returns the content for an extension on a response, or errors
	// if extension is not present
	Extension(name ExtensionName) (datamodel.Node, bool)

	// IsCancel returns true if this particular request is being cancelled
	Type() RequestType
}

type RequestType string

const (
	// RequestTypeNew means a new request
	RequestTypeNew = RequestType("New")

	// RequestTypeCancel means cancel the request referenced by request ID
	RequestTypeCancel = RequestType("Cancel")

	// RequestTypeUpdate means the extensions contain an update about this request
	RequestTypeUpdate = RequestType("Update")
)

// ResponseData describes a received Graphsync response
type ResponseData interface {
	// RequestID returns the request ID for this response
	RequestID() RequestID

	// Status returns the status for a response
	Status() ResponseStatusCode

	// Extension returns the content for an extension on a response, or errors
	// if extension is not present
	Extension(name ExtensionName) (datamodel.Node, bool)

	// Metadata returns a copy of the link metadata contained in this response
	Metadata() LinkMetadata
}

// LinkAction is a code that is used by message metadata to communicate the
// state and reason for blocks being included or not in a transfer
type LinkAction string

const (
	// LinkActionPresent means the linked block was present on this machine, and
	// is included a this message
	LinkActionPresent = LinkAction("Present")

	// LinkActionDuplicateNotSent means the linked block was present on this machine,
	// but I am not sending it (most likely duplicate)
	LinkActionDuplicateNotSent = LinkAction("DuplicateNotSent")

	// LinkActionMissing means I did not have the linked block, so I skipped over
	// this part of the traversal
	LinkActionMissing = LinkAction("Missing")

	// LinkActionDuplicateDAGSkipped means the DAG with this link points toward has already
	// been traversed entirely in the course of this request so I am skipping over it
	LinkActionDuplicateDAGSkipped = LinkAction("DuplicateDAGSkipped")
)

// DidFollowLink indicates whether the remote actually loaded the block and
// followed it in its selector traversal
func (l LinkAction) DidFollowLink() bool {
	return l == LinkActionPresent || l == LinkActionDuplicateNotSent
}

// LinkMetadataIterator is used to access individual link metadata through a
// LinkMetadata object
type LinkMetadataIterator func(cid.Cid, LinkAction)

// LinkMetadata is used to access link metadata through an Iterator
type LinkMetadata interface {
	// Length returns the number of metadata entries
	Length() int64
	// Iterate steps over individual metadata one by one using the provided
	// callback
	Iterate(LinkMetadataIterator)
}

// BlockData gives information about a block included in a graphsync response
type BlockData interface {
	// Link is the link/cid for the block
	Link() ipld.Link

	// BlockSize specifies the size of the block
	BlockSize() uint64

	// BlockSize specifies the amount of data actually transmitted over the network
	BlockSizeOnWire() uint64

	// The index of this block in the selector traversal
	Index() int64
}

// IncomingRequestHookActions are actions that a request hook can take to change
// behavior for the response
type IncomingRequestHookActions interface {
	SendExtensionData(ExtensionData)
	UsePersistenceOption(name string)
	UseLinkTargetNodePrototypeChooser(traversal.LinkTargetNodePrototypeChooser)
	TerminateWithError(error)
	ValidateRequest()
	PauseResponse()
}

// OutgoingBlockHookActions are actions that an outgoing block hook can take to
// change the execution of a request
type OutgoingBlockHookActions interface {
	SendExtensionData(ExtensionData)
	TerminateWithError(error)
	PauseResponse()
}

// OutgoingRequestHookActions are actions that an outgoing request hook can take
// to change the execution of a request
type OutgoingRequestHookActions interface {
	UsePersistenceOption(name string)
	UseLinkTargetNodePrototypeChooser(traversal.LinkTargetNodePrototypeChooser)
}

// IncomingResponseHookActions are actions that incoming response hook can take
// to change the execution of a request
type IncomingResponseHookActions interface {
	TerminateWithError(error)
	UpdateRequestWithExtensions(...ExtensionData)
}

// IncomingBlockHookActions are actions that incoming block hook can take
// to change the execution of a request
type IncomingBlockHookActions interface {
	TerminateWithError(error)
	UpdateRequestWithExtensions(...ExtensionData)
	PauseRequest()
}

// RequestUpdatedHookActions are actions that can be taken in a request updated hook to
// change execution of the response
type RequestUpdatedHookActions interface {
	TerminateWithError(error)
	SendExtensionData(ExtensionData)
	UnpauseResponse()
}

// RequestQueuedHookActions are actions that can be taken in a request queued hook to
// change execution of the response
type RequestQueuedHookActions interface {
	AugmentContext(func(reqCtx context.Context) context.Context)
}

// OnIncomingRequestQueuedHook is a hook that runs each time a new incoming request is added to the responder's task queue.
// It receives the peer that sent the request and all data about the request.
type OnIncomingRequestQueuedHook func(p peer.ID, request RequestData, hookActions RequestQueuedHookActions)

// OnIncomingRequestHook is a hook that runs each time a new request is received.
// It receives the peer that sent the request and all data about the request.
// It receives an interface for customizing the response to this request
type OnIncomingRequestHook func(p peer.ID, request RequestData, hookActions IncomingRequestHookActions)

// OnIncomingResponseHook is a hook that runs each time a new response is received.
// It receives the peer that sent the response and all data about the response.
// It receives an interface for customizing how we handle the ongoing execution of the request
type OnIncomingResponseHook func(p peer.ID, responseData ResponseData, hookActions IncomingResponseHookActions)

// OnIncomingBlockHook is a hook that runs each time a new block is validated as
// part of the response, regardless of whether it came locally or over the network
// It receives that sent the response, the most recent response, a link for the block received,
// and the size of the block received
// The difference between BlockSize & BlockSizeOnWire can be used to determine
// where the block came from (Local vs remote)
// It receives an interface for customizing how we handle the ongoing execution of the request
type OnIncomingBlockHook func(p peer.ID, responseData ResponseData, blockData BlockData, hookActions IncomingBlockHookActions)

// OnOutgoingRequestHook is a hook that runs immediately prior to sending a request
// It receives the peer we're sending a request to and all the data aobut the request
// It receives an interface for customizing how we handle executing this request
type OnOutgoingRequestHook func(p peer.ID, request RequestData, hookActions OutgoingRequestHookActions)

// OnOutgoingBlockHook is a hook that runs immediately after a requestor sends a new block
// on a response
// It receives the peer we're sending a request to, all the data aobut the request, a link for the block sent,
// and the size of the block sent
// It receives an interface for taking further action on the response
type OnOutgoingBlockHook func(p peer.ID, request RequestData, block BlockData, hookActions OutgoingBlockHookActions)

// OnRequestUpdatedHook is a hook that runs when an update to a request is received
// It receives the peer we're sending to, the original request, the request update
// It receives an interface to taking further action on the response
type OnRequestUpdatedHook func(p peer.ID, request RequestData, updateRequest RequestData, hookActions RequestUpdatedHookActions)

// OnBlockSentListener runs when a block is sent over the wire
type OnBlockSentListener func(p peer.ID, request RequestData, block BlockData)

// OnNetworkErrorListener runs when queued data is not able to be sent
type OnNetworkErrorListener func(p peer.ID, request RequestData, err error)

// OnReceiverNetworkErrorListener runs when errors occur receiving data over the wire
type OnReceiverNetworkErrorListener func(p peer.ID, err error)

// OnResponseCompletedListener provides a way to listen for when responder has finished serving a response
type OnResponseCompletedListener func(p peer.ID, request RequestData, status ResponseStatusCode)

// OnOutgoingRequestProcessingListener is called when a request actually begins processing (reaches
// the top of the outgoing request queue)
type OnOutgoingRequestProcessingListener func(p peer.ID, request RequestData, inProgressRequestCount int)

// OnRequestorCancelledListener provides a way to listen for responses the requestor canncels
type OnRequestorCancelledListener func(p peer.ID, request RequestData)

// UnregisterHookFunc is a function call to unregister a hook that was previously registered
type UnregisterHookFunc func()

// RequestStats offer statistics about request processing
type RequestStats struct {
	// TotalPeers is the number of peers that have active or pending requests
	TotalPeers uint64
	// Active is the total number of active requests being processing
	Active uint64
	// Pending is the total number of requests that are waiting to be processed
	Pending uint64
}

// ResponseStats offer statistics about memory allocations for responses
type ResponseStats struct {
	// MaxAllowedAllocatedTotal is the preconfigured limit on allocations
	// for all peers
	MaxAllowedAllocatedTotal uint64
	// MaxAllowedAllocatedPerPeer is the preconfigured limit on allocations
	// for an individual peer
	MaxAllowedAllocatedPerPeer uint64
	// TotalAllocatedAllPeers indicates the amount of memory allocated for blocks
	// across all peers
	TotalAllocatedAllPeers uint64
	// TotalPendingAllocations indicates the amount awaiting freeing up of memory
	TotalPendingAllocations uint64
	// NumPeersWithPendingAllocations indicates the number of peers that
	// have either maxed out their individual memory allocations or have
	// pending allocations cause the total limit has been reached.
	NumPeersWithPendingAllocations uint64
}

// Stats describes statistics about the Graphsync implementations
// current state
type Stats struct {
	// Stats for the graphsync requestor
	OutgoingRequests  RequestStats
	IncomingResponses ResponseStats

	// Stats for the graphsync responder
	IncomingRequests  RequestStats
	OutgoingResponses ResponseStats
}

// RequestState describes the current general state of a request
type RequestState uint64

// RequestStates describe a set of request IDs and their current state
type RequestStates map[RequestID]RequestState

const (
	// Queued means a request has been received and is queued for processing
	Queued RequestState = iota
	// Running means a request is actively sending or receiving data
	Running
	// Paused means a request is paused
	Paused
	// CompletingSend means we have processed a query and are waiting for data to
	// go over the network
	CompletingSend
)

func (rs RequestState) String() string {
	switch rs {
	case Queued:
		return "queued"
	case Running:
		return "running"
	case Paused:
		return "paused"
	case CompletingSend:
		return "completing send"
	default:
		return "unrecognized request state"
	}
}

// GraphExchange is a protocol that can exchange IPLD graphs based on a selector
type GraphExchange interface {
	// Request initiates a new GraphSync request to the given peer using the given selector spec.
	Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...ExtensionData) (<-chan ResponseProgress, <-chan error)

	// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
	RegisterPersistenceOption(name string, lsys ipld.LinkSystem) error

	// UnregisterPersistenceOption unregisters an alternate loader/storer combo
	UnregisterPersistenceOption(name string) error

	// RegisterIncomingRequestQueuedHook adds a hook that runs when a new incoming request is added to the responder's task queue.
	RegisterIncomingRequestQueuedHook(hook OnIncomingRequestQueuedHook) UnregisterHookFunc

	// RegisterIncomingRequestHook adds a hook that runs when a request is received
	RegisterIncomingRequestHook(hook OnIncomingRequestHook) UnregisterHookFunc

	// RegisterIncomingResponseHook adds a hook that runs when a response is received
	RegisterIncomingResponseHook(OnIncomingResponseHook) UnregisterHookFunc

	// RegisterIncomingBlockHook adds a hook that runs when a block is received and validated (put in block store)
	RegisterIncomingBlockHook(OnIncomingBlockHook) UnregisterHookFunc

	// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
	RegisterOutgoingRequestHook(hook OnOutgoingRequestHook) UnregisterHookFunc

	// RegisterOutgoingBlockHook adds a hook that runs every time a block is sent from a responder
	RegisterOutgoingBlockHook(hook OnOutgoingBlockHook) UnregisterHookFunc

	// RegisterRequestUpdatedHook adds a hook that runs every time an update to a request is received
	RegisterRequestUpdatedHook(hook OnRequestUpdatedHook) UnregisterHookFunc

	// RegisterOutgoingRequestProcessingListener adds a listener that gets called when a request actually begins processing (reaches
	// the top of the outgoing request queue)
	RegisterOutgoingRequestProcessingListener(listener OnOutgoingRequestProcessingListener) UnregisterHookFunc

	// RegisterCompletedResponseListener adds a listener on the responder for completed responses
	RegisterCompletedResponseListener(listener OnResponseCompletedListener) UnregisterHookFunc

	// RegisterRequestorCancelledListener adds a listener on the responder for
	// responses cancelled by the requestor
	RegisterRequestorCancelledListener(listener OnRequestorCancelledListener) UnregisterHookFunc

	// RegisterBlockSentListener adds a listener for when blocks are actually sent over the wire
	RegisterBlockSentListener(listener OnBlockSentListener) UnregisterHookFunc

	// RegisterNetworkErrorListener adds a listener for when errors occur sending data over the wire
	RegisterNetworkErrorListener(listener OnNetworkErrorListener) UnregisterHookFunc

	// RegisterReceiverNetworkErrorListener adds a listener for when errors occur receiving data over the wire
	RegisterReceiverNetworkErrorListener(listener OnReceiverNetworkErrorListener) UnregisterHookFunc

	// Pause pauses an in progress request or response (may take 1 or more blocks to process)
	Pause(context.Context, RequestID) error

	// Unpause unpauses a request or response that was paused
	// Can also send extensions with unpause
	Unpause(context.Context, RequestID, ...ExtensionData) error

	// Cancel cancels an in progress request or response
	Cancel(context.Context, RequestID) error

	// SendUpdate sends an update for an in progress request or response
	SendUpdate(context.Context, RequestID, ...ExtensionData) error

	// Stats produces insight on the current state of a graphsync exchange
	Stats() Stats
}
