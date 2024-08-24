package graphsync

import (
	"context"
	"errors"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/allocator"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/peerstate"
	"github.com/ipfs/go-graphsync/persistenceoptions"
	"github.com/ipfs/go-graphsync/requestmanager"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	requestorhooks "github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager"
	responderhooks "github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/queryexecutor"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/selectorvalidator"
	"github.com/ipfs/go-graphsync/taskqueue"
)

var log = logging.Logger("graphsync")

const maxRecursionDepth = 100
const defaultTotalMaxMemory = uint64(256 << 20)
const defaultMaxMemoryPerPeer = uint64(16 << 20)
const defaultMaxInProgressRequests = uint64(6)
const defaultMessageSendRetries = 10
const defaultSendMessageTimeout = 10 * time.Minute

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	network                            gsnet.GraphSyncNetwork
	linkSystem                         ipld.LinkSystem
	requestManager                     *requestmanager.RequestManager
	responseManager                    *responsemanager.ResponseManager
	queryExecutor                      *queryexecutor.QueryExecutor
	responseQueue                      taskqueue.TaskQueue
	requestQueue                       taskqueue.TaskQueue
	requestExecutor                    *executor.Executor
	responseAssembler                  *responseassembler.ResponseAssembler
	peerManager                        *peermanager.PeerMessageManager
	incomingRequestQueuedHooks         *responderhooks.IncomingRequestQueuedHooks
	incomingRequestHooks               *responderhooks.IncomingRequestHooks
	outgoingBlockHooks                 *responderhooks.OutgoingBlockHooks
	requestUpdatedHooks                *responderhooks.RequestUpdatedHooks
	outgoingRequestProcessingListeners *listeners.OutgoingRequestProcessingListeners
	completedResponseListeners         *listeners.CompletedResponseListeners
	requestorCancelledListeners        *listeners.RequestorCancelledListeners
	blockSentListeners                 *listeners.BlockSentListeners
	networkErrorListeners              *listeners.NetworkErrorListeners
	receiverErrorListeners             *listeners.NetworkReceiverErrorListeners
	incomingResponseHooks              *requestorhooks.IncomingResponseHooks
	outgoingRequestHooks               *requestorhooks.OutgoingRequestHooks
	incomingBlockHooks                 *requestorhooks.IncomingBlockHooks
	persistenceOptions                 *persistenceoptions.PersistenceOptions
	ctx                                context.Context
	cancel                             context.CancelFunc
	responseAllocator                  *allocator.Allocator
}

type graphsyncConfigOptions struct {
	totalMaxMemoryResponder              uint64
	maxMemoryPerPeerResponder            uint64
	maxInProgressIncomingRequests        uint64
	maxInProgressIncomingRequestsPerPeer uint64
	maxInProgressOutgoingRequests        uint64
	registerDefaultValidator             bool
	maxLinksPerOutgoingRequest           uint64
	maxLinksPerIncomingRequest           uint64
	messageSendRetries                   int
	sendMessageTimeout                   time.Duration
}

// Option defines the functional option type that can be used to configure
// graphsync instances
type Option func(*graphsyncConfigOptions)

// RejectAllRequestsByDefault means that without hooks registered
// that perform their own request validation, all requests are rejected
func RejectAllRequestsByDefault() Option {
	return func(gs *graphsyncConfigOptions) {
		gs.registerDefaultValidator = false
	}
}

// MaxMemoryResponder defines the maximum amount of memory the responder
// may consume queueing up messages for a response in total
func MaxMemoryResponder(totalMaxMemory uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.totalMaxMemoryResponder = totalMaxMemory
	}
}

// MaxMemoryPerPeerResponder defines the maximum amount of memory a peer
// may consume queueing up messages for a response
func MaxMemoryPerPeerResponder(maxMemoryPerPeer uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxMemoryPerPeerResponder = maxMemoryPerPeer
	}
}

// MaxInProgressIncomingRequests changes the maximum number of
// incoming graphsync requests that are processed in parallel (default 6)
func MaxInProgressIncomingRequests(maxInProgressIncomingRequests uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxInProgressIncomingRequests = maxInProgressIncomingRequests
	}
}

// MaxInProgressIncomingRequestsPerPeer changes the maximum number of
// incoming graphsync requests that are processed in parallel on a per-peer basis.
// The value is not set by default.
// Useful in an environment for very high bandwidth graphsync responders serving
// many peers
// Note: if for some reason this is set higher than MaxInProgressIncomingRequests
// it will simply have no effect.
// Note: setting a value of zero will have no effect
func MaxInProgressIncomingRequestsPerPeer(maxInProgressIncomingRequestsPerPeer uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxInProgressIncomingRequestsPerPeer = maxInProgressIncomingRequestsPerPeer
	}
}

// MaxInProgressOutgoingRequests changes the maximum number of
// outgoing graphsync requests that are processed in parallel (default 6)
func MaxInProgressOutgoingRequests(maxInProgressOutgoingRequests uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxInProgressOutgoingRequests = maxInProgressOutgoingRequests
	}
}

// MaxLinksPerOutgoingRequests changes the allowed number of links an outgoing
// request can traverse before failing
// A value of 0 = infinity, or no limit
func MaxLinksPerOutgoingRequests(maxLinksPerOutgoingRequest uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxLinksPerOutgoingRequest = maxLinksPerOutgoingRequest
	}
}

// MaxLinksPerIncomingRequests changes the allowed number of links an incoming
// request can traverse before failing
// A value of 0 = infinity, or no limit
func MaxLinksPerIncomingRequests(maxLinksPerIncomingRequest uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxLinksPerIncomingRequest = maxLinksPerIncomingRequest
	}
}

// MessageSendRetries sets the number of times graphsync will send
// attempt to send a message before giving up.
// Lower to increase the speed at which an unresponsive peer is
// detected.
//
// If not set, a default of 10 is used.
func MessageSendRetries(messageSendRetries int) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.messageSendRetries = messageSendRetries
	}
}

// SendMessageTimeout sets the amount of time graphsync will wait
// for a message to go across the wire before giving up and
// trying again (up to max retries).
// Lower to increase the speed at which an unresponsive peer is
// detected.
//
// If not set, a default of 10 minutes is used.
func SendMessageTimeout(sendMessageTimeout time.Duration) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.sendMessageTimeout = sendMessageTimeout
	}
}

// New creates a new GraphSync Exchange on the given network,
// and the given link loader+storer.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	linkSystem ipld.LinkSystem, options ...Option) graphsync.GraphExchange {
	ctx, cancel := context.WithCancel(parent)

	gsConfig := &graphsyncConfigOptions{
		totalMaxMemoryResponder:       defaultTotalMaxMemory,
		maxMemoryPerPeerResponder:     defaultMaxMemoryPerPeer,
		maxInProgressIncomingRequests: defaultMaxInProgressRequests,
		maxInProgressOutgoingRequests: defaultMaxInProgressRequests,
		registerDefaultValidator:      true,
		messageSendRetries:            defaultMessageSendRetries,
		sendMessageTimeout:            defaultSendMessageTimeout,
	}
	for _, option := range options {
		option(gsConfig)
	}
	incomingResponseHooks := requestorhooks.NewResponseHooks()
	outgoingRequestHooks := requestorhooks.NewRequestHooks()
	incomingBlockHooks := requestorhooks.NewBlockHooks()
	networkErrorListeners := listeners.NewNetworkErrorListeners()
	receiverErrorListeners := listeners.NewReceiverNetworkErrorListeners()
	outgoingRequestProcessingListeners := listeners.NewOutgoingRequestProcessingListeners()
	persistenceOptions := persistenceoptions.New()
	requestQueuedHooks := responderhooks.NewRequestQueuedHooks()
	incomingRequestHooks := responderhooks.NewRequestHooks(persistenceOptions)
	outgoingBlockHooks := responderhooks.NewBlockHooks()
	requestUpdatedHooks := responderhooks.NewUpdateHooks()
	completedResponseListeners := listeners.NewCompletedResponseListeners()
	requestorCancelledListeners := listeners.NewRequestorCancelledListeners()
	blockSentListeners := listeners.NewBlockSentListeners()
	if gsConfig.registerDefaultValidator {
		incomingRequestHooks.Register(selectorvalidator.SelectorValidator(maxRecursionDepth))
	}
	responseAllocator := allocator.NewAllocator(gsConfig.totalMaxMemoryResponder, gsConfig.maxMemoryPerPeerResponder)
	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network, responseAllocator, gsConfig.messageSendRetries, gsConfig.sendMessageTimeout)
	}
	peerManager := peermanager.NewMessageManager(ctx, createMessageQueue)

	requestQueue := taskqueue.NewTaskQueue(ctx)
	requestManager := requestmanager.New(ctx, persistenceOptions, linkSystem, outgoingRequestHooks, incomingResponseHooks, networkErrorListeners, outgoingRequestProcessingListeners, requestQueue, network.ConnectionManager(), gsConfig.maxLinksPerOutgoingRequest)
	requestExecutor := executor.NewExecutor(requestManager, incomingBlockHooks)
	responseAssembler := responseassembler.New(ctx, peerManager)
	var ptqopts []peertaskqueue.Option
	if gsConfig.maxInProgressIncomingRequestsPerPeer > 0 {
		ptqopts = append(ptqopts, peertaskqueue.MaxOutstandingWorkPerPeer(int(gsConfig.maxInProgressIncomingRequestsPerPeer)))
	}
	responseQueue := taskqueue.NewTaskQueue(ctx, ptqopts...)
	responseManager := responsemanager.New(
		ctx,
		linkSystem,
		responseAssembler,
		requestQueuedHooks,
		incomingRequestHooks,
		requestUpdatedHooks,
		completedResponseListeners,
		requestorCancelledListeners,
		blockSentListeners,
		networkErrorListeners,
		network.ConnectionManager(),
		gsConfig.maxLinksPerIncomingRequest,
		responseQueue)
	queryExecutor := queryexecutor.New(
		ctx,
		responseManager,
		outgoingBlockHooks,
		requestUpdatedHooks,
	)
	graphSync := &GraphSync{
		network:                     network,
		linkSystem:                  linkSystem,
		requestManager:              requestManager,
		responseManager:             responseManager,
		queryExecutor:               queryExecutor,
		responseQueue:               responseQueue,
		requestQueue:                requestQueue,
		requestExecutor:             requestExecutor,
		responseAssembler:           responseAssembler,
		peerManager:                 peerManager,
		incomingRequestQueuedHooks:  requestQueuedHooks,
		incomingRequestHooks:        incomingRequestHooks,
		outgoingBlockHooks:          outgoingBlockHooks,
		requestUpdatedHooks:         requestUpdatedHooks,
		completedResponseListeners:  completedResponseListeners,
		requestorCancelledListeners: requestorCancelledListeners,
		blockSentListeners:          blockSentListeners,
		networkErrorListeners:       networkErrorListeners,
		receiverErrorListeners:      receiverErrorListeners,
		incomingResponseHooks:       incomingResponseHooks,
		outgoingRequestHooks:        outgoingRequestHooks,
		incomingBlockHooks:          incomingBlockHooks,
		persistenceOptions:          persistenceOptions,
		ctx:                         ctx,
		cancel:                      cancel,
		responseAllocator:           responseAllocator,
	}

	requestManager.SetDelegate(peerManager)
	requestManager.Startup()
	requestQueue.Startup(gsConfig.maxInProgressOutgoingRequests, requestExecutor)
	responseManager.Startup()
	responseQueue.Startup(gsConfig.maxInProgressIncomingRequests, queryExecutor)
	network.SetDelegate((*graphSyncReceiver)(graphSync))

	return graphSync
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (gs *GraphSync) Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	var extNames []string
	for _, ext := range extensions {
		extNames = append(extNames, string(ext.Name))
	}
	ctx, _ = otel.Tracer("graphsync").Start(ctx, "request", trace.WithAttributes(
		attribute.String("peerID", p.Pretty()),
		attribute.String("root", root.String()),
		attribute.StringSlice("extensions", extNames),
	))
	return gs.requestManager.NewRequest(ctx, p, root, selector, extensions...)
}

// RegisterIncomingRequestHook adds a hook that runs when a request is received
// If overrideDefaultValidation is set to true, then if the hook does not error,
// it is considered to have "validated" the request -- and that validation supersedes
// the normal validation of requests Graphsync does (i.e. all selectors can be accepted)
func (gs *GraphSync) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return gs.incomingRequestHooks.Register(hook)
}

// RegisterIncomingRequestQueuedHook adds a hook that runs when a new incoming request is added
// to the responder's task queue.
func (gs *GraphSync) RegisterIncomingRequestQueuedHook(hook graphsync.OnIncomingRequestQueuedHook) graphsync.UnregisterHookFunc {
	return gs.incomingRequestQueuedHooks.Register(hook)
}

// RegisterIncomingResponseHook adds a hook that runs when a response is received
func (gs *GraphSync) RegisterIncomingResponseHook(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	return gs.incomingResponseHooks.Register(hook)
}

// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
func (gs *GraphSync) RegisterOutgoingRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	return gs.outgoingRequestHooks.Register(hook)
}

// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
func (gs *GraphSync) RegisterPersistenceOption(name string, lsys ipld.LinkSystem) error {
	return gs.persistenceOptions.Register(name, lsys)
}

// UnregisterPersistenceOption unregisters an alternate loader/storer combo
func (gs *GraphSync) UnregisterPersistenceOption(name string) error {
	return gs.persistenceOptions.Unregister(name)
}

// RegisterOutgoingBlockHook registers a hook that runs after each block is sent in a response
func (gs *GraphSync) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	return gs.outgoingBlockHooks.Register(hook)
}

// RegisterRequestUpdatedHook registers a hook that runs when an update to a request is received
func (gs *GraphSync) RegisterRequestUpdatedHook(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	return gs.requestUpdatedHooks.Register(hook)
}

// RegisterOutgoingRequestProcessingListener adds a listener that gets called when a request actually begins processing (reaches
// the top of the outgoing request queue)
func (gs *GraphSync) RegisterOutgoingRequestProcessingListener(listener graphsync.OnOutgoingRequestProcessingListener) graphsync.UnregisterHookFunc {
	return gs.outgoingRequestProcessingListeners.Register(listener)
}

// RegisterCompletedResponseListener adds a listener on the responder for completed responses
func (gs *GraphSync) RegisterCompletedResponseListener(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	return gs.completedResponseListeners.Register(listener)
}

// RegisterIncomingBlockHook adds a hook that runs when a block is received and validated (put in block store)
func (gs *GraphSync) RegisterIncomingBlockHook(hook graphsync.OnIncomingBlockHook) graphsync.UnregisterHookFunc {
	return gs.incomingBlockHooks.Register(hook)
}

// RegisterRequestorCancelledListener adds a listener on the responder for
// responses cancelled by the requestor
func (gs *GraphSync) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return gs.requestorCancelledListeners.Register(listener)
}

// RegisterBlockSentListener adds a listener for when blocks are actually sent over the wire
func (gs *GraphSync) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	return gs.blockSentListeners.Register(listener)
}

// RegisterNetworkErrorListener adds a listener for when errors occur sending data over the wire
func (gs *GraphSync) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.networkErrorListeners.Register(listener)
}

// RegisterReceiverNetworkErrorListener adds a listener for when errors occur receiving data over the wire
func (gs *GraphSync) RegisterReceiverNetworkErrorListener(listener graphsync.OnReceiverNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.receiverErrorListeners.Register(listener)
}

// Pause pauses an in progress request or response
func (gs *GraphSync) Pause(ctx context.Context, requestID graphsync.RequestID) error {
	var reqNotFound graphsync.RequestNotFoundErr
	if err := gs.requestManager.PauseRequest(ctx, requestID); !errors.As(err, &reqNotFound) {
		return err
	}
	return gs.responseManager.PauseResponse(ctx, requestID)
}

// Unpause unpauses a request or response that was paused
// Can also send extensions with unpause
func (gs *GraphSync) Unpause(ctx context.Context, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	var reqNotFound graphsync.RequestNotFoundErr
	if err := gs.requestManager.UnpauseRequest(ctx, requestID, extensions...); !errors.As(err, &reqNotFound) {
		return err
	}
	return gs.responseManager.UnpauseResponse(ctx, requestID, extensions...)
}

// Cancel cancels an in progress request or response
func (gs *GraphSync) Cancel(ctx context.Context, requestID graphsync.RequestID) error {
	var reqNotFound graphsync.RequestNotFoundErr
	if err := gs.requestManager.CancelRequest(ctx, requestID); !errors.As(err, &reqNotFound) {
		return err
	}
	return gs.responseManager.CancelResponse(ctx, requestID)
}

// SendUpdate sends an update for an in progress request or response
func (gs *GraphSync) SendUpdate(ctx context.Context, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	// TODO: error if len(extensions)==0?
	var reqNotFound graphsync.RequestNotFoundErr
	if err := gs.requestManager.UpdateRequest(ctx, requestID, extensions...); !errors.As(err, &reqNotFound) {
		return err
	}
	return gs.responseManager.UpdateResponse(ctx, requestID, extensions...)
}

// Stats produces insight on the current state of a graphsync exchange
func (gs *GraphSync) Stats() graphsync.Stats {
	outgoingRequestStats := gs.requestQueue.Stats()
	incomingRequestStats := gs.responseQueue.Stats()
	outgoingResponseStats := gs.responseAllocator.Stats()

	return graphsync.Stats{
		OutgoingRequests:  outgoingRequestStats,
		IncomingRequests:  incomingRequestStats,
		OutgoingResponses: outgoingResponseStats,
	}
}

// PeerState describes the state of graphsync for a given peer
type PeerState struct {
	OutgoingState peerstate.PeerState
	IncomingState peerstate.PeerState
}

// PeerState produces insight on the current state of a given peer
func (gs *GraphSync) PeerState(p peer.ID) PeerState {
	return PeerState{
		OutgoingState: gs.requestManager.PeerState(p),
		IncomingState: gs.responseManager.PeerState(p),
	}
}

type graphSyncReceiver GraphSync

func (gsr *graphSyncReceiver) graphSync() *GraphSync {
	return (*GraphSync)(gsr)
}

// ReceiveMessage is part of the networks Receiver interface and receives
// incoming messages from the network
func (gsr *graphSyncReceiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {

	requests := incoming.Requests()
	responses := incoming.Responses()
	blocks := incoming.Blocks()

	if len(requests) > 0 {
		gsr.graphSync().responseManager.ProcessRequests(ctx, sender, requests)
	}
	if len(responses) > 0 || len(blocks) > 0 {
		gsr.graphSync().requestManager.ProcessResponses(sender, responses, blocks)
	}
}

// ReceiveError is part of the network's Receiver interface and handles incoming
// errors from the network.
func (gsr *graphSyncReceiver) ReceiveError(p peer.ID, err error) {
	log.Infof("Graphsync ReceiveError from %s: %s", p, err)
	gsr.receiverErrorListeners.NotifyNetworkErrorListeners(p, err)
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Connected(p peer.ID) {
	gsr.graphSync().peerManager.Connected(p)
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Disconnected(p peer.ID) {
	gsr.graphSync().peerManager.Disconnected(p)
}
