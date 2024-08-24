package executor

import (
	"bytes"
	"context"
	"sync/atomic"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

var log = logging.Logger("gs_request_executor")

// Manager is an interface the Executor uses to interact with the request manager
type Manager interface {
	SendRequest(peer.ID, gsmsg.GraphSyncRequest)
	GetRequestTask(peer.ID, *peertask.Task, chan RequestTask)
	ReleaseRequestTask(peer.ID, *peertask.Task, error)
}

// BlockHooks run for each block loaded
type BlockHooks interface {
	ProcessBlockHooks(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData) hooks.UpdateResult
}

// ReconciledLoader is an interface that can be used to load blocks from a local store or a remote request
type ReconciledLoader interface {
	SetRemoteOnline(online bool)
	RetryLastLoad() types.AsyncLoadResult
	BlockReadOpener(lctx linking.LinkContext, link datamodel.Link) types.AsyncLoadResult
}

// Executor handles actually executing graphsync requests and verifying them.
// It has control of requests when they are in the "running" state, while
// the manager is in charge when requests are queued or paused
type Executor struct {
	manager    Manager
	blockHooks BlockHooks
}

// NewExecutor returns a new executor
func NewExecutor(
	manager Manager,
	blockHooks BlockHooks) *Executor {
	return &Executor{
		manager:    manager,
		blockHooks: blockHooks,
	}
}

func (e *Executor) ExecuteTask(ctx context.Context, pid peer.ID, task *peertask.Task) bool {
	requestTaskChan := make(chan RequestTask)
	var requestTask RequestTask
	e.manager.GetRequestTask(pid, task, requestTaskChan)
	select {
	case requestTask = <-requestTaskChan:
	case <-ctx.Done():
		return true
	}
	if requestTask.Empty {
		log.Info("Empty task on peer request stack")
		return false
	}

	_, span := otel.Tracer("graphsync").Start(trace.ContextWithSpan(ctx, requestTask.Span), "executeTask")
	defer span.End()

	log.Debugw("beginning request execution", "id", requestTask.Request.ID(), "peer", pid.String(), "root_cid", requestTask.Request.Root().String())
	err := e.traverse(requestTask)
	if err != nil {
		span.RecordError(err)
		if !ipldutil.IsContextCancelErr(err) {
			e.manager.SendRequest(requestTask.P, gsmsg.NewCancelRequest(requestTask.Request.ID()))
			requestTask.ReconciledLoader.SetRemoteOnline(false)
			if !isPausedErr(err) {
				span.SetStatus(codes.Error, err.Error())
				select {
				case <-requestTask.Ctx.Done():
				case requestTask.InProgressErr <- err:
				}
			}
		}
	}
	e.manager.ReleaseRequestTask(pid, task, err)
	log.Debugw("finishing response execution", "id", requestTask.Request.ID(), "peer", pid.String(), "root_cid", requestTask.Request.Root().String())
	return false
}

// RequestTask are parameters for a single request execution
type RequestTask struct {
	Ctx                  context.Context
	Span                 trace.Span
	Request              gsmsg.GraphSyncRequest
	LastResponse         *atomic.Value
	DoNotSendFirstBlocks int64
	PauseMessages        <-chan struct{}
	Traverser            ipldutil.Traverser
	P                    peer.ID
	InProgressErr        chan error
	Empty                bool
	ReconciledLoader     ReconciledLoader
}

func (e *Executor) traverse(rt RequestTask) error {
	requestSent := false
	// for initial request, start remote right away
	for {
		// check if traversal is complete
		isComplete, err := rt.Traverser.IsComplete()
		if isComplete {
			return err
		}
		// get current link request
		lnk, linkContext := rt.Traverser.CurrentRequest()
		// attempt to load
		log.Debugf("will load link=%s", lnk)
		result := rt.ReconciledLoader.BlockReadOpener(linkContext, lnk)
		// if we've only loaded locally so far and hit a missing block
		// initiate remote request and retry the load operation from remote
		if _, ok := result.Err.(graphsync.RemoteMissingBlockErr); ok && !requestSent {
			requestSent = true

			// tell the loader we're online now
			rt.ReconciledLoader.SetRemoteOnline(true)

			if err := e.startRemoteRequest(rt); err != nil {
				return err
			}
			// retry the load
			result = rt.ReconciledLoader.RetryLastLoad()
		}
		log.Debugf("successfully loaded link=%s, nBlocksRead=%d", lnk, rt.Traverser.NBlocksTraversed())
		// advance the traversal based on results
		err = e.advanceTraversal(rt, result)
		if err != nil {
			return err
		}

		// check for interrupts and run block hooks
		err = e.processResult(rt, lnk, result)
		if err != nil {
			return err
		}
	}
}

func (e *Executor) processBlockHooks(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData) error {
	result := e.blockHooks.ProcessBlockHooks(p, response, block)
	if len(result.Extensions) > 0 {
		updateRequest := gsmsg.NewUpdateRequest(response.RequestID(), result.Extensions...)
		e.manager.SendRequest(p, updateRequest)
	}
	return result.Err
}

func (e *Executor) onNewBlock(rt RequestTask, block graphsync.BlockData) error {
	response := rt.LastResponse.Load().(gsmsg.GraphSyncResponse)
	return e.processBlockHooks(rt.P, response, block)
}

func (e *Executor) advanceTraversal(rt RequestTask, result types.AsyncLoadResult) error {
	if result.Err != nil {
		// before processing result check for context cancel to avoid sending an additional error
		select {
		case <-rt.Ctx.Done():
			return ipldutil.ContextCancelError{}
		default:
		}
		select {
		case <-rt.Ctx.Done():
			return ipldutil.ContextCancelError{}
		case rt.InProgressErr <- result.Err:
			if _, ok := result.Err.(graphsync.RemoteMissingBlockErr); ok {
				rt.Traverser.Error(traversal.SkipMe{})
			} else {
				rt.Traverser.Error(result.Err)
			}
			return nil
		}
	}
	return rt.Traverser.Advance(bytes.NewBuffer(result.Data))
}

func (e *Executor) processResult(rt RequestTask, link datamodel.Link, result types.AsyncLoadResult) error {
	var err error
	if result.Err == nil {
		err = e.onNewBlock(rt, &blockData{link, result.Local, uint64(len(result.Data)), int64(rt.Traverser.NBlocksTraversed())})
	}
	select {
	case <-rt.PauseMessages:
		if err == nil {
			err = hooks.ErrPaused{}
		}
	default:
	}
	return err
}

func (e *Executor) startRemoteRequest(rt RequestTask) error {
	request := rt.Request
	doNotSendFirstBlocks := rt.DoNotSendFirstBlocks
	if doNotSendFirstBlocks < int64(rt.Traverser.NBlocksTraversed()) {
		doNotSendFirstBlocks = int64(rt.Traverser.NBlocksTraversed())
	}
	if doNotSendFirstBlocks > 0 {
		doNotSendFirstBlocksData := donotsendfirstblocks.EncodeDoNotSendFirstBlocks(doNotSendFirstBlocks)
		request = rt.Request.ReplaceExtensions([]graphsync.ExtensionData{{Name: graphsync.ExtensionsDoNotSendFirstBlocks, Data: doNotSendFirstBlocksData}})
	}
	log.Debugw("starting remote request", "id", rt.Request.ID(), "peer", rt.P.String(), "root_cid", rt.Request.Root().String())
	e.manager.SendRequest(rt.P, request)
	return nil
}

func isPausedErr(err error) bool {
	_, isPaused := err.(hooks.ErrPaused)
	return isPaused
}

type blockData struct {
	link  datamodel.Link
	local bool
	size  uint64
	index int64
}

// Link is the link/cid for the block
func (bd *blockData) Link() datamodel.Link {
	return bd.link
}

// BlockSize specifies the size of the block
func (bd *blockData) BlockSize() uint64 {
	return bd.size
}

// BlockSize specifies the amount of data actually transmitted over the network
func (bd *blockData) BlockSizeOnWire() uint64 {
	if bd.local {
		return 0
	}
	return bd.size
}

func (bd *blockData) Index() int64 {
	return bd.index
}
