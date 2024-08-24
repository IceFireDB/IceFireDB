package queryexecutor

import (
	"bytes"
	"context"
	"io"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
)

var log = logging.Logger("gs-queryexecutor")

type errorString string

func (e errorString) Error() string {
	return string(e)
}

const ErrNetworkError = errorString("network error")
const ErrCancelledByCommand = errorString("response cancelled by responder")

// ErrFirstBlockLoad indicates the traversal was unable to load the very first block in the traversal
const ErrFirstBlockLoad = errorString("Unable to load first block")

// ResponseTask returns all information needed to execute a given response
type ResponseTask struct {
	Empty          bool
	Ctx            context.Context
	Span           trace.Span
	Request        gsmsg.GraphSyncRequest
	Loader         ipld.BlockReadOpener
	Traverser      ipldutil.Traverser
	Signals        ResponseSignals
	ResponseStream ResponseStream
}

// ResponseSignals are message channels to communicate between the manager and the QueryExecutor
type ResponseSignals struct {
	PauseSignal  chan struct{}
	UpdateSignal chan struct{}
	ErrSignal    chan error
}

// QueryExecutor is responsible for performing individual requests by executing their traversals
type QueryExecutor struct {
	ctx         context.Context
	manager     Manager
	blockHooks  BlockHooks
	updateHooks UpdateHooks
}

// New creates a new QueryExecutor
func New(ctx context.Context,
	manager Manager,
	blockHooks BlockHooks,
	updateHooks UpdateHooks,
) *QueryExecutor {
	qm := &QueryExecutor{
		blockHooks:  blockHooks,
		updateHooks: updateHooks,
		manager:     manager,
		ctx:         ctx,
	}
	return qm
}

// ExecuteTask takes a single task and executes its traversal it describes. For each block, it
// checks for signals on the task's ResponseSignals, updates on the QueryExecutor's UpdateHooks,
// and uses the ResponseAssembler to build and send a response, while also triggering any of
// the QueryExecutor's BlockHooks. Traversal continues until complete, or a signal or hook
// suggests we should stop or pause.
func (qe *QueryExecutor) ExecuteTask(_ context.Context, pid peer.ID, task *peertask.Task) bool {
	// StartTask lets us block until this task is at the top of the execution stack
	responseTaskChan := make(chan ResponseTask)
	var rt ResponseTask
	qe.manager.StartTask(task, pid, responseTaskChan)
	select {
	case rt = <-responseTaskChan:
	case <-qe.ctx.Done():
		return true
	}
	if rt.Empty {
		log.Info("Empty task on peer request stack")
		return false
	}

	ctx, span := otel.Tracer("graphsync").Start(trace.ContextWithSpan(qe.ctx, rt.Span), "executeTask")
	defer span.End()

	log.Debugw("beginning response execution", "id", rt.Request.ID(), "peer", pid.String(), "root_cid", rt.Request.Root().String())
	err := qe.executeQuery(ctx, pid, rt)
	if err != nil {
		span.RecordError(err)
		if _, isPaused := err.(hooks.ErrPaused); !isPaused {
			span.SetStatus(codes.Error, err.Error())
		}
	}
	qe.manager.FinishTask(task, pid, err)
	log.Debugw("finishing response execution", "id", rt.Request.ID(), "peer", pid.String(), "root_cid", rt.Request.Root().String())
	return false
}

func (qe *QueryExecutor) executeQuery(
	ctx context.Context, p peer.ID, rt ResponseTask) error {

	// Execute the traversal operation, continue until we have reason to stop (error, pause, complete)
	err := qe.runTraversal(ctx, p, rt)

	_, isPaused := err.(hooks.ErrPaused)
	if isPaused {
		return err
	}

	if err == ErrNetworkError || ipldutil.IsContextCancelErr(err) {
		rt.ResponseStream.ClearRequest()
		return err
	}

	// Close out the response, either temporarily (pause) or permanently (cancel, fail, complete)
	return rt.ResponseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		switch err {
		case nil:
			rb.FinishRequest()
		case ErrFirstBlockLoad:
			rb.FinishWithError(graphsync.RequestFailedContentNotFound)
		case ErrCancelledByCommand:
			rb.FinishWithError(graphsync.RequestCancelled)
		default:
			rb.FinishWithError(graphsync.RequestFailedUnknown)
		}
		return err
	})
}

// checkForUpdates is called on each block traversed to ensure no outstanding signals
// or updates need to be handled during the current transaction
func (qe *QueryExecutor) checkForUpdates(
	p peer.ID, taskData ResponseTask, rb responseassembler.ResponseBuilder) error {
	for {
		select {
		case <-taskData.Signals.PauseSignal:
			rb.PauseRequest()
			return hooks.ErrPaused{}
		case err := <-taskData.Signals.ErrSignal:
			return err
		case <-taskData.Signals.UpdateSignal:
			updateChan := make(chan []gsmsg.GraphSyncRequest)
			qe.manager.GetUpdates(taskData.Request.ID(), updateChan)
			select {
			case updates := <-updateChan:
				for _, update := range updates {
					result := qe.updateHooks.ProcessUpdateHooks(p, taskData.Request, update)
					for _, extension := range result.Extensions {
						// if there is something to send to the client for this update, build it into the
						// response that will be sent with the current transaction
						rb.SendExtensionData(extension)
					}
					if result.Err != nil {
						return result.Err
					}
				}
			case <-qe.ctx.Done():
			}
		default:
			return nil
		}
	}
}

func (qe *QueryExecutor) runTraversal(ctx context.Context, p peer.ID, taskData ResponseTask) error {
	for {
		traverser := taskData.Traverser
		isComplete, err := traverser.IsComplete()
		if isComplete {
			if err != nil {
				log.Errorf("traversal completion check failed, nBlocksRead=%d, err=%s", traverser.NBlocksTraversed(), err)
				if (traverser.NBlocksTraversed() == 0 && err == traversal.SkipMe{}) {
					return ErrFirstBlockLoad
				}
			} else {
				log.Debugf("traversal completed successfully, nBlocksRead=%d", traverser.NBlocksTraversed())
			}
			return err
		}
		lnk, lnkCtx := taskData.Traverser.CurrentRequest()
		ctx, span := otel.Tracer("graphsync").Start(ctx, "processBlock", trace.WithAttributes(
			attribute.String("cid", lnk.String()),
		))
		data, err := qe.loadBlock(ctx, taskData, lnk, lnkCtx)
		if err != nil {
			span.End()
			return err
		}
		err = qe.sendResponse(ctx, p, taskData, lnk, data)
		if err != nil {
			span.End()
			return err
		}
		span.End()
	}
}

func (qe *QueryExecutor) loadBlock(ctx context.Context, taskData ResponseTask, lnk ipld.Link, lnkCtx ipld.LinkContext) ([]byte, error) {
	_, span := otel.Tracer("graphsync").Start(ctx, "loadBlock")
	defer span.End()

	log.Debugf("will load link=%s", lnk)
	result, err := taskData.Loader(lnkCtx, lnk)

	if err != nil {
		log.Errorf("failed to load link=%s, nBlocksRead=%d, err=%s", lnk, taskData.Traverser.NBlocksTraversed(), err)
		taskData.Traverser.Error(traversal.SkipMe{})
		return nil, nil
	}

	blockBuffer, ok := result.(*bytes.Buffer)
	if !ok {
		blockBuffer = new(bytes.Buffer)
		_, err = io.Copy(blockBuffer, result)
		if err != nil {
			log.Errorf("failed to write to buffer, link=%s, nBlocksRead=%d, err=%s", lnk, taskData.Traverser.NBlocksTraversed(), err)
			taskData.Traverser.Error(err)
			return nil, err
		}
	}
	data := blockBuffer.Bytes()
	err = taskData.Traverser.Advance(blockBuffer)
	if err != nil {
		log.Errorf("failed to advance traversal, link=%s, nBlocksRead=%d, err=%s", lnk, taskData.Traverser.NBlocksTraversed(), err)
		return data, err
	}
	log.Debugf("successfully loaded link=%s, nBlocksRead=%d", lnk, taskData.Traverser.NBlocksTraversed())
	return data, nil
}

func (qe *QueryExecutor) sendResponse(ctx context.Context, p peer.ID, taskData ResponseTask, link ipld.Link, data []byte) error {
	// Execute a transaction for this block, including any other queued operations
	return taskData.ResponseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		ctx, span := otel.Tracer("graphsync").Start(ctx, "sendBlock", trace.WithLinks(trace.LinkFromContext(rb.Context())))
		defer span.End()
		// Ensure that any updates that have occurred till now are integrated into the response
		err := qe.checkForUpdates(p, taskData, rb)
		// On any error other than a pause, we bail, if it's a pause then we continue processing _this_ block
		if _, ok := err.(hooks.ErrPaused); !ok && err != nil {
			return err
		}
		blockData := rb.SendResponse(link, data)
		if blockData.BlockSize() > 0 {
			_, span := otel.Tracer("graphsync").Start(ctx, "processBlockHooks")
			result := qe.blockHooks.ProcessBlockHooks(p, taskData.Request, blockData)
			span.End()
			for _, extension := range result.Extensions {
				rb.SendExtensionData(extension)
			}
			if _, ok := result.Err.(hooks.ErrPaused); ok {
				rb.PauseRequest()
			}
			if result.Err != nil {
				return result.Err // halts the traversal and returns to the top-level `err`
			}
		}
		return err
	})
}

// Manager providers an interface to the response manager
type Manager interface {
	StartTask(task *peertask.Task, p peer.ID, responseTaskChan chan<- ResponseTask)
	GetUpdates(requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest)
	FinishTask(task *peertask.Task, p peer.ID, err error)
}

// BlockHooks is an interface for processing block hooks
type BlockHooks interface {
	ProcessBlockHooks(p peer.ID, request graphsync.RequestData, blockData graphsync.BlockData) hooks.BlockResult
}

// UpdateHooks is an interface for processing update hooks
type UpdateHooks interface {
	ProcessUpdateHooks(p peer.ID, request graphsync.RequestData, update graphsync.RequestData) hooks.UpdateResult
}

// ResponseStream is an interface that returns sender interfaces for peer responses.
type ResponseStream interface {
	ClearRequest()
	Transaction(transaction responseassembler.Transaction) error
}
