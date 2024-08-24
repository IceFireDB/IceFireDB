package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/peerstate"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/reconciledloader"
)

// The code in this file implements the internal thread for the request manager.
// These functions can modify the internal state of the RequestManager

func (rm *RequestManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	defer rm.cleanupInProcessRequests()

	for {
		select {
		case message := <-rm.messages:
			message.handle(rm)
		case <-rm.ctx.Done():
			return
		}
	}
}

func (rm *RequestManager) cleanupInProcessRequests() {
	for _, requestStatus := range rm.inProgressRequestStatuses {
		requestStatus.cancelFn()
	}
}

func (rm *RequestManager) newRequest(parentSpan trace.Span, p peer.ID, root ipld.Link, selector ipld.Node, extensions []graphsync.ExtensionData) (gsmsg.GraphSyncRequest, chan graphsync.ResponseProgress, chan error) {
	requestID := graphsync.NewRequestID()

	parentSpan.SetAttributes(attribute.String("requestID", requestID.String()))
	ctx, span := otel.Tracer("graphsync").Start(trace.ContextWithSpan(rm.ctx, parentSpan), "newRequest")
	defer span.End()

	log.Infow("graphsync request initiated", "request id", requestID.String(), "peer", p, "root", root)

	request, hooksResult, lsys, err := rm.validateRequest(requestID, p, root, selector, extensions)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		defer parentSpan.End()
		rp, err := rm.singleErrorResponse(err)
		return request, rp, err
	}
	doNotSendFirstBlocksData, has := request.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
	var doNotSendFirstBlocks int64
	if has {
		doNotSendFirstBlocks, err = donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			defer parentSpan.End()
			rp, err := rm.singleErrorResponse(err)
			return request, rp, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	requestStatus := &inProgressRequestStatus{
		ctx:                  ctx,
		span:                 parentSpan,
		startTime:            time.Now(),
		cancelFn:             cancel,
		p:                    p,
		pauseMessages:        make(chan struct{}, 1),
		doNotSendFirstBlocks: doNotSendFirstBlocks,
		request:              request,
		state:                graphsync.Queued,
		nodeStyleChooser:     hooksResult.CustomChooser,
		inProgressChan:       make(chan graphsync.ResponseProgress),
		inProgressErr:        make(chan error),
		lsys:                 lsys,
	}
	requestStatus.lastResponse.Store(gsmsg.NewResponse(request.ID(), graphsync.RequestAcknowledged, nil))
	rm.inProgressRequestStatuses[request.ID()] = requestStatus

	rm.connManager.Protect(p, requestID.Tag())
	rm.requestQueue.PushTask(p, peertask.Task{Topic: requestID, Priority: math.MaxInt32, Work: 1})
	return request, requestStatus.inProgressChan, requestStatus.inProgressErr
}

func (rm *RequestManager) requestTask(requestID graphsync.RequestID) executor.RequestTask {
	ipr, ok := rm.inProgressRequestStatuses[requestID]
	if !ok {
		return executor.RequestTask{Empty: true}
	}
	log.Infow("graphsync request processing begins", "request id", requestID.String(), "peer", ipr.p, "total time", time.Since(ipr.startTime))

	if ipr.traverser == nil {
		var budget *traversal.Budget
		if rm.maxLinksPerRequest > 0 {
			budget = &traversal.Budget{
				NodeBudget: math.MaxInt64,
				LinkBudget: int64(rm.maxLinksPerRequest),
			}
		}
		// the traverser has its own context because we want to fail on block boundaries, in the executor,
		// and make sure all blocks included up to the termination message
		// are processed and passed in the response channel
		ctx, cancel := context.WithCancel(trace.ContextWithSpan(rm.ctx, ipr.span))
		ipr.traverserCancel = cancel
		ipr.traverser = ipldutil.TraversalBuilder{
			Root:     cidlink.Link{Cid: ipr.request.Root()},
			Selector: ipr.request.Selector(),
			Visitor: func(tp traversal.Progress, node ipld.Node, tr traversal.VisitReason) error {
				if lbn, ok := node.(datamodel.LargeBytesNode); ok {
					s, err := lbn.AsLargeBytes()
					if err != nil {
						log.Warnf("error %s in AsLargeBytes at path %s", err.Error(), tp.Path)
					}
					_, err = io.Copy(ioutil.Discard, s)
					if err != nil {
						log.Warnf("error %s reading bytes from reader at path %s", err.Error(), tp.Path)
					}
				}
				select {
				case <-ctx.Done():
				case ipr.inProgressChan <- graphsync.ResponseProgress{
					Node:      node,
					Path:      tp.Path,
					LastBlock: tp.LastBlock,
				}:
				}
				return nil
			},
			Chooser:    ipr.nodeStyleChooser,
			LinkSystem: rm.linkSystem,
			Budget:     budget,
		}.Start(ctx)

		ipr.reconciledLoader = reconciledloader.NewReconciledLoader(ipr.request.ID(), ipr.lsys)
		inProgressCount := len(rm.inProgressRequestStatuses)
		rm.outgoingRequestProcessingListeners.NotifyOutgoingRequestProcessingListeners(ipr.p, ipr.request, inProgressCount)
	}

	ipr.state = graphsync.Running
	return executor.RequestTask{
		Ctx:                  ipr.ctx,
		Span:                 ipr.span,
		Request:              ipr.request,
		LastResponse:         &ipr.lastResponse,
		DoNotSendFirstBlocks: ipr.doNotSendFirstBlocks,
		PauseMessages:        ipr.pauseMessages,
		Traverser:            ipr.traverser,
		P:                    ipr.p,
		InProgressErr:        ipr.inProgressErr,
		ReconciledLoader:     ipr.reconciledLoader,
		Empty:                false,
	}
}

func (rm *RequestManager) getRequestTask(p peer.ID, task *peertask.Task) executor.RequestTask {
	requestID := task.Topic.(graphsync.RequestID)
	requestExecution := rm.requestTask(requestID)
	if requestExecution.Empty {
		rm.requestQueue.TaskDone(p, task)
	}
	return requestExecution
}

func (rm *RequestManager) terminateRequest(requestID graphsync.RequestID, ipr *inProgressRequestStatus) {
	_, span := otel.Tracer("graphsync").Start(trace.ContextWithSpan(rm.ctx, ipr.span), "terminateRequest")
	defer span.End()
	defer ipr.span.End() // parent span for this whole request

	if ipr.terminalError != nil {
		select {
		case ipr.inProgressErr <- ipr.terminalError:
		case <-rm.ctx.Done():
		}
	}
	rm.connManager.Unprotect(ipr.p, requestID.Tag())
	delete(rm.inProgressRequestStatuses, requestID)
	ipr.cancelFn()
	if ipr.reconciledLoader != nil {
		ipr.reconciledLoader.Cleanup(rm.ctx)
	}
	if ipr.traverser != nil {
		ipr.traverserCancel()
		ipr.traverser.Shutdown(rm.ctx)
	}
	// make sure context is not closed before closing channels (could cause send
	// on close channel otherwise)
	select {
	case <-rm.ctx.Done():
		return
	default:
	}
	close(ipr.inProgressChan)
	close(ipr.inProgressErr)
	for _, onTerminated := range ipr.onTerminated {
		select {
		case <-rm.ctx.Done():
		case onTerminated <- nil:
		}
	}
}

func (rm *RequestManager) releaseRequestTask(p peer.ID, task *peertask.Task, err error) {
	requestID := task.Topic.(graphsync.RequestID)
	rm.requestQueue.TaskDone(p, task)

	ipr, ok := rm.inProgressRequestStatuses[requestID]
	if !ok {
		return
	}
	if _, ok := err.(hooks.ErrPaused); ok {
		ipr.state = graphsync.Paused
		return
	}
	log.Infow("graphsync request complete", "request id", requestID.String(), "peer", ipr.p, "total time", time.Since(ipr.startTime))
	rm.terminateRequest(requestID, ipr)
}

func (rm *RequestManager) cancelRequest(requestID graphsync.RequestID, onTerminated chan<- error, terminalError error) {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[requestID]
	if !ok {
		if onTerminated != nil {
			select {
			case onTerminated <- graphsync.RequestNotFoundErr{}:
			case <-rm.ctx.Done():
			}
		}
		return
	}

	if onTerminated != nil {
		inProgressRequestStatus.onTerminated = append(inProgressRequestStatus.onTerminated, onTerminated)
	}
	rm.SendRequest(inProgressRequestStatus.p, gsmsg.NewCancelRequest(requestID))
	rm.cancelOnError(requestID, inProgressRequestStatus, terminalError)
}

func (rm *RequestManager) cancelOnError(requestID graphsync.RequestID, ipr *inProgressRequestStatus, terminalError error) {
	if ipr.terminalError == nil {
		ipr.terminalError = terminalError
	}
	if ipr.state != graphsync.Running {
		rm.terminateRequest(requestID, ipr)
	} else {
		ipr.cancelFn()
		ipr.reconciledLoader.SetRemoteOnline(false)
	}
}

func (rm *RequestManager) processResponses(p peer.ID,
	responses []gsmsg.GraphSyncResponse,
	blks []blocks.Block) {

	log.Debugf("beginning processing responses for peer %s", p)
	requestIds := make([]string, 0, len(responses))
	for _, r := range responses {
		requestIds = append(requestIds, r.RequestID().String())
	}
	ctx, span := otel.Tracer("graphsync").Start(rm.ctx, "processResponses", trace.WithAttributes(
		attribute.String("peerID", p.Pretty()),
		attribute.StringSlice("requestIDs", requestIds),
		attribute.Int("blockCount", len(blks)),
	))
	defer span.End()
	filteredResponses := rm.processExtensions(responses, p)
	filteredResponses = rm.filterResponsesForPeer(filteredResponses, p)
	blkMap := make(map[cid.Cid][]byte, len(blks))
	for _, blk := range blks {
		blkMap[blk.Cid()] = blk.RawData()
	}
	for _, response := range filteredResponses {
		reconciledLoader := rm.inProgressRequestStatuses[response.RequestID()].reconciledLoader
		if reconciledLoader != nil {
			reconciledLoader.IngestResponse(response.Metadata(), trace.LinkFromContext(ctx), blkMap)
		}
	}
	rm.updateLastResponses(filteredResponses)
	rm.processTerminations(filteredResponses)
	log.Debugf("end processing responses for peer %s", p)
}

func (rm *RequestManager) filterResponsesForPeer(responses []gsmsg.GraphSyncResponse, p peer.ID) []gsmsg.GraphSyncResponse {
	responsesForPeer := make([]gsmsg.GraphSyncResponse, 0, len(responses))
	for _, response := range responses {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok || requestStatus.p != p {
			continue
		}
		responsesForPeer = append(responsesForPeer, response)
	}
	return responsesForPeer
}

func (rm *RequestManager) processExtensions(responses []gsmsg.GraphSyncResponse, p peer.ID) []gsmsg.GraphSyncResponse {
	remainingResponses := make([]gsmsg.GraphSyncResponse, 0, len(responses))
	for _, response := range responses {
		success := rm.processExtensionsForResponse(p, response)
		if success {
			remainingResponses = append(remainingResponses, response)
		}
	}
	return remainingResponses
}

func (rm *RequestManager) updateLastResponses(responses []gsmsg.GraphSyncResponse) {
	for _, response := range responses {
		rm.inProgressRequestStatuses[response.RequestID()].lastResponse.Store(response)
	}
}

func (rm *RequestManager) processExtensionsForResponse(p peer.ID, response gsmsg.GraphSyncResponse) bool {
	result := rm.responseHooks.ProcessResponseHooks(p, response)
	if len(result.Extensions) > 0 {
		updateRequest := gsmsg.NewUpdateRequest(response.RequestID(), result.Extensions...)
		rm.SendRequest(p, updateRequest)
	}
	if result.Err != nil {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok {
			return false
		}
		rm.SendRequest(requestStatus.p, gsmsg.NewCancelRequest(response.RequestID()))
		rm.cancelOnError(response.RequestID(), requestStatus, result.Err)
		return false
	}
	return true
}

func (rm *RequestManager) processTerminations(responses []gsmsg.GraphSyncResponse) {
	for _, response := range responses {
		if response.Status().IsTerminal() {
			if response.Status().IsFailure() {
				rm.cancelOnError(response.RequestID(), rm.inProgressRequestStatuses[response.RequestID()], response.Status().AsError())
			}
			ipr, ok := rm.inProgressRequestStatuses[response.RequestID()]
			if ok && ipr.reconciledLoader != nil {
				ipr.reconciledLoader.SetRemoteOnline(false)
			}
		}
	}
}

func (rm *RequestManager) validateRequest(requestID graphsync.RequestID, p peer.ID, root ipld.Link, selectorSpec ipld.Node, extensions []graphsync.ExtensionData) (gsmsg.GraphSyncRequest, hooks.RequestResult, *linking.LinkSystem, error) {
	_, err := ipldutil.EncodeNode(selectorSpec)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, nil, err
	}
	_, err = selector.ParseSelector(selectorSpec)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, nil, err
	}
	asCidLink, ok := root.(cidlink.Link)
	if !ok {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, nil, fmt.Errorf("request failed: link has no cid")
	}
	request := gsmsg.NewRequest(requestID, asCidLink.Cid, selectorSpec, defaultPriority, extensions...)
	hooksResult := rm.requestHooks.ProcessRequestHooks(p, request)
	if hooksResult.PersistenceOption != "" {
		dedupData, err := dedupkey.EncodeDedupKey(hooksResult.PersistenceOption)
		if err != nil {
			return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, nil, err
		}
		request = request.ReplaceExtensions([]graphsync.ExtensionData{
			{
				Name: graphsync.ExtensionDeDupByKey,
				Data: dedupData,
			},
		})
	}
	lsys := rm.linkSystem
	if hooksResult.PersistenceOption != "" {
		var has bool
		lsys, has = rm.persistenceOptions.GetLinkSystem(hooksResult.PersistenceOption)
		if !has {
			return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, nil, errors.New("unknown persistence option")
		}
	}
	return request, hooksResult, &lsys, nil
}

func (rm *RequestManager) unpause(id graphsync.RequestID, extensions []graphsync.ExtensionData) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if inProgressRequestStatus.state != graphsync.Paused {
		return errors.New("request is not paused")
	}
	inProgressRequestStatus.state = graphsync.Queued
	inProgressRequestStatus.request = inProgressRequestStatus.request.ReplaceExtensions(extensions)
	rm.requestQueue.PushTask(inProgressRequestStatus.p, peertask.Task{Topic: id, Priority: math.MaxInt32, Work: 1})
	return nil
}

func (rm *RequestManager) pause(id graphsync.RequestID) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if inProgressRequestStatus.state == graphsync.Paused {
		return errors.New("request is already paused")
	}
	select {
	case inProgressRequestStatus.pauseMessages <- struct{}{}:
	default:
	}
	return nil
}

func (rm *RequestManager) update(id graphsync.RequestID, extensions []graphsync.ExtensionData) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	updateRequest := gsmsg.NewUpdateRequest(id, extensions...)
	rm.SendRequest(inProgressRequestStatus.p, updateRequest)
	return nil
}

func (rm *RequestManager) peerStats(p peer.ID) peerstate.PeerState {
	var peerState peerstate.PeerState
	rm.requestQueue.WithPeerTopics(p, func(peerTopics *peertracker.PeerTrackerTopics) {
		requestStates := make(graphsync.RequestStates)
		for id, ipr := range rm.inProgressRequestStatuses {
			if ipr.p == p {
				requestStates[id] = graphsync.RequestState(ipr.state)
			}
		}
		peerState = peerstate.PeerState{RequestStates: requestStates, TaskQueueState: fromPeerTopics(peerTopics)}
	})
	return peerState
}

func fromPeerTopics(pt *peertracker.PeerTrackerTopics) peerstate.TaskQueueState {
	if pt == nil {
		return peerstate.TaskQueueState{}
	}
	active := make([]graphsync.RequestID, 0, len(pt.Active))
	for _, topic := range pt.Active {
		active = append(active, topic.(graphsync.RequestID))
	}
	pending := make([]graphsync.RequestID, 0, len(pt.Pending))
	for _, topic := range pt.Pending {
		pending = append(pending, topic.(graphsync.RequestID))
	}
	return peerstate.TaskQueueState{
		Active:  active,
		Pending: pending,
	}
}
