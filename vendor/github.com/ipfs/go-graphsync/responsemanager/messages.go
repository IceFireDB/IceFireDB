package responsemanager

import (
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/peerstate"
	"github.com/ipfs/go-graphsync/responsemanager/queryexecutor"
)

type processRequestsMessage struct {
	p        peer.ID
	requests []gsmsg.GraphSyncRequest
}

func (prm *processRequestsMessage) handle(rm *ResponseManager) {
	rm.processRequests(prm.p, prm.requests)
}

type updateRequestMessage struct {
	requestID  graphsync.RequestID
	extensions []graphsync.ExtensionData
	response   chan error
}

func (urm *updateRequestMessage) handle(rm *ResponseManager) {
	err := rm.updateRequest(urm.requestID, urm.extensions)
	select {
	case <-rm.ctx.Done():
	case urm.response <- err:
	}
}

type pauseRequestMessage struct {
	requestID graphsync.RequestID
	response  chan error
}

func (prm *pauseRequestMessage) handle(rm *ResponseManager) {
	err := rm.pauseRequest(prm.requestID)
	select {
	case <-rm.ctx.Done():
	case prm.response <- err:
	}
}

type errorRequestMessage struct {
	requestID graphsync.RequestID
	err       error
	response  chan error
}

func (erm *errorRequestMessage) handle(rm *ResponseManager) {
	err := rm.abortRequest(rm.ctx, erm.requestID, erm.err)
	select {
	case <-rm.ctx.Done():
	case erm.response <- err:
	}
}

type synchronizeMessage struct {
	sync chan error
}

func (sm *synchronizeMessage) handle(rm *ResponseManager) {
	select {
	case <-rm.ctx.Done():
	case sm.sync <- nil:
	}
}

type unpauseRequestMessage struct {
	requestID  graphsync.RequestID
	response   chan error
	extensions []graphsync.ExtensionData
}

func (urm *unpauseRequestMessage) handle(rm *ResponseManager) {
	err := rm.unpauseRequest(urm.requestID, urm.extensions...)
	select {
	case <-rm.ctx.Done():
	case urm.response <- err:
	}
}

type responseUpdateRequest struct {
	requestID  graphsync.RequestID
	updateChan chan<- []gsmsg.GraphSyncRequest
}

func (rur *responseUpdateRequest) handle(rm *ResponseManager) {
	updates := rm.getUpdates(rur.requestID)
	select {
	case <-rm.ctx.Done():
	case rur.updateChan <- updates:
	}
}

type finishTaskRequest struct {
	task *peertask.Task
	p    peer.ID
	err  error
	done chan struct{}
}

func (ftr *finishTaskRequest) handle(rm *ResponseManager) {
	rm.finishTask(ftr.task, ftr.p, ftr.err)
	select {
	case <-rm.ctx.Done():
	case ftr.done <- struct{}{}:
	}
}

type startTaskRequest struct {
	task         *peertask.Task
	p            peer.ID
	taskDataChan chan<- queryexecutor.ResponseTask
}

func (str *startTaskRequest) handle(rm *ResponseManager) {
	taskData := rm.startTask(str.task, str.p)

	select {
	case <-rm.ctx.Done():
	case str.taskDataChan <- taskData:
	}
}

type peerStateMessage struct {
	p             peer.ID
	peerStatsChan chan<- peerstate.PeerState
}

func (psm *peerStateMessage) handle(rm *ResponseManager) {
	peerState := rm.peerState(psm.p)
	select {
	case psm.peerStatsChan <- peerState:
	case <-rm.ctx.Done():
	}
}

type terminateRequestMessage struct {
	requestID graphsync.RequestID
	done      chan<- struct{}
}

func (trm *terminateRequestMessage) handle(rm *ResponseManager) {
	rm.terminateRequest(trm.requestID)
	select {
	case <-rm.ctx.Done():
	case trm.done <- struct{}{}:
	}
}
