package responsemanager

import (
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
)

// RequestCloser can cancel request on a network error
type RequestCloser interface {
	TerminateRequest(requestID graphsync.RequestID)
	CloseWithNetworkError(requestID graphsync.RequestID)
}

type subscriber struct {
	p                     peer.ID
	request               gsmsg.GraphSyncRequest
	requestCloser         RequestCloser
	blockSentListeners    BlockSentListeners
	networkErrorListeners NetworkErrorListeners
	completedListeners    CompletedListeners
	connManager           network.ConnManager
}

func (s *subscriber) OnNext(_ notifications.Topic, event notifications.Event) {
	responseEvent, ok := event.(messagequeue.Event)
	if !ok {
		return
	}
	switch responseEvent.Name {
	case messagequeue.Error:
		s.requestCloser.CloseWithNetworkError(s.request.ID())
		responseCode := responseEvent.Metadata.ResponseCodes[s.request.ID()]
		if responseCode.IsTerminal() {
			s.requestCloser.TerminateRequest(s.request.ID())
		}
		s.networkErrorListeners.NotifyNetworkErrorListeners(s.p, s.request, responseEvent.Err)
	case messagequeue.Sent:
		blockDatas := responseEvent.Metadata.BlockData[s.request.ID()]
		for _, blockData := range blockDatas {
			s.blockSentListeners.NotifyBlockSentListeners(s.p, s.request, blockData)
		}
		responseCode := responseEvent.Metadata.ResponseCodes[s.request.ID()]
		if responseCode.IsTerminal() {
			s.requestCloser.TerminateRequest(s.request.ID())
			s.completedListeners.NotifyCompletedListeners(s.p, s.request, responseCode)
		}
	}
}

func (s *subscriber) OnClose(_ notifications.Topic) {

}
