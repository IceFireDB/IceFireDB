package pubsub

import (
	"iter"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageBatch allows a user to batch related messages and then publish them at
// once. This allows the Scheduler to define an order for outgoing RPCs.
// This helps bandwidth constrained peers.
type MessageBatch struct {
	mu       sync.Mutex
	messages []*Message
}

func (mb *MessageBatch) add(msg *Message) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.messages = append(mb.messages, msg)
}

func (mb *MessageBatch) take() []*Message {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	messages := mb.messages
	mb.messages = nil
	return messages
}

type messageBatchAndPublishOptions struct {
	messages []*Message
	opts     *BatchPublishOptions
}

// RPCScheduler schedules outgoing RPCs.
type RPCScheduler interface {
	// AddRPC adds an RPC to the scheduler.
	AddRPC(peer peer.ID, msgID string, rpc *RPC)
	// All returns an ordered iterator of RPCs.
	All() iter.Seq2[peer.ID, *RPC]
}

type pendingRPC struct {
	peer peer.ID
	rpc  *RPC
}

// RoundRobinMessageIDScheduler schedules outgoing RPCs in round-robin order of message IDs.
type RoundRobinMessageIDScheduler struct {
	rpcs map[string][]pendingRPC
}

func (s *RoundRobinMessageIDScheduler) AddRPC(peer peer.ID, msgID string, rpc *RPC) {
	if s.rpcs == nil {
		s.rpcs = make(map[string][]pendingRPC)
	}
	s.rpcs[msgID] = append(s.rpcs[msgID], pendingRPC{peer: peer, rpc: rpc})
}

func (s *RoundRobinMessageIDScheduler) All() iter.Seq2[peer.ID, *RPC] {
	return func(yield func(peer.ID, *RPC) bool) {
		for len(s.rpcs) > 0 {
			for msgID, rpcs := range s.rpcs {
				if len(rpcs) == 0 {
					delete(s.rpcs, msgID)
					continue
				}
				if !yield(rpcs[0].peer, rpcs[0].rpc) {
					return
				}

				s.rpcs[msgID] = rpcs[1:]
			}
		}
	}
}
