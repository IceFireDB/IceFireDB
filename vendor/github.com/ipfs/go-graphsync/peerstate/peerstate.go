package peerstate

import (
	"fmt"

	"github.com/ipfs/go-graphsync"
)

// TaskQueueState describes the the set of requests for a given peer in a task queue
type TaskQueueState struct {
	Active  []graphsync.RequestID
	Pending []graphsync.RequestID
}

// PeerState tracks the over all state of a given peer for either
// incoming or outgoing requests
type PeerState struct {
	graphsync.RequestStates
	TaskQueueState
}

// Diagnostics compares request states with the current state of the task queue to identify unexpected
// states or inconsistences between the tracked task queue and the tracked requests
func (ps PeerState) Diagnostics() map[graphsync.RequestID][]string {
	matchedActiveQueue := make(map[graphsync.RequestID]struct{}, len(ps.RequestStates))
	matchedPendingQueue := make(map[graphsync.RequestID]struct{}, len(ps.RequestStates))
	diagnostics := make(map[graphsync.RequestID][]string)
	for _, id := range ps.TaskQueueState.Active {
		status, ok := ps.RequestStates[id]
		if ok {
			matchedActiveQueue[id] = struct{}{}
			if status != graphsync.Running {
				diagnostics[id] = append(diagnostics[id], fmt.Sprintf("expected request with id %s in active task queue to be in running state, but was %s", id.String(), status))
			}
		} else {
			diagnostics[id] = append(diagnostics[id], fmt.Sprintf("request with id %s in active task queue but appears to have no tracked state", id.String()))
		}
	}
	for _, id := range ps.TaskQueueState.Pending {
		status, ok := ps.RequestStates[id]
		if ok {
			matchedPendingQueue[id] = struct{}{}
			if status != graphsync.Queued {
				diagnostics[id] = append(diagnostics[id], fmt.Sprintf("expected request with id %s in pending task queue to be in queued state, but was %s", id.String(), status))
			}
		} else {
			diagnostics[id] = append(diagnostics[id], fmt.Sprintf("request with id %s in pending task queue but appears to have no tracked state", id.String()))
		}
	}
	for id, state := range ps.RequestStates {
		if state == graphsync.Running {
			if _, ok := matchedActiveQueue[id]; !ok {
				diagnostics[id] = append(diagnostics[id], fmt.Sprintf("request with id %s in running state is not in the active task queue", id.String()))
			}
		}
		if state == graphsync.Queued {
			if _, ok := matchedPendingQueue[id]; !ok {
				diagnostics[id] = append(diagnostics[id], fmt.Sprintf("request with id %s in queued state is not in the pending task queue", id.String()))
			}
		}
	}
	return diagnostics
}
