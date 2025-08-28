package httpnet

import (
	"errors"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

var errThresholdCrossed = errors.New("the peer crossed the error threshold")

type errorTracker struct {
	ht *Network

	mux    sync.RWMutex
	errors map[peer.ID]int
}

func newErrorTracker(ht *Network) *errorTracker {
	return &errorTracker{
		ht:     ht,
		errors: make(map[peer.ID]int),
	}
}

func (et *errorTracker) stopTracking(p peer.ID) {
	et.mux.Lock()
	delete(et.errors, p)
	et.mux.Unlock()
}

// logErrors adds n to the current error count for p. If the total count is above the threshold, then an error is returned. If n is 0, the the total count is reset to 0.
func (et *errorTracker) logErrors(p peer.ID, n int, threshold int) error {
	et.mux.Lock()
	defer et.mux.Unlock()

	if n == 0 { // reset error count
		delete(et.errors, p)
		return nil
	}
	count := et.errors[p]
	total := count + n
	et.errors[p] = total
	if total > threshold {
		return errThresholdCrossed
	}
	return nil
}
