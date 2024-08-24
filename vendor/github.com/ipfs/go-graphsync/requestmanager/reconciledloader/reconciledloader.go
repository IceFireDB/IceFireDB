/*
Package reconciledloader implements a block loader that can load from two different sources:
- a local store
- a series of remote responses for a given graphsync selector query

It verifies the sequence of remote responses matches the sequence
of loads called from a local selector traversal.

The reconciled loader also tracks whether or not there is a remote request in progress.

When there is no request in progress, it loads from the local store only.

When there is a request in progress, waits for remote responses before loading, and only calls
upon the local store for duplicate blocks and when traversing paths the remote was missing.

The reconciled loader assumes:
1. A single thread is calling AsyncLoad to load blocks
2. When a request is online, a seperate thread may call IngestResponse
3. Either thread may call SetRemoteState or Cleanup
4. The remote sends metadata for all blocks it traverses in the query (per GraphSync protocol spec) - whether or not
the actual block is sent.
*/
package reconciledloader

import (
	"context"
	"errors"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/reconciledloader/traversalrecord"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

var log = logging.Logger("gs-reconciledlaoder")

type settableWriter interface {
	SetBytes([]byte) error
}

type byteReader interface {
	Bytes() []byte
}

type loadAttempt struct {
	link        datamodel.Link
	linkContext linking.LinkContext
	successful  bool
	usedRemote  bool
}

func (lr loadAttempt) empty() bool {
	return lr.link == nil
}

// ReconciledLoader is an instance of the reconciled loader
type ReconciledLoader struct {
	requestID             graphsync.RequestID
	lsys                  *linking.LinkSystem
	mostRecentLoadAttempt loadAttempt
	traversalRecord       *traversalrecord.TraversalRecord
	pathTracker           pathTracker

	lock        *sync.Mutex
	signal      *sync.Cond
	open        bool
	verifier    *traversalrecord.Verifier
	remoteQueue remoteQueue
}

// NewReconciledLoader returns a new reconciled loader for the given requestID & localStore
func NewReconciledLoader(requestID graphsync.RequestID, localStore *linking.LinkSystem) *ReconciledLoader {
	lock := &sync.Mutex{}
	traversalRecord := traversalrecord.NewTraversalRecord()
	return &ReconciledLoader{
		requestID:       requestID,
		lsys:            localStore,
		lock:            lock,
		signal:          sync.NewCond(lock),
		traversalRecord: traversalRecord,
	}
}

// SetRemoteState records whether or not the request is online
func (rl *ReconciledLoader) SetRemoteOnline(online bool) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	wasOpen := rl.open
	rl.open = online
	if !rl.open && wasOpen {
		// if the queue is closing, trigger any expecting new items
		rl.signal.Signal()
		return
	}
	if rl.open && !wasOpen {
		// if we're opening a remote request, we need to reverify against what we've loaded so far
		rl.verifier = traversalrecord.NewVerifier(rl.traversalRecord)
	}
}

// Cleanup frees up some memory resources for this loader prior to throwing it away
func (rl *ReconciledLoader) Cleanup(ctx context.Context) {
	rl.lock.Lock()
	rl.remoteQueue.clear()
	rl.lock.Unlock()
}

// RetryLastLoad retries the last offline load, assuming one is present
func (rl *ReconciledLoader) RetryLastLoad() types.AsyncLoadResult {
	if rl.mostRecentLoadAttempt.link == nil {
		return types.AsyncLoadResult{Err: errors.New("cannot retry offline load when none is present")}
	}
	retryLoadAttempt := rl.mostRecentLoadAttempt
	rl.mostRecentLoadAttempt = loadAttempt{}
	if retryLoadAttempt.usedRemote {
		rl.lock.Lock()
		rl.remoteQueue.retryLast()
		rl.lock.Unlock()
	}
	return rl.BlockReadOpener(retryLoadAttempt.linkContext, retryLoadAttempt.link)
}
