package httpnet

import (
	"context"
	"sync"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/go-cid"
)

// requestTracker tracks requests to CIDs so that we can cancel all ongoing
// requests to a single CID.
type requestTracker struct {
	mux  sync.Mutex
	ctxs map[cid.Cid]*ctxMap
}

type ctxMap struct {
	mux sync.RWMutex
	m   map[context.Context]context.CancelFunc
}

// newRequestTracker creates a new requestTracker.  A request tracker provides
// a context for a CID-request. All contexts for a given CID can be
// cancelled at once with cancelRequest().
func newRequestTracker() *requestTracker {
	return &requestTracker{
		ctxs: make(map[cid.Cid]*ctxMap),
	}
}

// requestContext returns a new context to make a request for a cid. The
// context will be cancelled if cancelRequest() is called for the same
// CID.
func (rc *requestTracker) requestContext(ctx context.Context, c cid.Cid) (context.Context, context.CancelFunc) {
	var cidCtxs *ctxMap
	var ok bool
	rc.mux.Lock()
	{
		cidCtxs, ok = rc.ctxs[c]
		if !ok {
			cidCtxs = &ctxMap{m: make(map[context.Context]context.CancelFunc)}
			rc.ctxs[c] = cidCtxs
		}
	}
	rc.mux.Unlock()

	// derive the context we will return
	rCtx, rCancel := context.WithCancel(ctx)

	// store it
	cidCtxs.mux.Lock()
	{
		cidCtxs.m[rCtx] = rCancel
	}
	cidCtxs.mux.Unlock()
	// Special cancel function to clean up entry
	return rCtx, func() {
		rCancel()
		cidCtxs.mux.Lock()
		delete(cidCtxs.m, rCtx)
		cidCtxs.mux.Unlock()
	}
}

// cancelRequest cancels all contexts obtained via requestContext for the
// given CID.
func (rc *requestTracker) cancelRequest(c cid.Cid) {
	var cidCtxs *ctxMap
	var ok bool
	rc.mux.Lock()
	{
		cidCtxs, ok = rc.ctxs[c]
		delete(rc.ctxs, c)
	}
	rc.mux.Unlock()

	if !ok {
		return
	}

	log.Debugf("cancelling all requests for %s", c)
	cidCtxs.mux.Lock()
	{
		for _, cancel := range cidCtxs.m {
			cancel()
		}
	}
	cidCtxs.mux.Unlock()
}

// cleanEmptyRequests uses a single lock to perform tracker-cleanup for a
// given list wantlist when no contexts for the entry CID exist.
func (rc *requestTracker) cleanEmptyRequests(wantlist []bsmsg.Entry) {
	rc.mux.Lock()
	{
		for _, e := range wantlist {
			cidCtxs, ok := rc.ctxs[e.Cid]
			if !ok {
				continue
			}
			cidCtxs.mux.RLock()
			if len(cidCtxs.m) == 0 {
				delete(rc.ctxs, e.Cid)
			}
			cidCtxs.mux.RUnlock()

		}
	}
	rc.mux.Unlock()
}
