package swarm

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

// DialWorerFunc is used by DialSync to spawn a new dial worker
type dialWorkerFunc func(context.Context, peer.ID, <-chan dialRequest) error

// newDialSync constructs a new DialSync
func newDialSync(worker dialWorkerFunc) *DialSync {
	return &DialSync{
		dials:      make(map[peer.ID]*activeDial),
		dialWorker: worker,
	}
}

// DialSync is a dial synchronization helper that ensures that at most one dial
// to any given peer is active at any given time.
type DialSync struct {
	dials      map[peer.ID]*activeDial
	dialsLk    sync.Mutex
	dialWorker dialWorkerFunc
}

type activeDial struct {
	id     peer.ID
	refCnt int

	ctx    context.Context
	cancel func()

	reqch chan dialRequest

	ds *DialSync
}

func (ad *activeDial) decref() {
	ad.ds.dialsLk.Lock()
	ad.refCnt--
	if ad.refCnt == 0 {
		ad.cancel()
		close(ad.reqch)
		delete(ad.ds.dials, ad.id)
	}
	ad.ds.dialsLk.Unlock()
}

func (ad *activeDial) dial(ctx context.Context, p peer.ID) (*Conn, error) {
	dialCtx := ad.ctx

	if forceDirect, reason := network.GetForceDirectDial(ctx); forceDirect {
		dialCtx = network.WithForceDirectDial(dialCtx, reason)
	}
	if simConnect, reason := network.GetSimultaneousConnect(ctx); simConnect {
		dialCtx = network.WithSimultaneousConnect(dialCtx, reason)
	}

	resch := make(chan dialResponse, 1)
	select {
	case ad.reqch <- dialRequest{ctx: dialCtx, resch: resch}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-resch:
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ds *DialSync) getActiveDial(p peer.ID) (*activeDial, error) {
	ds.dialsLk.Lock()
	defer ds.dialsLk.Unlock()

	actd, ok := ds.dials[p]
	if !ok {
		// This code intentionally uses the background context. Otherwise, if the first call
		// to Dial is canceled, subsequent dial calls will also be canceled.
		// XXX: this also breaks direct connection logic. We will need to pipe the
		// information through some other way.
		adctx, cancel := context.WithCancel(context.Background())
		actd = &activeDial{
			id:     p,
			ctx:    adctx,
			cancel: cancel,
			reqch:  make(chan dialRequest),
			ds:     ds,
		}

		err := ds.dialWorker(adctx, p, actd.reqch)
		if err != nil {
			cancel()
			return nil, err
		}

		ds.dials[p] = actd
	}

	// increase ref count before dropping dialsLk
	actd.refCnt++

	return actd, nil
}

// DialLock initiates a dial to the given peer if there are none in progress
// then waits for the dial to that peer to complete.
func (ds *DialSync) DialLock(ctx context.Context, p peer.ID) (*Conn, error) {
	ad, err := ds.getActiveDial(p)
	if err != nil {
		return nil, err
	}

	defer ad.decref()
	return ad.dial(ctx, p)
}
