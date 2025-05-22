package httpnet

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"go.uber.org/multierr"
)

// pinger pings connected hosts on regular intervals
// and tracks their latency.
type pinger struct {
	ht *Network

	latenciesLock sync.RWMutex
	latencies     map[peer.ID]time.Duration

	pingsLock sync.RWMutex
	pings     map[peer.ID]context.CancelFunc
}

func newPinger(ht *Network, pingCid string) *pinger {
	return &pinger{
		ht:        ht,
		latencies: make(map[peer.ID]time.Duration),
		pings:     make(map[peer.ID]context.CancelFunc),
	}
}

// ping sends a ping packet to the first known url of the given peer and
// returns the result with the latency for this peer. The result is also
// recorded.
func (pngr *pinger) ping(ctx context.Context, p peer.ID) ping.Result {
	pi := pngr.ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return ping.Result{
			Error: ErrNoHTTPAddresses,
		}
	}

	method := "GET"
	if supportsHave(pngr.ht.host.Peerstore(), p) {
		method = "HEAD"
	}

	results := make(chan ping.Result, len(urls))
	for _, u := range urls {
		go func(u network.ParsedURL) {
			start := time.Now()
			err := pngr.ht.connectToURL(ctx, p, u, method)
			if err != nil {
				log.Debug(err)
				results <- ping.Result{Error: err}
				return
			}
			results <- ping.Result{
				RTT: time.Since(start),
			}
		}(u)
	}

	var result ping.Result
	var errors error
	for i := 0; i < len(urls); i++ {
		r := <-results
		if r.Error != nil {
			errors = multierr.Append(errors, r.Error)
			continue
		}
		result.RTT += r.RTT
	}
	close(results)

	lenErrors := len(multierr.Errors(errors))
	// if all urls failed return that, otherwise ignore.
	if lenErrors == len(urls) {
		return ping.Result{
			Error: errors,
		}
	}
	result.RTT = result.RTT / time.Duration(len(urls)-lenErrors)

	//log.Debugf("ping latency %s %s", p, result.RTT)
	pngr.recordLatency(p, result.RTT)
	return result
}

// latency returns the recorded latency for the given peer.
func (pngr *pinger) latency(p peer.ID) time.Duration {
	var lat time.Duration
	pngr.latenciesLock.RLock()
	{
		lat = pngr.latencies[p]
	}
	pngr.latenciesLock.RUnlock()
	return lat
}

// recordLatency stores a new latency measurement for the given peer using an
// Exponetially Weighted Moving Average similar to LatencyEWMA from the
// peerstore.
func (pngr *pinger) recordLatency(p peer.ID, next time.Duration) {
	nextf := float64(next)
	s := 0.1
	pngr.latenciesLock.Lock()
	{
		ewma, found := pngr.latencies[p]
		ewmaf := float64(ewma)
		if !found {
			pngr.latencies[p] = next // when no data, just take it as the mean.
		} else {
			nextf = ((1.0 - s) * ewmaf) + (s * nextf)
			pngr.latencies[p] = time.Duration(nextf)
		}
	}
	pngr.latenciesLock.Unlock()
}

func (pngr *pinger) startPinging(p peer.ID) {
	pngr.pingsLock.Lock()
	defer pngr.pingsLock.Unlock()

	_, ok := pngr.pings[p]
	if ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	pngr.pings[p] = cancel

	go func(ctx context.Context, p peer.ID) {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pngr.ping(ctx, p)
			}
		}
	}(ctx, p)

}

func (pngr *pinger) stopPinging(p peer.ID) {
	pngr.pingsLock.Lock()
	{
		cancel, ok := pngr.pings[p]
		if ok {
			cancel()
		}
		delete(pngr.pings, p)
	}
	pngr.pingsLock.Unlock()
	pngr.latenciesLock.Lock()
	delete(pngr.latencies, p)
	pngr.latenciesLock.Unlock()

}

func (pngr *pinger) isPinging(p peer.ID) bool {
	pngr.pingsLock.RLock()
	defer pngr.pingsLock.RUnlock()

	_, ok := pngr.pings[p]
	return ok
}
