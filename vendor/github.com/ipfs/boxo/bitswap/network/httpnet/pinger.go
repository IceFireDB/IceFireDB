package httpnet

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// errProbesInCooldown is returned by ping when every endpoint of the peer is
// in cooldown, so no probe was sent.
var errProbesInCooldown = errors.New("all peer endpoints are in cooldown, no probe sent")

// pinger is the registry of connected HTTP peers and their latency estimate.
// The latency EWMA is seeded from the Connect probe and updated with response
// times of real retrieval requests. ping sends an on-demand probe only when
// asked.
type pinger struct {
	ht *Network

	latenciesLock sync.RWMutex
	latencies     map[peer.ID]time.Duration

	connectedLock sync.RWMutex
	connected     map[peer.ID]struct{}
}

func newPinger(ht *Network) *pinger {
	return &pinger{
		ht:        ht,
		latencies: make(map[peer.ID]time.Duration),
		connected: make(map[peer.ID]struct{}),
	}
}

// ping sends a probe to the known urls of the given peer and returns the
// result with the latency for this peer. The result is also recorded. URLs
// whose host is in cooldown are skipped; when every url is cooling down no
// request is made at all and the result carries errProbesInCooldown.
func (pngr *pinger) ping(ctx context.Context, p peer.ID) ping.Result {
	pi := pngr.ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return ping.Result{
			Error: ErrNoHTTPAddresses,
		}
	}

	// Do not probe hosts that asked us to back off.
	probeURLs := make([]network.ParsedURL, 0, len(urls))
	for _, u := range urls {
		if _, cooling := pngr.ht.cooldownTracker.inCooldown(u.URL.Host); !cooling {
			probeURLs = append(probeURLs, u)
		}
	}
	if len(probeURLs) == 0 {
		return ping.Result{
			Error: errProbesInCooldown,
		}
	}

	method := "GET"
	if supportsHave(pngr.ht.host.Peerstore(), p) {
		method = "HEAD"
	}

	results := make(chan ping.Result, len(probeURLs))
	for _, u := range probeURLs {
		go func(u network.ParsedURL) {
			start := time.Now()
			_, _, err := pngr.ht.connectToURL(ctx, p, u, method)
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
	var errs []error
	for range probeURLs {
		r := <-results
		if r.Error != nil {
			errs = append(errs, r.Error)
			continue
		}
		result.RTT += r.RTT
	}
	close(results)

	lenErrors := len(errs)
	// if all urls failed return that, otherwise ignore.
	if lenErrors == len(probeURLs) {
		return ping.Result{
			Error: errors.Join(errs...),
		}
	}
	result.RTT = result.RTT / time.Duration(len(probeURLs)-lenErrors)

	pngr.recordLatencyIfConnected(p, result.RTT)
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

// recordLatencyIfConnected records the measurement only while the peer is in
// the connected registry, so a sample from an in-flight request cannot
// resurrect latency state for a peer that just disconnected.
func (pngr *pinger) recordLatencyIfConnected(p peer.ID, next time.Duration) {
	// Coarse monotonic clocks (Windows ticks at ~0.5ms) measure a fast
	// round-trip over a pooled connection as zero, which downstream reads
	// as "no latency data". Floor the sample so a measured peer never
	// looks unmeasured.
	if next <= 0 {
		next = time.Millisecond
	}

	pngr.connectedLock.RLock()
	defer pngr.connectedLock.RUnlock()

	if _, ok := pngr.connected[p]; !ok {
		return
	}
	pngr.recordLatency(p, next)
}

// markConnected adds the peer to the connected registry. It is idempotent.
func (pngr *pinger) markConnected(p peer.ID) {
	pngr.connectedLock.Lock()
	defer pngr.connectedLock.Unlock()

	if _, ok := pngr.connected[p]; ok {
		log.Debugf("already connected to %s", p)
		return
	}

	log.Debugf("marking %s as connected", p)
	pngr.connected[p] = struct{}{}
}

// markDisconnected removes the peer from the connected registry and wipes its
// recorded latency, so a later reconnection starts from a fresh measurement.
// Lock order: connectedLock, then latenciesLock. recordLatencyIfConnected
// nests the same way, so no recording path can repopulate the latency of a
// peer once this returns.
func (pngr *pinger) markDisconnected(p peer.ID) {
	log.Debugf("marking %s as disconnected", p)
	pngr.connectedLock.Lock()
	{
		delete(pngr.connected, p)

		pngr.latenciesLock.Lock()
		delete(pngr.latencies, p)
		pngr.latenciesLock.Unlock()
	}
	pngr.connectedLock.Unlock()
}

// isConnected reports whether the peer is in the connected registry.
func (pngr *pinger) isConnected(p peer.ID) bool {
	pngr.connectedLock.RLock()
	defer pngr.connectedLock.RUnlock()

	_, ok := pngr.connected[p]
	return ok
}
