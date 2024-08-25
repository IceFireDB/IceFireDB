package dht

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	ks "github.com/whyrusleeping/go-keyspace"
	"gonum.org/v1/gonum/mathext"
)

const (
	// optProvIndividualThresholdCertainty describes how sure we want to be that an individual peer that
	// we find during walking the DHT actually belongs to the k-closest peers based on the current network size
	// estimation.
	optProvIndividualThresholdCertainty = 0.9

	// optProvSetThresholdStrictness describes the probability that the set of closest peers is actually further
	// away then the calculated set threshold. Put differently, what is the probability that we are too strict and
	// don't terminate the process early because we can't find any closer peers.
	optProvSetThresholdStrictness = 0.1

	// optProvReturnRatio corresponds to how many ADD_PROVIDER RPCs must have completed (regardless of success)
	// before we return to the user. The ratio of 0.75 equals 15 RPC as it is based on the Kademlia bucket size.
	optProvReturnRatio = 0.75
)

type addProviderRPCState int

const (
	scheduled addProviderRPCState = iota + 1
	success
	failure
)

type optimisticState struct {
	// context for all ADD_PROVIDER RPCs
	putCtx context.Context

	// reference to the DHT
	dht *IpfsDHT

	// the most recent network size estimation
	networkSize int32

	// a channel indicating when an ADD_PROVIDER RPC completed (successful or not)
	doneChan chan struct{}

	// tracks which peers we have stored the provider records with
	peerStatesLk sync.RWMutex
	peerStates   map[peer.ID]addProviderRPCState

	// the key to provide
	key string

	// the key to provide transformed into the Kademlia key space
	ksKey ks.Key

	// distance threshold for individual peers. If peers are closer than this number we store
	// the provider records right away.
	individualThreshold float64

	// distance threshold for the set of bucketSize closest peers. If the average distance of the bucketSize
	// closest peers is below this number we stop the DHT walk and store the remaining provider records.
	// "remaining" because we have likely already stored some on peers that were below the individualThreshold.
	setThreshold float64

	// number of completed (regardless of success) ADD_PROVIDER RPCs before we return control back to the user.
	returnThreshold int

	// putProvDone counts the ADD_PROVIDER RPCs that have completed (successful and unsuccessful)
	putProvDone atomic.Int32
}

func (dht *IpfsDHT) newOptimisticState(ctx context.Context, key string) (*optimisticState, error) {
	// get network size and err out if there is no reasonable estimate
	networkSize, err := dht.nsEstimator.NetworkSize()
	if err != nil {
		return nil, err
	}

	individualThreshold := mathext.GammaIncRegInv(float64(dht.bucketSize), 1-optProvIndividualThresholdCertainty) / float64(networkSize)
	setThreshold := mathext.GammaIncRegInv(float64(dht.bucketSize)/2.0+1, 1-optProvSetThresholdStrictness) / float64(networkSize)
	returnThreshold := int(math.Ceil(float64(dht.bucketSize) * optProvReturnRatio))

	return &optimisticState{
		putCtx:              ctx,
		dht:                 dht,
		key:                 key,
		doneChan:            make(chan struct{}, returnThreshold), // buffered channel to not miss events
		ksKey:               ks.XORKeySpace.Key([]byte(key)),
		networkSize:         networkSize,
		peerStates:          map[peer.ID]addProviderRPCState{},
		individualThreshold: individualThreshold,
		setThreshold:        setThreshold,
		returnThreshold:     returnThreshold,
		putProvDone:         atomic.Int32{},
	}, nil
}

func (dht *IpfsDHT) optimisticProvide(outerCtx context.Context, keyMH multihash.Multihash) error {
	key := string(keyMH)

	if key == "" {
		return fmt.Errorf("can't lookup empty key")
	}

	// initialize new context for all putProvider operations.
	// We don't want to give the outer context to the put operations as we return early before all
	// put operations have finished to avoid the long tail of the latency distribution. If we
	// provided the outer context the put operations may be cancelled depending on what happens
	// with the context on the user side.
	putCtx, putCtxCancel := context.WithTimeout(context.Background(), time.Minute)

	es, err := dht.newOptimisticState(putCtx, key)
	if err != nil {
		putCtxCancel()
		return err
	}

	// initialize context that finishes when this function returns
	innerCtx, innerCtxCancel := context.WithCancel(outerCtx)
	defer innerCtxCancel()

	go func() {
		select {
		case <-outerCtx.Done():
			// If the outer context gets cancelled while we're still in this function. We stop all
			// pending put operations.
			putCtxCancel()
		case <-innerCtx.Done():
			// We have returned from this function. Ignore cancellations of the outer context and continue
			// with the remaining put operations.
		}
	}()

	lookupRes, err := dht.runLookupWithFollowup(outerCtx, key, dht.pmGetClosestPeers(key), es.stopFn)
	if err != nil {
		return err
	}

	// Store the provider records with all the closest peers we haven't already contacted/scheduled interaction with.
	es.peerStatesLk.Lock()
	for _, p := range lookupRes.peers {
		if _, found := es.peerStates[p]; found {
			continue
		}

		go es.putProviderRecord(p)
		es.peerStates[p] = scheduled
	}
	es.peerStatesLk.Unlock()

	// wait until a threshold number of RPCs have completed
	es.waitForRPCs()

	if err := outerCtx.Err(); err != nil || !lookupRes.completed { // likely the "completed" field is false but that's not a given
		return err
	}

	// tracking lookup results for network size estimator as "completed" is true
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// refresh the cpl for this key as the query was successful
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return nil
}

func (os *optimisticState) stopFn(qps *qpeerset.QueryPeerset) bool {
	os.peerStatesLk.Lock()
	defer os.peerStatesLk.Unlock()

	// get currently known closest peers and check if any of them is already very close.
	// If so -> store provider records straight away.
	closest := qps.GetClosestNInStates(os.dht.bucketSize, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
	distances := make([]float64, os.dht.bucketSize)
	for i, p := range closest {
		// calculate distance of peer p to the target key
		distances[i] = netsize.NormedDistance(p, os.ksKey)

		// Check if we have already scheduled interaction or have actually interacted with that peer
		if _, found := os.peerStates[p]; found {
			continue
		}

		// Check if peer is close enough to store the provider record with
		if distances[i] > os.individualThreshold {
			continue
		}

		// peer is indeed very close already -> store the provider record directly with it!
		go os.putProviderRecord(p)

		// keep track that we've scheduled storing a provider record with that peer
		os.peerStates[p] = scheduled
	}

	// count number of peers we have scheduled to contact or have already successfully contacted via the above method
	scheduledAndSuccessCount := 0
	for _, s := range os.peerStates {
		if s == scheduled || s == success {
			scheduledAndSuccessCount += 1
		}
	}

	// if we have already contacted/scheduled the RPC for more than bucketSize peers stop the procedure
	if scheduledAndSuccessCount >= os.dht.bucketSize {
		return true
	}

	// calculate average distance of the set of closest peers
	sum := 0.0
	for _, d := range distances {
		sum += d
	}
	avg := sum / float64(len(distances))

	// if the average is below the set threshold stop the procedure
	return avg < os.setThreshold
}

func (os *optimisticState) putProviderRecord(pid peer.ID) {
	err := os.dht.protoMessenger.PutProviderAddrs(os.putCtx, pid, []byte(os.key), peer.AddrInfo{
		ID:    os.dht.self,
		Addrs: os.dht.filterAddrs(os.dht.host.Addrs()),
	})
	os.peerStatesLk.Lock()
	if err != nil {
		os.peerStates[pid] = failure
	} else {
		os.peerStates[pid] = success
	}
	os.peerStatesLk.Unlock()

	// indicate that this ADD_PROVIDER RPC has completed
	os.doneChan <- struct{}{}
}

// waitForRPCs waits for a subset of ADD_PROVIDER RPCs to complete and then acquire a lease on
// a bound channel to return early back to the user and prevent unbound asynchronicity. If
// there are already too many requests in-flight we are just waiting for our current set to
// finish.
func (os *optimisticState) waitForRPCs() {
	os.peerStatesLk.RLock()
	rpcCount := len(os.peerStates)
	os.peerStatesLk.RUnlock()

	// returnThreshold can't be larger than the total number issued RPCs
	if os.returnThreshold > rpcCount {
		os.returnThreshold = rpcCount
	}

	// Wait until returnThreshold ADD_PROVIDER RPCs have returned
	for range os.doneChan {
		if int(os.putProvDone.Add(1)) == os.returnThreshold {
			break
		}
	}
	// At this point only a subset of all ADD_PROVIDER RPCs have completed.
	// We want to give control back to the user as soon as possible because
	// it is highly likely that at least one of the remaining RPCs will time
	// out and thus slow down the whole processes. The provider records will
	// already be available with less than the total number of RPCs having
	// finished. This has been investigated here:
	// https://github.com/protocol/network-measurements/blob/master/results/rfm17-provider-record-liveness.md

	// For the remaining ADD_PROVIDER RPCs try to acquire a lease on the optProvJobsPool channel.
	// If that worked we need to consume the doneChan and release the acquired lease on the
	// optProvJobsPool channel.
	remaining := rpcCount - int(os.putProvDone.Load())
	for i := 0; i < remaining; i++ {
		select {
		case os.dht.optProvJobsPool <- struct{}{}:
			// We were able to acquire a lease on the optProvJobsPool channel.
			// Consume doneChan to release the acquired lease again.
			go os.consumeDoneChan(rpcCount)
		case <-os.doneChan:
			// We were not able to acquire a lease but an ADD_PROVIDER RPC resolved.
			if int(os.putProvDone.Add(1)) == rpcCount {
				close(os.doneChan)
			}
		}
	}
}

func (os *optimisticState) consumeDoneChan(until int) {
	// Wait for an RPC to finish
	<-os.doneChan

	// Release acquired lease for other's to get a spot
	<-os.dht.optProvJobsPool

	// If all RPCs have finished, close the channel.
	if int(os.putProvDone.Add(1)) == until {
		close(os.doneChan)
	}
}
