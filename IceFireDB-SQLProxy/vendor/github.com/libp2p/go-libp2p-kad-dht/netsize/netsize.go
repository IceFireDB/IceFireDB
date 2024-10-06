package netsize

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// invalidEstimate indicates that we currently have no valid estimate cached.
const invalidEstimate int32 = -1

var (
	ErrNotEnoughData   = fmt.Errorf("not enough data")
	ErrWrongNumOfPeers = fmt.Errorf("expected bucket size number of peers")
)

var (
	logger                   = logging.Logger("dht/netsize")
	MaxMeasurementAge        = 2 * time.Hour
	MinMeasurementsThreshold = 5
	MaxMeasurementsThreshold = 150
	keyspaceMaxInt, _        = new(big.Int).SetString(strings.Repeat("1", 256), 2)
	keyspaceMaxFloat         = new(big.Float).SetInt(keyspaceMaxInt)
)

type Estimator struct {
	localID    kbucket.ID
	rt         *kbucket.RoutingTable
	bucketSize int

	measurementsLk sync.RWMutex
	measurements   map[int][]measurement

	netSizeCache int32
}

func NewEstimator(localID peer.ID, rt *kbucket.RoutingTable, bucketSize int) *Estimator {
	// initialize map to hold measurement observations
	measurements := map[int][]measurement{}
	for i := 0; i < bucketSize; i++ {
		measurements[i] = []measurement{}
	}

	return &Estimator{
		localID:      kbucket.ConvertPeerID(localID),
		rt:           rt,
		bucketSize:   bucketSize,
		measurements: measurements,
		netSizeCache: invalidEstimate,
	}
}

// NormedDistance calculates the normed XOR distance of the given keys (from 0 to 1).
func NormedDistance(p peer.ID, k ks.Key) float64 {
	pKey := ks.XORKeySpace.Key([]byte(p))
	ksDistance := new(big.Float).SetInt(pKey.Distance(k))
	normedDist, _ := new(big.Float).Quo(ksDistance, keyspaceMaxFloat).Float64()
	return normedDist
}

type measurement struct {
	distance  float64
	weight    float64
	timestamp time.Time
}

// Track tracks the list of peers for the given key to incorporate in the next network size estimate.
// key is expected **NOT** to be in the kademlia keyspace and peers is expected to be a sorted list of
// the closest peers to the given key (the closest first).
// This function expects peers to have the same length as the routing table bucket size. It also
// strips old and limits the number of data points (favouring new).
func (e *Estimator) Track(key string, peers []peer.ID) error {
	e.measurementsLk.Lock()
	defer e.measurementsLk.Unlock()

	// sanity check
	if len(peers) != e.bucketSize {
		return ErrWrongNumOfPeers
	}

	logger.Debugw("Tracking peers for key", "key", key)

	now := time.Now()

	// invalidate cache
	atomic.StoreInt32(&e.netSizeCache, invalidEstimate)

	// Calculate weight for the peer distances.
	weight := e.calcWeight(key, peers)

	// Map given key to the Kademlia key space (hash it)
	ksKey := ks.XORKeySpace.Key([]byte(key))

	// the maximum age timestamp of the measurement data points
	maxAgeTs := now.Add(-MaxMeasurementAge)

	for i, p := range peers {
		// Construct measurement struct
		m := measurement{
			distance:  NormedDistance(p, ksKey),
			weight:    weight,
			timestamp: now,
		}

		measurements := append(e.measurements[i], m)

		// find the smallest index of a measurement that is still in the allowed time window
		// all measurements with a lower index should be discarded as they are too old
		n := len(measurements)
		idx := sort.Search(n, func(j int) bool {
			return measurements[j].timestamp.After(maxAgeTs)
		})

		// if measurements are outside the allowed time window remove them.
		// idx == n - there is no measurement in the allowed time window -> reset slice
		// idx == 0 - the normal case where we only have valid entries
		// idx != 0 - there is a mix of valid and obsolete entries
		if idx != 0 {
			x := make([]measurement, n-idx)
			copy(x, measurements[idx:])
			measurements = x
		}

		// if the number of data points exceed the max threshold, strip oldest measurement data points.
		if len(measurements) > MaxMeasurementsThreshold {
			measurements = measurements[len(measurements)-MaxMeasurementsThreshold:]
		}

		e.measurements[i] = measurements
	}

	return nil
}

// NetworkSize instructs the Estimator to calculate the current network size estimate.
func (e *Estimator) NetworkSize() (int32, error) {

	// return cached calculation lock-free (fast path)
	if estimate := atomic.LoadInt32(&e.netSizeCache); estimate != invalidEstimate {
		logger.Debugw("Cached network size estimation", "estimate", estimate)
		return estimate, nil
	}

	e.measurementsLk.Lock()
	defer e.measurementsLk.Unlock()

	// Check a second time. This is needed because we maybe had to wait on another goroutine doing the computation.
	// Then the computation was just finished by the other goroutine, and we don't need to redo it.
	if estimate := e.netSizeCache; estimate != invalidEstimate {
		logger.Debugw("Cached network size estimation", "estimate", estimate)
		return estimate, nil
	}

	// remove obsolete data points
	e.garbageCollect()

	// initialize slices for linear fit
	xs := make([]float64, e.bucketSize)
	ys := make([]float64, e.bucketSize)
	yerrs := make([]float64, e.bucketSize)

	for i := 0; i < e.bucketSize; i++ {
		observationCount := len(e.measurements[i])

		// If we don't have enough data to reasonably calculate the network size, return early
		if observationCount < MinMeasurementsThreshold {
			return 0, ErrNotEnoughData
		}

		// Calculate Average Distance
		sumDistances := 0.0
		sumWeights := 0.0
		for _, m := range e.measurements[i] {
			sumDistances += m.weight * m.distance
			sumWeights += m.weight
		}
		distanceAvg := sumDistances / sumWeights

		// Calculate standard deviation
		sumWeightedDiffs := 0.0
		for _, m := range e.measurements[i] {
			diff := m.distance - distanceAvg
			sumWeightedDiffs += m.weight * diff * diff
		}
		variance := sumWeightedDiffs / (float64(observationCount-1) / float64(observationCount) * sumWeights)
		distanceStd := math.Sqrt(variance)

		// Track calculations
		xs[i] = float64(i + 1)
		ys[i] = distanceAvg
		yerrs[i] = distanceStd
	}

	// Calculate linear regression (assumes the line goes through the origin)
	var x2Sum, xySum float64
	for i, xi := range xs {
		yi := ys[i]
		xySum += yerrs[i] * xi * yi
		x2Sum += yerrs[i] * xi * xi
	}
	slope := xySum / x2Sum

	// calculate final network size
	netSize := int32(1/slope - 1)

	// cache network size estimation
	atomic.StoreInt32(&e.netSizeCache, netSize)

	logger.Debugw("New network size estimation", "estimate", netSize)
	return netSize, nil
}

// calcWeight weighs data points exponentially less if they fall into a non-full bucket.
// It weighs distance estimates based on their CPLs and bucket levels.
// Bucket Level: 20 -> 1/2^0 -> weight: 1
// Bucket Level: 17 -> 1/2^3 -> weight: 1/8
// Bucket Level: 10 -> 1/2^10 -> weight: 1/1024
//
// It can happen that the routing table doesn't have a full bucket, but we are tracking here
// a list of peers that would theoretically have been suitable for that bucket. Let's imagine
// there are only 13 peers in bucket 3 although there is space for 20. Now, the Track function
// gets a peers list (len 20) where all peers fall into bucket 3. The weight of this set of peers
// should be 1 instead of 1/2^7.
// I actually thought this cannot happen as peers would have been added to the routing table before
// the Track function gets called. But they seem sometimes not to be added.
func (e *Estimator) calcWeight(key string, peers []peer.ID) float64 {

	cpl := kbucket.CommonPrefixLen(kbucket.ConvertKey(key), e.localID)
	bucketLevel := e.rt.NPeersForCpl(uint(cpl))

	if bucketLevel < e.bucketSize {
		// routing table doesn't have a full bucket. Check how many peers would fit into that bucket
		peerLevel := 0
		for _, p := range peers {
			if cpl == kbucket.CommonPrefixLen(kbucket.ConvertPeerID(p), e.localID) {
				peerLevel += 1
			}
		}

		if peerLevel > bucketLevel {
			return math.Pow(2, float64(peerLevel-e.bucketSize))
		}
	}

	return math.Pow(2, float64(bucketLevel-e.bucketSize))
}

// garbageCollect removes all measurements from the list that fell out of the measurement time window.
func (e *Estimator) garbageCollect() {
	logger.Debug("Running garbage collection")

	// the maximum age timestamp of the measurement data points
	maxAgeTs := time.Now().Add(-MaxMeasurementAge)

	for i := 0; i < e.bucketSize; i++ {

		// find the smallest index of a measurement that is still in the allowed time window
		// all measurements with a lower index should be discarded as they are too old
		n := len(e.measurements[i])
		idx := sort.Search(n, func(j int) bool {
			return e.measurements[i][j].timestamp.After(maxAgeTs)
		})

		// if measurements are outside the allowed time window remove them.
		// idx == n - there is no measurement in the allowed time window -> reset slice
		// idx == 0 - the normal case where we only have valid entries
		// idx != 0 - there is a mix of valid and obsolete entries
		if idx == n {
			e.measurements[i] = []measurement{}
		} else if idx != 0 {
			e.measurements[i] = e.measurements[i][idx:]
		}
	}
}
