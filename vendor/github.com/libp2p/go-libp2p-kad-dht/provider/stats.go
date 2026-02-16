package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad/trie"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/timeseries"
	"github.com/libp2p/go-libp2p-kad-dht/provider/stats"
)

func (s *SweepingProvider) Stats() stats.Stats {
	now := time.Now()
	snapshot := stats.Stats{
		Closed: s.closed(),
	}

	if snapshot.Closed {
		return snapshot
	}

	// Queue metrics
	snapshot.Queues = stats.Queues{
		PendingKeyProvides:      int64(s.provideQueue.Size()),
		PendingRegionProvides:   int64(s.provideQueue.NumRegions()),
		PendingRegionReprovides: int64(s.reprovideQueue.Size()),
	}

	s.avgPrefixLenLk.Lock()
	avgPrefixLenCached := s.cachedAvgPrefixLen
	s.avgPrefixLenLk.Unlock()

	// Connectivity status
	var status string
	if s.connectivity.IsOnline() {
		status = "online"
	} else {
		if avgPrefixLenCached >= 0 {
			status = "disconnected"
		} else {
			status = "offline"
		}
	}
	snapshot.Connectivity = stats.Connectivity{
		Status: status,
		Since:  s.connectivity.LastStateChange(),
	}

	// Schedule information
	s.scheduleLk.Lock()
	var scheduleSize int64
	var avgPrefixLen float64
	nextPrefix := s.scheduleCursor
	ok, nextReprovideOffset := trie.Find(s.schedule, nextPrefix)
	if avgPrefixLenCached >= 0 && !s.schedule.IsEmptyLeaf() {
		prefixSum := 0.
		for k := range keyspace.KeysIter(s.schedule, s.order) {
			prefixSum += float64(k.BitLen())
			scheduleSize++
		}
		avgPrefixLen = prefixSum / float64(scheduleSize)
	} else {
		scheduleSize = int64(s.schedule.Size())
		avgPrefixLen = float64(avgPrefixLenCached)
	}
	s.scheduleLk.Unlock()

	currentOffset := s.currentTimeOffset()
	nextReprovideAt := time.Time{}
	if ok {
		nextReprovideAt = now.Add(s.timeUntil(nextReprovideOffset))
	}

	var keys int64 = -1 // Default value if keyStore.Size() fails
	if keyCount, err := s.keystore.Size(context.Background()); err == nil {
		keys = int64(keyCount)
	}
	snapshot.Schedule = stats.Schedule{
		Keys:                keys,
		Regions:             scheduleSize,
		AvgPrefixLength:     avgPrefixLen,
		NextReprovideAt:     nextReprovideAt,
		NextReprovidePrefix: nextPrefix,
	}

	// Worker pool status
	workerStats := s.workerPool.Stats()
	active := 0
	for _, v := range workerStats.Used {
		active += v
	}
	snapshot.Workers = stats.Workers{
		Max:                      workerStats.Max,
		Active:                   active,
		ActivePeriodic:           workerStats.Used[periodicWorker],
		ActiveBurst:              workerStats.Used[burstWorker],
		DedicatedPeriodic:        workerStats.Reserves[periodicWorker],
		DedicatedBurst:           workerStats.Reserves[burstWorker],
		QueuedPeriodic:           workerStats.Queued[periodicWorker],
		QueuedBurst:              workerStats.Queued[burstWorker],
		MaxProvideConnsPerWorker: s.maxProvideConnsPerWorker,
	}

	// Timing information
	snapshot.Timing = stats.Timing{
		Uptime:             time.Since(s.startedAt),
		ReprovidesInterval: s.reprovideInterval,
		CycleStart:         now.Add(-currentOffset),
		CurrentTimeOffset:  currentOffset,
		MaxReprovideDelay:  s.maxReprovideDelay,
	}

	ongoingOps := stats.OngoingOperations{
		RegionProvides:   int(s.stats.ongoingProvides.opCount.Load()),
		KeyProvides:      int(s.stats.ongoingProvides.keyCount.Load()),
		RegionReprovides: int(s.stats.ongoingReprovides.opCount.Load()),
		KeyReprovides:    int(s.stats.ongoingReprovides.keyCount.Load()),
	}

	// Take snapshots of cycle stats data while holding the lock
	s.stats.cycleStatsLk.Lock()

	// We need to clean up the CycleStats with an appropriate deadline. This
	// deadline should be reprovideInterval + maxReprovideDelay + reprovideDuration.
	// But since we don't know reprovideDuration yet, we do a two-step cleanup.

	// First, clean up reprovideDuration with 2*reprovideInterval to get an initial average
	s.stats.reprovideDuration.Cleanup(2 * s.reprovideInterval)
	reprovideDurationAvg := s.stats.reprovideDuration.Avg()

	// Now calculate the proper deadline: reprovideInterval + maxReprovideDelay + reprovideDuration
	statsDeadline := s.reprovideInterval + s.maxReprovideDelay + time.Duration(reprovideDurationAvg)

	// Clean up all CycleStats with the calculated deadline
	s.stats.reprovideDuration.Cleanup(statsDeadline)
	s.stats.keysPerReprovide.Cleanup(statsDeadline)
	s.stats.peers.Cleanup(statsDeadline)
	s.stats.reachable.Cleanup(statsDeadline)

	// Capture data for calculations outside the lock
	keysPerProvideSum := s.stats.keysPerProvide.Sum()
	provideDurationSum := s.stats.provideDuration.Sum()
	keysPerReprovideSum := s.stats.keysPerReprovide.Sum()
	reprovideDurationSum := s.stats.reprovideDuration.Sum()
	reprovideDurationAvg = s.stats.reprovideDuration.Avg()
	keysPerReprovideAvg := s.stats.keysPerReprovide.Avg()
	reprovideDurationCount := int64(s.stats.reprovideDuration.Count())
	peersSum := s.stats.peers.Sum()
	peersFullyCovered := s.stats.peers.FullyCovered()
	reachableSum := s.stats.reachable.Sum()
	avgRegionSize := s.stats.regionSize.Avg()
	avgHoldersAvg := s.stats.avgHolders.Avg()

	keysProvidedPerMinute := 0.
	if time.Duration(provideDurationSum) > 0 {
		keysProvidedPerMinute = float64(keysPerProvideSum) / time.Duration(provideDurationSum).Minutes()
	}
	keysReprovidedPerMinute := 0.
	if time.Duration(reprovideDurationSum) > 0 {
		keysReprovidedPerMinute = float64(keysPerReprovideSum) / time.Duration(reprovideDurationSum).Minutes()
	}
	s.stats.cycleStatsLk.Unlock()

	pastOps := stats.PastOperations{
		RecordsProvided: int64(s.stats.recordsProvided.Load()),
		KeysProvided:    int64(s.stats.keysProvided.Load()),
		KeysFailed:      int64(s.stats.keysFailed.Load()),

		KeysProvidedPerMinute:     keysProvidedPerMinute,
		KeysReprovidedPerMinute:   keysReprovidedPerMinute,
		RegionReprovideDuration:   time.Duration(reprovideDurationAvg),
		AvgKeysPerReprovide:       keysPerReprovideAvg,
		RegionReprovidedLastCycle: reprovideDurationCount,
	}

	snapshot.Operations = stats.Operations{
		Ongoing: ongoingOps,
		Past:    pastOps,
	}

	snapshot.Network = stats.Network{ // in the last reprovide cycle
		Peers:                    int(peersSum),
		CompleteKeyspaceCoverage: peersFullyCovered,
		Reachable:                int(reachableSum),
		AvgRegionSize:            avgRegionSize,
		AvgHolders:               avgHoldersAvg,
		ReplicationFactor:        s.replicationFactor,
	}

	return snapshot
}

// operationStats tracks provider operation metrics over time windows.
type operationStats struct {
	// Cumulative counters since provider started
	recordsProvided atomic.Int64 // total provider records sent
	keysProvided    atomic.Int64 // total keys successfully provided
	keysFailed      atomic.Int64 // total keys that failed to provide

	// Current ongoing operations
	ongoingProvides   ongoingOpStats // active provide operations
	ongoingReprovides ongoingOpStats // active reprovide operations

	// Time-windowed metrics for provide operations
	keysPerProvide  timeseries.IntTimeSeries // keys provided per operation
	provideDuration timeseries.IntTimeSeries // duration of provide operations

	// Time-windowed metrics for reprovide operations (by keyspace region)
	keysPerReprovide  timeseries.CycleStats // keys reprovided per region
	reprovideDuration timeseries.CycleStats // duration of reprovide operations per region

	// Network topology metrics (by keyspace region)
	peers      timeseries.CycleStats      // number of peers per region
	reachable  timeseries.CycleStats      // number of reachable peers per region
	regionSize timeseries.IntTimeSeries   // average size of keyspace regions
	avgHolders timeseries.FloatTimeSeries // average holders per key (weighted)

	cycleStatsLk sync.Mutex // protects cycle-based statistics
}

func newOperationStats(reprovideInterval, maxDelay time.Duration) operationStats {
	return operationStats{
		keysPerProvide:  timeseries.NewIntTimeSeries(reprovideInterval),
		provideDuration: timeseries.NewIntTimeSeries(reprovideInterval),
		regionSize:      timeseries.NewIntTimeSeries(reprovideInterval),
		avgHolders:      timeseries.NewFloatTimeSeries(reprovideInterval),

		keysPerReprovide:  timeseries.NewCycleStats(maxDelay),
		reprovideDuration: timeseries.NewCycleStats(maxDelay),
		peers:             timeseries.NewCycleStats(maxDelay),
		reachable:         timeseries.NewCycleStats(maxDelay),
	}
}

// addProvidedRecords increments the total count of provider records sent.
func (s *operationStats) addProvidedRecords(count int) {
	s.recordsProvided.Add(int64(count))
}

// addCompletedKeys updates the counts of successfully provided and failed keys.
func (s *operationStats) addCompletedKeys(successes, failures int) {
	s.keysProvided.Add(int64(successes))
	s.keysFailed.Add(int64(failures))
}

// ongoingOpStats tracks currently active operations.
type ongoingOpStats struct {
	opCount  atomic.Int64 // number of active operations
	keyCount atomic.Int64 // total keys being processed in active operations
}

// start records the beginning of a new operation with the given number of keys.
func (s *ongoingOpStats) start(keyCount int) {
	s.opCount.Add(1)
	s.keyCount.Add(int64(keyCount))
}

// addKeys adds more keys to the current active operations.
func (s *ongoingOpStats) addKeys(keyCount int) {
	s.keyCount.Add(int64(keyCount))
}

// finish records the completion of an operation and removes its keys from the active count.
func (s *ongoingOpStats) finish(keyCount int) {
	s.opCount.Add(-1)
	s.keyCount.Add(-int64(keyCount))
}
