// Package stats defines the data structures for provider performance metrics
// and statistics exported by the libp2p kademlia DHT provider.
package stats

import (
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
)

type Stats struct {
	Closed       bool         `json:"closed"`
	Connectivity Connectivity `json:"connectivity"`
	Queues       Queues       `json:"queues"`
	Schedule     Schedule     `json:"schedule"`
	Workers      Workers      `json:"workers"`
	Timing       Timing       `json:"timing"`
	Operations   Operations   `json:"operations"`
	Network      Network      `json:"network"`
}

type Queues struct {
	PendingKeyProvides      int64 `json:"pending_key_provides"`
	PendingRegionProvides   int64 `json:"pending_region_provides"`
	PendingRegionReprovides int64 `json:"pending_region_reprovides"`
}

type Connectivity struct {
	Status string    `json:"status"`
	Since  time.Time `json:"since"`
}

type Schedule struct {
	Keys                int64      `json:"keys"`
	Regions             int64      `json:"regions"`
	AvgPrefixLength     float64    `json:"avg_prefix_length"`
	NextReprovideAt     time.Time  `json:"next_reprovide_at"`
	NextReprovidePrefix bitstr.Key `json:"next_reprovide_prefix"`
}

type Workers struct {
	Max                      int `json:"max"`
	Active                   int `json:"active"`
	ActivePeriodic           int `json:"active_periodic"`
	ActiveBurst              int `json:"active_burst"`
	DedicatedPeriodic        int `json:"dedicated_periodic"`
	DedicatedBurst           int `json:"dedicated_burst"`
	QueuedPeriodic           int `json:"queued_periodic"`
	QueuedBurst              int `json:"queued_burst"`
	MaxProvideConnsPerWorker int `json:"max_provide_conns_per_worker"`
}

type Timing struct {
	Uptime             time.Duration `json:"uptime"`
	ReprovidesInterval time.Duration `json:"reprovides_interval"`
	CycleStart         time.Time     `json:"cycle_start"`
	CurrentTimeOffset  time.Duration `json:"current_time_offset"`
	MaxReprovideDelay  time.Duration `json:"max_reprovide_delay"`
}

type Operations struct {
	Ongoing OngoingOperations `json:"ongoing"`
	Past    PastOperations    `json:"past"`
}

type OngoingOperations struct {
	RegionProvides   int `json:"region_provides"`
	KeyProvides      int `json:"key_provides"`
	RegionReprovides int `json:"region_reprovides"`
	KeyReprovides    int `json:"key_reprovides"`
}

type PastOperations struct {
	// Cumulative totals since provider started
	RecordsProvided int64 `json:"records_provided"` // total provider records sent
	KeysProvided    int64 `json:"keys_provided"`    // total keys successfully provided
	KeysFailed      int64 `json:"keys_failed"`      // total keys that failed to provide

	// Performance metrics from last reprovide cycle
	KeysProvidedPerMinute     float64       `json:"keys_provided_per_minute"`      // provide rate
	KeysReprovidedPerMinute   float64       `json:"keys_reprovided_per_minute"`    // reprovide rate
	RegionReprovideDuration   time.Duration `json:"reprovide_duration"`            // avg time per region
	AvgKeysPerReprovide       float64       `json:"avg_keys_per_reprovide"`        // avg keys per region
	RegionReprovidedLastCycle int64         `json:"regions_reprovided_last_cycle"` // regions processed
}

type Network struct {
	// Peer counts from last reprovide cycle
	Peers     int `json:"peers"`     // total peers contacted
	Reachable int `json:"reachable"` // peers that responded successfully

	// Keyspace coverage analysis
	CompleteKeyspaceCoverage bool    `json:"complete_keyspace_coverage"` // whether all regions were covered
	AvgRegionSize            float64 `json:"avg_region_size"`            // average size of keyspace regions
	AvgHolders               float64 `json:"avg_holders"`                // average holders per key
	ReplicationFactor        int     `json:"replication_factor"`         // target replication factor
}
