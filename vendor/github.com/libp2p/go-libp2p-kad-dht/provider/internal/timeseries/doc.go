// Package timeseries provides time-windowed data structures for collecting and
// analyzing performance metrics in the libp2p Kademlia DHT provider.
//
// This package contains three main types of time series collectors:
//
// IntTimeSeries maintains a rolling window of integer values with automatic
// cleanup of expired entries. It's used for tracking counts and durations over
// time, such as the number of keys provided or operation durations.
//
// FloatTimeSeries maintains a rolling window of weighted float values, useful
// for computing weighted averages. Each entry has a value and a weight,
// allowing for more sophisticated statistical calculations.
//
// CycleStats tracks statistics organized by keyspace prefixes with
// deadline-based cleanup. It uses a trie structure to efficiently aggregate
// statistics across different regions of the DHT keyspace. This is
// particularly useful for tracking reprovide operations that cover different
// keyspace regions. The cleanup deadline is provided dynamically, allowing
// adaptive retention based on actual reprovide cycle durations.
//
// All types are thread-safe and designed for high-frequency updates with
// minimal lock contention. The retention periods are configurable and
// typically align with the provider's reprovide intervals.
//
// Example usage:
//
//	// Track operation counts over the last hour
//	counts := NewIntTimeSeries(time.Hour)
//	counts.Add(5)
//	total := counts.Sum()
//	average := counts.Avg()
//
//	// Track weighted averages
//	averages := NewFloatTimeSeries(time.Hour)
//	averages.Add(3.5, 10) // value=3.5, weight=10
//	weightedAvg := averages.Avg()
//
//	// Track keyspace region statistics
//	stats := NewCycleStats(time.Minute)
//	stats.Add("101", 42) // prefix "101", value 42
//	stats.Cleanup(2 * time.Hour) // cleanup entries older than 2 hours
//	total := stats.Sum()
package timeseries
