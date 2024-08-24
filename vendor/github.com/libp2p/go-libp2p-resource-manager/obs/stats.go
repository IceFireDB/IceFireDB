package obs

import (
	"context"
	"strings"

	rcmgr "github.com/libp2p/go-libp2p-resource-manager"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	metricNamespace = "rcmgr/"
	conns           = stats.Int64(metricNamespace+"connections", "Number of Connections", stats.UnitDimensionless)

	peerConns         = stats.Int64(metricNamespace+"peer/connections", "Number of connections this peer has", stats.UnitDimensionless)
	peerConnsNegative = stats.Int64(metricNamespace+"peer/connections_negative", "Number of connections this peer had. This is used to get the current connection number per peer histogram by subtracting this from the peer/connections histogram", stats.UnitDimensionless)

	streams = stats.Int64(metricNamespace+"streams", "Number of Streams", stats.UnitDimensionless)

	peerStreams         = stats.Int64(metricNamespace+"peer/streams", "Number of streams this peer has", stats.UnitDimensionless)
	peerStreamsNegative = stats.Int64(metricNamespace+"peer/streams_negative", "Number of streams this peer had. This is used to get the current streams number per peer histogram by subtracting this from the peer/streams histogram", stats.UnitDimensionless)

	memory             = stats.Int64(metricNamespace+"memory", "Amount of memory reserved as reported to the Resource Manager", stats.UnitDimensionless)
	peerMemory         = stats.Int64(metricNamespace+"peer/memory", "Amount of memory currently reseved for peer", stats.UnitDimensionless)
	peerMemoryNegative = stats.Int64(metricNamespace+"peer/memory_negative", "Amount of memory previously reseved for peer. This is used to get the current memory per peer histogram by subtracting this from the peer/memory histogram", stats.UnitDimensionless)

	connMemory         = stats.Int64(metricNamespace+"conn/memory", "Amount of memory currently reseved for the connection", stats.UnitDimensionless)
	connMemoryNegative = stats.Int64(metricNamespace+"conn/memory_negative", "Amount of memory previously reseved for the connection.  This is used to get the current memory per connection histogram by subtracting this from the conn/memory histogram", stats.UnitDimensionless)

	fds = stats.Int64(metricNamespace+"fds", "Number of fds as reported to the Resource Manager", stats.UnitDimensionless)

	blockedResources = stats.Int64(metricNamespace+"blocked_resources", "Number of resource requests blocked", stats.UnitDimensionless)
)

var (
	directionTag, _ = tag.NewKey("dir")
	scopeTag, _     = tag.NewKey("scope")
	serviceTag, _   = tag.NewKey("service")
	protocolTag, _  = tag.NewKey("protocol")
	resourceTag, _  = tag.NewKey("resource")
)

var (
	ConnView = &view.View{Measure: conns, Aggregation: view.Sum(), TagKeys: []tag.Key{directionTag, scopeTag}}

	oneTenThenExpDistribution = []float64{
		1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 16.1, 32.1, 64.1, 128.1, 256.1,
	}

	PeerConnsView = &view.View{
		Measure:     peerConns,
		Aggregation: view.Distribution(oneTenThenExpDistribution...),
		TagKeys:     []tag.Key{directionTag},
	}
	PeerConnsNegativeView = &view.View{
		Measure:     peerConnsNegative,
		Aggregation: view.Distribution(oneTenThenExpDistribution...),
		TagKeys:     []tag.Key{directionTag},
	}

	StreamView             = &view.View{Measure: streams, Aggregation: view.Sum(), TagKeys: []tag.Key{directionTag, scopeTag, serviceTag, protocolTag}}
	PeerStreamsView        = &view.View{Measure: peerStreams, Aggregation: view.Distribution(oneTenThenExpDistribution...), TagKeys: []tag.Key{directionTag}}
	PeerStreamNegativeView = &view.View{Measure: peerStreamsNegative, Aggregation: view.Distribution(oneTenThenExpDistribution...), TagKeys: []tag.Key{directionTag}}

	MemoryView = &view.View{Measure: memory, Aggregation: view.Sum(), TagKeys: []tag.Key{scopeTag, serviceTag, protocolTag}}

	memDistribution = []float64{
		1 << 10,   // 1KB
		4 << 10,   // 4KB
		32 << 10,  // 32KB
		1 << 20,   // 1MB
		32 << 20,  // 32MB
		256 << 20, // 256MB
		512 << 20, // 512MB
		1 << 30,   // 1GB
		2 << 30,   // 2GB
		4 << 30,   // 4GB
	}
	PeerMemoryView = &view.View{
		Measure:     peerMemory,
		Aggregation: view.Distribution(memDistribution...),
	}
	PeerMemoryNegativeView = &view.View{
		Measure:     peerMemoryNegative,
		Aggregation: view.Distribution(memDistribution...),
	}

	// Not setup yet. Memory isn't attached to a given connection.
	ConnMemoryView = &view.View{
		Measure:     connMemory,
		Aggregation: view.Distribution(memDistribution...),
	}
	ConnMemoryNegativeView = &view.View{
		Measure:     connMemoryNegative,
		Aggregation: view.Distribution(memDistribution...),
	}

	FDsView = &view.View{Measure: fds, Aggregation: view.Sum(), TagKeys: []tag.Key{scopeTag}}

	BlockedResourcesView = &view.View{
		Measure:     blockedResources,
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{scopeTag, resourceTag},
	}
)

var DefaultViews []*view.View = []*view.View{
	ConnView,
	PeerConnsView,
	PeerConnsNegativeView,
	FDsView,

	StreamView,
	PeerStreamsView,
	PeerStreamNegativeView,

	MemoryView,
	PeerMemoryView,
	PeerMemoryNegativeView,

	BlockedResourcesView,
}

// StatsTraceReporter reports stats on the resource manager using its traces.
type StatsTraceReporter struct{}

func NewStatsTraceReporter() (StatsTraceReporter, error) {
	// TODO tell prometheus the system limits
	return StatsTraceReporter{}, nil
}

func (r StatsTraceReporter) ConsumeEvent(evt rcmgr.TraceEvt) {
	ctx := context.Background()

	switch evt.Type {
	case rcmgr.TraceAddStreamEvt, rcmgr.TraceRemoveStreamEvt:
		if p := rcmgr.ParsePeerScopeName(evt.Name); p.Validate() == nil {
			// Aggregated peer stats. Counts how many peers have N number of streams open.
			// Uses two buckets aggregations. One to count how many streams the
			// peer has now. The other to count the negative value, or how many
			// streams did the peer use to have. When looking at the data you
			// take the difference from the two.

			oldStreamsOut := int64(evt.StreamsOut - evt.DeltaOut)
			peerStreamsOut := int64(evt.StreamsOut)
			if oldStreamsOut != peerStreamsOut {
				if oldStreamsOut != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "outbound")}, peerStreamsNegative.M(oldStreamsOut))
				}
				if peerStreamsOut != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "outbound")}, peerStreams.M(peerStreamsOut))
				}
			}

			oldStreamsIn := int64(evt.StreamsIn - evt.DeltaIn)
			peerStreamsIn := int64(evt.StreamsIn)
			if oldStreamsIn != peerStreamsIn {
				if oldStreamsIn != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "inbound")}, peerStreamsNegative.M(oldStreamsIn))
				}
				if peerStreamsIn != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "inbound")}, peerStreams.M(peerStreamsIn))
				}
			}
		} else {
			var tags []tag.Mutator
			if rcmgr.IsSystemScope(evt.Name) || rcmgr.IsTransientScope(evt.Name) {
				tags = append(tags, tag.Upsert(scopeTag, evt.Name))
			} else if svc := rcmgr.ParseServiceScopeName(evt.Name); svc != "" {
				tags = append(tags, tag.Upsert(scopeTag, "service"), tag.Upsert(serviceTag, svc))
			} else if proto := rcmgr.ParseProtocolScopeName(evt.Name); proto != "" {
				tags = append(tags, tag.Upsert(scopeTag, "protocol"), tag.Upsert(protocolTag, proto))
			} else {
				// Not measuring connscope, servicepeer and protocolpeer. Lots of data, and
				// you can use aggregated peer stats + service stats to infer
				// this.
				break
			}

			if evt.DeltaOut != 0 {
				stats.RecordWithTags(
					ctx,
					append([]tag.Mutator{tag.Upsert(directionTag, "outbound")}, tags...),
					streams.M(int64(evt.DeltaOut)),
				)
			}

			if evt.DeltaIn != 0 {
				stats.RecordWithTags(
					ctx,
					append([]tag.Mutator{tag.Upsert(directionTag, "inbound")}, tags...),
					streams.M(int64(evt.DeltaIn)),
				)
			}
		}

	case rcmgr.TraceAddConnEvt, rcmgr.TraceRemoveConnEvt:
		if p := rcmgr.ParsePeerScopeName(evt.Name); p.Validate() == nil {
			// Aggregated peer stats. Counts how many peers have N number of connections.
			// Uses two buckets aggregations. One to count how many streams the
			// peer has now. The other to count the negative value, or how many
			// conns did the peer use to have. When looking at the data you
			// take the difference from the two.

			oldConnsOut := int64(evt.ConnsOut - evt.DeltaOut)
			connsOut := int64(evt.ConnsOut)
			if oldConnsOut != connsOut {
				if oldConnsOut != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "outbound")}, peerConnsNegative.M(oldConnsOut))
				}
				if connsOut != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "outbound")}, peerConns.M(connsOut))
				}
			}

			oldConnsIn := int64(evt.ConnsIn - evt.DeltaIn)
			connsIn := int64(evt.ConnsIn)
			if oldConnsIn != connsIn {
				if oldConnsIn != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "inbound")}, peerConnsNegative.M(oldConnsIn))
				}
				if connsIn != 0 {
					stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(directionTag, "inbound")}, peerConns.M(connsIn))
				}
			}
		} else {
			var tags []tag.Mutator
			if rcmgr.IsSystemScope(evt.Name) || rcmgr.IsTransientScope(evt.Name) {
				tags = append(tags, tag.Upsert(scopeTag, evt.Name))
			} else if rcmgr.IsConnScope(evt.Name) {
				// Not measuring this. I don't think it's useful.
				break
			} else {
				// This could be a span
				break
			}

			if evt.DeltaOut != 0 {
				stats.RecordWithTags(
					ctx,
					append([]tag.Mutator{tag.Upsert(directionTag, "outbound")}, tags...),
					conns.M(int64(evt.DeltaOut)),
				)
			}

			if evt.DeltaIn != 0 {
				stats.RecordWithTags(
					ctx,
					append([]tag.Mutator{tag.Upsert(directionTag, "inbound")}, tags...),
					conns.M(int64(evt.DeltaIn)),
				)
			}

			// Represents the delta in fds
			if evt.Delta != 0 {
				stats.RecordWithTags(
					ctx,
					tags,
					fds.M(int64(evt.Delta)),
				)
			}
		}
	case rcmgr.TraceReserveMemoryEvt, rcmgr.TraceReleaseMemoryEvt:
		if p := rcmgr.ParsePeerScopeName(evt.Name); p.Validate() == nil {
			oldMem := evt.Memory - evt.Delta
			if oldMem != evt.Memory {
				if oldMem != 0 {
					stats.Record(ctx, peerMemoryNegative.M(oldMem))
				}
				if evt.Memory != 0 {
					stats.Record(ctx, peerMemory.M(evt.Memory))
				}
			}
		} else if rcmgr.IsConnScope(evt.Name) {
			oldMem := evt.Memory - evt.Delta
			if oldMem != evt.Memory {
				if oldMem != 0 {
					stats.Record(ctx, connMemoryNegative.M(oldMem))
				}
				if evt.Memory != 0 {
					stats.Record(ctx, connMemory.M(evt.Memory))
				}
			}
		} else {
			var tags []tag.Mutator
			if rcmgr.IsSystemScope(evt.Name) || rcmgr.IsTransientScope(evt.Name) {
				tags = append(tags, tag.Upsert(scopeTag, evt.Name))
			} else if svc := rcmgr.ParseServiceScopeName(evt.Name); svc != "" {
				tags = append(tags, tag.Upsert(scopeTag, "service"), tag.Upsert(serviceTag, svc))
			} else if proto := rcmgr.ParseProtocolScopeName(evt.Name); proto != "" {
				tags = append(tags, tag.Upsert(scopeTag, "protocol"), tag.Upsert(protocolTag, proto))
			} else {
				// Not measuring connscope, servicepeer and protocolpeer. Lots of data, and
				// you can use aggregated peer stats + service stats to infer
				// this.
				break
			}

			if evt.Delta != 0 {
				stats.RecordWithTags(ctx, tags, memory.M(int64(evt.Delta)))
			}
		}

	case rcmgr.TraceBlockAddConnEvt, rcmgr.TraceBlockAddStreamEvt, rcmgr.TraceBlockReserveMemoryEvt:
		var resource string
		if evt.Type == rcmgr.TraceBlockAddConnEvt {
			resource = "connection"
		} else if evt.Type == rcmgr.TraceBlockAddStreamEvt {
			resource = "stream"
		} else {
			resource = "memory"
		}

		// Only the top scopeName. We don't want to get the peerid here.
		scopeName := strings.SplitN(evt.Name, ":", 2)[0]
		// Drop the connection or stream id
		scopeName = strings.SplitN(scopeName, "-", 2)[0]

		// If something else gets added here, make sure to update the size hint
		// below when we make `tagsWithDir`.
		tags := []tag.Mutator{tag.Upsert(scopeTag, scopeName), tag.Upsert(resourceTag, resource)}

		if evt.DeltaIn != 0 {
			tagsWithDir := make([]tag.Mutator, 0, 3)
			tagsWithDir = append(tagsWithDir, tag.Insert(directionTag, "inbound"))
			tagsWithDir = append(tagsWithDir, tags...)
			stats.RecordWithTags(ctx, tagsWithDir[0:], blockedResources.M(int64(1)))
		}

		if evt.DeltaOut != 0 {
			tagsWithDir := make([]tag.Mutator, 0, 3)
			tagsWithDir = append(tagsWithDir, tag.Insert(directionTag, "outbound"))
			tagsWithDir = append(tagsWithDir, tags...)
			stats.RecordWithTags(ctx, tagsWithDir, blockedResources.M(int64(1)))
		}

		if evt.Delta != 0 {
			stats.RecordWithTags(ctx, tags, blockedResources.M(1))
		}
	}
}
