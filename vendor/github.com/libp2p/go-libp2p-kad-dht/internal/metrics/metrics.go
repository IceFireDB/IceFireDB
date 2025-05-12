package metrics

import (
	"context"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	defaultBytesDistribution        = []float64{1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296}
	defaultMillisecondsDistribution = []float64{0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000}
)

// Unit dimensions for stats
const (
	unitMessage      = "{message}"
	unitCount        = "{count}"
	unitError        = "{error}"
	unitBytes        = "By"
	unitMilliseconds = "ms"
)

// Attribute Keys
const (
	KeyMessageType = "message_type"
	KeyPeerID      = "peer_id"
	// KeyInstanceID identifies a dht instance by the pointer address.
	// Useful for differentiating between different dhts that have the same peer id.
	KeyInstanceID = "instance_id"
)

// UpsertMessageType is a convenience upserts the message type
// of a pb.Message into the KeyMessageType.
func UpsertMessageType(m *pb.Message) attribute.KeyValue {
	return attribute.Key(KeyMessageType).String(m.Type.String())
}

var (
	meter = otel.Meter("github.com/libp2p/go-libp2p-kad-dht")

	// dht net metrics
	receivedMessages, _ = meter.Int64Counter(
		"rpc.inbound.messages",
		metric.WithDescription("Total number of messages received per RPC"),
		metric.WithUnit(unitMessage),
	)
	receivedMessageErrors, _ = meter.Int64Counter(
		"rpc.inbound.message_errors",
		metric.WithDescription("Total number of errors for messages received per RPC"),
		metric.WithUnit(unitError),
	)
	receivedBytes, _ = meter.Int64Histogram(
		"rpc.inbound.bytes",
		metric.WithDescription("Total received bytes per RPC"),
		metric.WithUnit(unitBytes),
		metric.WithExplicitBucketBoundaries(defaultBytesDistribution...),
	)
	inboundRequestLatency, _ = meter.Float64Histogram(
		"rpc.inbound.request_latency",
		metric.WithDescription("Latency per RPC"),
		metric.WithUnit(unitMilliseconds),
		metric.WithExplicitBucketBoundaries(defaultMillisecondsDistribution...),
	)

	// message manager metrics
	outboundRequestLatency, _ = meter.Float64Histogram(
		"rpc.outbound.request_latency",
		metric.WithDescription("Latency per RPC"),
		metric.WithUnit(unitMilliseconds),
		metric.WithExplicitBucketBoundaries(defaultMillisecondsDistribution...),
	)
	sentMessages, _ = meter.Int64Counter(
		"rpc.outbound.messages",
		metric.WithDescription("Total number of messages sent per RPC"),
		metric.WithUnit(unitMessage),
	)
	sentMessageErrors, _ = meter.Int64Counter(
		"rpc.outbound.message_errors",
		metric.WithDescription("Total number of errors for messages sent per RPC"),
		metric.WithUnit(unitError),
	)
	sentRequests, _ = meter.Int64Counter(
		"rpc.outbound.requests",
		metric.WithDescription("Total number of requests sent per RPC"),
		metric.WithUnit(unitCount),
	)
	sentRequestErrors, _ = meter.Int64Counter(
		"rpc.outbound.request_errors",
		metric.WithDescription("Total number of errors for requests sent per RPC"),
		metric.WithUnit(unitError),
	)
	sentBytes, _ = meter.Int64Histogram(
		"rpc.outbound.bytes",
		metric.WithDescription("Total sent bytes per RPC"),
		metric.WithUnit(unitBytes),
		metric.WithExplicitBucketBoundaries(defaultBytesDistribution...),
	)
	networkSize, _ = meter.Int64Gauge(
		"network.size",
		metric.WithDescription("Network size estimation"),
		metric.WithUnit(unitCount),
	)
)

func RecordMessageRecvOK(ctx context.Context, msgLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	receivedMessages.Add(ctx, 1, attrSetOpt)
	receivedBytes.Record(ctx, msgLen, attrSetOpt)
}

func RecordMessageRecvErr(ctx context.Context, messageType string, msgLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)
	attrOpt := metric.WithAttributes(attribute.Key(KeyMessageType).String("UNKNOWN"))

	receivedMessages.Add(ctx, 1, attrOpt, attrSetOpt)
	receivedMessageErrors.Add(ctx, 1, attrOpt, attrSetOpt)
	receivedBytes.Record(ctx, msgLen, attrOpt, attrSetOpt)
}

func RecordMessageHandleErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	receivedMessageErrors.Add(ctx, 1, attrSetOpt)
}

func RecordRequestLatency(ctx context.Context, latencyMs float64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	inboundRequestLatency.Record(ctx, latencyMs, attrSetOpt)
}

func RecordRequestSendErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentRequests.Add(ctx, 1, attrSetOpt)
	sentRequestErrors.Add(ctx, 1, attrSetOpt)
}

func RecordRequestSendOK(ctx context.Context, sentBytesLen int64, latencyMs float64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentRequests.Add(ctx, 1, attrSetOpt)
	sentBytes.Record(ctx, 1, attrSetOpt)
	outboundRequestLatency.Record(ctx, latencyMs, attrSetOpt)
}

func RecordMessageSendOK(ctx context.Context, sentBytesLen int64) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentMessages.Add(ctx, 1, attrSetOpt)
	sentBytes.Record(ctx, sentBytesLen, attrSetOpt)
}

func RecordMessageSendErr(ctx context.Context) {
	ctxAttrSet := AttributesFromContext(ctx)
	attrSetOpt := metric.WithAttributeSet(ctxAttrSet)

	sentMessages.Add(ctx, 1, attrSetOpt)
	sentMessageErrors.Add(ctx, 1, attrSetOpt)
}

func RecordNetworkSize(ctx context.Context, ns int64) {
	networkSize.Record(ctx, int64(ns))
}
