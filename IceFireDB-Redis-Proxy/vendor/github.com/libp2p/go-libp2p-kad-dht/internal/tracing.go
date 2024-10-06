package internal

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/multiformats/go-multibase"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-libp2p-kad-dht").Start(ctx, fmt.Sprintf("KademliaDHT.%s", name), opts...)
}

// KeyAsAttribute format a DHT key into a suitable tracing attribute.
// DHT keys can be either valid utf-8 or binary, when they are derived from, for example, a multihash.
// Tracing (and notably OpenTelemetry+grpc exporter) requires valid utf-8 for string attributes.
func KeyAsAttribute(name string, key string) attribute.KeyValue {
	b := []byte(key)
	if utf8.Valid(b) {
		return attribute.String(name, key)
	}
	encoded, err := multibase.Encode(multibase.Base58BTC, b)
	if err != nil {
		// should be unreachable
		panic(err)
	}
	return attribute.String(name, encoded)
}
