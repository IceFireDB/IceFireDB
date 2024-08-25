package internal

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Deprecated: use github.com/ipfs/boxo/blockservice/internal.StartSpan
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-blockservice").Start(ctx, fmt.Sprintf("Blockservice.%s", name), opts...)
}
