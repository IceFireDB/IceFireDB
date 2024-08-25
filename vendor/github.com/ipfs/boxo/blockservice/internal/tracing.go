package internal

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return startSpan(ctx, "Blockservice."+name, opts...)
}

// outline logic so the string concatenation can be inlined and executed at compile time
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("blockservice").Start(ctx, name, opts...)
}
