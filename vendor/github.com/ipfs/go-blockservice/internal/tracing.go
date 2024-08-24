package internal

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-blockservice").Start(ctx, fmt.Sprintf("Blockservice.%s", name), opts...)
}
