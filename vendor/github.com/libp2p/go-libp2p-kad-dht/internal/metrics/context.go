package metrics

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

type attributeCtxKey struct{}

// ContextWithAttributes adds attribute.KeyValue to a given context.
// These can be later accessed via AttributesFromContext.
//
// Calling this again will overwrite the previous set attributes.
func ContextWithAttributes(ctx context.Context, kv ...attribute.KeyValue) context.Context {
	return context.WithValue(ctx, attributeCtxKey{}, attribute.NewSet(kv...))
}

// AttributesFromContext returns attribute.Set that was addded to ctx via ContextWithAttributes
func AttributesFromContext(ctx context.Context) attribute.Set {
	ctxVal, _ := ctx.Value(attributeCtxKey{}).(attribute.Set)
	return ctxVal
}
