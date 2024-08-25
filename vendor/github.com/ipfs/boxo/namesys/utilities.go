package namesys

import (
	"context"
	"strings"

	"github.com/ipfs/boxo/path"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type resolver interface {
	resolveOnceAsync(context.Context, path.Path, ResolveOptions) <-chan AsyncResult
}

// resolve is a helper for implementing Resolver.ResolveN using resolveOnce.
func resolve(ctx context.Context, r resolver, p path.Path, options ResolveOptions) (result Result, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = ErrResolveFailed
	resCh := resolveAsync(ctx, r, p, options)

	for res := range resCh {
		result.Path, result.TTL, result.LastMod, err = res.Path, res.TTL, res.LastMod, res.Err
		if err != nil {
			break
		}
	}

	return result, err
}

func resolveAsync(ctx context.Context, r resolver, p path.Path, options ResolveOptions) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "ResolveAsync")
	defer span.End()

	resCh := r.resolveOnceAsync(ctx, p, options)
	depth := options.Depth
	outCh := make(chan AsyncResult, 1)

	go func() {
		defer close(outCh)
		ctx, span := startSpan(ctx, "ResolveAsync.Worker")
		defer span.End()

		var subCh <-chan AsyncResult
		var cancelSub context.CancelFunc
		defer func() {
			if cancelSub != nil {
				cancelSub()
			}
		}()

		for {
			select {
			case res, ok := <-resCh:
				if !ok {
					resCh = nil
					break
				}

				if res.Err != nil {
					emitResult(ctx, outCh, res)
					return
				}

				log.Debugf("resolved %s to %s", p.String(), res.Path.String())

				if !res.Path.Mutable() {
					emitResult(ctx, outCh, res)
					break
				}

				if depth == 1 {
					res.Err = ErrResolveRecursion
					emitResult(ctx, outCh, res)
					break
				}

				subOpts := options
				if subOpts.Depth > 1 {
					subOpts.Depth--
				}

				var subCtx context.Context
				if cancelSub != nil {
					// Cancel previous recursive resolve since it won't be used anyways
					cancelSub()
				}

				subCtx, cancelSub = context.WithCancel(ctx)
				_ = cancelSub

				subCh = resolveAsync(subCtx, r, res.Path, subOpts)
			case res, ok := <-subCh:
				if !ok {
					subCh = nil
					break
				}

				// We don't bother returning here in case of context timeout as there is
				// no good reason to do that, and we may still be able to emit a result
				emitResult(ctx, outCh, res)
			case <-ctx.Done():
				return
			}
			if resCh == nil && subCh == nil {
				return
			}
		}
	}()
	return outCh
}

func emitResult(ctx context.Context, outCh chan<- AsyncResult, r AsyncResult) {
	select {
	case outCh <- r:
	case <-ctx.Done():
	}
}

func joinPaths(resolvedBase, unresolvedPath path.Path) (path.Path, error) {
	if resolvedBase == nil {
		return nil, nil
	}

	segments := unresolvedPath.Segments()[2:]
	if strings.HasSuffix(unresolvedPath.String(), "/") {
		segments = append(segments, "")
	}

	// simple optimization
	if len(segments) == 0 {
		return resolvedBase, nil
	}

	return path.Join(resolvedBase, segments...)
}

var tracer = otel.Tracer("boxo/namesys")

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, "Namesys."+name)
}
