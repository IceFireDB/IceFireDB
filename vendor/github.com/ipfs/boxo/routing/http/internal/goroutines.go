package internal

import (
	"context"
	"slices"
	"sync"
)

// DoBatch processes a slice of items with concurrency no higher than
// maxConcurrency by splitting it into batches no larger than maxBatchSize. If
// an error is returned for any batch, the process is short-circuited and the
// error is immediately returned.
func DoBatch[A any](ctx context.Context, maxBatchSize, maxConcurrency int, items []A, f func(context.Context, []A) error) error {
	if len(items) == 0 {
		return nil
	}
	batches := slices.Collect(slices.Chunk(items, maxBatchSize))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	batchChan := make(chan []A)
	errChan := make(chan error)
	wg := sync.WaitGroup{}
	for range min(maxConcurrency, len(batches)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case batch, ok := <-batchChan:
					if !ok {
						return
					}
					err := f(workerCtx, batch)
					if err != nil {
						select {
						case errChan <- err:
						case <-workerCtx.Done():
							return
						}
					}
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	// work sender
	go func() {
		defer func() {
			close(batchChan)
			wg.Wait()
			close(errChan)
		}()
		for _, batch := range batches {
			select {
			case batchChan <- batch:
			case <-workerCtx.Done():
				return
			}
		}
	}()

	// receive any errors
	select {
	case err, ok := <-errChan:
		if !ok {
			// we finished without any errors, congratulations
			return nil
		}
		// short circuit on the first error we get canceling the worker ctx and
		// thus all workers,
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
