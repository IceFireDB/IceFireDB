package internal

import (
	"context"
	"sync"

	"github.com/samber/lo"
)

// DoBatch processes a slice of items with concurrency no higher than maxConcurrency by splitting it into batches no larger than maxBatchSize.
// If an error is returned for any batch, the process is short-circuited and the error is immediately returned.
func DoBatch[A any](ctx context.Context, maxBatchSize, maxConcurrency int, items []A, f func(context.Context, []A) error) error {
	if len(items) == 0 {
		return nil
	}
	batches := lo.Chunk(items, maxBatchSize)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	batchChan := make(chan []A)
	errChan := make(chan error)
	wg := sync.WaitGroup{}
	for i := 0; i < maxConcurrency && i < len(batches); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case batch := <-batchChan:
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
		defer close(errChan)
		defer wg.Wait()
		for _, batch := range batches {
			select {
			case batchChan <- batch:
			case <-workerCtx.Done():
				return
			}
		}
		cancel()
	}()

	// receive any errors
	select {
	case err, ok := <-errChan:
		if !ok {
			// we finished without any errors, congratulations
			return nil
		}
		// short circuit on the first error we get
		// canceling the worker ctx and thus all workers,
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
