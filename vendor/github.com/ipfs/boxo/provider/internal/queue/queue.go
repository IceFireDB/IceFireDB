// Package queue only remains to provide functionality to remove items from
// the old provider queue datastore.
//
// TODO: remove thos package after kubo v.39.0
package queue

import (
	"context"
	"fmt"

	datastore "github.com/ipfs/go-datastore"
	namespace "github.com/ipfs/go-datastore/namespace"
	query "github.com/ipfs/go-datastore/query"
)

// ClearDatastore clears any entries from the previous queue from the datastore.
func ClearDatastore(ds datastore.Batching) (int, error) {
	const batchSize = 4096

	ds = namespace.Wrap(ds, datastore.NewKey("/queue"))
	ctx := context.Background()

	qry := query.Query{
		KeysOnly: true,
	}
	results, err := ds.Query(ctx, qry)
	if err != nil {
		return 0, fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	batch, err := ds.Batch(ctx)
	if err != nil {
		return 0, fmt.Errorf("cannot create datastore batch: %w", err)
	}

	var rmCount, writeCount int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if writeCount >= batchSize {
			writeCount = 0
			if err = batch.Commit(ctx); err != nil {
				return 0, fmt.Errorf("cannot commit datastore updates: %w", err)
			}
		}
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		if err = batch.Delete(ctx, datastore.NewKey(result.Key)); err != nil {
			return 0, fmt.Errorf("cannot delete key from datastore: %w", err)
		}
		rmCount++
		writeCount++
	}

	if err = batch.Commit(ctx); err != nil {
		return 0, fmt.Errorf("cannot commit datastore updated: %w", err)
	}
	if err = ds.Sync(ctx, datastore.NewKey("")); err != nil {
		return 0, fmt.Errorf("cannot sync datastore: %w", err)
	}

	return rmCount, nil
}
