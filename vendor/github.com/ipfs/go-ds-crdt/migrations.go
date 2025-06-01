package crdt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// Use this to detect if we need to run migrations.
var version uint64 = 1

func (store *Datastore) versionKey() ds.Key {
	return store.namespace.ChildString(versionKey)
}

func (store *Datastore) getVersion(ctx context.Context) (uint64, error) {
	versionK := store.versionKey()
	data, err := store.store.Get(ctx, versionK)
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	v, n := binary.Uvarint(data)
	if n <= 0 {
		return v, errors.New("error decoding version")
	}
	return v - 1, nil
}

func (store *Datastore) setVersion(ctx context.Context, v uint64) error {
	versionK := store.versionKey()
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v+1)
	if n == 0 {
		return errors.New("error encoding version")
	}

	return store.store.Put(ctx, versionK, buf[0:n])
}

func (store *Datastore) applyMigrations(ctx context.Context) error {
	v, err := store.getVersion(ctx)
	if err != nil {
		return err
	}

	switch v {
	case 0: // need to migrate
		err := store.migrate0to1(ctx)
		if err != nil {
			return err
		}

		err = store.setVersion(ctx, 1)
		if err != nil {
			return err
		}
		fallthrough

	case version:
		store.logger.Infof("CRDT database format v%d", version)
		return nil
	}
	return nil
}

// migrate0to1 re-sets all the values and priorities of previously tombstoned
// elements to deal with the aftermath of
// https://github.com/ipfs/go-ds-crdt/issues/238. This bug caused that the
// values/priorities of certain elements was wrong depending on tombstone
// arrival order.
func (store *Datastore) migrate0to1(ctx context.Context) error {
	// 1. Find keys for which we have tombstones
	// 2. Loop them
	// 3. Find/set best value for them

	s := store.set
	tombsPrefix := s.keyPrefix(tombsNs) // /ns/tombs
	q := query.Query{
		Prefix:   tombsPrefix.String(),
		KeysOnly: true,
	}

	var rStore = store.store
	var wStore ds.Write = store.store
	var err error
	batchingDs, batching := wStore.(ds.Batching)
	if batching {
		wStore, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	results, err := rStore.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	// Results are not going to be ordered per key (I tested). Therefore,
	// we can keep a list of keys in memory to avoid findingBestValue for
	// every tombstone block entry, or we can repeat the operation every
	// time there is a tombstone for the same key.  Given this is a one
	// time operation that only affects tombstoned keys, we opt to
	// de-duplicate.

	var total int
	doneKeys := make(map[string]struct{})
	for r := range results.Next() {
		if r.Error != nil {
			return r.Error
		}

		// Switch from /ns/tombs/key/block to /key
		dskey := ds.NewKey(
			strings.TrimPrefix(r.Key, tombsPrefix.String()))
		// Switch from /key/block to /key
		key := dskey.Parent().String()
		if _, ok := doneKeys[key]; ok {
			continue
		}
		doneKeys[key] = struct{}{}

		valueK := s.valueKey(key)
		v, p, err := s.findBestValue(ctx, key, nil)
		if err != nil {
			return fmt.Errorf("error finding best value for %s: %w", key, err)
		}

		if v == nil {
			wStore.Delete(ctx, valueK)
			wStore.Delete(ctx, s.priorityKey(key))
		} else {
			wStore.Put(ctx, valueK, v)
			s.setPriority(ctx, wStore, key, p)
		}
		total++
	}

	if batching {
		err := wStore.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}

	s.logger.Infof("Migration v0 to v1 finished (%d elements affected)", total)
	return nil
}
