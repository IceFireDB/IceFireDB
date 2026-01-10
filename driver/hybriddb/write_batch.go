package hybriddb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
	keys   [][]byte // Track keys that will be modified for cache invalidation
}

func (w *WriteBatch) Put(key, value []byte) {
	w.wbatch.Put(key, value)
	// Track the key for cache invalidation after commit
	w.keys = append(w.keys, copyBytes(key))
}

func (w *WriteBatch) Delete(key []byte) {
	w.wbatch.Delete(key)
	// Track the key for cache invalidation after commit
	w.keys = append(w.keys, copyBytes(key))
}

func (w *WriteBatch) Commit() error {
	err := w.db.db.Write(w.wbatch, nil)
	if err != nil {
		return err
	}

	// Invalidate cache for all keys that were modified in this batch
	for _, key := range w.keys {
		w.db.cache.Del(string(key))
	}

	// Reset the keys tracker for potential reuse
	w.keys = w.keys[:0]

	return nil
}

func (w *WriteBatch) SyncCommit() error {
	err := w.db.db.Write(w.wbatch, w.db.syncOpts)
	if err != nil {
		return err
	}

	// Invalidate cache for all keys that were modified in this batch
	for _, key := range w.keys {
		w.db.cache.Del(string(key))
	}

	// Reset the keys tracker for potential reuse
	w.keys = w.keys[:0]

	return nil
}

func (w *WriteBatch) Rollback() error {
	w.wbatch.Reset()
	return nil
}

func (w *WriteBatch) Close() {
	w.wbatch.Reset()
}

func (w *WriteBatch) Data() []byte {
	return w.wbatch.Dump()
}
