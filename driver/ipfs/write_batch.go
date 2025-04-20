package ipfs

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
)

var _ = context.Background // Reference context to prevent unused import warning

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
	err    error
	
	// Track ipfs operations for rollback
	ipfsOps []func() error
}

func (w *WriteBatch) Put(key, value []byte) {
	// Add to LevelDB batch
	w.wbatch.Put(key, value)
	
	// Track ipfs operation
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Put(w.db.ctx, key, value)
	})
	
	w.db.cache.Del(key)
}

func (w *WriteBatch) Delete(key []byte) {
	// Delete from LevelDB batch
	w.wbatch.Delete(key)
	
	// Track ipfs operation
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Delete(w.db.ctx, key)
	})
	
	w.db.cache.Del(key)
}

func (w *WriteBatch) Commit() error {
	if w.err != nil {
		return w.err
	}
	
	// Commit LevelDB batch
	if err := w.db.localDB.Write(w.wbatch, nil); err != nil {
		return err
	}
	
	// Execute ipfs operations
	for _, op := range w.ipfsOps {
		if err := op(); err != nil {
			return err
		}
	}
	
	return nil
}

func (w *WriteBatch) SyncCommit() error {
	if w.err != nil {
		return w.err
	}
	
	// Sync commit LevelDB batch
	if err := w.db.localDB.Write(w.wbatch, w.db.syncOpts); err != nil {
		return err
	}
	
	// Execute ipfs operations
	for _, op := range w.ipfsOps {
		if err := op(); err != nil {
			return err
		}
	}
	
	return nil
}

func (w *WriteBatch) Rollback() error {
	w.wbatch.Reset()
	w.ipfsOps = nil
	return nil
}

func (w *WriteBatch) Close() {
	w.wbatch.Reset()
	w.ipfsOps = nil
}

func (w *WriteBatch) Data() []byte {
	return w.wbatch.Dump()
}
