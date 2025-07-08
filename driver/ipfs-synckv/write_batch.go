package ipfs_synckv

import (
	"context"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

var _ = context.Background // Reference context to prevent unused import warning

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
	err    error

	// Track ipfs operations to be executed first
	ipfsOps []func() error
}

func (w *WriteBatch) Put(key, value []byte) {
	if w.err != nil {
		return
	}
	// --- Versioning ---
	w.db.versionMu.Lock()
	w.db.versions[string(key)]++
	version := w.db.versions[string(key)]
	versionBytes := []byte(fmt.Sprintf("%d", version))
	metaKey := append([]byte("_meta:"), key...)
	w.db.versionMu.Unlock()

	// --- Prepare local and IPFS operations ---
	// Encrypt value for IPFS if needed
	ipfsValue := value
	if w.db.encryptionKey != nil {
		ipfsValue = encrypt(value, w.db.encryptionKey)
	}

	// Add local operations to the leveldb batch
	w.wbatch.Put(key, value)
	w.wbatch.Put(metaKey, versionBytes)

	// Add IPFS operations to the pre-commit queue
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Put(w.db.ctx, key, ipfsValue)
	})
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Put(w.db.ctx, metaKey, versionBytes)
	})

	w.db.cache.Del(key)
}

func (w *WriteBatch) Delete(key []byte) {
	if w.err != nil {
		return
	}
	metaKey := append([]byte("_meta:"), key...)

	// Add local delete to the leveldb batch
	w.wbatch.Delete(key)
	w.wbatch.Delete(metaKey)

	// Add IPFS delete to the pre-commit queue
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Delete(w.db.ctx, key)
	})
	w.ipfsOps = append(w.ipfsOps, func() error {
		return w.db.ipfsDB.Delete(w.db.ctx, metaKey)
	})

	w.db.cache.Del(key)
}

// commit executes the batch with given write options.
func (w *WriteBatch) commit(sync bool) error {
	if w.err != nil {
		return w.err
	}

	// 1. Execute all IPFS operations first.
	for _, op := range w.ipfsOps {
		if err := op(); err != nil {
			// If any IPFS operation fails, abort the entire batch.
			// The local wbatch will be discarded.
			return fmt.Errorf("ipfs operation failed during batch commit, aborting: %w", err)
		}
	}

	// 2. If all IPFS operations succeed, commit the local leveldb batch.
	var wo *leveldb.WriteOptions
	if sync {
		wo = w.db.syncOpts
	}
	return w.db.localDB.Write(w.wbatch, wo)
}

func (w *WriteBatch) Commit() error {
	return w.commit(false)
}

func (w *WriteBatch) SyncCommit() error {
	return w.commit(true)
}

func (w *WriteBatch) Rollback() error {
	w.wbatch.Reset()
	w.ipfsOps = nil
	w.err = nil
	return nil
}

func (w *WriteBatch) Close() {
	w.wbatch.Reset()
	w.ipfsOps = nil
}

func (w.WriteBatch) Data() []byte {
	return w.wbatch.Dump()
}
