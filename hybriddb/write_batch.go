package hybriddb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type writeBatch struct {
	db     *store
	wbatch *leveldb.Batch
}

func (w *writeBatch) Put(key, value []byte) {
	w.wbatch.Put(key, value)
	w.db.cache.Del(key)
}

func (w *writeBatch) Delete(key []byte) {
	w.wbatch.Delete(key)
	w.db.cache.Del(key)
}

func (w *writeBatch) Commit() error {
	return w.db.db.Write(w.wbatch, nil)
}

func (w *writeBatch) SyncCommit() error {
	return w.db.db.Write(w.wbatch, w.db.syncOpts)
}

func (w *writeBatch) Rollback() error {
	w.wbatch.Reset()
	return nil
}

func (w *writeBatch) Close() {
	w.wbatch.Reset()
}

func (w *writeBatch) Data() []byte {
	return w.wbatch.Dump()
}
