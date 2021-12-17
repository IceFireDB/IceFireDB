package ipfs

import (
	"bytes"
	"github.com/syndtr/goleveldb/leveldb"
)

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
	err    error
}

func (w *WriteBatch) Put(key, value []byte) {
	buf := bytes.NewBuffer(value)
	cid, err := w.db.remoteShell.Add(buf)
	if err != nil {
		w.err = err
		return
	}
	w.wbatch.Put(key, []byte(cid))
	w.db.cache.Del(key)
}

func (w *WriteBatch) Delete(key []byte) {
	w.wbatch.Delete(key)
	w.db.cache.Del(key)
}

func (w *WriteBatch) Commit() error {
	if w.err != nil {
		return w.err
	}
	return w.db.db.Write(w.wbatch, nil)
}

func (w *WriteBatch) SyncCommit() error {
	return w.db.db.Write(w.wbatch, w.db.syncOpts)
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
