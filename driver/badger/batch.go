package badger

import (
	"github.com/dgraph-io/badger/v3"
)

type WriteBatch struct {
	db *badger.DB
	wb *badger.WriteBatch
}

func (w *WriteBatch) Put(key, value []byte) {
	w.wb.Set(key, value)
}

func (w *WriteBatch) Delete(key []byte) {
	w.wb.Delete(key)
}

func (w *WriteBatch) Commit() error {
	return w.wb.Flush()
}

func (w *WriteBatch) SyncCommit() error {
	return w.wb.Flush()
}

func (w *WriteBatch) Rollback() error {
	w.wb.Cancel()
	return nil
}

func (w *WriteBatch) Close() {
}

func (w *WriteBatch) Data() []byte {
	return nil
}
