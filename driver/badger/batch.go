package badger

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type WriteBatch struct {
	db   *badger.DB
	wb   *badger.WriteBatch
	lock sync.Mutex
}

func (w *WriteBatch) getWriteBatch() *badger.WriteBatch {
	if w.wb == nil {
		w.wb = w.db.NewWriteBatchAt(timeTs())
	}
	return w.wb
}

func (w *WriteBatch) Put(key, value []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.getWriteBatch().Set(key, value)
}

func (w *WriteBatch) Delete(key []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.getWriteBatch().Delete(key)
}

func (w *WriteBatch) Commit() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	var err error
	if w.wb != nil {
		err = w.wb.Flush()
	}
	return err
}

func (w *WriteBatch) SyncCommit() error {
	return w.Commit()
}

func (w *WriteBatch) Rollback() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.wb != nil {
		w.wb = nil
	}
	return nil
}

func (w *WriteBatch) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.wb = nil
}

func (w *WriteBatch) Data() []byte {
	return nil
}
