package badger

import (
	"sync"

	"github.com/dgraph-io/badger/v3"
)

type WriteBatch struct {
	db   *badger.DB
	wb   *badger.WriteBatch
	lock sync.Mutex
}

func (w *WriteBatch) getWriteBatch() *badger.WriteBatch {
	if w.wb == nil {
		w.wb = w.db.NewWriteBatch()
	}
	return w.wb
}

func (w *WriteBatch) Put(key, value []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	printf("wb put %s: %s", key, value)
	w.getWriteBatch().Set(key, value)
}

func (w *WriteBatch) Delete(key []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	printf("wb delete %s", key)
	w.getWriteBatch().Delete(key)
}

func (w *WriteBatch) Commit() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	var err error
	if w.wb != nil {
		err = w.wb.Flush()
	}
	printf("wb commit: %s", err)
	return err
}

func (w *WriteBatch) SyncCommit() error {
	printf("wb sync commit")
	return w.wb.Flush()
}

func (w *WriteBatch) Rollback() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	printf("wb rollback")
	if w.wb != nil {
		// w.wb.Cancel()
		w.wb = nil
	}
	return nil
}

func (w *WriteBatch) Close() {
	printf("wb close")
}

func (w *WriteBatch) Data() []byte {
	printf("wb data")
	return nil
}
