package duckdb

import (
	"sync"
)

type batchOp struct {
	key   []byte
	value []byte
	delete bool
}

type WriteBatch struct {
	db   *DB
	ops  []batchOp
	lock sync.Mutex
}

func (w *WriteBatch) Put(key, value []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.ops = append(w.ops, batchOp{
		key:   key,
		value: value,
		delete: false,
	})
}

func (w *WriteBatch) Delete(key []byte) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.ops = append(w.ops, batchOp{
		key:    key,
		delete: true,
	})
}

func (w *WriteBatch) Commit() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	tx, err := w.db.conn.Begin()
	if err != nil {
		return err
	}

	for _, op := range w.ops {
		if op.delete {
			_, err = tx.Exec("DELETE FROM kv_store WHERE key = ?", op.key)
		} else {
			_, err = tx.Exec("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", op.key, op.value)
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	// Clear the operations after successful commit
	w.ops = make([]batchOp, 0)
	return nil
}

func (w *WriteBatch) SyncCommit() error {
	return w.Commit()
}

func (w *WriteBatch) Rollback() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	// Clear the operations without executing them
	w.ops = make([]batchOp, 0)
	return nil
}

func (w *WriteBatch) Data() []byte {
	// Not implemented for DuckDB write batch
	return nil
}

func (w *WriteBatch) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.ops = nil
}