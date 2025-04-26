package column

import (
	"sync"
)

type WriteBatch struct {
	db  *DB
	ops []operation
	mu  sync.Mutex
}

type operation struct {
	key    []byte
	value  []byte
	delete bool
}

func (w *WriteBatch) Put(key, value []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = append(w.ops, operation{key, value, false})
}

func (w *WriteBatch) Delete(key []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = append(w.ops, operation{key, nil, true})
}

func (w *WriteBatch) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.db.mu.Lock()
	defer w.db.mu.Unlock()
	for _, op := range w.ops {
		if op.delete {
			delete(w.db.kvData, string(op.key))
		} else {
			w.db.kvData[string(op.key)] = op.value
		}
	}
	return nil
}

func (w *WriteBatch) SyncCommit() error {
	return w.Commit()
}

func (w *WriteBatch) Rollback() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = nil
	return nil
}

func (w *WriteBatch) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ops = nil
}

func (w *WriteBatch) Data() []byte {
	return nil
}
