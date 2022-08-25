package crdt

type WriteBatch struct {
	db *DB
}

func (w *WriteBatch) Put(key, value []byte) {
	w.db.Put(key, value)
}

func (w *WriteBatch) Delete(key []byte) {
	w.db.Delete(key)
}

func (w *WriteBatch) Commit() error {
	return nil
}

func (w *WriteBatch) SyncCommit() error {
	return nil
}

func (w *WriteBatch) Rollback() error {
	return nil
}

func (w *WriteBatch) Close() {
}

func (w *WriteBatch) Data() []byte {
	return nil
}
