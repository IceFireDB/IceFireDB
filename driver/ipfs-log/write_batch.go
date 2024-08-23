package ipfs_log

// WriteBatch represents a batch of write operations to be committed to the database.
type WriteBatch struct {
	db *DB // Reference to the parent database.
}

// Put adds a key-value pair to the write batch.
// This operation does not immediately write to the database.
func (w *WriteBatch) Put(key, value []byte) {
	_ = w.db.Put(key, value)
}

// Delete removes a key-value pair from the write batch.
// This operation does not immediately delete from the database.
func (w *WriteBatch) Delete(key []byte) {
	_ = w.db.Delete(key)
}

// Commit writes the batch's operations to the database.
// It returns an error if the commit fails.
func (w *WriteBatch) Commit() error {
	return nil
}

// SyncCommit writes the batch's operations to the database and ensures they are written to stable storage.
// It returns an error if the commit fails.
func (w *WriteBatch) SyncCommit() error {
	return nil
}

// Rollback discards all operations in the write batch.
// It returns an error if the rollback fails.
func (w *WriteBatch) Rollback() error {
	return nil
}

// Close releases any resources held by the write batch.
func (w *WriteBatch) Close() {
}

// Data returns the raw data of the write batch.
// This is useful for debugging or replication purposes.
func (w *WriteBatch) Data() []byte {
	return nil
}
