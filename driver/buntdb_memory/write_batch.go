package buntdb_memory

import (
	"github.com/tidwall/buntdb"
	. "github.com/ledisdb/ledisdb/store/driver"
)

var (
	_ IWriteBatch = (*writeBatch)(nil)
	_ IBatch = (*writeBatch)(nil)
)

type writeBatch struct {
	db    *DB
	ops   []batchOp
}

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

func (w *writeBatch) Put(key, value []byte) {
	w.ops = append(w.ops, batchOp{
		key:   key,
		value: value,
	})
}

func (w *writeBatch) Delete(key []byte) {
	w.ops = append(w.ops, batchOp{
		key:    key,
		delete: true,
	})
}

func (w *writeBatch) Commit() error {
	w.db.lock.Lock()
	defer w.db.lock.Unlock()

	return w.db.db.Update(func(tx *buntdb.Tx) error {
		for _, op := range w.ops {
			if op.delete {
				if _, err := tx.Delete(string(op.key)); err != nil {
					return err
				}
			} else {
				if _, _, err := tx.Set(string(op.key), string(op.value), nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (w *writeBatch) Close() {
	w.ops = nil
}

func (w *writeBatch) Data() []byte {
	// Serialize batch operations (simplified example)
	var data []byte
	for _, op := range w.ops {
		if op.delete {
			data = append(data, 'D')
			data = append(data, op.key...)
		} else {
			data = append(data, 'P')
			data = append(data, op.key...)
			data = append(data, '=')
			data = append(data, op.value...)
		}
		data = append(data, '\n')
	}
	return data
}

func (w *writeBatch) Rollback() error {
	w.ops = nil
	return nil
}

func (w *writeBatch) SyncCommit() error {
	return w.Commit()
}
