package buntdb_memory

import (
	"sync"

	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/tidwall/buntdb"
)

type DB struct {
	db   *buntdb.DB
	lock sync.RWMutex
}

const StorageName = "buntdb-memory"

var _ driver.IDB = (*DB)(nil)

func NewDB() (*DB, error) {
	bdb, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, err
	}
	return &DB{db: bdb}, nil
}

func (d *DB) Close() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.db.Close()
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var value []byte
	err := d.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(key))
		if err != nil {
		if err == buntdb.ErrNotFound {
			return buntdb.ErrNotFound
		}
			return err
		}
		value = []byte(val)
		return nil
	})
	return value, err
}

func (d *DB) Put(key, value []byte) error {
	return d.SyncPut(key, value)
}

func (d *DB) SyncPut(key, value []byte) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	return d.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(string(key), string(value), nil)
		return err
	})
}

func (d *DB) Delete(key []byte) error {
	return d.SyncDelete(key)
}

func (d *DB) SyncDelete(key []byte) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	return d.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(string(key))
		return err
	})
}

func (d *DB) NewIterator() driver.IIterator {
	return &iterator{db: d.db}
}

func (d *DB) Type() string {
	return StorageName
}

func (d *DB) NewBatch() IBatch {
	return d.NewWriteBatch()
}

func (d *DB) NewWriteBatch() driver.IWriteBatch {
	return &writeBatch{db: d}
}

func (d *DB) Compact() error {
	// BuntDB handles compaction automatically in memory mode
	// No manual compaction needed
	return nil
}

func (d *DB) NewSnapshot() (driver.ISnapshot, error) {
	return newSnapshot(d.db)
}

func (d *DB) GetStorageEngine() interface{} {
	return d
}

var ErrNotFound = buntdb.ErrNotFound
