package column

import (
	"sync"
	"github.com/ledisdb/ledisdb/store/driver"
)

const StorageName = "column"

type Metrics struct {
	// Add actual metric fields here
}

type DB struct {
	data    map[string][]byte
	mu      sync.RWMutex
	Metrics *Metrics
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Compact() error {
	return nil
}

func (db *DB) Delete(key []byte) error {
	delete(db.data, string(key))
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	val, exists := db.data[string(key)]
	if !exists {
		return nil, nil
	}
	return val, nil
}

func (db *DB) NewIterator() driver.IIterator {
	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}
	return &Iterator{keys: keys}
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	return &Snapshot{db: db}, nil
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	return &WriteBatch{db: db}
}

func (db *DB) Put(key, value []byte) error {
	db.data[string(key)] = value
	return nil
}

func (db *DB) SyncDelete(key []byte) error {
	return db.Delete(key)
}

func (db *DB) SyncPut(key, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) GetStorageEngine() interface{} {
	return db
}
