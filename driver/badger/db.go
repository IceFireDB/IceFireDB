package badger

import (
	"errors"

	"github.com/dgraph-io/badger/v3"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

var _ driver.IDB = (*DB)(nil)

type DB struct {
	cfg          *config.Config
	opts         badger.Options
	db           *badger.DB
	iteratorOpts badger.IteratorOptions
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var v []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		v, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return v, err
}

func (db *DB) Delete(key []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) SyncDelete(key []byte) error {
	return db.Delete(key)
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	wb := &WriteBatch{
		db: db.db,
		wb: db.db.NewWriteBatch(),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	tnx := db.db.NewTransaction(false)
	it := &Iterator{
		db: db.db,
		it: tnx.NewIterator(db.iteratorOpts),
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	s := &Snapshot{
		db:  db.db,
	}

	return s, nil
}

func (db *DB) Compact() error {
	return nil
}

func (db *DB) GetStorageEngine() interface{} {
	return db.db
}
