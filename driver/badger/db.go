package badger

import (
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
	v := []byte{}
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		v, err = item.ValueCopy(v)
		if err != nil {
			return err
		}
		return nil
	})
	// key not found
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	// key exist but value can be slice with 0 cap
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
		wb: db.db.NewWriteBatchAt(timeTs()),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	tnx := db.db.NewTransactionAt(timeTs(), false)
	it := &Iterator{
		db:  db.db,
		it:  tnx.NewIterator(db.iteratorOpts),
		txn: tnx,
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	s := &Snapshot{
		db: db.db,
	}

	return s, nil
}

func (db *DB) Compact() error {
	return nil
}

func (db *DB) GetStorageEngine() interface{} {
	return db.db
}
