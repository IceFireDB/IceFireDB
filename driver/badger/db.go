package badger

import (
	"errors"
	"io"
	"log"
	// "os"

	"github.com/dgraph-io/badger/v3"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

var _ driver.IDB = (*DB)(nil)

func init() {
	log.SetOutput(io.Discard)
	// log.SetOutput(os.Stderr)
}

type DB struct {
	cfg          *config.Config
	opts         badger.Options
	db           *badger.DB
	iteratorOpts badger.IteratorOptions
}

func (db *DB) Close() error {
	printf("db close")
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	printf("db put %s=%s", key, value)
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
	printf("db get %s=%s, err: %s", string(key), string(v), err)
	return v, err
}

func (db *DB) Delete(key []byte) error {
	printf("db delete %s", key)
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	printf("db sync put %s=%s", key, value)
	return db.Put(key, value)
}

func (db *DB) SyncDelete(key []byte) error {
	printf("db sync delete %s", key)
	return db.Delete(key)
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	printf("new wb")
	wb := &WriteBatch{
		db: db.db,
		wb: db.db.NewWriteBatch(),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	printf("new it")
	tnx := db.db.NewTransaction(false)
	it := &Iterator{
		db: db.db,
		it: tnx.NewIterator(db.iteratorOpts),
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	printf("new snap")
	s := &Snapshot{
		db:  db.db,
	}

	return s, nil
}

func (db *DB) Compact() error {
	printf("db compact")
	return nil
}

func (db *DB) GetStorageEngine() interface{} {
	printf("db engine")
	return db.db
}
