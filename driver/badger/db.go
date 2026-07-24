package badger

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

var _ driver.IDB = (*DB)(nil)

type DB struct {
	cfg           *config.Config
	opts          badger.Options
	db            *badger.DB
	iteratorOpts  badger.IteratorOptions
	encryptionKey []byte // For encryption support
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value)
		if db.encryptionKey != nil {
			e.WithMeta(1) // Mark as encrypted
		}
		return txn.SetEntry(e)
	})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	v := []byte{}
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		// Handle encrypted data if needed
		if db.encryptionKey != nil && item.UserMeta() == 1 {
			return item.Value(func(val []byte) error {
				// Add decryption logic here
				v = append([]byte{}, val...)
				return nil
			})
		}

		v, err = item.ValueCopy(v)
		if err != nil {
			return err
		}
		return nil
	})

	if err == badger.ErrKeyNotFound {
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
	opts := db.iteratorOpts
	opts.PrefetchSize = 100 // Optimized prefetch
	it := &Iterator{
		db:      db.db,
		it:      tnx.NewIterator(opts),
		txn:     tnx,
		ownsTxn: true,
	}
	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	// A read transaction created now pins a point-in-time read timestamp, so
	// Get and iteration observe a consistent state isolated from concurrent
	// writes. (NewTransactionAt is unavailable outside managed mode.)
	return &Snapshot{
		db:  db.db,
		txn: db.db.NewTransaction(false),
	}, nil
}

// NewStream creates a new stream for bulk operations
func (db *DB) NewStream() *badger.Stream {
	return db.db.NewStream()
}

func (db *DB) Compact() error {
	// badger auto-compacts the LSM tree. The driver-level Compact() reclaims
	// value-log space via GC; ErrNoRewrite (nothing to reclaim) is not an error.
	if err := db.db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		return err
	}
	return nil
}

func (db *DB) GetStorageEngine() any {
	return db.db
}
