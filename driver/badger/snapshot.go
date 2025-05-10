package badger

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ledisdb/ledisdb/store/driver"
)

type Snapshot struct {
	db *badger.DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *Snapshot) NewIterator() driver.IIterator {
	tnx := s.db.NewTransaction(false)
	it := &Iterator{
		db:  s.db,
		it:  tnx.NewIterator(badger.DefaultIteratorOptions),
		txn: tnx,
	}
	return it
}

func (s *Snapshot) Close() {
}
