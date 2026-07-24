package badger

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ledisdb/ledisdb/store/driver"
)

// Snapshot is a point-in-time read view of the database. The read transaction
// is created at snapshot time and reused by both Get and iteration, so both
// observe the same consistent state regardless of concurrent writes.
type Snapshot struct {
	db  *badger.DB
	txn *badger.Txn
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	item, err := s.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (s *Snapshot) NewIterator() driver.IIterator {
	// Reuse the snapshot's pinned read transaction. ownsTxn is false so the
	// iterator's Close() does not discard the txn that Get still relies on;
	// Snapshot.Close() owns the txn lifecycle.
	return &Iterator{
		db:      s.db,
		txn:     s.txn,
		it:      s.txn.NewIterator(badger.DefaultIteratorOptions),
		ownsTxn: false,
	}
}

func (s *Snapshot) Close() {
	if s.txn != nil {
		s.txn.Discard()
		s.txn = nil
	}
}
