package badger

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
	"github.com/ledisdb/ledisdb/store/driver"
)

type Snapshot struct {
	db *badger.DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := s.db.Backup(buf, 0)
	return buf.Bytes(), err
}

func (s *Snapshot) NewIterator() driver.IIterator {
	tnx := s.db.NewTransactionAt(timeTs(), false)
	it := &Iterator{
		db:  s.db,
		it:  tnx.NewIterator(badger.DefaultIteratorOptions),
		txn: tnx,
	}
	return it
}

func (s *Snapshot) Close() {
}
