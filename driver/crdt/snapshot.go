package crdt

import (
	"github.com/ledisdb/ledisdb/store/driver"
)

type Snapshot struct {
	db *DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (s *Snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		db: s.db,
	}
	return it
}

func (s *Snapshot) Close() {
}
