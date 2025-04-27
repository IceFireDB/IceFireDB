package buntdb_memory

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/tidwall/buntdb"
)

type snapshot struct {
	db *buntdb.DB
	tx *buntdb.Tx
}

func newSnapshot(db *buntdb.DB) (*snapshot, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &snapshot{db: db, tx: tx}, nil
}

func (s *snapshot) Get(key []byte) ([]byte, error) {
	val, err := s.tx.Get(string(key))
	if err != nil {
		if err == buntdb.ErrNotFound {
			return nil, buntdb.ErrNotFound
		}
		return nil, err
	}
	return []byte(val), nil
}

func (s *snapshot) Close() {
	if s.tx != nil {
		_ = s.tx.Rollback()
	}
}

func (s *snapshot) NewIterator() driver.IIterator {
	return &iterator{db: s.db, tx: s.tx}
}
