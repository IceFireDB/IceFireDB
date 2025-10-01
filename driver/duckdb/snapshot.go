package duckdb

import (
	"database/sql"

	"github.com/ledisdb/ledisdb/store/driver"
)

type Snapshot struct {
	db *DB
	tx *sql.Tx
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.tx.QueryRow(
		"SELECT value FROM kv_store WHERE key = ?",
		key,
	).Scan(&value)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	return value, err
}

func (s *Snapshot) NewIterator() driver.IIterator {
	return &Iterator{
		db: s.db,
		tx: s.tx,
	}
}

func (s *Snapshot) Close() {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
}