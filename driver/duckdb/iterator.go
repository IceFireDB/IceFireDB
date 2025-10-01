package duckdb

import (
	"database/sql"
)

type Iterator struct {
	db      *DB
	tx      *sql.Tx
	rows    *sql.Rows
	current struct {
		key   []byte
		value []byte
	}
	valid bool
}

func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.current.key
}

func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.current.value
}

func (it *Iterator) Close() error {
	if it.rows != nil {
		return it.rows.Close()
	}
	return nil
}

func (it *Iterator) Valid() bool {
	return it.valid
}

func (it *Iterator) Next() {
	if it.rows == nil {
		it.valid = false
		return
	}

	it.valid = it.rows.Next()
	if !it.valid {
		return
	}

	err := it.rows.Scan(&it.current.key, &it.current.value)
	if err != nil {
		it.valid = false
	}
}

func (it *Iterator) Prev() {
	// DuckDB doesn't natively support reverse iteration in the same way
	// We'll implement this by storing the current position and seeking backwards
	// This is a simplified implementation
	it.valid = false
}

func (it *Iterator) First() {
	if it.rows != nil {
		it.rows.Close()
	}

	var err error
	if it.tx != nil {
		it.rows, err = it.tx.Query("SELECT key, value FROM kv_store ORDER BY key LIMIT 1000")
	} else {
		it.rows, err = it.db.conn.Query("SELECT key, value FROM kv_store ORDER BY key LIMIT 1000")
	}
	if err != nil {
		it.valid = false
		return
	}

	it.Next()
}

func (it *Iterator) Last() {
	if it.rows != nil {
		it.rows.Close()
	}

	var err error
	if it.tx != nil {
		it.rows, err = it.tx.Query("SELECT key, value FROM kv_store ORDER BY key DESC LIMIT 1")
	} else {
		it.rows, err = it.db.conn.Query("SELECT key, value FROM kv_store ORDER BY key DESC LIMIT 1")
	}
	if err != nil {
		it.valid = false
		return
	}

	it.Next()
}

func (it *Iterator) Seek(key []byte) {
	if it.rows != nil {
		it.rows.Close()
	}

	var err error
	if it.tx != nil {
		it.rows, err = it.tx.Query(
			"SELECT key, value FROM kv_store WHERE key >= ? ORDER BY key LIMIT 1000",
			key,
		)
	} else {
		it.rows, err = it.db.conn.Query(
			"SELECT key, value FROM kv_store WHERE key >= ? ORDER BY key LIMIT 1000",
			key,
		)
	}
	if err != nil {
		it.valid = false
		return
	}

	it.Next()
}