package duckdb

import (
	"database/sql"
	"fmt"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

var _ driver.IDB = (*DB)(nil)

type DB struct {
	cfg  *config.Config
	path string
	conn *sql.DB
}

func NewDuckDBConnection(path string) (*sql.DB, error) {
	// Use a simpler connection string
	connStr := fmt.Sprintf("%s", path)
	
	// Try to open the database
	conn, err := sql.Open("duckdb", connStr)
	if err != nil {
		return nil, err
	}

	// Test the connection
	err = conn.Ping()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (db *DB) initializeTable() error {
	_, err := db.conn.Exec(`
		CREATE TABLE IF NOT EXISTS kv_store (
			key BLOB PRIMARY KEY,
			value BLOB
		)
	`)
	return err
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Put(key, value []byte) error {
	_, err := db.conn.Exec(
		"INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
		key, value,
	)
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var value []byte
	err := db.conn.QueryRow(
		"SELECT value FROM kv_store WHERE key = ?",
		key,
	).Scan(&value)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	return value, err
}

func (db *DB) Delete(key []byte) error {
	_, err := db.conn.Exec(
		"DELETE FROM kv_store WHERE key = ?",
		key,
	)
	return err
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) SyncDelete(key []byte) error {
	return db.Delete(key)
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	return &WriteBatch{
		db:  db,
		ops: make([]batchOp, 0),
	}
}

func (db *DB) NewIterator() driver.IIterator {
	return &Iterator{
		db: db,
	}
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	// Note: DuckDB provides read-committed isolation by default
	// For strict snapshot isolation, consider using a different storage engine
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		db: db,
		tx: tx,
	}, nil
}

func (db *DB) Compact() error {
	// DuckDB doesn't require explicit compaction like some other databases
	// We can run VACUUM to optimize the database
	_, err := db.conn.Exec("VACUUM")
	return err
}

func (db *DB) GetStorageEngine() interface{} {
	return db.conn
}