package duckdb

import (
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

const StorageName = "duckdb"

var _ driver.Store = (*Store)(nil)

func init() {
	driver.Register(Store{})
}

type Store struct {
}

func (s Store) String() string {
	return StorageName
}

func (s Store) Open(path string, cfg *config.Config) (driver.IDB, error) {
	db := new(DB)
	db.cfg = cfg
	db.path = path

	// DuckDB expects a file path, but we receive a directory path
	// Create a DuckDB database file inside the directory
	dbPath := path + "/duckdb.db"

	var err error
	db.conn, err = NewDuckDBConnection(dbPath)
	if err != nil {
		return nil, err
	}

	// Initialize the key-value table
	err = db.initializeTable()
	if err != nil {
		db.conn.Close()
		return nil, err
	}

	return db, nil
}

func (s Store) Repair(path string, cfg *config.Config) error {
	// For DuckDB, repair typically involves checking and potentially recreating the database
	// Since DuckDB is a single file, we can try to open it and if that fails, recreate it
	dbPath := path + "/duckdb.db"
	
	conn, err := NewDuckDBConnection(dbPath)
	if err != nil {
		// If we can't open, try to create a new database
		conn, err = NewDuckDBConnection(dbPath)
		if err != nil {
			return err
		}
		defer conn.Close()
		
		// Initialize the table
		db := &DB{conn: conn}
		return db.initializeTable()
	}
	defer conn.Close()
	
	// Check if the table exists and is valid
	var tableExists bool
	err = conn.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'kv_store'").Scan(&tableExists)
	if err != nil || !tableExists {
		// Table doesn't exist, create it
		db := &DB{conn: conn}
		return db.initializeTable()
	}
	
	return nil
}