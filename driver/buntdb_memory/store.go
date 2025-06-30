package buntdb_memory

import (
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

var _ driver.Store = (*MemoryStore)(nil)

func init() {
	driver.Register(MemoryStore{})
}

type MemoryStore struct{}

func (s MemoryStore) String() string {
	return "buntdb-memory"
}

func (s MemoryStore) Open(path string, cfg *config.Config) (driver.IDB, error) {
	return NewDB()
}

func (s MemoryStore) Repair(path string, cfg *config.Config) error {
	// No repair needed for in-memory database
	return nil
}
