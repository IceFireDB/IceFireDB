package buntdb_memory

import "github.com/ledisdb/ledisdb/store/driver"

// IBatch represents a batch operation interface
type IBatch interface {
	driver.IWriteBatch
	// Additional batch methods if needed
}
