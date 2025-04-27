package buntdb_memory

import "github.com/ledisdb/ledisdb/store/driver"


// Storage implements the driver.IDB interface
type Storage interface {
	driver.IDB
}

// Iterator implements the driver.IIterator interface
type Iterator interface {
	driver.IIterator
}
