package ipfs_log

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

// Snapshot represents a read-only snapshot of the database at a particular point in time.
type Snapshot struct {
	db  *DB               // Reference to the parent database.
	snp *leveldb.Snapshot // The underlying leveldb snapshot.
}

// Get retrieves the value for a key from the snapshot.
// It returns the value and any error encountered.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.snp.Get(key, s.db.iteratorOpts)
}

// NewIterator creates a new iterator for the snapshot.
// It returns an IIterator interface which can be used to iterate over the snapshot's data.
func (s *Snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		it: s.snp.NewIterator(nil, s.db.iteratorOpts),
	}
	return it
}

// Close releases the snapshot, making it unusable.
func (s *Snapshot) Close() {
	s.snp.Release()
}
