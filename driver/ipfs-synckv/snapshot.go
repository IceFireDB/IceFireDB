package ipfs_synckv

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

type Snapshot struct {
	db  *DB
	snp *leveldb.Snapshot
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	// Snapshots read from the local DB state at a point in time.
	// The data in localDB is always unencrypted.
	return s.snp.Get(key, s.db.iteratorOpts)
}

func (s *Snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		it:     s.snp.NewIterator(nil, s.db.iteratorOpts),
		db:     s.db, // Pass the db reference
		ipfsDB: s.db.ipfsDB,
		ctx:    s.db.ctx,
	}
	return it
}

func (s *Snapshot) Close() {
	s.snp.Release()
}
