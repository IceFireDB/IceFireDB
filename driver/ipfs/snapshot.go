package ipfs

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

type Snapshot struct {
	db  *DB
	snp *leveldb.Snapshot
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.snp.Get(key, s.db.iteratorOpts)
}

func (s *Snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		it:     s.snp.NewIterator(nil, s.db.iteratorOpts),
		ipfsDB: s.db.ipfsDB,
		ctx:    s.db.ctx,
	}
	return it
}

func (s *Snapshot) Close() {
	s.snp.Release()
}
