package hybriddb

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

type snapshot struct {
	db  *store
	snp *leveldb.Snapshot
}

func (s *snapshot) Get(key []byte) ([]byte, error) {
	return s.snp.Get(key, s.db.iteratorOpts)
}

func (s *snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		s.snp.NewIterator(nil, s.db.iteratorOpts),
	}
	return it
}

func (s *snapshot) Close() {
	s.snp.Release()
}
