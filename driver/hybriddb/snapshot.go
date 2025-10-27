package hybriddb

import (
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

type Snapshot struct {
	db  *DB
	snp *leveldb.Snapshot
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	v, err := s.snp.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil // Not found
		}
		return nil, err // Other LevelDB error
	}
	return v, nil
}

func (s *Snapshot) NewIterator() driver.IIterator {
	it := &Iterator{
		s.snp.NewIterator(nil, s.db.iteratorOpts),
	}
	return it
}

func (s *Snapshot) Close() {
	s.snp.Release()
}
