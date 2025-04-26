package column

import "github.com/ledisdb/ledisdb/store/driver"

type Snapshot struct {
	db *DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	val, exists := s.db.data[string(key)]
	if !exists {
		return nil, nil
	}
	return val, nil
}

func (s *Snapshot) NewIterator() driver.IIterator {
	return s.db.NewIterator()
}

func (s *Snapshot) Close() {}
