package column

import "github.com/ledisdb/ledisdb/store/driver"

type Snapshot struct {
	db *DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	val, exists := s.db.kvData[string(key)]
	if !exists {
		return nil, nil
	}
	return val, nil
}

func (s *Snapshot) NewIterator() driver.IIterator {
	return s.db.NewIterator()
}

func (s *Snapshot) Close() {}
