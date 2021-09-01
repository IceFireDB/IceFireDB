package badger

import (
	"bytes"
	"log"

	"github.com/dgraph-io/badger/v3"
	"github.com/ledisdb/ledisdb/store/driver"
)

type Snapshot struct {
	db  *badger.DB
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	log.Printf("snap get: %s\n", key)
	buf := new(bytes.Buffer)
	_, err := s.db.Backup(buf, 0)
	return buf.Bytes(), err
}

func (s *Snapshot) NewIterator() driver.IIterator {
	log.Printf("snap new it\n")
	tnx := s.db.NewTransaction(false)
	it := &Iterator{
		db: s.db,
		it: tnx.NewIterator(badger.DefaultIteratorOptions),
	}
	return it
}

func (s *Snapshot) Close() {
	log.Printf("snap close\n")
}
