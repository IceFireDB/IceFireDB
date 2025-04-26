package column

import (
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

type Store struct{}

func (s *Store) Open(path string, cfg *config.Config) (driver.IDB, error) {
	return &DB{
		kvData:    make(map[string][]byte),
		hashData:  make(map[string]map[string][]byte),
		listData:  make(map[string][][]byte),
		setData:   make(map[string]map[string]struct{}),
		zsetData:  make(map[string]map[string]float64),
		Metrics:   &Metrics{},
	}, nil
}

func (s *Store) Compact() error {
	return nil
}

func (s *Store) Repair(path string, cfg *config.Config) error {
	return nil
}

func (s *Store) String() string {
	return "column"
}

func init() {
	driver.Register(&Store{})
}
