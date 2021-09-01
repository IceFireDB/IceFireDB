package badger

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
)

const StorageName = "badger"

const defaultFilterBits int = 10

var _ driver.Store = (*Store)(nil)

func init() {
	driver.Register(Store{})
}

type Store struct {
}

func (s Store) String() string {
	return StorageName
}

func (s Store) Open(path string, cfg *config.Config) (driver.IDB, error) {
	db := new(DB)
	db.cfg = cfg
	db.opts = badger.DefaultOptions(path)
	db.iteratorOpts = badger.DefaultIteratorOptions
	var err error
	db.db, err = badger.Open(db.opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s Store) Repair(path string, cfg *config.Config) error {
	return nil
}
