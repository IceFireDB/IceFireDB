package badger

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
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
	db.opts.MemTableSize = 1000 << 20
	db.opts.NumGoroutines = 100
	db.opts.MetricsEnabled = false
	db.opts.Compression = options.ZSTD
	db.opts.ZSTDCompressionLevel = 3
	db.opts.IndexCacheSize = 1000 << 20
	db.opts.DetectConflicts = false
	db.opts.NumCompactors = 100
	db.opts.NumMemtables = 100
	db.opts.BlockCacheSize = 1000 << 20

	db.iteratorOpts = badger.DefaultIteratorOptions
	var err error
	db.db, err = badger.Open(db.opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s Store) Repair(path string, cfg *config.Config) error {
	// Open database with default options
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return err
	}
	defer db.Close()
	
	// Run value log GC to clean up any corrupted data
	for {
		err := db.RunValueLogGC(0.7)
		if err == badger.ErrNoRewrite {
			break
		}
		if err != nil {
			return err
		}
	}
	
	return nil
}
