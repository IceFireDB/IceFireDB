package hybriddb

import (
	"io/fs"
	"os"

	"github.com/dgraph-io/ristretto"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

// StorageName hybrid store backend
const StorageName = "hybriddb"

// Config hybrid config
type Config struct {
	HotCacheSize int64
}

// DefaultConfig default config
var DefaultConfig = Config{
	HotCacheSize: defaultHotCacheSize,
}

func init() {
	// register ledis store
	driver.Register(hybrid{})
}

type hybrid struct{}

func (s hybrid) String() string {
	return StorageName
}

func (s hybrid) Open(path string, cfg *config.Config) (driver.IDB, error) {
	if err := os.MkdirAll(path, fs.ModePerm); err != nil {
		return nil, err
	}

	db := new(store)
	db.path = path
	db.cfg = &cfg.LevelDB

	db.initOpts()

	var err error
	db.db, err = leveldb.OpenFile(db.path, db.opts)
	if err != nil {
		return nil, err
	}

	if DefaultConfig.HotCacheSize <= 0 {
		DefaultConfig.HotCacheSize = defaultHotCacheSize
	}
	// here we use default value, later add config support
	db.cache, err = ristretto.NewCache(&ristretto.Config{
		MaxCost:     DefaultConfig.HotCacheSize * mb,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Cost: func(value interface{}) int64 {
			return int64(len(value.([]byte)))
		},
	})

	if err != nil {
		return nil, err
	}
	
	return db, nil
}

func (s hybrid) Repair(path string, cfg *config.Config) error {
	db, err := leveldb.RecoverFile(path, newOptions(&cfg.LevelDB))
	if err != nil {
		return err
	}

	db.Close()
	return nil
}
