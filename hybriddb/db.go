package hybriddb

import (
	"io/fs"
	"os"

	"github.com/dgraph-io/ristretto"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const defaultFilterBits int = 10

type Store struct{}

func (s Store) String() string {
	return DBName
}

func (s Store) Open(path string, cfg *config.Config) (driver.IDB, error) {
	if err := os.MkdirAll(path, fs.ModePerm); err != nil {
		return nil, err
	}

	db := new(DB)
	db.path = path
	db.cfg = &cfg.LevelDB

	db.initOpts()

	var err error
	db.db, err = leveldb.OpenFile(db.path, db.opts)

	if err != nil {
		return nil, err
	}
	// here we use default value, later add config support
	db.cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 102400,
		MaxCost:     1024 * 1024 * 1024, // 1G
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

func (s Store) Repair(path string, cfg *config.Config) error {
	db, err := leveldb.RecoverFile(path, newOptions(&cfg.LevelDB))
	if err != nil {
		return err
	}

	db.Close()
	return nil
}

type DB struct {
	path string

	cfg *config.LevelDBConfig

	db *leveldb.DB

	opts *opt.Options

	iteratorOpts *opt.ReadOptions

	syncOpts *opt.WriteOptions

	cache *ristretto.Cache

	filter filter.Filter
}

func (s *DB) GetStorageEngine() interface{} {
	return s.db
}

func (db *DB) initOpts() {
	db.opts = newOptions(db.cfg)

	db.iteratorOpts = &opt.ReadOptions{}
	db.iteratorOpts.DontFillCache = true

	db.syncOpts = &opt.WriteOptions{}
	db.syncOpts.Sync = true
}

func newOptions(cfg *config.LevelDBConfig) *opt.Options {
	opts := &opt.Options{}
	opts.ErrorIfMissing = false

	opts.BlockCacheCapacity = cfg.CacheSize

	// we must use bloomfilter
	opts.Filter = filter.NewBloomFilter(defaultFilterBits)

	if !cfg.Compression {
		opts.Compression = opt.NoCompression
	} else {
		opts.Compression = opt.SnappyCompression
	}

	opts.BlockSize = cfg.BlockSize
	opts.WriteBuffer = cfg.WriteBufferSize
	opts.OpenFilesCacheCapacity = cfg.MaxOpenFiles

	// here we use default value, later add config support
	opts.CompactionTableSize = 32 * 1024 * 1024
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	return opts
}

func (db *DB) Close() error {
	db.cache.Close()
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	// log.Println("put", string(key), string(value))
	err := db.db.Put(key, value, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// log.Println("get", string(key))
	if v, ok := db.cache.Get(key); ok {
		return v.([]byte), nil
	}
	v, err := db.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	db.cache.Set(key, v, 0)
	return v, nil
}

func (db *DB) Delete(key []byte) error {
	// log.Println("del", string(key))
	err := db.db.Delete(key, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	// log.Println("SyncPut", string(key), string(value))
	err := db.db.Put(key, value, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncDelete(key []byte) error {
	// log.Println("del", string(key))
	err := db.db.Delete(key, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	wb := &WriteBatch{
		db:     db,
		wbatch: new(leveldb.Batch),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	it := &Iterator{
		db.db.NewIterator(nil, db.iteratorOpts),
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	snapshot, err := db.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	s := &Snapshot{
		db:  db,
		snp: snapshot,
	}

	return s, nil
}

func (db *DB) Compact() error {
	return db.db.CompactRange(util.Range{
		Start: nil,
		Limit: nil,
	})
}

func init() {
	driver.Register(Store{})
}
