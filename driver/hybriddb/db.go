package hybriddb

import (
	"fmt"
	"io/fs"
	"os"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	StorageName                = "hybriddb"
	MB                         = 1024 * 1024
	defaultHotCacheSize        = 1024 // unit:MB 1G
	defaultHotCacheNumCounters = 1e7  // unit:byte 10m
	defaultFilterBits          = 10
)

// cacheItem is a struct that holds both the key and value.
// We store this in the cache so that we can access the key
// during eviction.
type cacheItem struct {
	key   []byte
	value []byte
}

type Config struct {
	HotCacheSize int64
}

var DefaultConfig = Config{
	HotCacheSize: defaultHotCacheSize,
}

func init() {
	driver.Register(Store{})
}

type Store struct{}

func (s Store) String() string {
	return StorageName
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

	if DefaultConfig.HotCacheSize <= 0 {
		DefaultConfig.HotCacheSize = defaultHotCacheSize
	}

	// Ristretto (hot tier) configuration
	db.cache, err = ristretto.NewCache(&ristretto.Config[[]byte, *cacheItem]{
		MaxCost:     DefaultConfig.HotCacheSize * MB,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Metrics:     true,
		// The cost is the size of the value in bytes.
		Cost: func(item *cacheItem) int64 {
			return int64(len(item.value))
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
	defer db.Close()
	return nil
}

type DB struct {
	path string
	cfg  *config.LevelDBConfig
	db   *leveldb.DB // Cold tier storage
	opts *opt.Options

	iteratorOpts *opt.ReadOptions
	syncOpts     *opt.WriteOptions

	cache *ristretto.Cache[[]byte, *cacheItem] // Hot tier storage

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
	opts.Filter = filter.NewBloomFilter(defaultFilterBits)

	if !cfg.Compression {
		opts.Compression = opt.NoCompression
	} else {
		opts.Compression = opt.SnappyCompression
	}

	opts.BlockSize = cfg.BlockSize
	opts.WriteBuffer = cfg.WriteBufferSize
	opts.OpenFilesCacheCapacity = cfg.MaxOpenFiles
	opts.CompactionTableSize = 32 * 1024 * 1024
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	return opts
}

func (db *DB) Close() error {
	// Close the cache and wait for all OnEvict writes to complete.
	db.cache.Close()
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	// Write-through caching: write to persistent storage first
	if err := db.db.Put(key, value, nil); err != nil {
		return err
	}
	
	// Then update the cache to ensure consistency
	item := &cacheItem{key: key, value: value}
	db.cache.Set(key, item, int64(len(value)))
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 1. Check hot tier
	if item, ok := db.cache.Get(key); ok {
		return item.value, nil
	}

	// 2. Check cold tier
	v, err := db.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil // Not found in either tier
		}
		return nil, err // Other LevelDB error
	}

	// 3. Promote to hot tier
	if v != nil {
		item := &cacheItem{key: key, value: v}
		db.cache.Set(key, item, int64(len(v)))
	}

	return v, nil
}

func (db *DB) Delete(key []byte) error {
	// Write-through caching: delete from persistent storage first
	if err := db.db.Delete(key, nil); err != nil {
		return err
	}
	
	// Then remove from cache to ensure consistency
	db.cache.Del(key)
	return nil
}

func (db *DB) SyncPut(key, value []byte) error {
	// Write-through caching: write to persistent storage first (with sync)
	if err := db.db.Put(key, value, db.syncOpts); err != nil {
		return err
	}
	
	// Then update the cache to ensure consistency
	item := &cacheItem{key: key, value: value}
	db.cache.Set(key, item, int64(len(value)))
	return nil
}

func (db *DB) SyncDelete(key []byte) error {
	// Write-through caching: delete from persistent storage first (with sync)
	if err := db.db.Delete(key, db.syncOpts); err != nil {
		return err
	}
	
	// Then remove from cache to ensure consistency
	db.cache.Del(key)
	return nil
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

func (db *DB) Metrics() (tit string, metrics []map[string]interface{}) {
	tit = "hybriddb cache"
	if db.cache == nil || db.cache.Metrics == nil {
		return tit, nil
	}
	m := db.cache.Metrics
	metrics = []map[string]interface{}{
		{"used_cost": m.CostAdded() - m.CostEvicted()},
		{"cost_added": m.CostAdded()},
		{"cost_evicted": m.CostEvicted()},
		{"hits": m.Hits()},
		{"misses": m.Misses()},
		{"ratio": fmt.Sprintf("%.2f", m.Ratio())},
		{"keys_added": m.KeysAdded()},
		{"keys_evicted": m.KeysEvicted()},
		{"keys_updated": m.KeysUpdated()},
		{"gets_kept": m.GetsKept()},
		{"gets_dropped": m.GetsDropped()},
		{"sets_dropped": m.SetsDropped()},
		{"sets_rejected": m.SetsRejected()},
	}
	return
}
