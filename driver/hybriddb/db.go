package hybriddb

import (
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"

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
	defaultCacheTTL            = 5 * time.Minute
	maxKeySize                 = 1024 * 1024      // 1MB max key size
	maxValueSize               = 64 * 1024 * 1024 // 64MB max value size
)

// cacheItem is a struct that holds both the key and value.
// We store this in the cache so that we can access the key
// during eviction.
type cacheItem struct {
	key      []byte
	value    []byte
	expires  time.Time
	accesses int64
}

type Config struct {
	HotCacheSize      int64
	CacheTTL          time.Duration
	MaxKeySize        int64
	MaxValueSize      int64
	EnableMetrics     bool
	EnableCompression bool
}

var DefaultConfig = Config{
	HotCacheSize:      defaultHotCacheSize,
	CacheTTL:          defaultCacheTTL,
	MaxKeySize:        maxKeySize,
	MaxValueSize:      maxValueSize,
	EnableMetrics:     true,
	EnableCompression: true,
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
	db.config = DefaultConfig
	db.stats = &Stats{
		StartTime: time.Now(),
	}

	db.initOpts()

	var err error
	db.db, err = leveldb.OpenFile(db.path, db.opts)
	if err != nil {
		return nil, err
	}

	if db.config.HotCacheSize <= 0 {
		db.config.HotCacheSize = defaultHotCacheSize
	}

	// Ristretto (hot tier) configuration
	db.cache, err = ristretto.NewCache(&ristretto.Config[[]byte, *cacheItem]{
		MaxCost:     db.config.HotCacheSize * MB,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Metrics:     db.config.EnableMetrics,
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

	// Performance and security enhancements
	mu     sync.RWMutex
	stats  *Stats
	closed bool
	config Config
}

// Stats holds performance and operational statistics
type Stats struct {
	CacheHits    int64
	CacheMisses  int64
	ReadOps      int64
	WriteOps     int64
	DeleteOps    int64
	BytesRead    int64
	BytesWritten int64
	Errors       int64
	StartTime    time.Time
}

func (s *DB) GetStorageEngine() interface{} {
	return s.db
}

// validateKeyValue performs security and size validation
func (db *DB) validateKeyValue(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	if int64(len(key)) > db.config.MaxKeySize {
		return fmt.Errorf("key size %d exceeds maximum allowed %d", len(key), db.config.MaxKeySize)
	}

	if int64(len(value)) > db.config.MaxValueSize {
		return fmt.Errorf("value size %d exceeds maximum allowed %d", len(value), db.config.MaxValueSize)
	}

	return nil
}

// isExpired checks if a cache item has expired
func (item *cacheItem) isExpired() bool {
	return !item.expires.IsZero() && time.Now().After(item.expires)
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
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("database already closed")
	}

	// Close the cache and wait for all OnEvict writes to complete.
	db.cache.Close()
	db.closed = true
	return db.db.Close()
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	if err := db.validateKeyValue(key, value); err != nil {
		db.stats.Errors++
		return err
	}

	// Write-through caching: write to persistent storage first
	if err := db.db.Put(key, value, nil); err != nil {
		db.stats.Errors++
		return err
	}

	db.stats.WriteOps++
	db.stats.BytesWritten += int64(len(key) + len(value))

	// Then update the cache to ensure consistency
	item := &cacheItem{
		key:      key,
		value:    value,
		expires:  time.Now().Add(db.config.CacheTTL),
		accesses: 1,
	}
	db.cache.Set(key, item, int64(len(value)))
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, fmt.Errorf("database is closed")
	}

	if len(key) == 0 {
		db.stats.Errors++
		return nil, fmt.Errorf("key cannot be empty")
	}

	db.stats.ReadOps++

	// 1. Check hot tier
	if item, ok := db.cache.Get(key); ok {
		if !item.isExpired() {
			item.accesses++
			db.stats.CacheHits++
			db.stats.BytesRead += int64(len(item.value))
			return item.value, nil
		}
		// Remove expired item from cache
		db.cache.Del(key)
	}

	db.stats.CacheMisses++

	// 2. Check cold tier
	v, err := db.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil // Not found in either tier
		}
		db.stats.Errors++
		return nil, err // Other LevelDB error
	}

	// 3. Promote to hot tier
	if v != nil {
		db.stats.BytesRead += int64(len(v))
		item := &cacheItem{
			key:      key,
			value:    v,
			expires:  time.Now().Add(db.config.CacheTTL),
			accesses: 1,
		}
		db.cache.Set(key, item, int64(len(v)))
	}

	return v, nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	if len(key) == 0 {
		db.stats.Errors++
		return fmt.Errorf("key cannot be empty")
	}

	// Write-through caching: delete from persistent storage first
	if err := db.db.Delete(key, nil); err != nil {
		db.stats.Errors++
		return err
	}

	db.stats.DeleteOps++

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

// GetStats returns comprehensive performance and operational statistics
func (db *DB) GetStats() *Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.stats == nil {
		return &Stats{}
	}

	// Return a copy to avoid race conditions
	stats := *db.stats
	return &stats
}

func (db *DB) Metrics() (tit string, metrics []map[string]interface{}) {
	tit = "hybriddb cache"

	stats := db.GetStats()

	// Add cache metrics if available
	var cacheMetrics []map[string]interface{}
	if db.cache != nil && db.cache.Metrics != nil {
		m := db.cache.Metrics
		cacheMetrics = []map[string]interface{}{
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
	}

	// Add operational statistics
	operationalMetrics := []map[string]interface{}{
		{"cache_hits": stats.CacheHits},
		{"cache_misses": stats.CacheMisses},
		{"cache_hit_ratio": fmt.Sprintf("%.2f%%", float64(stats.CacheHits)/float64(stats.CacheHits+stats.CacheMisses)*100)},
		{"read_operations": stats.ReadOps},
		{"write_operations": stats.WriteOps},
		{"delete_operations": stats.DeleteOps},
		{"bytes_read": stats.BytesRead},
		{"bytes_written": stats.BytesWritten},
		{"errors": stats.Errors},
		{"uptime": time.Since(stats.StartTime).String()},
		{"closed": db.closed},
	}

	// Combine all metrics
	metrics = append(operationalMetrics, cacheMetrics...)
	return
}
