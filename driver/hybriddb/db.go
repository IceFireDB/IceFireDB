package hybriddb

import (
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	StorageName       = "hybriddb"
	defaultFilterBits = 10
	maxKeySize        = 1024 * 1024
	maxValueSize      = 64 * 1024 * 1024
)

type Config struct {
	MaxKeySize        int64
	MaxValueSize      int64
	EnableMetrics     bool
	EnableCompression bool
}

var DefaultConfig = Config{
	MaxKeySize:    maxKeySize,
	MaxValueSize:  maxValueSize,
	EnableMetrics: true,
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
	db   *leveldb.DB
	opts *opt.Options

	iteratorOpts *opt.ReadOptions
	syncOpts     *opt.WriteOptions

	filter filter.Filter

	mu     sync.RWMutex
	stats  *Stats
	closed bool
	config Config
}

type Stats struct {
	ReadOps      int64
	WriteOps     int64
	DeleteOps    int64
	BytesRead    int64
	BytesWritten int64
	Errors       int64
	StartTime    time.Time
}

func (s *DB) GetStorageEngine() any {
	return s.db
}

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

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
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

	db.stats.WriteOps++
	db.stats.BytesWritten += int64(len(key) + len(value))

	return db.db.Put(key, value, nil)
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

	v, err := db.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		db.stats.Errors++
		return nil, err
	}

	db.stats.BytesRead += int64(len(v))
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

	db.stats.DeleteOps++

	return db.db.Delete(key, nil)
}

func (db *DB) SyncPut(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	if err := db.validateKeyValue(key, value); err != nil {
		db.stats.Errors++
		return err
	}

	db.stats.WriteOps++
	db.stats.BytesWritten += int64(len(key) + len(value))

	return db.db.Put(key, value, db.syncOpts)
}

func (db *DB) SyncDelete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	if len(key) == 0 {
		db.stats.Errors++
		return fmt.Errorf("key cannot be empty")
	}

	db.stats.DeleteOps++

	return db.db.Delete(key, db.syncOpts)
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	wb := &WriteBatch{
		db:     db,
		wbatch: new(leveldb.Batch),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	it := &Iterator{
		db.db.NewIterator(nil, db.iteratorOpts),
	}
	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
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
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.db.CompactRange(util.Range{
		Start: nil,
		Limit: nil,
	})
}

func (db *DB) GetStats() *Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.stats == nil {
		return &Stats{}
	}

	stats := *db.stats
	return &stats
}

func (db *DB) Metrics() (tit string, metrics []map[string]any) {
	tit = "hybriddb"

	stats := db.GetStats()

	operationalMetrics := []map[string]any{
		{"read_operations": stats.ReadOps},
		{"write_operations": stats.WriteOps},
		{"delete_operations": stats.DeleteOps},
		{"bytes_read": stats.BytesRead},
		{"bytes_written": stats.BytesWritten},
		{"errors": stats.Errors},
		{"uptime": time.Since(stats.StartTime).String()},
		{"closed": db.closed},
	}

	return tit, operationalMetrics
}
