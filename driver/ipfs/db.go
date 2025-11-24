package ipfs

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"

	"github.com/dgraph-io/ristretto"
	shell "github.com/ipfs/go-ipfs-api"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	StorageName                      = "ipfs"
	MB                         int64 = 1024 * 1024
	defaultHotCacheSize        int64 = 1024 // unit:MB 1G
	defaultHotCacheNumCounters int64 = 1e7  // unit:byte 10m
	defaultFilterBits          int   = 10
)

type Config struct {
	HotCacheSize       int64
	EndPointConnection string
}

var IpfsDefaultConfig = Config{
	HotCacheSize:       defaultHotCacheSize,
	EndPointConnection: "http://localhost:5001",
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
	if IpfsDefaultConfig.HotCacheSize <= 0 {
		IpfsDefaultConfig.HotCacheSize = defaultHotCacheSize
	}

	// here we use default value, later add config support
	db.cache, err = ristretto.NewCache(&ristretto.Config{
		MaxCost:     IpfsDefaultConfig.HotCacheSize * MB,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Metrics:     true,
		Cost: func(value any) int64 {
			return int64(len(value.([]byte)))
		},
	})

	if err != nil {
		return nil, err
	}

	sh := shell.NewShell(IpfsDefaultConfig.EndPointConnection)
	db.remoteShell = sh

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

	remoteShell *shell.Shell
}

func (s *DB) GetStorageEngine() any {
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
	err := db.db.Put(key, value, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if v, ok := db.cache.Get(key); ok {
		return v.([]byte), nil
	}
	v, err := db.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	reader, err := db.remoteShell.Cat(string(v))
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	db.cache.Set(key, data, 0)
	return data, nil
}

func (db *DB) Delete(key []byte) error {
	err := db.db.Delete(key, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	err := db.db.Put(key, value, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncDelete(key []byte) error {
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
		it:     db.db.NewIterator(nil, db.iteratorOpts),
		rShell: db.remoteShell,
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

func (db *DB) Metrics() (tit string, metrics []map[string]any) {
	tit = "hybriddb cache"
	costAdd := db.cache.Metrics.CostAdded()
	costEvicted := db.cache.Metrics.CostEvicted()
	metrics = []map[string]any{
		{"used_cost": costAdd - costEvicted},                     // Current memory usage (bytes)
		{"cost_added": costAdd},                                  // Total memory sum of data added in history, incrementing (bytes)
		{"cost_evicted": costEvicted},                            // Free total memory, incrementing (bytes)
		{"hits": db.cache.Metrics.Hits()},                        // hits
		{"misses": db.cache.Metrics.Misses()},                    // misses
		{"ratio": fmt.Sprintf("%.2f", db.cache.Metrics.Ratio())}, // hits / (hists + misses)
		{"keys_added": db.cache.Metrics.KeysAdded()},             // number of keys added
		{"keys_evicted": db.cache.Metrics.KeysEvicted()},         // delete key times
		{"keys_updated": db.cache.Metrics.KeysUpdated()},         // update key times
		{"gets_kept": db.cache.Metrics.GetsKept()},               // get total number of times the command is executed
		// GetsDropped is the number of Get counter increments that are dropped
		// internally.
		{"gets_dropped": db.cache.Metrics.GetsDropped()},
		// SetsDropped is the number of Set calls that don't make it into internal
		// buffers (due to contention or some other reason).
		{"sets_dropped": db.cache.Metrics.SetsDropped()},
		// SetsRejected is the number of Set calls rejected by the policy (TinyLFU).
		{"sets_rejected": db.cache.Metrics.SetsRejected()},
	}
	return
}
