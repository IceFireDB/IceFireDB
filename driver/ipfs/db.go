package ipfs

import (
	"bytes"
	"encoding/hex"
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

var EndPointConnection = "localhost:5001"

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
	fmt.Print("ipfs.Store.Open \n")

	if err := os.MkdirAll(path, fs.ModePerm); err != nil {
		return nil, err
	}

	db := new(DB)
	db.path = path
	db.cfg = &cfg.LevelDB

	db.initOpts()
	db.encryptKey, _ = hex.DecodeString("44667768254d593b7ea48c3327c18a651f6031554ca4f5e3e641f6ff1ea72e98")

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
		MaxCost:     DefaultConfig.HotCacheSize * MB,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Metrics:     true,
		Cost: func(value interface{}) int64 {
			return int64(len(value.([]byte)))
		},
	})

	sh := shell.NewShell(EndPointConnection)
	db.remoteShell = sh

	if err != nil {
		return nil, err
	}
	return db, nil
}

func (s Store) Repair(path string, cfg *config.Config) error {
	fmt.Print("ipfs.Store.Repair \n")
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

	filter      filter.Filter
	remoteShell *shell.Shell
	encryptKey  []byte
}

func (s *DB) GetStorageEngine() interface{} {
	fmt.Print("ipfs.db.GetStorageEngine \n")
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
	fmt.Print("ipfs.db.Close \n")
	db.cache.Close()
	return db.db.Close()
}

func (db *DB) Put1(key, value []byte) error {
	fmt.Print("ipfs.db.put \n")
	err := db.db.Put(key, value, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) Put(key, value []byte) error {
	fmt.Print("ipfs.db.put \n")
	//	data := encrypt(value, w.db.encryptKey)

	buf := bytes.NewBuffer(value)
	//fmt.Printf("%s\n", buf)
	cid, err := db.remoteShell.Add(buf)
	if err != nil {
		fmt.Print("ipfs.add err\n")
	}
	fmt.Println("cid=", cid)

	err = db.db.Put(key, []byte(cid), nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) Geti1(key []byte) ([]byte, error) {
	fmt.Print("ipfs.db.Get \n")
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

func (db *DB) Get(key []byte) ([]byte, error) {
	fmt.Print("ipfs.db.Get \n")
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

	dData := decrypt(data, db.encryptKey)

	fmt.Println("data=", string(dData))

	db.cache.Set(key, dData, 0)
	return dData, nil
}

func (db *DB) Delete(key []byte) error {
	fmt.Print("ipfs.db.Delete \n")
	err := db.db.Delete(key, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	fmt.Print("ipfs.db.SyncPut \n")
	err := db.db.Put(key, value, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncDelete(key []byte) error {
	fmt.Print("ipfs.db.SyncDelete \n")
	err := db.db.Delete(key, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	fmt.Print("ipfs.db.NewWriteBatch \n")
	wb := &WriteBatch{
		db:     db,
		wbatch: new(leveldb.Batch),
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	fmt.Print("ipfs.db.NewIterator \n")
	it := &Iterator{
		it:     db.db.NewIterator(nil, db.iteratorOpts),
		rShell: db.remoteShell,
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	fmt.Print("ipfs.db.NewSnapshot \n")
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
	costAdd := db.cache.Metrics.CostAdded()
	costEvicted := db.cache.Metrics.CostEvicted()
	metrics = []map[string]interface{}{
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
