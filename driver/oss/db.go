package oss

import (
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/philippgille/gokv/encoding"
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

const (
	StorageName                      = "oss"
	MB                         int64 = 1024 * 1024
	defaultHotCacheSize        int64 = 1024 // unit:MB 1G
	defaultHotCacheNumCounters int64 = 1e7  // unit:byte 10m
	defaultFilterBits          int   = 10


)

type Config struct {
	HotCacheSize int64
	EndPointConnection  string
	AccessKey string
	Secretkey  string 
}

var OssDefaultConfig = Config{
	HotCacheSize:       defaultHotCacheSize,
	EndPointConnection: "http://192.168.4.7:7480",
	AccessKey :   "AKIAIOSFODNN7EXAMPLE",
	Secretkey :   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	
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
	if OssDefaultConfig.HotCacheSize <= 0 {
		OssDefaultConfig.HotCacheSize = defaultHotCacheSize
	}
	// here we use default value, later add config support
	db.cache, err = ristretto.NewCache(&ristretto.Config{
		MaxCost:     OssDefaultConfig.HotCacheSize * MB,
		NumCounters: defaultHotCacheNumCounters,
		BufferItems: 64,
		Metrics:     true,
		Cost: func(value interface{}) int64 {
			return int64(len(value.([]byte)))
		},
	})

	if err != nil {
		return nil, err
	}
	
	
	db.options = Options{
		BucketName:         "gokv",
		AWSaccessKeyID:     OssDefaultConfig.AccessKey,
		AWSsecretAccessKey: OssDefaultConfig.Secretkey,
		Region:             endpoints.UsEast1RegionID, //endpoints.UsWest2RegionID,
		CustomEndpoint:         OssDefaultConfig.EndPointConnection,
		UsePathStyleAddressing: true,
		Codec: encoding.Gob,
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
	options  Options
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


func (db *DB) S3EncodeMetaKey(key []byte) []byte {
	bas64key := base64.StdEncoding.EncodeToString(key)
	bkey := []byte(bas64key)
	return bkey
}



func (db *DB) S3Put(key, value []byte) error {
	client, err := NewClient(db.options)
	if err != nil {
		fmt.Println("new client error")
		return  err
	}
	bkey := db.S3EncodeMetaKey(key)
	sskey := string(bkey)
	err = client.Set(string(sskey), string(value))
	if err != nil {
		//fmt.Println(err)
		//fmt.Println("err key = : " ,bkey)
		//fmt.Println("err value = : " ,value)
	}
	
	//fmt.Println(" ok s3 put ", string(key))
	return err
}
func (db *DB) Put(key, value []byte) error {
	var err error

	err = db.S3Put(key, value )
	if err != nil {
		return err
	}
	
	err = db.db.Put(key, value, nil)
	if err == nil {
		db.cache.Del(key)
	}

	return err
}


func (db *DB) S3Get(key []byte) ([]byte, error) {
	value:= new(string)

	client, err := NewClient(db.options)
	if err != nil {
		return nil, err
	}

	sskey:= db.S3EncodeMetaKey(key)

	//found, err := client.Get(string(key), value)
	found, err := client.Get(string(sskey), value)
	if err != nil {
		//fmt.Println(err)
		return nil , err
	}

	if found != true{
		//fmt.Println(" S3 not found key=" , string(sskey))
		return nil, nil
	}

	//db.cache.Set(key, data, 0)
	return []byte(*value), nil
}

/*
func (db *DB) Get(key []byte) ([]byte, error) {
	if v, ok := db.cache.Get(key); ok {
		return v.([]byte), nil
	}
	v, err := db.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
	
		v, err = db.S3Get(key)
		if err != nil{
			return nil, nil
		}else {
			return nil, nil
		}
	}
	db.cache.Set(key, v, 0)
	return v, nil
}
*/


func (db *DB) Get(key []byte) ([]byte, error) {
	v, err := db.S3Get(key)
	if err != nil{
		//fmt.Println("  s3 get ", err)
		return nil, nil
	}

	return v, nil
}

func (db *DB) S3Delete(key []byte) error {

	client, err := NewClient(db.options)
	if err != nil {
		return  err
	}

	sskey:= db.S3EncodeMetaKey(key)
	err = client.Delete(string(sskey))
	if err == nil {
		//fmt.Printf("Expected an error")
	}
	return err
}

func (db *DB) Delete(key []byte) error {

	db.S3Delete(key)
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
