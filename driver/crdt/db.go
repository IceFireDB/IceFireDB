package crdt

import (
	"bytes"
	"context"
	"encoding/hex"
	"io/fs"
	"log"
	"os"
	"unicode/utf8"

	"github.com/IceFireDB/icefiredb-crdt-kv/kv"
	"github.com/dgraph-io/badger/v4"
	"github.com/ipfs/go-datastore"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	StorageName      = "crdt"
	defaultNamespace = "ifdb"
)

var NilBytes = []byte{0}

type Config struct {
	ServiceName         string
	DataSyncChannel     string
	NetDiscoveryChannel string
}

var DefaultConfig = Config{
	ServiceName:         "icefiredb",
	DataSyncChannel:     "icefiredb-data",
	NetDiscoveryChannel: "icefiredb-net",
}

func init() {
	driver.Register(Store{})
	log.SetFlags(log.Lshortfile)
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
	db.ctx = context.Background()
	db.path = path
	var err error
	kvcfg := kv.Config{
		NodeServiceName:     DefaultConfig.ServiceName,
		DataStorePath:       path,
		DataSyncChannel:     DefaultConfig.DataSyncChannel,
		NetDiscoveryChannel: DefaultConfig.NetDiscoveryChannel,
		Namespace:           defaultNamespace,
		Logger:              logrus.New(),
	}

	db.db, err = kv.NewCRDTKeyValueDB(context.Background(), kvcfg)
	if err != nil {
		return nil, err
	}

	db.leveldb, err = leveldb.OpenFile(db.path, newOptions(&cfg.LevelDB))

	if err != nil {
		return nil, err
	}

	db.namespace = kvcfg.Namespace
	db.iteratorOpts = badger.DefaultIteratorOptions

	return db, nil
}

func newOptions(cfg *config.LevelDBConfig) *opt.Options {
	opts := &opt.Options{}
	opts.ErrorIfMissing = false

	opts.BlockCacheCapacity = cfg.CacheSize

	//must use bloomfilter
	opts.Filter = filter.NewBloomFilter(10)

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

func (s Store) Repair(path string, cfg *config.Config) error {
	db, err := s.Open(path, cfg)
	if err != nil {
		return err
	}
	return db.(*DB).db.Repair()
}

type DB struct {
	ctx          context.Context
	path         string
	db           *kv.CRDTKeyValueDB
	namespace    string
	leveldb      *leveldb.DB
	iteratorOpts badger.IteratorOptions
}

func (d *DB) GetLevelDB() *leveldb.DB {
	return d.leveldb
}

func (s *DB) GetStorageEngine() interface{} {
	return s.db
}

func (db *DB) Close() error {
	db.db.Close()
	return nil
}

func (db *DB) Put(key, value []byte) error {
	k := db.EncodeKey(key)
	if value == nil {
		value = NilBytes
	}
	err := db.db.Put(db.ctx, k, value)
	if err != nil {
		logrus.Errorf("failed to put key-value pair: key=%s, value=%s, error=%v", string(key), string(value), err)
	}
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	key = db.EncodeKey(key)
	val, err := db.db.Get(db.ctx, key)
	if err == datastore.ErrNotFound {
		return nil, nil
	}
	return val, err
}

func (db *DB) Delete(key []byte) error {
	key = db.EncodeKey(key)
	return db.db.Delete(db.ctx, key)
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) SyncDelete(key []byte) error {
	return db.Delete(key)
}

func (db *DB) NewWriteBatch() driver.IWriteBatch {
	wb := &WriteBatch{
		db: db,
	}
	return wb
}

func (db *DB) NewIterator() driver.IIterator {
	it := &Iterator{
		db: db,
	}
	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	s := &Snapshot{
		db: db,
	}

	return s, nil
}

func (db *DB) Compact() error {
	return nil
}

func (dn *DB) EncodeKey(key []byte) []byte {
	if len(key) > 0 && !utf8.Valid(key) {
		buf := bytes.NewBuffer(nil)
		tem := make([]byte, 2)
		for k, b := range key {
			if b > 127 {
				hex.Encode(tem, key[k:k+1])
				buf.Write(tem)
			} else {
				buf.WriteByte(b)
			}
		}
		key = buf.Bytes()
	}
	return key
}
