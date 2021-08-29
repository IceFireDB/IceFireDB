package hybriddb

import (
	"github.com/dgraph-io/ristretto"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	defaultFilterBits          int   = 10
	mb                         int64 = 1024 * 1024
	defaultHotCacheSize        int64 = 1024 // unit:MB 1G
	defaultHotCacheNumCounters int64 = 1e7  // unit:byte 10m
)

type store struct {
	path string

	cfg *config.LevelDBConfig

	db *leveldb.DB

	opts *opt.Options

	iteratorOpts *opt.ReadOptions

	syncOpts *opt.WriteOptions

	cache *ristretto.Cache

	filter filter.Filter
}

func (s *store) GetStorageEngine() interface{} {
	return s.db
}

func (s *store) initOpts() {
	s.opts = newOptions(s.cfg)

	s.iteratorOpts = &opt.ReadOptions{}
	s.iteratorOpts.DontFillCache = true

	s.syncOpts = &opt.WriteOptions{}
	s.syncOpts.Sync = true
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

func (s *store) Close() error {
	s.cache.Close()
	return s.db.Close()
}

func (s *store) Put(key, value []byte) error {
	err := s.db.Put(key, value, nil)
	if err == nil {
		s.cache.Del(key)
	}
	return err
}

func (s *store) Get(key []byte) ([]byte, error) {
	if v, ok := s.cache.Get(key); ok {
		return v.([]byte), nil
	}
	v, err := s.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	s.cache.Set(key, v, 0)
	return v, nil
}

func (s *store) Delete(key []byte) error {
	err := s.db.Delete(key, nil)
	if err == nil {
		s.cache.Del(key)
	}
	return err
}

func (s *store) SyncPut(key []byte, value []byte) error {
	err := s.db.Put(key, value, s.syncOpts)
	if err == nil {
		s.cache.Del(key)
	}
	return err
}

func (s *store) SyncDelete(key []byte) error {
	err := s.db.Delete(key, s.syncOpts)
	if err == nil {
		s.cache.Del(key)
	}
	return err
}

func (s *store) NewWriteBatch() driver.IWriteBatch {
	wb := &writeBatch{
		db:     s,
		wbatch: new(leveldb.Batch),
	}
	return wb
}

func (s *store) NewIterator() driver.IIterator {
	it := &Iterator{
		s.db.NewIterator(nil, s.iteratorOpts),
	}

	return it
}

func (s *store) NewSnapshot() (driver.ISnapshot, error) {
	ldbSnapshot, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return &snapshot{
		db:  s,
		snp: ldbSnapshot,
	}, nil
}

func (s *store) Compact() error {
	return s.db.CompactRange(util.Range{
		Start: nil,
		Limit: nil,
	})
}
