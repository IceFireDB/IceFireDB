package orbitdb
 
import (
	"context"
	"fmt"
	"github.com/ipfs/go-ipfs/core/node/libp2p"
	"io/fs"
	"os"

	"github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"

	orbitdb2 "berty.tech/go-orbit-db"
	oiface "berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/basestore"
	//"berty.tech/go-orbit-db/stores/operation"
	"github.com/dgraph-io/ristretto"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	//mock "github.com/ipfs/go-ipfs/core/mock"
	iface "github.com/ipfs/interface-go-ipfs-core"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/syndtr/goleveldb/leveldb/filter"
)

const (
	StorageName                      = "orbitdb"
	MB                         int64 = 1024 * 1024
	defaultHotCacheSize        int64 = 1024 // unit:MB 1G
	defaultHotCacheNumCounters int64 = 1e7  // unit:byte 10m
	defaultFilterBits          int   = 10
)

type Config struct {
	HotCacheSize       int64
	Pubsubid        string
}

var OrbitdbDefaultConfig = Config{
	HotCacheSize:       defaultHotCacheSize,
	Pubsubid:    "",
}

func init() {
	driver.Register(Store{})
}

type Store struct{}

func (s Store) String() string {
	return StorageName
}


func createCoreAPI( core *ipfsCore.IpfsNode) iface.CoreAPI{
        api, _ := coreapi.NewCoreAPI(core)
        return api
}
/*
func createMockNet(ctx context.Context) mocknet.Mocknet {
        return mocknet.New(ctx)
}
*/


func loadPlugins(repoPath string) (*loader.PluginLoader, error) {
	plugins, err := loader.NewPluginLoader(repoPath)
	if err != nil {
		return nil, fmt.Errorf("error loading plugins: %s", err)
	}

	if err := plugins.Initialize(); err != nil {
		return nil, fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return nil, fmt.Errorf("error initializing plugins: %s", err)
	}
	return plugins, nil
}





func createIPFSNode(ctx context.Context) (*ipfsCore.IpfsNode) {
/*
	r, err1 := fsrepo.Open("/root/.ipfs")
	if err1 != nil { 
		return nil
	}
*/
        core, _ := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
                Online: true,
              //  Repo: r,
                Host:   libp2p.DefaultHostOption, //mock.MockHostOption(m),
                ExtraOpts: map[string]bool{
                        "pubsub": true,
                },
        })
       
        return core
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
	
	ctx, cancel := context.WithCancel(context.Background())
	db.ctx = ctx
	db.cancelFunc = cancel
	

	loadPlugins("/root/.ipfs")
	node := createIPFSNode(ctx)

	db1IPFS := createCoreAPI( node)
	odb, _ := orbitdb2.NewOrbitDB(ctx, db1IPFS, &orbitdb2.NewOrbitDBOptions{Directory: &path,})

	var db1  oiface.KeyValueStore

	if  len(OrbitdbDefaultConfig.Pubsubid) !=0 {
		db1, _= odb.KeyValue(ctx, OrbitdbDefaultConfig.Pubsubid, nil)
	}else {
		db1, _= odb.KeyValue(ctx, "orbit-db-tests", nil)
	}	

	fmt.Println("or db address: " , db1.Address().String())
	db.kv_db = db1

	return db, nil
}

func (s Store) Repair(path string, cfg *config.Config) error {
	return nil
}

type DB struct {
	path string
	
	basestore.BaseStore
	
	cfg *config.LevelDBConfig
	
	db *leveldb.DB
	opts *opt.Options
	filter filter.Filter
	
	cache *ristretto.Cache
	iteratorOpts *opt.ReadOptions
	syncOpts *opt.WriteOptions

	ctx context.Context
	core * ipfsCore.IpfsNode
	kv_db   oiface.KeyValueStore	
	orbitdb  oiface.OrbitDB  

	cancelFunc  context.CancelFunc
}

func (s *DB) GetStorageEngine() interface{} {
	return s.db
	//return nil
}




func (db *DB) Close() error {
	db.core.Close() 
	db.kv_db.Drop() 
	db.orbitdb.Close() 
	db.cancelFunc()
	return  nil
}

func (db *DB) Put(key, value []byte) error {
	kk:=string(key)

	_, err := db.kv_db.Put(db.ctx, kk, value)
	err = db.db.Put(key, value, nil)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	v, err := db.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		kk:=string(key)
		value, err := db.kv_db.Get(db.ctx, kk)
		if err == nil{
			return nil, nil
		}
		v = value
	}
	db.cache.Set(key, v, 0)

	return v, nil
}

func (db *DB) Delete(key []byte) error {
	db.db.Delete(key, nil)
	kk:=string(key)
	var err error
	_, err = db.kv_db.Delete(db.ctx, kk)

	return  err
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	kk:=string(key)

	_, err := db.kv_db.Put(db.ctx, kk, value)

	return err
}

func (db *DB) SyncDelete(key []byte) error {
	kk:=string(key)
	var err error
	_, err = db.kv_db.Delete(db.ctx, kk)
	
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
