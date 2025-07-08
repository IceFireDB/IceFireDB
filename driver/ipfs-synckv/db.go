package ipfs_synckv

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"sync"

	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	"github.com/IceFireDB/icefiredb-ipfs-log/stores/levelkv"
	"github.com/dgraph-io/ristretto"
	shell "github.com/ipfs/go-ipfs-api"
	"github.com/ipfs/kubo/core"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
)

const (
	StorageName                      = "ipfs-synckv"
	MB                         int64 = 1024 * 1024
	defaultHotCacheSize        int64 = 1024 // unit:MB 1G
	defaultHotCacheNumCounters int64 = 1e7  // unit:byte 10m
	defaultFilterBits          int   = 10
)

type Config struct {
	HotCacheSize       int64
	EndPointConnection string
	EncryptionKey      []byte // For AES-256, this should be a 32-byte key
}

var IpfsDefaultConfig = Config{
	HotCacheSize:       defaultHotCacheSize,
	EndPointConnection: "http://localhost:5001",
	EncryptionKey:      nil, // Encryption is off by default
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
	db.versions = make(map[string]uint64)

	// Set encryption key if provided
	if len(IpfsDefaultConfig.EncryptionKey) > 0 {
		db.encryptionKey = IpfsDefaultConfig.EncryptionKey
	}

	db.initOpts()

	var err error
	db.localDB, err = leveldb.OpenFile(db.path, db.opts)

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
		Cost: func(value interface{}) int64 {
			return int64(len(value.([]byte)))
		},
	})

	if err != nil {
		return nil, err
	}

	sh := shell.NewShell(IpfsDefaultConfig.EndPointConnection)
	db.remoteShell = sh

	// Initialize ipfs-log components
	db.ctx = context.Background()
	db.logger = zap.NewNop()

	// Create IPFS node and API
	node, api, err := iflog.CreateNode(db.ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPFS node: %w", err)
	}
	db.ipfsNode = node

	// Create ipfs-log event log with proper namespace
	ev, err := iflog.NewIpfsLog(db.ctx, api, "/ipfs/iflog-event/icefiredb", &iflog.EventOptions{
		Directory: path,
		Logger:    db.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create IPFS log: %w", err)
	}
	db.ipfsLog = ev

	// Connect to network and load existing data
	if err := ev.AnnounceConnect(db.ctx, db.ipfsNode); err != nil {
		return nil, fmt.Errorf("failed to connect to network: %w", err)
	}

	// Initialize LevelKVDB
	db.ipfsDB, err = levelkv.NewLevelKVDB(db.ctx, ev, db.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create LevelKVDB: %w", err)
	}

	// Load existing data from disk
	if err := ev.LoadDisk(db.ctx); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
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

	localDB *leveldb.DB
	opts    *opt.Options

	iteratorOpts *opt.ReadOptions
	syncOpts     *opt.WriteOptions

	cache         *ristretto.Cache
	filter        filter.Filter
	remoteShell   *shell.Shell
	encryptionKey []byte

	ctx      context.Context
	ipfsDB   *levelkv.LevelKV
	ipfsLog  *iflog.IpfsLog
	ipfsNode *core.IpfsNode // Stores the IPFS node returned by iflog.CreateNode()
	logger   *zap.Logger

	// For decentralized consistency
	peerID    peer.ID
	versionMu sync.RWMutex
	versions  map[string]uint64 // Key to version counter
}

func (s *DB) GetStorageEngine() interface{} {
	return s.localDB
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
	return db.localDB.Close()
}

func (db *DB) Put(key, value []byte) error {
	db.versionMu.Lock()
	defer db.versionMu.Unlock()

	// Increment version
	db.versions[string(key)]++
	version := db.versions[string(key)]
	versionBytes := []byte(fmt.Sprintf("%d", version))

	// Prepare data for IPFS (encrypt if key is available)
	ipfsValue := value
	if db.encryptionKey != nil {
		ipfsValue = encrypt(value, db.encryptionKey)
	}

	// --- Atomic Write (IPFS-first) ---
	// 1. Write to IPFS first. This is the source of truth for the network.
	ipfsErr := db.ipfsDB.Put(db.ctx, key, ipfsValue)
	if ipfsErr != nil {
		// If IPFS write fails, we abort the whole operation.
		return fmt.Errorf("ipfs write failed, aborting put: %w", ipfsErr)
	}

	// 2. Write to local DB and metadata only after IPFS success.
	var wg sync.WaitGroup
	var localErr, metaErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		localErr = db.localDB.Put(key, value, nil)
	}()
	go func() {
		defer wg.Done()
		// Store version in both local and ipfs for consistency checks
		metaKey := append([]byte("_meta:"), key...)
		metaErr = db.localDB.Put(metaKey, versionBytes, nil)
		_ = db.ipfsDB.Put(db.ctx, metaKey, versionBytes) // Also store version on IPFS
	}()
	wg.Wait()

	if localErr != nil || metaErr != nil {
		// This indicates a critical local failure. The data is on IPFS but not fully on local disk.
		// A more robust solution might involve a local recovery process.
		return fmt.Errorf("local write failed after successful ipfs write: localErr=%v, metaErr=%v", localErr, metaErr)
	}

	db.cache.Set(key, value, 0)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if v, ok := db.cache.Get(key); ok {
		return v.([]byte), nil
	}

	var wg sync.WaitGroup
	var ipfsValue, localValue, ipfsVersionBytes, localVersionBytes []byte
	var ipfsErr, localErr, ipfsVersionErr, localVersionErr error

	wg.Add(2)

	// Concurrently get from localDB and ipfsDB
	go func() {
		defer wg.Done()
		metaKey := append([]byte("_meta:"), key...)
		localValue, localErr = db.localDB.Get(key, nil)
		localVersionBytes, localVersionErr = db.localDB.Get(metaKey, nil)
	}()

	go func() {
		defer wg.Done()
		metaKey := append([]byte("_meta:"), key...)
		ipfsValue, ipfsErr = db.ipfsDB.Get(key)
		ipfsVersionBytes, ipfsVersionErr = db.ipfsDB.Get(metaKey)
	}()

	wg.Wait()

	// Decrypt IPFS value if needed
	if ipfsErr == nil && ipfsValue != nil && db.encryptionKey != nil {
		ipfsValue = decrypt(ipfsValue, db.encryptionKey)
	}

	// --- Conflict Resolution ---
	var value []byte
	localExists := localErr == nil && localValue != nil
	ipfsExists := ipfsErr == nil && ipfsValue != nil

	switch {
	case ipfsExists && localExists:
		// Both exist, compare versions correctly
		localVer, _ := strconv.ParseUint(string(localVersionBytes), 10, 64)
		ipfsVer, _ := strconv.ParseUint(string(ipfsVersionBytes), 10, 64)

		if ipfsVer > localVer {
			value = ipfsValue
			// Heal local store with the newer value
			_ = db.localDB.Put(key, ipfsValue, nil)
			_ = db.localDB.Put(append([]byte("_meta:"), key...), ipfsVersionBytes, nil)
		} else {
			value = localValue
		}

	case ipfsExists:
		value = ipfsValue
		// Heal local store
		_ = db.localDB.Put(key, ipfsValue, nil)
		_ = db.localDB.Put(append([]byte("_meta:"), key...), ipfsVersionBytes, nil)

	case localExists:
		value = localValue

	case localErr == leveldb.ErrNotFound:
		return nil, nil // Not found in either store

	default:
		return nil, fmt.Errorf("ipfs error: %v, local error: %v", ipfsErr, localErr)
	}

	if value != nil {
		db.cache.Set(key, value, 0)
	}
	return value, nil
}

func (db *DB) Delete(key []byte) error {
	var wg sync.WaitGroup
	var localErr, ipfsErr error
	wg.Add(2)

	go func() {
		defer wg.Done()
		localErr = db.localDB.Delete(key, nil)
		_ = db.localDB.Delete(append([]byte("_meta:"), key...), nil) // Delete meta
	}()

	go func() {
		defer wg.Done()
		ipfsErr = db.ipfsDB.Delete(db.ctx, key)
		_ = db.ipfsDB.Delete(db.ctx, append([]byte("_meta:"), key...)) // Delete meta
	}()

	wg.Wait()

	if localErr != nil || ipfsErr != nil {
		return fmt.Errorf("local error: %v, ipfs error: %v", localErr, ipfsErr)
	}
	db.cache.Del(key)
	return nil
}

func (db *DB) ConnectPeer(ctx context.Context, addr string) error {
	// Parse multiaddress
	maddr, err := ma.NewMultiaddr(addr)
	if err != nil {
		return fmt.Errorf("invalid multiaddress: %w", err)
	}

	// Get peer info from multiaddress
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("invalid peer address: %w", err)
	}

	// Connect to peer
	if err := db.ipfsNode.PeerHost.Connect(ctx, *peerInfo); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	// Tag peer to maintain connection
	db.ipfsNode.PeerHost.ConnManager().TagPeer(peerInfo.ID, "keep", 100)
	return nil
}

func (db *DB) ListPeers() []string {
	peers := db.ipfsNode.PeerHost.Peerstore().Peers()
	var peerAddrs []string
	for _, p := range peers {
		peerAddrs = append(peerAddrs, p.String())
	}
	return peerAddrs
}

func (db *DB) AnnounceConnect(ctx context.Context) error {
	return db.ipfsLog.AnnounceConnect(ctx, db.ipfsNode)
}

func (db *DB) LoadDisk(ctx context.Context) error {
	return db.ipfsLog.LoadDisk(ctx)
}

func (db *DB) GetMultiaddrs() []string {
	var addrs []string
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", db.ipfsNode.PeerHost.ID().String()))
	for _, a := range db.ipfsNode.PeerHost.Addrs() {
		addrs = append(addrs, a.Encapsulate(hostAddr).String())
	}
	return addrs
}

func (db *DB) SyncPut(key []byte, value []byte) error {
	err := db.localDB.Put(key, value, db.syncOpts)
	if err == nil {
		db.cache.Del(key)
	}
	return err
}

func (db *DB) SyncDelete(key []byte) error {
	err := db.localDB.Delete(key, db.syncOpts)
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
		it:     db.localDB.NewIterator(nil, db.iteratorOpts),
		ipfsDB: db.ipfsDB,
		ctx:    db.ctx,
	}

	return it
}

func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	snapshot, err := db.localDB.GetSnapshot()
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
	return db.localDB.CompactRange(util.Range{
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
