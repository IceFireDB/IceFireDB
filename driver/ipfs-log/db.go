package ipfs_log

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	iflog "github.com/IceFireDB/icefiredb-ipfs-log"
	"github.com/IceFireDB/icefiredb-ipfs-log/stores/levelkv"
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"go.uber.org/zap"
)

const (
	// StorageName is the name of the storage driver
	StorageName = "ipfs-log"
)

// init registers the Store implementation with the driver package
func init() {
	driver.Register(Store{})
}

// Store is the implementation of the storage driver
type Store struct{}

// String returns the name of the storage driver
func (s Store) String() string {
	return StorageName
}

// Dbname is the default name for the database
var Dbname = "ifdb-event-kv-ipfs-log"

// Open initializes and opens the database
func (s Store) Open(path string, cfg *config.Config) (driver.IDB, error) {
	// Check if the environment variable for Dbname is set
	DbnameENV := os.Getenv("IPFS_LOG_DBNAME")
	if DbnameENV != "" {
		Dbname = DbnameENV
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(path, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Initialize the DB struct
	db := &DB{
		ctx:  context.TODO(),
		path: path,
	}

	var err error
	ctx := context.TODO()
	// Create the IPFS node
	node, api, err := iflog.CreateNode(ctx, db.path)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPFS node: %w", err)
	}

	// Build host multiaddress
	hostAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", node.PeerHost.ID().Pretty()))
	if err != nil {
		return nil, fmt.Errorf("failed to create multiaddress: %w", err)
	}
	for _, a := range node.PeerHost.Addrs() {
		fmt.Println(a.Encapsulate(hostAddr).String())
	}
	fmt.Println("multi node communication identifier:", Dbname)

	// Initialize the logger
	logger := zap.NewNop()

	// Create the IPFS log
	ev, err := iflog.NewIpfsLog(ctx, api, Dbname, &iflog.EventOptions{
		Directory: db.path,
		Logger:    logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create IPFS log: %w", err)
	}

	// Announce and connect to the IPFS network
	if err := ev.AnnounceConnect(ctx, node); err != nil {
		return nil, fmt.Errorf("failed to announce and connect: %w", err)
	}

	// Initialize the LevelKVDB
	db.db, err = levelkv.NewLevelKVDB(ctx, ev, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create LevelKVDB: %w", err)
	}

	// Initialize the iterator options
	db.iteratorOpts = &opt.ReadOptions{}
	// Open the leveldb in memory storage
	db.leveldb, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	return db, nil
}

// Repair is a no-op for this storage driver
func (s Store) Repair(path string, cfg *config.Config) error {
	return nil
}

// DB represents the database instance
type DB struct {
	ctx          context.Context
	path         string
	db           *levelkv.LevelKV
	iteratorOpts *opt.ReadOptions
	leveldb      *leveldb.DB
}

// GetLevelDB returns the underlying leveldb instance
func (d *DB) GetLevelDB() *leveldb.DB {
	return d.leveldb
}

// GetStorageEngine returns the storage engine instance
func (s *DB) GetStorageEngine() interface{} {
	return s.db
}

// Close closes the database instances
func (db *DB) Close() error {
	// Close the leveldb instance
	if err := db.leveldb.Close(); err != nil {
		return fmt.Errorf("failed to close leveldb: %w", err)
	}
	// Close the LevelKVDB instance (assuming it doesn't return an error)
	db.db.Close()
	return nil
}

// Put inserts a key-value pair into the database
func (db *DB) Put(key, value []byte) error {
	return db.db.Put(db.ctx, key, value)
}

// Get retrieves a value by key from the database
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.db.Get(key)
}

// Delete removes a key-value pair from the database
func (db *DB) Delete(key []byte) error {
	return db.db.Delete(db.ctx, key)
}

// SyncPut is a synchronous version of Put
func (db *DB) SyncPut(key []byte, value []byte) error {
	return db.Put(key, value)
}

// SyncDelete is a synchronous version of Delete
func (db *DB) SyncDelete(key []byte) error {
	return db.Delete(key)
}

// NewWriteBatch creates a new write batch for batch operations
func (db *DB) NewWriteBatch() driver.IWriteBatch {
	return &WriteBatch{
		db: db,
	}
}

// NewIterator creates a new iterator for iterating over the database
func (db *DB) NewIterator() driver.IIterator {
	return &Iterator{
		it: db.db.NewIterator(nil, db.iteratorOpts),
	}
}

// NewSnapshot creates a new snapshot of the database
func (db *DB) NewSnapshot() (driver.ISnapshot, error) {
	return &Snapshot{
		db: db,
	}, nil
}

// Compact is a no-op for this storage driver
func (db *DB) Compact() error {
	return nil
}
