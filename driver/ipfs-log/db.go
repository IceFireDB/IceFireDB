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
	StorageName = "ipfs-log"
)

var Dbname = "ifdb-event-kv-test"

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
	db.ctx = context.TODO()
	db.path = path
	var err error

	ctx := context.TODO()
	node, api, err := iflog.CreateNode(ctx, db.path)
	if err != nil {
		return nil, err
	}
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", node.PeerHost.ID().Pretty()))
	for _, a := range node.PeerHost.Addrs() {
		fmt.Println(a.Encapsulate(hostAddr).String())
	}
	fmt.Println("multi node communication identifier:", Dbname)

	logger := zap.NewNop()

	ev, err := iflog.NewIpfsLog(ctx, api, Dbname, &iflog.EventOptions{
		Directory: db.path,
		Logger:    logger,
	})

	if err != nil {
		return nil, err
	}

	if err := ev.AnnounceConnect(ctx, node); err != nil {
		return nil, err
	}

	db.db, err = levelkv.NewLevelKVDB(ctx, ev, logger)
	if err != nil {
		return nil, err
	}
	db.iteratorOpts = &opt.ReadOptions{}
	db.leveldb, err = leveldb.Open(storage.NewMemStorage(), nil)
	return db, err
}

func (s Store) Repair(path string, cfg *config.Config) error {
	return nil
}

type DB struct {
	ctx          context.Context
	path         string
	db           *levelkv.LevelKV
	iteratorOpts *opt.ReadOptions
	leveldb      *leveldb.DB
}

func (d *DB) GetLevelDB() *leveldb.DB {
	return d.leveldb
}

func (s *DB) GetStorageEngine() interface{} {
	return s.db
}

func (db *DB) Close() error {
	db.leveldb.Close()
	db.db.Close()
	return nil
}

func (db *DB) Put(key, value []byte) error {
	return db.db.Put(db.ctx, key, value)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.db.Get(key)
}

func (db *DB) Delete(key []byte) error {
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
		it: db.db.NewIterator(nil, db.iteratorOpts),
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
