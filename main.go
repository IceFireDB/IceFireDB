package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"sync/atomic"

	"github.com/joho/godotenv"

	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/ledisdb/ledisdb/store/driver"
	rafthub "github.com/tidwall/uhaha"

	_ "github.com/IceFireDB/IceFireDB/driver/badger"
	"github.com/IceFireDB/IceFireDB/driver/hybriddb"
	"github.com/IceFireDB/IceFireDB/driver/ipfs"
	ipfs_synckv "github.com/IceFireDB/IceFireDB/driver/ipfs-synckv"
)

var (
	// storageBackend select storage Engine
	storageBackend string
	// pprofAddr is the listen address for the optional pprof server.
	pprofAddr string
	// enablePprof gates the pprof profiling server (off by default).
	enablePprof bool
	// debug
	debug bool
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println("error loading environment variables file", err)
	}
}

func main() {
	conf.Name = "IceFireDB"
	conf.Version = "1.0.0-rc.1"
	conf.GitSHA = BuildVersion
	conf.Flag.Custom = true
	confInit(&conf)
	conf.DataDirReady = func(dir string) {
		cfg, l, d, err := openStorage(dir, storageBackend)
		if err != nil {
			log.Fatalf("failed to initialize storage backend %q: %v", storageBackend, err)
		}
		ldsCfg, le, ldb = cfg, l, d

		// Register backend-specific metrics with the INFO endpoint.
		switch storageBackend {
		case hybriddb.StorageName:
			serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*hybriddb.DB).Metrics)
		case ipfs.StorageName:
			serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*ipfs.DB).Metrics)
		case ipfs_synckv.StorageName:
			serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*ipfs_synckv.DB).Metrics)
		}
	}
	if enablePprof {
		// pprof for profiling — opt-in, loopback by default.
		go func() {
			log.Printf("pprof listening on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof server stopped: %v", err)
			}
		}()
	}
	conf.Snapshot = snapshot
	conf.Restore = restore
	conf.ConnOpened = connOpened
	conf.ConnClosed = connClosed
	//conf.CmdRewriteFunc = utils.RedisCmdRewrite

	fmt.Printf("start with Storage Engine: %s\n", storageBackend)
	rafthub.Main(conf)
}

// openStorage opens the ledis database for the given data dir and storage
// backend, returning the config and handles. It returns an error instead of
// aborting so callers (and tests) can decide how to handle failure.
func openStorage(dir, backend string) (*lediscfg.Config, *ledis.Ledis, *ledis.DB, error) {
	cfg := lediscfg.NewConfigDefault()
	cfg.DataDir = filepath.Join(dir, "main.db")
	cfg.Databases = 1
	cfg.DBName = backend

	l, err := ledis.Open(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	d, err := l.Select(0)
	if err != nil {
		l.Close()
		return nil, nil, nil, err
	}
	return cfg, l, d, nil
}

// storageDriver returns the low-level storage driver backing the active ledis
// database. Used by Raft snapshot/restore so the logic is backend-agnostic.
func storageDriver() driver.IDB {
	return ldb.GetSDB().GetDriver()
}

type snap struct {
	s driver.ISnapshot
}

func (s *snap) Done(path string) { s.s.Close() }

func (s *snap) Persist(wr io.Writer) error {
	return writeSnapshot(s.s, wr)
}

func snapshot(data interface{}) (rafthub.Snapshot, error) {
	s, err := storageDriver().NewSnapshot()
	if err != nil {
		return nil, err
	}
	return &snap{s: s}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	return nil, restoreSnapshot(storageDriver(), rd)
}

func connOpened(addr string) (context interface{}, accept bool) {
	atomic.AddInt64(&respClientNum, 1)
	return nil, true
}

func connClosed(context interface{}, addr string) {
	atomic.AddInt64(&respClientNum, -1)
	return
}
