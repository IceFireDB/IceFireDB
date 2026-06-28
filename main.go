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
	// pprof listen
	pprofAddr string
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
	conf.Version = "1.0.1"
	conf.GitSHA = BuildVersion
	conf.Flag.Custom = true
	confInit(&conf)
	conf.DataDirReady = func(dir string) {
		ldsCfg = lediscfg.NewConfigDefault()
		ldsCfg.DataDir = filepath.Join(dir, "main.db")
		ldsCfg.Databases = 1
		ldsCfg.DBName = storageBackend

		var err error
		le, err = ledis.Open(ldsCfg)
		if err != nil {
			log.Printf("failed to open ledis database: %v", err)
			return
		}

		ldb, err = le.Select(0)
		if err != nil {
			log.Printf("failed to select ledis database: %v", err)
			return
		}

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
	if debug {
		// pprof for profiling
		go func() {
			http.ListenAndServe(pprofAddr, nil)
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
