/*
 * @Author: gitsrc
 * @Date: 2021-03-08 13:09:44
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-20 10:50:01
 * @FilePath: /IceFireDB/main.go
 */

package main

import (
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/gitsrc/IceFireDB/hybriddb"

	"github.com/gitsrc/IceFireDB/utils"
	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/sds"
	rafthub "github.com/tidwall/uhaha"
)

var (
	// storageBackend select storage Engine
	storageBackend string
	// pprof listen
	pprofAddr string
	// debug
	debug bool
)

func main() {
	conf.Name = "IceFireDB"
	conf.Version = "1.0.0"
	conf.GitSHA = BuildVersion
	conf.Flag.Custom = true
	confInit(&conf)
	conf.DataDirReady = func(dir string) {
		os.RemoveAll(filepath.Join(dir, "main.db"))

		ldsCfg = lediscfg.NewConfigDefault()
		ldsCfg.DataDir = filepath.Join(dir, "main.db")
		ldsCfg.Databases = 1
		ldsCfg.DBName = storageBackend

		var err error
		le, err = ledis.Open(ldsCfg)
		if err != nil {
			panic(err)
		}

		ldb, err = le.Select(0)
		if err != nil {
			panic(err)
		}

		// Obtain the leveldb object and handle it carefully
		driver := ldb.GetSDB().GetDriver().GetStorageEngine()
		var ok bool
		if db, ok = driver.(*leveldb.DB); !ok {
			panic("unsupported storage is caused")
		}
		if storageBackend == hybriddb.StorageName {
			serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*hybriddb.DB).Metrics)
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
	conf.CmdRewriteFunc = utils.RedisCmdRewrite
	rafthub.Main(conf)
}

type snap struct {
	s *leveldb.Snapshot
}

func (s *snap) Done(path string) {}
func (s *snap) Persist(wr io.Writer) error {
	sw := sds.NewWriter(wr)
	iter := s.s.NewIterator(nil, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := sw.WriteBytes(iter.Key()); err != nil {
			return err
		}
		if err := sw.WriteBytes(iter.Value()); err != nil {
			return err
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}
	return sw.Flush()
}

func snapshot(data interface{}) (rafthub.Snapshot, error) {
	s, err := db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &snap{s: s}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	sr := sds.NewReader(rd)
	var batch leveldb.Batch
	for {
		key, err := sr.ReadBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		value, err := sr.ReadBytes()
		if err != nil {
			return nil, err
		}
		batch.Put(key, value)
		if batch.Len() == 1000 {
			if err := db.Write(&batch, nil); err != nil {
				return nil, err
			}
			batch.Reset()
		}
	}
	if err := db.Write(&batch, nil); err != nil {
		return nil, err
	}
	return nil, nil
}

func connOpened(addr string) (context interface{}, accept bool) {
	atomic.AddInt64(&respClientNum, 1)
	return nil, true
}

func connClosed(context interface{}, addr string) {
	atomic.AddInt64(&respClientNum, -1)
	return
}
