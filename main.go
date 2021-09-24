/*
 * @Author: gitsrc
 * @Date: 2021-03-08 13:09:44
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-20 10:50:01
 * @FilePath: /IceFireDB/main.go
 */

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/IceFireDB/kit/pkg/models"

	"github.com/gitsrc/IceFireDB/driver/badger"
	"github.com/gitsrc/IceFireDB/hybriddb"

	"github.com/gitsrc/IceFireDB/utils"
	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/ledisdb/ledisdb/store"
	"github.com/siddontang/go/snappy"
	"github.com/syndtr/goleveldb/leveldb"
	rafthub "github.com/tidwall/uhaha"
)

const defaultSlotNum = 128

var (
	// storageBackend select storage Engine
	storageBackend string
	// pprof listen
	pprofAddr string
	// debug
	debug bool
	// db (slot) number
	slotNum int = 128
	// coordinator type
	coordinatorType string
	// coordinator address
	coordinatorAddr string
	// self ip register to coordinator
	announceIP string
	// self port register to coordinator
	announcePort int

	app *App
)

func main() {
	var err error
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
		ldsCfg.Databases = slotNum

		var err error
		le, err = ledis.Open(ldsCfg)
		if err != nil {
			panic(err)
		}

		ldb, err = NewLedisDBs(le, slotNum)
		if err != nil {
			panic(err)
		}

		db0, err := ldb.GetDB(0)
		// Obtain the leveldb object and handle it carefully
		driver := db0.GetSDB().GetDriver().GetStorageEngine()
		switch v := driver.(type) {
		case *leveldb.DB:
		case *badger.DB:
		default:
			panic(fmt.Errorf("unsupported storage is caused: %T", v))
		}
		if storageBackend == hybriddb.StorageName {
			// serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*hybriddb.DB).Metrics)
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
	notifyCh := make(chan bool, 3)
	notifyCh <- false // set init state
	conf.NotifyCh = notifyCh
	go handleLeaderState(conf.Name, notifyCh)

	app, err = NewApp()
	if err != nil {
		panic(err)
	}

	fmt.Printf("start with Storage Engine: %s\n", storageBackend)
	rafthub.Main(conf)
}

func handleLeaderState(name string, c <-chan bool) {
	if coordinatorAddr == "" || coordinatorType == "" || announceIP == "" || announcePort == 0 {
		return
	}
	clinet, err := models.NewClient(coordinatorType, coordinatorAddr, "", time.Second*5)
	if err != nil {
		panic(err)
	}
	store := models.NewStore(clinet, name)
	server := &models.Server{
		ID:      -1,
		GroupId: -1,
		Addr:    fmt.Sprintf("%s:%d", announceIP, announcePort),
	}
	for {
		leader, ok := <-c
		if !ok {
			return
		}
		for {
			select {
			case l, ok := <-c:
				if !ok {
					break
				}
				leader = l
			default:
			}
			break
		}
		// register state to coordinator
		if leader {
			server.Type = models.ServerTypeLeader
		} else {
			server.Type = models.ServerTypeFollower
		}
		err = store.UpdateServer(server)
		if err != nil {
			// todo log
			fmt.Println("report leader state", err)
		}
	}
}

type snap struct {
	s        *store.Snapshot
	commitID uint64
}

func (s *snap) Done(path string) {
	if s.s == nil {
		return
	}
	s.s.Close()
}
func (s *snap) Persist(w io.Writer) (err error) {
	var snap = s.s
	wb := bufio.NewWriterSize(w, 4096)

	h := &ledis.DumpHead{s.commitID}

	if err = h.Write(wb); err != nil {
		return err
	}

	it := snap.NewIterator()
	defer it.Close()
	it.SeekToFirst()

	compressBuf := make([]byte, 4096)

	var key []byte
	var value []byte
	for ; it.Valid(); it.Next() {
		key = it.RawKey()
		value = it.RawValue()

		if key, err = snappy.Encode(compressBuf, key); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint16(len(key))); err != nil {
			return err
		}

		if _, err = wb.Write(key); err != nil {
			return err
		}

		if value, err = snappy.Encode(compressBuf, value); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}

		if _, err = wb.Write(value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

func snapshot(data interface{}) (rafthub.Snapshot, error) {
	s, commitID, err := ldb.le.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &snap{s: s, commitID: commitID}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	return ldb.le.LoadDump(rd)
}

func connOpened(addr string) (context interface{}, accept bool) {
	atomic.AddInt64(&respClientNum, 1)
	return nil, true
}

func connClosed(context interface{}, addr string) {
	atomic.AddInt64(&respClientNum, -1)
	return
}
