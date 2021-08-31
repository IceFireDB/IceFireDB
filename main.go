/*
 * @Author: gitsrc
 * @Date: 2021-03-08 13:09:44
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-20 10:50:01
 * @FilePath: /IceFireDB/main.go
 */

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gitsrc/IceFireDB/hybriddb"

	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/sds"
	rafthub "github.com/tidwall/uhaha"
)

// select storage Engine
var DBName string

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
		ldsCfg.DBName = DBName

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
		if DBName == hybriddb.DBName {
			serverInfo.RegisterExtInfo(ldb.GetSDB().GetDriver().(*hybriddb.DB).Metrics)
		}
	}
	go func() {
		http.ListenAndServe(":26063", nil)
	}()
	conf.Snapshot = snapshot
	conf.Restore = restore
	conf.ConnOpened = connOpened
	conf.ConnClosed = connClosed
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

const usage = `{{NAME}} version: {{VERSION}} ({{GITSHA}})

Usage: {{NAME}} [-n id] [-a addr] [options]

Basic options:
  -h               : display help, this screen
  -a addr          : bind to address  (default: 127.0.0.1:11001)
  -n id            : node ID  (default: 1)
  -d dir           : data directory  (default: data)
  -j addr          : leader address of a cluster to join
  -l level         : log level  (default: info) [debug,verb,info,warn,silent]

Security options:
  --tls-cert path  : path to TLS certificate
  --tls-key path   : path to TLS private key
  --auth auth      : cluster authorization, shared by all servers and clients

Networking options: 
  --advertise addr : advertise address  (default: network bound address)

Store options: 
  --hot-cache-size int : memory cache capacity，unit:MB (default 1024)
  --db-name string     : select a db to use, it will overwrite the config's db name

Advanced options:
  --nosync         : turn off syncing data to disk after every write. This leads
                     to faster write operations but opens up the chance for data
                     loss due to catastrophic events such as power failure.
  --openreads      : allow followers to process read commands, but with the 
                     possibility of returning stale data.
  --localtime      : have the raft machine time synchronized with the local
                     server rather than the public internet. This will run the 
                     risk of time shifts when the local server time is
                     drastically changed during live operation. 
  --restore path   : restore a raft machine from a snapshot file. This will
                     start a brand new single-node cluster using the snapshot as
                     initial data. The other nodes must be re-joined. This
                     operation is ignored when a data directory already exists.
                     Cannot be used with -j flag.
  --init-run-quit  : initialize a bootstrap operation and then quit.
`

func confInit(conf *rafthub.Config) {
	flag.Usage = func() {
		w := os.Stderr
		for _, arg := range os.Args {
			if arg == "-h" || arg == "--help" {
				w = os.Stdout
				break
			}
		}
		s := usage
		s = strings.ReplaceAll(s, "{{VERSION}}", conf.Version)
		if conf.GitSHA == "" {
			s = strings.ReplaceAll(s, " ({{GITSHA}})", "")
			s = strings.ReplaceAll(s, "{{GITSHA}}", "")
		} else {
			s = strings.ReplaceAll(s, "{{GITSHA}}", conf.GitSHA)
		}
		s = strings.ReplaceAll(s, "{{NAME}}", conf.Name)
		if conf.Flag.Usage != nil {
			s = conf.Flag.Usage(s)
		}
		s = strings.ReplaceAll(s, "{{USAGE}}", "")
		w.Write([]byte(s))
		if w == os.Stdout {
			os.Exit(0)
		}
	}
	var backend string
	var testNode string
	flag.StringVar(&conf.Addr, "a", conf.Addr, "")
	flag.StringVar(&conf.NodeID, "n", conf.NodeID, "")
	flag.StringVar(&conf.DataDir, "d", conf.DataDir, "")
	flag.StringVar(&conf.JoinAddr, "j", conf.JoinAddr, "")
	flag.StringVar(&conf.LogLevel, "l", conf.LogLevel, "")
	flag.StringVar(&backend, "backend", "leveldb", "")
	flag.StringVar(&conf.TLSCertPath, "tls-cert", conf.TLSCertPath, "")
	flag.StringVar(&conf.TLSKeyPath, "tls-key", conf.TLSKeyPath, "")
	flag.BoolVar(&conf.NoSync, "nosync", conf.NoSync, "")
	flag.BoolVar(&conf.OpenReads, "openreads", conf.OpenReads, "")
	flag.StringVar(&conf.BackupPath, "restore", conf.BackupPath, "")
	flag.BoolVar(&conf.LocalTime, "localtime", conf.LocalTime, "")
	flag.StringVar(&conf.Auth, "auth", conf.Auth, "")
	flag.StringVar(&conf.Advertise, "advertise", conf.Advertise, "")
	flag.StringVar(&testNode, "t", "", "")
	flag.BoolVar(&conf.TryErrors, "try-errors", conf.TryErrors, "")
	flag.BoolVar(&conf.InitRunQuit, "init-run-quit", conf.InitRunQuit, "")
	flag.Int64Var(&hybriddb.DefaultConfig.HotCacheSize, "hot-cache-size", hybriddb.DefaultConfig.HotCacheSize, "")
	flag.StringVar(&DBName, "db-name", hybriddb.DBName, "")
	flag.Parse()

	switch backend {
	case "leveldb":
		conf.Backend = rafthub.LevelDB
	case "bolt":
		conf.Backend = rafthub.Bolt
	default:
		_, _ = fmt.Fprintf(os.Stderr, "invalid --backend: '%s'\n", backend)
	}
	switch testNode {
	case "1", "2", "3", "4", "5", "6", "7", "8", "9":
		if conf.Addr == "" {
			conf.Addr = ":1100" + testNode
		} else {
			conf.Addr = conf.Addr[:len(conf.Addr)-1] + testNode
		}
		conf.NodeID = testNode
		if testNode != "1" {
			conf.JoinAddr = conf.Addr[:len(conf.Addr)-1] + "1"
		}
	case "":
	default:
		_, _ = fmt.Fprintf(os.Stderr, "invalid usage of test flag -t\n")
		os.Exit(1)
	}
	if conf.TLSCertPath != "" && conf.TLSKeyPath == "" {
		_, _ = fmt.Fprintf(os.Stderr,
			"flag --tls-key cannot be empty when --tls-cert is provided\n")
		os.Exit(1)
	} else if conf.TLSCertPath == "" && conf.TLSKeyPath != "" {
		_, _ = fmt.Fprintf(os.Stderr,
			"flag --tls-cert cannot be empty when --tls-key is provided\n")
		os.Exit(1)
	}
	if conf.Advertise != "" {
		colon := strings.IndexByte(conf.Advertise, ':')
		if colon == -1 {
			_, _ = fmt.Fprintf(os.Stderr, "flag --advertise is missing port number\n")
			os.Exit(1)
		}
		_, err := strconv.ParseUint(conf.Advertise[colon+1:], 10, 16)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "flat --advertise port number invalid\n")
			os.Exit(1)
		}
	}
}

func connOpened(addr string) (context interface{}, accept bool) {
	atomic.AddInt64(&respClientNum, 1)
	return nil, true
}

func connClosed(context interface{}, addr string) {
	atomic.AddInt64(&respClientNum, -1)
	return
}
