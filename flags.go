package main

import (
	"flag"
	"fmt"
	"github.com/IceFireDB/IceFireDB/driver/log"
	"os"
	"strconv"
	"strings"

	"github.com/IceFireDB/IceFireDB/driver/crdt"

	rafthub "github.com/tidwall/uhaha"

	"github.com/IceFireDB/IceFireDB/driver/hybriddb"
	"github.com/IceFireDB/IceFireDB/driver/ipfs"

	//"github.com/IceFireDB/IceFireDB/driver/orbitdb"
	"github.com/IceFireDB/IceFireDB/driver/oss"
)

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
  --raft-backend   : Raft storage backend. 
  --storage-backend : Storage backend.
  --ipfs-endpoint	: ipfs endpoint connect . 
  --pubsub-id		: orbitdb pub sub .
  --oss-endpoint	: aws oss endpoint connect . 
  --oss-ak			: aws oss access key.
  --oss-sk			: aws oss secret key
  --log-dbname		: log driver db name, multi node communication identifier

P2P options:
  --servicename    : Service Discovery Identification
  --nettopic       : Node discovery channel
  --datatopic      : Pubsub data synchronization channel
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
		//if w == os.Stdout {
		//	os.Exit(0)
		//}
		//fmt.Println(s)
	}
	var raftBackend string
	var testNode string
	flag.StringVar(&conf.Addr, "a", conf.Addr, "")
	flag.StringVar(&conf.NodeID, "n", conf.NodeID, "")
	flag.StringVar(&conf.DataDir, "d", conf.DataDir, "")
	flag.StringVar(&conf.JoinAddr, "j", conf.JoinAddr, "")
	flag.StringVar(&conf.LogLevel, "l", conf.LogLevel, "")
	flag.StringVar(&raftBackend, "raft-backend", "leveldb", "")
	flag.StringVar(&conf.TLSCertPath, "tls-cert", conf.TLSCertPath, "")
	flag.StringVar(&conf.TLSKeyPath, "tls-key", conf.TLSKeyPath, "")
	flag.BoolVar(&conf.NoSync, "nosync", conf.NoSync, "")
	flag.BoolVar(&conf.OpenReads, "openreads", conf.OpenReads, "")
	flag.StringVar(&conf.BackupPath, "restore", conf.BackupPath, "")
	flag.BoolVar(&conf.LocalTime, "localtime", conf.LocalTime, "")
	flag.StringVar(&conf.Auth, "auth", conf.Auth, "")
	flag.StringVar(&conf.Advertise, "advertise", conf.Advertise, "")
	flag.StringVar(&testNode, "t", "", "")

	flag.StringVar(&ipfs.IpfsDefaultConfig.EndPointConnection, "ipfs-endpoint", "", "")
	flag.StringVar(&oss.OssDefaultConfig.EndPointConnection, "oss-endpoint", "", "")
	flag.StringVar(&oss.OssDefaultConfig.AccessKey, "oss-ak", "", "")
	flag.StringVar(&oss.OssDefaultConfig.Secretkey, "oss-sk", "", "")
	//flag.StringVar(&orbitdb.OrbitdbDefaultConfig.Pubsubid, "pubsub-id", "", "")

	flag.BoolVar(&conf.TryErrors, "try-errors", conf.TryErrors, "")
	flag.BoolVar(&conf.InitRunQuit, "init-run-quit", conf.InitRunQuit, "")
	flag.Int64Var(&hybriddb.DefaultConfig.HotCacheSize, "hot-cache-size", hybriddb.DefaultConfig.HotCacheSize, "")
	flag.StringVar(&storageBackend, "storage-backend", "goleveldb", "")
	flag.StringVar(&pprofAddr, "pprof-addr", ":26063", "")
	flag.BoolVar(&debug, "debug", false, "")
	// p2p
	flag.StringVar(&crdt.DefaultConfig.ServiceName, "servicename", crdt.DefaultConfig.ServiceName, "")
	flag.StringVar(&crdt.DefaultConfig.DataSyncChannel, "datatopic", crdt.DefaultConfig.DataSyncChannel, "")
	flag.StringVar(&crdt.DefaultConfig.NetDiscoveryChannel, "nettopic", crdt.DefaultConfig.NetDiscoveryChannel, "")
	// log driver
	flag.StringVar(&log.Dbname, "log-dbname", log.Dbname, "")
	flag.Parse()

	switch raftBackend {
	case "leveldb":
		conf.Backend = rafthub.LevelDB
	case "bolt":
		conf.Backend = rafthub.Bolt
	default:
		_, _ = fmt.Fprintf(os.Stderr, "invalid --backend: '%s'\n", raftBackend)
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
