// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package uhaha

import (
	"bufio"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/gomodule/redigo/redis"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog/v2"
	"github.com/tidwall/rtime"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	raftleveldb "github.com/tidwall/raft-leveldb"
)

// Main entrypoint for the cluster node. This must be called once and only
// once, and as the last call in the Go main() function. There are no return
// values as all application operations, logging, and I/O will be forever
// transferred.
func Main(conf Config) {
	confInit(&conf)
	conf.AddService(redisService())

	hclogger, log := logInit(conf)
	tm := remoteTimeInit(conf, log)
	dir, data := dataDirInit(conf, log)
	m := machineInit(conf, dir, data, log)
	tlscfg := tlsInit(conf, log)
	svr, addr := serverInit(conf, tlscfg, log)
	trans := transportInit(conf, tlscfg, svr, hclogger, log)
	lstore, sstore := storeInit(conf, dir, log)
	snaps := snapshotInit(conf, dir, m, hclogger, log)
	ra := raftInit(conf, hclogger, m, lstore, sstore, snaps, trans, log)

	joinClusterIfNeeded(conf, ra, addr, tlscfg, log)
	startUserServices(conf, svr, m, ra, log)

	go runMaintainServers(ra)
	go runWriteApplier(conf, m, ra)
	go runLogLoadedPoller(conf, m, ra, tlscfg, log)
	go runTicker(conf, tm, m, ra, log)

	log.Fatal(svr.serve())
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

// Config is the configuration for managing the behavior of the application.
// This must be fill out prior and then passed to the uhaha.Main() function.
type Config struct {
	cmds      map[string]command // appended by AddCommand
	catchall  command            // set by AddCatchallCommand
	services  []serviceEntry     // appended by AddService
	jsonType  reflect.Type       // used by UseJSONSnapshots
	jsonSnaps bool               // used by UseJSONSnapshots

	// Name gives the server application a name. Default "uhaha-app"
	Name string

	// Version of the application. Default "0.0.0"
	Version string

	// GitSHA of the application.
	GitSHA string

	// Flag is used to manage the application startup flags.
	Flag struct {
		// Custom tells Main to not automatically parse the application startup
		// flags. When set it is up to the user to parse the os.Args manually
		// or with a different library.
		Custom bool
		// Usage is an optional function that allows for altering the usage
		// message.
		Usage func(usage string) string
		// PreParse is an optional function that allows for adding command line
		// flags before the user flags are parsed.
		PreParse func()
		// PostParse is an optional function that fires after user flags are
		// parsed.
		PostParse func()
	}

	// Snapshot fires when a snapshot
	Snapshot func(data interface{}) (Snapshot, error)

	// Restore returns a data object that is fully restored from the previous
	// snapshot using the input Reader. A restore operation on happens once,
	// if needed, at the start of the application.
	Restore func(rd io.Reader) (data interface{}, err error)

	// UseJSONSnapshots is a convienence field that tells the machine to use
	// JSON as the format for all snapshots and restores. This may be good for
	// small simple data models which have types that can be fully marshalled
	// into JSON, ie. all imporant data fields need to exportable (Capitalized).
	// For more complicated or specialized data, it's proabably best to assign
	// custom functions to the Config.Snapshot and Config.Restore fields.
	// It's invalid to set this field while also setting Snapshot and/or
	// Restore. Default false
	UseJSONSnapshots bool

	// Tick fires at regular intervals as specified by TickDelay. This function
	// can be used to make updates to the database.
	Tick func(m Machine)

	// DataDirReady is an optional callback function that fires containing the
	// path to the directory where all the logs and snapshots are stored.
	DataDirReady func(dir string)

	// LogReady is an optional callback function that fires when the logger has
	// been initialized. The logger is can be safely used concurrently.
	LogReady func(log Logger)

	// ServerReady is an optional callback function that fires when the server
	// socket is listening and is ready to accept incoming connections. The
	// network address, auth, and tls-config are provided to allow for
	// background connections to be made to self, if desired.
	ServerReady func(addr, auth string, tlscfg *tls.Config)

	// ConnOpened is an optional callback function that fires when a new
	// network connection was opened on this machine. You can accept or deny
	// the connection, and optionally provide a client-specific context that
	// stick around until the connection is closed with ConnClosed.
	ConnOpened func(addr string) (context interface{}, accept bool)

	// ConnClosed is an optional callback function that fires when a network
	// connection has been closed on this machine.
	ConnClosed func(context interface{}, addr string)

	// ResponseFilter is and options function used to filter every response
	// prior to send into a client connection.
	ResponseFilter ResponseFilter

	// StateChange is an optional callback function that fires when the raft
	// state has changed.
	StateChange func(state State)

	// LocalConnector is an optional callback function that returns a new
	// connector that allows for establishing "local" connections through
	// the Redis protocol. A local connection bypasses the network and
	// communicates directly with this server, though the same process.
	LocalConnector func(lconn LocalConnector)

	LocalTime   bool          // default false
	TickDelay   time.Duration // default 200ms
	BackupPath  string        // default ""
	InitialData interface{}   // default nil
	NodeID      string        // default "1"
	Addr        string        // default ":11001"
	DataDir     string        // default "data"
	LogOutput   io.Writer     // default os.Stderr
	LogLevel    string        // default "notice"
	JoinAddr    string        // default ""
	Backend     Backend       // default LevelDB
	NoSync      bool          // default false
	OpenReads   bool          // default false
	MaxPool     int           // default 8
	TLSCertPath string        // default ""
	TLSKeyPath  string        // default ""
	Auth        string        // default ""
	Advertise   string        // default ""
	TryErrors   bool          // default false (return TRY instead of MOVED)
	InitRunQuit bool          // default false
}

// State captures the state of a Raft node: Follower, Candidate, Leader,
// or Shutdown.
type State byte

const (
	// Follower is the initial state of a Raft node.
	Follower State = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	Shutdown
)

func (state State) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// The Backend database format used for storing Raft logs and meta data.
type Backend int

const (
	// LevelDB is an on-disk LSM (LSM log-structured merge-tree) database. This
	// format is optimized for fast sequential writes, which is ideal for most
	// Raft implementations. This is the default format used by Uhaha.
	LevelDB Backend = iota
	// Bolt is an on-disk single-file b+tree database. This format has been a
	// popular choice for Go-based Raft implementations for years.
	Bolt
)

func (conf *Config) def() {
	if conf.Addr == "" {
		conf.Addr = "127.0.0.1:11001"
	}
	if conf.Version == "" {
		conf.Version = "0.0.0"
	}
	if conf.Name == "" {
		conf.Name = "uhaha-app"
	}
	if conf.NodeID == "" {
		conf.NodeID = "1"
	}
	if conf.DataDir == "" {
		conf.DataDir = "data"
	}
	if conf.LogLevel == "" {
		conf.LogLevel = "info"
	}
	if conf.LogOutput == nil {
		conf.LogOutput = os.Stderr
	}
	if conf.TickDelay == 0 {
		conf.TickDelay = time.Millisecond * 200
	}
	if conf.MaxPool == 0 {
		conf.MaxPool = 8
	}
}

func confInit(conf *Config) {
	conf.def()
	if conf.Flag.Custom {
		return
	}
	flag.Usage = func() {
		w := os.Stderr
		for _, arg := range os.Args {
			if arg == "-h" || arg == "--help" {
				w = os.Stdout
				break
			}
		}
		s := usage
		s = strings.Replace(s, "{{VERSION}}", conf.Version, -1)
		if conf.GitSHA == "" {
			s = strings.Replace(s, " ({{GITSHA}})", "", -1)
			s = strings.Replace(s, "{{GITSHA}}", "", -1)
		} else {
			s = strings.Replace(s, "{{GITSHA}}", conf.GitSHA, -1)
		}
		s = strings.Replace(s, "{{NAME}}", conf.Name, -1)
		if conf.Flag.Usage != nil {
			s = conf.Flag.Usage(s)
		}
		s = strings.Replace(s, "{{USAGE}}", "", -1)
		w.Write([]byte(s))
		if w == os.Stdout {
			os.Exit(0)
		}
	}
	var backend string
	var testNode string
	// var vers bool
	// flag.BoolVar(&vers, "v", false, "")
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
	if conf.Flag.PreParse != nil {
		conf.Flag.PreParse()
	}
	flag.Parse()
	// if vers {
	// 	fmt.Printf("%s\n", versline(*conf))
	// 	os.Exit(0)
	// }
	switch backend {
	case "leveldb":
		conf.Backend = LevelDB
	case "bolt":
		conf.Backend = Bolt
	default:
		fmt.Fprintf(os.Stderr, "invalid --backend: '%s'\n", backend)
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
		fmt.Fprintf(os.Stderr, "invalid usage of test flag -t\n")
		os.Exit(1)
	}
	if conf.TLSCertPath != "" && conf.TLSKeyPath == "" {
		fmt.Fprintf(os.Stderr,
			"flag --tls-key cannot be empty when --tls-cert is provided\n")
		os.Exit(1)
	} else if conf.TLSCertPath == "" && conf.TLSKeyPath != "" {
		fmt.Fprintf(os.Stderr,
			"flag --tls-cert cannot be empty when --tls-key is provided\n")
		os.Exit(1)
	}
	if conf.Advertise != "" {
		colon := strings.IndexByte(conf.Advertise, ':')
		if colon == -1 {
			fmt.Fprintf(os.Stderr, "flag --advertise is missing port number\n")
			os.Exit(1)
		}
		_, err := strconv.ParseUint(conf.Advertise[colon+1:], 10, 16)
		if err != nil {
			fmt.Fprintf(os.Stderr, "flat --advertise port number invalid\n")
			os.Exit(1)
		}
	}
	if conf.Flag.PostParse != nil {
		conf.Flag.PostParse()
	}
	if conf.UseJSONSnapshots {
		if conf.Restore != nil || conf.Snapshot != nil {
			fmt.Fprintf(os.Stderr,
				"UseJSONSnapshots: Restore or Snapshot are set\n")
			os.Exit(1)
		}
		if conf.InitialData != nil {
			t := reflect.TypeOf(conf.InitialData)
			if t.Kind() != reflect.Ptr {
				fmt.Fprintf(os.Stderr,
					"UseJSONSnapshots: InitialData is not a pointer\n")
				os.Exit(1)
			}
			conf.jsonType = t.Elem()
		}
		conf.jsonSnaps = true
	}
}

func (conf *Config) addCommand(kind byte, name string,
	fn func(m Machine, args []string) (interface{}, error),
) {
	name = strings.ToLower(name)
	if conf.cmds == nil {
		conf.cmds = make(map[string]command)
	}
	conf.cmds[name] = command{kind, func(m Machine, ra *raftWrap,
		args []string) (interface{}, error) {
		return fn(m, args)
	}}
}

// AddCatchallCommand adds a intermediate command that will execute for any
// input that was not previously defined with AddIntermediateCommand,
// AddWriteCommand, or AddReadCommand.
func (conf *Config) AddCatchallCommand(
	fn func(m Machine, args []string) (interface{}, error),
) {
	conf.catchall = command{'s', func(m Machine, ra *raftWrap,
		args []string) (interface{}, error) {
		return fn(m, args)
	}}
}

// AddIntermediateCommand adds a command that is for peforming client and system
// specific operations. It *is not* intended for working with the machine data,
// and doing so will risk data corruption.
func (conf *Config) AddIntermediateCommand(name string,
	fn func(m Machine, args []string) (interface{}, error),
) {
	conf.addCommand('s', name, fn)
}

// AddReadCommand adds a command for reading machine data.
func (conf *Config) AddReadCommand(name string,
	fn func(m Machine, args []string) (interface{}, error),
) {
	conf.addCommand('r', name, fn)
}

// AddWriteCommand adds a command for reading or altering machine data.
func (conf *Config) AddWriteCommand(name string,
	fn func(m Machine, args []string) (interface{}, error),
) {
	conf.addCommand('w', name, fn)
}

// AddService adds a custom client network service, such as HTTP or gRPC.
// By default, a Redis compatible service is already included.
func (conf *Config) AddService(
	name string,
	sniff func(rd io.Reader) bool,
	acceptor func(s Service, ln net.Listener),
) {
	conf.services = append(conf.services, serviceEntry{name, sniff, acceptor})
}

type jsonSnapshotType struct{ jsdata []byte }

func (s *jsonSnapshotType) Done(path string) {}
func (s *jsonSnapshotType) Persist(wr io.Writer) error {
	_, err := wr.Write(s.jsdata)
	return err
}
func jsonSnapshot(data interface{}) (Snapshot, error) {
	if data == nil {
		return &jsonSnapshotType{}, nil
	}
	jsdata, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &jsonSnapshotType{jsdata: jsdata}, nil
}

func jsonRestore(rd io.Reader, typ reflect.Type) (interface{}, error) {
	jsdata, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, nil
	}
	data := reflect.New(typ).Interface()
	if err = json.Unmarshal(jsdata, data); err != nil {
		return nil, err
	}
	return data, err
}

func versline(conf Config) string {
	sha := ""
	if conf.GitSHA != "" {
		sha = " (" + conf.GitSHA + ")"
	}
	return fmt.Sprintf("%s version %s%s", conf.Name, conf.Version, sha)
}

func logInit(conf Config) (hclog.Logger, *redlog.Logger) {
	var log *redlog.Logger
	logLevel := conf.LogLevel
	wr := conf.LogOutput
	lopts := *redlog.DefaultOptions
	lopts.Filter =
		func(line string, tty bool) (msg string, app byte, level int) {
			line = stateChangeFilter(line, log)
			return redlog.HashicorpRaftFilter(line, tty)
		}
	lopts.App = 'S'
	switch logLevel {
	case "debug":
		lopts.Level = 0
	case "verbose", "verb":
		lopts.Level = 1
	case "notice", "info":
		lopts.Level = 2
	case "warning", "warn":
		lopts.Level = 3
	case "quiet", "silent":
		lopts.Level = 3
		wr = io.Discard
	default:
		fmt.Fprintf(os.Stderr, "invalid -loglevel: %s\n", logLevel)
		os.Exit(1)
	}
	log = redlog.New(wr, &lopts)
	hclopts := *hclog.DefaultOptions
	hclopts.Color = hclog.ColorOff
	hclopts.Output = log
	if conf.LogReady != nil {
		conf.LogReady(log)
	}
	log.Warningf("starting %s", versline(conf))
	return hclog.New(&hclopts), log
}

func stateChangeFilter(line string, log *redlog.Logger) string {
	if strings.Contains(line, "entering ") {
		if strings.Contains(line, "entering candidate state") {
			log.SetApp('C')
		} else if strings.Contains(line, "entering follower state") {
			log.SetApp('F')
		} else if strings.Contains(line, "entering leader state") {
			log.SetApp('L')
		}
	}
	return line
}

type restoreData struct {
	data  interface{}
	ts    int64
	seed  int64
	start int64
}

func dataDirInit(conf Config, log *redlog.Logger) (string, *restoreData) {
	var rdata *restoreData
	dir := filepath.Join(conf.DataDir, conf.Name, conf.NodeID)
	if conf.BackupPath != "" {
		_, err := os.Stat(dir)
		if err == nil {
			log.Warningf("backup restore ignored: "+
				"data directory already exists: path=%s", dir)
			return dir, nil
		}
		log.Printf("restoring backup: path=%s", conf.BackupPath)
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		rdata, err = dataDirRestoreBackup(conf, dir, log)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("recovery successful")
	} else {
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatal(err)
		}
	}
	if conf.DataDirReady != nil {
		conf.DataDirReady(dir)
	}
	return dir, rdata
}

func dataDirRestoreBackup(conf Config, dir string, log *redlog.Logger,
) (rdata *restoreData, err error) {
	rdata = new(restoreData)
	f, err := os.Open(conf.BackupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	rdata.start, rdata.ts, rdata.seed, err = readSnapHead(gr)
	if err != nil {
		return nil, err
	}
	if conf.Restore != nil {
		rdata.data, err = conf.Restore(gr)
		if err != nil {
			return nil, err
		}
	} else if conf.jsonSnaps {
		rdata.data, err = func(rd io.Reader) (data interface{}, err error) {
			return jsonRestore(rd, conf.jsonType)
		}(gr)
		if err != nil {
			return nil, err
		}
	} else {
		rdata.data = conf.InitialData
	}
	return rdata, nil
}

func storeInit(conf Config, dir string, log *redlog.Logger,
) (raft.LogStore, raft.StableStore) {
	switch conf.Backend {
	case Bolt:
		store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "store"))
		if err != nil {
			log.Fatalf("bolt store open: %s", err)
		}
		return store, store
	case LevelDB:
		dur := raftleveldb.High
		if conf.NoSync {
			dur = raftleveldb.Medium
		}
		store, err := raftleveldb.NewLevelDBStore(
			filepath.Join(dir, "store"), dur)
		if err != nil {
			log.Fatalf("leveldb store open: %s", err)
		}
		return store, store
	default:
		log.Fatalf("invalid backend")
	}
	return nil, nil
}

func snapshotInit(conf Config, dir string, m *machine, hclogger hclog.Logger,
	log *redlog.Logger,
) raft.SnapshotStore {
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 3, hclogger)
	if err != nil {
		log.Fatal(err)
	}
	m.snaps = snaps
	return snaps
}

func machineInit(conf Config, dir string, rdata *restoreData,
	log *redlog.Logger,
) *machine {
	m := new(machine)
	m.dir = dir
	m.vers = versline(conf)
	m.tickedSig = sync.NewCond(&m.mu)
	m.created = time.Now().UnixNano()
	m.wrC = make(chan *writeRequestFuture, 1024)
	m.tickDelay = conf.TickDelay
	m.openReads = conf.OpenReads
	if rdata != nil {
		m.data = rdata.data
		m.start = rdata.start
		m.seed = rdata.seed
		m.ts = rdata.ts
	} else {
		m.data = conf.InitialData
	}
	m.log = log
	m.connClosed = conf.ConnClosed
	m.connOpened = conf.ConnOpened
	m.snapshot = conf.Snapshot
	m.restore = conf.Restore
	m.jsonSnaps = conf.jsonSnaps
	m.jsonType = conf.jsonType
	m.tick = conf.Tick
	m.commands = map[string]command{
		"tick":    {'w', cmdTICK},
		"barrier": {'w', cmdBARRIER},
		"raft":    {'s', cmdRAFT},
		"cluster": {'s', cmdCLUSTER},
		"machine": {'r', cmdMACHINE},
		"version": {'s', cmdVERSION},
	}
	if conf.TryErrors {
		delete(m.commands, "cluster")
	}
	for k, v := range conf.cmds {
		if _, ok := m.commands[k]; !ok {
			m.commands[k] = v
		}
	}
	m.catchall = conf.catchall
	return m
}

type remoteTime struct {
	remote bool       // use remote
	mu     sync.Mutex // lock times
	rtime  time.Time  // remote time
	ltime  time.Time  // local time
	ctime  time.Time  // calcd time
}

func (rt *remoteTime) Now() time.Time {
	if !rt.remote {
		return time.Now()
	}
	rt.mu.Lock()
	ctime := rt.rtime.Add(time.Since(rt.ltime))
	if !ctime.After(rt.ctime) {
		// ensure time is monotonic and increasing
		ctime = rt.ctime.Add(1)
		rt.ctime = ctime
	}
	rt.mu.Unlock()
	return ctime
}

// remoteTimeInit initializes the remote time fetching services, and
// continueously runs it in the background to keep synchronized.
func remoteTimeInit(conf Config, log *redlog.Logger) *remoteTime {
	rt := new(remoteTime)
	if conf.LocalTime {
		log.Warning("using local time")
		return rt
	}
	var wg sync.WaitGroup
	var once int32
	wg.Add(1)
	go func() {
		for {
			tm := rtime.Now()
			if tm.IsZero() {
				time.Sleep(time.Second)
				continue
			}
			rt.mu.Lock()
			if tm.After(rt.rtime) {
				rt.ltime = time.Now()
				rt.rtime = tm
				log.Debugf("synchronized time: %s", rt.rtime)
				if atomic.LoadInt32(&once) == 0 {
					atomic.StoreInt32(&once, 1)
					wg.Done()
				}
			}
			rt.mu.Unlock()
			time.Sleep(time.Second * 30)
		}
	}()
	go func() {
		time.Sleep(time.Second * 2)
		if atomic.LoadInt32(&once) != 0 {
			return
		}
		for {
			log.Warning("synchronized time: waiting for internet connection")
			if atomic.LoadInt32(&once) != 0 {
				break
			}
			time.Sleep(time.Second * 5)
		}
	}()
	wg.Wait()
	log.Printf("synchronized time")
	return rt
}

func raftInit(conf Config, hclogger hclog.Logger, fsm raft.FSM,
	logStore raft.LogStore, stableStore raft.StableStore,
	snaps raft.SnapshotStore, trans raft.Transport, log *redlog.Logger,
) *raftWrap {
	rconf := raft.DefaultConfig()
	rconf.Logger = hclogger
	rconf.LocalID = raft.ServerID(conf.NodeID)
	ra, err := raft.NewRaft(rconf, fsm, logStore, stableStore, snaps, trans)
	if err != nil {
		log.Fatal(err)
	}
	if conf.StateChange != nil {
		// monitor the state changes.
		lstate := raft.Shutdown
		conf.StateChange(Shutdown)
		go func() {
			for {
				state := ra.State()
				if state != lstate {
					lstate = state
					switch state {
					case raft.Candidate:
						conf.StateChange(Candidate)
					case raft.Follower:
						conf.StateChange(Follower)
					case raft.Leader:
						conf.StateChange(Leader)
					case raft.Shutdown:
						conf.StateChange(Shutdown)
					}
				}
				time.Sleep(time.Second / 4)
			}
		}()
	}
	return &raftWrap{
		Raft:      ra,
		conf:      conf,
		advertise: conf.Advertise,
	}
}

// joinClusterIfNeeded attempts to make this server join a Raft cluster. If
// the server already belongs to a cluster or if the server is bootstrapping
// then this operation is ignored.
func joinClusterIfNeeded(conf Config, ra *raftWrap, addr net.Addr,
	tlscfg *tls.Config, log *redlog.Logger,
) {
	// Get the current Raft cluster configuration for determining whether this
	// server needs to bootstrap a new cluster, or join/re-join an existing
	// cluster.
	f := ra.GetConfiguration()
	if err := f.Error(); err != nil {
		log.Fatalf("could not get Raft configuration: %v", err)
	}
	var addrStr string
	if ra.advertise != "" {
		addrStr = conf.Advertise
	} else {
		addrStr = addr.String()
	}
	cfg := f.Configuration()
	servers := cfg.Servers
	if len(servers) == 0 {
		// Empty configuration. Either bootstrap or join an existing cluster.
		if conf.JoinAddr == "" {
			// No '-join' flag provided.
			// Bootstrap new cluster.
			log.Noticef("bootstrapping new cluster")

			var configuration raft.Configuration
			configuration.Servers = []raft.Server{
				{
					ID:      raft.ServerID(conf.NodeID),
					Address: raft.ServerAddress(addrStr),
				},
			}
			err := ra.BootstrapCluster(configuration).Error()
			if err != nil && err != raft.ErrCantBootstrap {
				log.Fatalf("bootstrap: %s", err)
			}
		} else {
			// Joining an existing cluster
			joinAddr := conf.JoinAddr
			log.Noticef("joining existing cluster at %v", joinAddr)
			err := func() error {
				for {
					conn, err := RedisDial(joinAddr, conf.Auth, tlscfg)
					if err != nil {
						return err
					}
					defer conn.Close()
					res, err := redis.String(conn.Do("raft", "server", "add",
						conf.NodeID, addrStr))
					if err != nil {
						if strings.HasPrefix(err.Error(), "MOVED ") {
							parts := strings.Split(err.Error(), " ")
							if len(parts) == 3 {
								joinAddr = parts[2]
								time.Sleep(time.Millisecond * 100)
								continue
							}
						}
						return err
					}
					if res != "1" {
						return fmt.Errorf("'1', got '%s'", res)
					}
					return nil
				}
			}()
			if err != nil {
				log.Fatalf("raft server add: %v", err)
			}
		}
	} else {
		if conf.JoinAddr != "" {
			log.Warningf("ignoring join request because server already " +
				"belongs to a cluster")
		}
		if ra.advertise != "" {
			// Check that the address is the same as before
			found := false
			same := true
			before := ra.advertise
			for _, s := range servers {
				if string(s.ID) == conf.NodeID {
					found = true
					if string(s.Address) != ra.advertise {
						same = false
						before = string(s.Address)
						break
					}
				}
			}
			if !found {
				log.Fatalf("advertise address changed but node not found\n")
			} else if !same {
				log.Fatalf("advertise address change from \"%s\" to \"%s\" ",
					before, ra.advertise)
			}
		}
	}
}

// RedisDial is a helper function that dials out to another Uhaha server with
// redis protocol and using the provded TLS config and Auth token. The TLS/Auth
// must be correct in order to establish a connection.
func RedisDial(addr, auth string, tlscfg *tls.Config) (redis.Conn, error) {
	var conn redis.Conn
	var err error
	if tlscfg != nil {
		conn, err = redis.Dial("tcp", addr,
			redis.DialUseTLS(true), redis.DialTLSConfig(tlscfg))
	} else {
		conn, err = redis.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	if auth != "" {
		res, err := redis.String(conn.Do("auth", auth))
		if err != nil {
			conn.Close()
			return nil, err
		}
		if res != "OK" {
			conn.Close()
			return nil, fmt.Errorf("'OK', got '%s'", res)
		}
	}
	return conn, nil
}

func startUserServices(conf Config, svr *splitServer, m *machine, ra *raftWrap,
	log *redlog.Logger,
) {
	// rearrange so that services with nil sniffers are last
	var nilServices []serviceEntry
	var services []serviceEntry
	for i := 0; i < len(conf.services); i++ {
		if conf.services[i].sniff == nil {
			nilServices = append(nilServices, conf.services[i])
		} else {
			services = append(services, conf.services[i])
		}
	}
	conf.services = append(services, nilServices...)
	for _, s := range conf.services {
		ln := svr.split(func(rd io.Reader) (n int, ok bool) {
			if s.sniff == nil {
				return 0, true
			}
			return 0, s.sniff(rd)
		})
		filt := conf.ResponseFilter
		go s.serve(newService(m, ra, conf.Auth, s.name, filt), ln)
	}
	if conf.InitRunQuit {
		log.Notice("init run quit")
		os.Exit(0)
	}
}

type serverExtra struct {
	reachable  bool   // server is reachable
	remoteAddr string // remote tcp address
	advertise  string // advertise address
	lastError  error  // last error, if any
}

type raftWrap struct {
	*raft.Raft
	conf      Config
	advertise string
	mu        sync.RWMutex
	extra     map[string]serverExtra
}

func (ra *raftWrap) getExtraForAddr(addr string) (extra serverExtra, ok bool) {
	if ra.advertise == "" {
		return extra, false
	}
	ra.mu.RLock()
	defer ra.mu.RUnlock()
	for eaddr, extra := range ra.extra {
		if eaddr == addr || extra.advertise == addr ||
			extra.remoteAddr == addr {
			return extra, true
		}
	}
	return extra, false
}

type serverEntry struct {
	id      string
	address string
	resolve string
	leader  bool
}

func (e *serverEntry) clusterID() string {
	src := sha1.Sum([]byte(e.id))
	return hex.EncodeToString(src[:])
}

func (e *serverEntry) host() string {
	idx := strings.LastIndexByte(e.address, ':')
	if idx == -1 {
		return ""
	}
	return e.address[:idx]
}

func (e *serverEntry) port() int {
	idx := strings.LastIndexByte(e.address, ':')
	if idx == -1 {
		return 0
	}
	port, _ := strconv.Atoi(e.address[idx+1:])
	return port
}

func (ra *raftWrap) getServerList() ([]serverEntry, error) {
	leader := string(ra.Leader())
	f := ra.GetConfiguration()
	err := f.Error()
	if err != nil {
		return nil, err
	}
	cfg := f.Configuration()
	var servers []serverEntry
	for _, s := range cfg.Servers {
		var entry serverEntry
		entry.id = string(s.ID)
		entry.address = string(s.Address)
		extra, ok := ra.getExtraForAddr(entry.address)
		if ok {
			entry.resolve = extra.remoteAddr
		} else {
			entry.resolve = entry.address
		}
		entry.leader = entry.resolve == leader || entry.address == leader
		servers = append(servers, entry)
	}
	return servers, nil
}

func runMaintainServers(ra *raftWrap) {
	if ra.advertise == "" {
		return
	}
	for {
		f := ra.GetConfiguration()
		if err := f.Error(); err != nil {
			time.Sleep(time.Second)
			continue
		}
		cfg := f.Configuration()
		var wg sync.WaitGroup
		wg.Add(len(cfg.Servers))
		for _, svr := range cfg.Servers {
			go func(addr string) {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", addr, time.Second*5)
				if err == nil {
					defer c.Close()
				}
				ra.mu.Lock()
				defer ra.mu.Unlock()
				if ra.extra == nil {
					ra.extra = make(map[string]serverExtra)
				}
				extra := ra.extra[addr]
				if err != nil {
					extra.reachable = false
					extra.lastError = err
				} else {
					extra.reachable = true
					extra.lastError = nil
					extra.remoteAddr = c.RemoteAddr().String()
					extra.advertise = addr
				}
				ra.extra[addr] = extra
			}(string(svr.Address))
		}
		wg.Wait()
		time.Sleep(time.Second)
	}
}

func getLeaderAdvertiseAddr(ra *raftWrap) string {
	leader := string(ra.Leader())
	if ra.advertise == "" {
		return leader
	}
	if leader == "" {
		return ""
	}
	extra, ok := ra.getExtraForAddr(leader)
	if !ok {
		return ""
	}
	return extra.advertise
}

func errRaftConvert(ra *raftWrap, err error) error {
	if ra.conf.TryErrors {
		if err == raft.ErrNotLeader {
			leader := getLeaderAdvertiseAddr(ra)
			if leader != "" {
				return fmt.Errorf("TRY %s", leader)
			}
		}
		return err
	}
	switch err {
	case raft.ErrNotLeader, raft.ErrLeadershipLost,
		raft.ErrLeadershipTransferInProgress:
		leader := getLeaderAdvertiseAddr(ra)
		if leader != "" {
			return fmt.Errorf("MOVED 0 %s", leader)
		}
		fallthrough
	case raft.ErrRaftShutdown, raft.ErrTransportShutdown:
		return fmt.Errorf("CLUSTERDOWN %s", err)
	}
	return err
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	return append(dst, buf[:n]...)
}

// runWriteApplier is a background routine that handles all write requests.
// It's job is to apply the request to the Raft log and returns the result to
// writeRequest.
func runWriteApplier(conf Config, m *machine, ra *raftWrap) {
	var maxReqs = 256 // TODO: make configurable
	for {
		// Gather up as many requests (up to 256) into a single list.
		var reqs []*writeRequestFuture
		r := <-m.wrC
		reqs = append(reqs, r)
		var done bool
		for !done {
			select {
			case r := <-m.wrC:
				reqs = append(reqs, r)
				done = len(reqs) == maxReqs
			default:
				done = true
			}
		}
		// Combined multiple requests the data to a single, snappy-encoded,
		// message using the following binary format:
		// (count, cmd...)
		//   - count: uvarint
		//   - cmd: (count, args...)
		//     - count: uvarint
		//     - arg: (count, byte...)
		//       - count: uvarint
		var data []byte
		data = appendUvarint(data, uint64(len(reqs)))
		for _, r := range reqs {
			data = appendUvarint(data, uint64(len(r.args)))
			for _, arg := range r.args {
				data = appendUvarint(data, uint64(len(arg)))
				data = append(data, arg...)
			}
		}
		data = snappy.Encode(nil, data)

		// Apply the data and read back the messages
		resps, err := func() ([]applyResp, error) {
			// THE ONLY APPLY CALL IN THE CODEBASE SO ENJOY IT
			f := ra.Apply(data, 0)
			err := f.Error()
			if err != nil {
				return nil, err
			}
			return f.Response().([]applyResp), nil
		}()
		if err != nil {
			for _, r := range reqs {
				r.err = errRaftConvert(ra, err)
				r.wg.Done()
			}
		} else {
			for i := range reqs {
				reqs[i].resp = resps[i].resp
				reqs[i].elap = resps[i].elap
				reqs[i].err = resps[i].err
				reqs[i].wg.Done()
			}
		}
	}
}

var errLeaderUnknown = errors.New("leader unknown")

func getClusterLastIndex(ra *raftWrap, tlscfg *tls.Config, auth string,
) (uint64, error) {
	if ra.State() == raft.Leader {
		return ra.LastIndex(), nil
	}
	addr := getLeaderAdvertiseAddr(ra)
	if addr == "" {
		return 0, errLeaderUnknown
	}
	conn, err := RedisDial(addr, auth, tlscfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	args, err := redis.Strings(conn.Do("raft", "info", "last_log_index"))
	if err != nil {
		return 0, err
	}
	if len(args) != 2 {
		return 0, errors.New("invalid response")
	}
	lastIndex, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}

// runLogLoadedPoller is a background routine that reports on raft log progress
// and also maintains the m.logLoaded atomic boolean for open read systems.
func runLogLoadedPoller(conf Config, m *machine, ra *raftWrap,
	tlscfg *tls.Config, log *redlog.Logger,
) {
	var loaded bool
	var lastPerc string
	lastPrint := time.Now()
	for {
		// load the last index from the cluster leader
		lastIndex, err := getClusterLastIndex(ra, tlscfg, conf.Auth)
		if err != nil {
			if err != errLeaderUnknown {
				log.Warningf("cluster_last_index: %v", err)
			} else {
				// This service is probably a candidate, flip the loaded
				// off to begin printing log progress.
				loaded = false
				atomic.StoreInt32(&m.logLoaded, 0)
			}
			time.Sleep(time.Second)
			continue
		}

		// update machine with the known leader last index and determine
		// the load progress and how many logs are remaining.
		m.mu.Lock()
		m.logRemain = lastIndex - m.appliedIndex
		if lastIndex == 0 {
			m.logPercent = 0
		} else {
			m.logPercent = float64(m.appliedIndex-m.firstIndex) /
				float64(lastIndex-m.firstIndex)
		}
		lpercent := m.logPercent
		remain := m.logRemain
		m.mu.Unlock()

		if !loaded {
			// Print progress status to console log
			perc := fmt.Sprintf("%.1f%%", lpercent*100)
			if remain < 5 {
				log.Printf("logs loaded: ready for commands")
				loaded = true
				atomic.StoreInt32(&m.logLoaded, 1)
			} else if perc != "0.0%" && perc != lastPerc {
				msg := fmt.Sprintf("logs progress: %.1f%%, remaining=%d",
					lpercent*100, remain)
				now := time.Now()
				if now.Sub(lastPrint) > time.Second*5 {
					log.Print(msg)
					lastPrint = now
				} else {
					log.Verb(msg)
				}
			}
			lastPerc = perc
		}
		time.Sleep(time.Second / 5)
	}
}

// runTicker is a background routine that keeps the raft machine time and
// random seed updated.
func runTicker(conf Config, rt *remoteTime, m *machine, ra *raftWrap,
	log *redlog.Logger,
) {
	rbuf := make([]byte, 4096)
	var rnb []byte
	for {
		start := time.Now()
		ts := rt.Now().UnixNano()
		if len(rnb) == 0 {
			n, err := rand.Read(rbuf[:])
			if err != nil || n != len(rbuf) {
				log.Panic(err)
			}
			rnb = rbuf[:]
		}
		seed := int64(binary.LittleEndian.Uint64(rnb))
		rnb = rnb[8:]
		req := new(writeRequestFuture)
		req.args = []string{
			"tick",
			strconv.FormatInt(ts, 10),
			strconv.FormatInt(seed, 10),
		}
		req.wg.Add(1)
		m.wrC <- req
		req.wg.Wait()
		m.mu.Lock()
		if req.err == nil {
			l := req.resp.(raft.Log)
			m.tickedIndex = l.Index
			m.tickedTerm = l.Term
		} else {
			m.tickedIndex = 0
			m.tickedTerm = 0
		}
		m.tickedSig.Broadcast()
		m.mu.Unlock()
		dur := time.Since(start)
		delay := m.tickDelay - dur
		if delay < 1 {
			delay = 1
		}
		time.Sleep(delay)
	}
}

const transportMarker = "8e35747e37d192d9a819021ba2a02909"

type transportStream struct {
	net.Listener
	auth   string
	tlscfg *tls.Config
}

func (s *transportStream) Dial(addr raft.ServerAddress, timeout time.Duration,
) (conn net.Conn, err error) {
	if timeout <= 0 {
		if s.tlscfg != nil {
			conn, err = tls.Dial("tcp", string(addr), s.tlscfg)
		} else {
			conn, err = net.Dial("tcp", string(addr))
		}
	} else {
		if s.tlscfg != nil {
			conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout},
				"tcp", string(addr), s.tlscfg)
		} else {
			conn, err = net.DialTimeout("tcp", string(addr), timeout)
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte(transportMarker)); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Write([]byte(s.auth)); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func transportInit(conf Config, tlscfg *tls.Config, svr *splitServer,
	hclogger hclog.Logger, log *redlog.Logger,
) raft.Transport {
	ln := svr.split(func(r io.Reader) (n int, ok bool) {
		rd := bufio.NewReader(r)
		for i := 0; i < len(transportMarker); i++ {
			b, err := rd.ReadByte()
			if err != nil || b != transportMarker[i] {
				return 0, false
			}
		}
		for i := 0; i < len(conf.Auth); i++ {
			b, err := rd.ReadByte()
			if err != nil || b != conf.Auth[i] {
				return 0, false
			}
		}
		return len(transportMarker) + len(conf.Auth), true
	})
	stream := new(transportStream)
	stream.Listener = ln
	stream.auth = conf.Auth
	stream.tlscfg = tlscfg
	return raft.NewNetworkTransport(stream, conf.MaxPool, 0, log)
}

func serverInit(conf Config, tlscfg *tls.Config, log *redlog.Logger,
) (*splitServer, net.Addr) {
	var ln net.Listener
	var err error
	if tlscfg != nil {
		ln, err = tls.Listen("tcp4", conf.Addr, tlscfg)
	} else {
		ln, err = net.Listen("tcp4", conf.Addr)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("server listening at %s", ln.Addr())
	if conf.Advertise != "" {
		log.Printf("server advertising as %s", conf.Advertise)
	}
	if conf.ServerReady != nil {
		conf.ServerReady(ln.Addr().String(), conf.Auth, tlscfg)
	}
	return newSplitServer(ln, log), ln.Addr()
}

func parseTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	tlscfg := &tls.Config{
		Certificates: []tls.Certificate{pair},
	}
	for _, cert := range pair.Certificate {
		pcert, err := x509.ParseCertificate(cert)
		if err != nil {
			return nil, err
		}
		if len(pcert.DNSNames) > 0 {
			tlscfg.ServerName = pcert.DNSNames[0]
			break
		}
	}
	return tlscfg, nil
}

func tlsInit(conf Config, log *redlog.Logger) *tls.Config {
	if conf.TLSCertPath == "" || conf.TLSKeyPath == "" {
		return nil
	}
	tlscfg, err := parseTLSConfig(conf.TLSCertPath, conf.TLSKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	return tlscfg
}

// splitServer split a sinle server socket/listener into multiple logical
// listeners. For our use case, there is one transport listener and one client
// listener sharing the same server socket.
type splitServer struct {
	ln       net.Listener
	log      *redlog.Logger
	matchers []*matcher
}

func newSplitServer(ln net.Listener, log *redlog.Logger) *splitServer {
	return &splitServer{ln: ln, log: log}
}

func (m *splitServer) serve() error {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			if m.log != nil {
				m.log.Error(err)
			}
			continue
		}
		conn := &conn{Conn: c, matching: true}
		var matched bool
		for _, ma := range m.matchers {
			conn.bufpos = 0
			if n, ok := ma.sniff(conn); ok {
				conn.buffer = conn.buffer[n:]
				conn.matching = false
				ma.ln.next <- conn
				matched = true
				break
			}
		}
		if !matched {
			c.Close()
		}
	}
}

func (m *splitServer) split(sniff func(r io.Reader) (n int, ok bool),
) net.Listener {
	ln := &listener{addr: m.ln.Addr(), next: make(chan net.Conn)}
	m.matchers = append(m.matchers, &matcher{sniff, ln})
	return ln
}

type matcher struct {
	sniff func(r io.Reader) (n int, matched bool)
	ln    *listener
}

type conn struct {
	net.Conn
	matching bool
	buffer   []byte
	bufpos   int
}

func (c *conn) Read(p []byte) (n int, err error) {
	if c.matching {
		// matching mode
		if c.bufpos == len(c.buffer) {
			// need more buffer
			packet := make([]byte, 4096)
			nn, err := c.Conn.Read(packet)
			if err != nil {
				return 0, err
			}
			if nn == 0 {
				return 0, nil
			}
			c.buffer = append(c.buffer, packet[:nn]...)
		}
		copy(p, c.buffer[c.bufpos:])
		if len(p) < len(c.buffer)-c.bufpos {
			n = len(p)
		} else {
			n = len(c.buffer) - c.bufpos
		}
		c.bufpos += n
		return n, nil
	}
	if len(c.buffer) > 0 {
		// normal mode but with a buffer
		copy(p, c.buffer)
		if len(p) < len(c.buffer) {
			n = len(p)
			c.buffer = c.buffer[len(p):]
			if len(c.buffer) == 0 {
				c.buffer = nil
			}
		} else {
			n = len(c.buffer)
			c.buffer = nil
		}
		return n, nil
	}
	// normal mode, no buffer
	return c.Conn.Read(p)
}

// listener is a split network listener
type listener struct {
	addr net.Addr
	next chan net.Conn
}

func (l *listener) Accept() (net.Conn, error) {
	return <-l.next, nil
}
func (l *listener) Addr() net.Addr {
	return l.addr
}
func (l *listener) Close() error {
	return errors.New("disabled")
}

func (m *machine) Snapshot() (raft.FSMSnapshot, error) {
	snapshot := m.snapshot
	if snapshot == nil {
		if m.jsonSnaps {
			snapshot = jsonSnapshot
		} else {
			return nil, errors.New("snapshots are disabled")
		}
	}
	usnap, err := snapshot(m.Data())
	if err != nil {
		return nil, err
	}
	snap := &fsmSnap{
		dir:   m.dir,
		snap:  usnap,
		seed:  m.seed,
		ts:    m.ts,
		start: m.start,
	}
	return snap, nil
}

func readSnapHead(r io.Reader) (start, ts, seed int64, err error) {
	var head [32]byte
	n, err := io.ReadFull(r, head[:])
	if err != nil {
		return 0, 0, 0, err
	}
	if n != 32 {
		return 0, 0, 0, errors.New("invalid read")
	}
	if string(head[:8]) != "SNAP0001" {
		return 0, 0, 0, errors.New("invalid snapshot signature")
	}
	start = int64(binary.LittleEndian.Uint64(head[8:]))
	ts = int64(binary.LittleEndian.Uint64(head[16:]))
	seed = int64(binary.LittleEndian.Uint64(head[24:]))
	return start, ts, seed, nil
}

func (m *machine) Restore(rc io.ReadCloser) error {
	restore := m.restore
	if restore == nil {
		if m.jsonSnaps {
			restore = func(rd io.Reader) (data interface{}, err error) {
				return jsonRestore(rd, m.jsonType)
			}
		} else {
			return errors.New("snapshot restoring is disabled")
		}
	}
	gr, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	start, ts, seed, err := readSnapHead(gr)
	if err != nil {
		return err
	}
	m.start = start
	m.ts = ts
	m.seed = seed
	m.data, err = restore(gr)
	return err
}

func (m *machine) Context() interface{} {
	return nil
}

// A Snapshot is an interface that allows for Raft snapshots to be taken.
type Snapshot interface {
	Persist(io.Writer) error
	Done(path string)
}

type fsmSnap struct {
	id    string
	dir   string
	snap  Snapshot
	ts    int64
	seed  int64
	start int64
}

func (s *fsmSnap) Persist(sink raft.SnapshotSink) error {
	s.id = sink.ID()
	gw := gzip.NewWriter(sink)
	var head [32]byte
	copy(head[:], "SNAP0001")
	binary.LittleEndian.PutUint64(head[8:], uint64(s.start))
	binary.LittleEndian.PutUint64(head[16:], uint64(s.ts))
	binary.LittleEndian.PutUint64(head[24:], uint64(s.seed))
	n, err := gw.Write(head[:])
	if err != nil {
		return err
	}
	if n != 32 {
		return errors.New("invalid write")
	}
	if err := s.snap.Persist(gw); err != nil {
		return err
	}
	return gw.Close()
}

func (s *fsmSnap) Release() {
	path := filepath.Join(s.dir, "snapshots", s.id, "state.bin")
	if _, err := readSnapInfo(s.id, path); err != nil {
		path = ""
	}
	s.snap.Done(path)
}

var errWrongNumArgsRaft = errors.New("wrong number of arguments, try RAFT HELP")
var errWrongNumArgsCluster = errors.New("wrong number of arguments, " +
	"try CLUSTER HELP")

func errUnknownRaftCommand(args []string) error {
	var cmd string
	for _, arg := range args {
		cmd += arg + " "
	}
	return fmt.Errorf("unknown raft command '%s', try RAFT HELP",
		strings.TrimSpace(cmd))
}

func errUnknownClusterCommand(args []string) error {
	var cmd string
	for _, arg := range args {
		cmd += arg + " "
	}
	return fmt.Errorf("unknown subcommand or wrong number of arguments for "+
		"'%s', try CLUSTER HELP",
		strings.TrimSpace(cmd))
}

// ErrSyntax is returned where there was a syntax error
var ErrSyntax = errors.New("syntax error")

// ErrNotLeader is returned when the raft leader is unknown
var ErrNotLeader = raft.ErrNotLeader

type command struct {
	kind byte // 's' system, 'r' read, 'w' write
	fn   func(m Machine, ra *raftWrap, args []string) (interface{}, error)
}

// The Machine interface is passed to every command. It includes the user data
// and various utilities that should be used from Write, Read, and Intermediate
// commands.
//
// It's important to note that the Data(), Now(), and Rand() functions can be
// used safely for Write and Read commands, but are not available for
// Intermediate commands. The Context() is ONLY available for Intermediate
// commands.
//
// A call to Rand() and Now() from inside of a Read command will always return
// back the same last known value of it's respective type. While, from a Write
// command, you'll get freshly generated values. This is to ensure that
// the every single command ALWAYS generates the same series of data on every
// server.
type Machine interface {
	// Data is the original user data interface that was assigned at startup.
	// It's safe to alter the data in this interface while inside a Write
	// command, but it's only safe to read the data from this interface for
	// Read commands.
	// For Intermediate commands, it's not safe to read or write data from
	// this interface and you should use at your own risk.
	Data() interface{}
	// Now generates a stable timestamp that is synced with internet time
	// and for Write commands is always monotonical increasing. It's made to
	// be a trusted source of time for performing operations on the user data.
	// Always use this function instead of the builtin time.Now().
	// Returns nil for Intermediate Commands.
	Now() time.Time
	// Rand is a random number generator that must be used instead of the
	// standard Go packages `crypto/rand` and `math/rand`. For Write commands
	// the values returned from this generator are crypto seeded, guaranteed
	// to be reproduced in exact order when the server restarts, and identical
	// across all machines in the cluster. The underlying implementation is
	// PCG. Check out http://www.pcg-random.org/ for more information.
	// Returns nil for Intermediate Commands.
	Rand() Rand
	// Utility logger for printing information to the local server log.
	Log() Logger
	// Context returns the connection context that was defined in from the
	// Config.ConnOpened callback.
	// Only available for Intermediate and Read commands.
	// Returns nil for Write Commands.
	Context() interface{}
}

type machine struct {
	snapshot   func(data interface{}) (Snapshot, error)
	restore    func(rd io.Reader) (data interface{}, err error)
	connOpened func(addr string) (context interface{}, accept bool)
	connClosed func(context interface{}, addr string)
	jsonSnaps  bool               //
	jsonType   reflect.Type       //
	snaps      raft.SnapshotStore //
	dir        string             //
	vers       string             // version line
	tick       func(m Machine)    //
	created    int64              // machine instance created timestamp
	commands   map[string]command // command table
	catchall   command            // catchall command
	log        Logger             // shared logger
	openReads  bool               // open reads on by default
	tickDelay  time.Duration      // ticker delay

	mu           sync.RWMutex // protect all things in group
	firstIndex   uint64       // first applied index
	appliedIndex uint64       // last applied index (stable state)
	readers      int32        // (atomic counter) number of current readers
	tickedIndex  uint64       // index of last tick
	tickedTerm   uint64       // term of last tick
	tickedSig    *sync.Cond   // signal when ticked
	logPercent   float64      // percentage of log loaded
	logRemain    uint64       // non-applied log entries
	logLoaded    int32        // (atomic bool) log is loaded, allow open reads
	snap         bool         // snapshot in progress
	start        int64        // !! PERSISTED !! first non-zero timestamp
	ts           int64        // !! PERSISTED !! current timestamp
	seed         int64        // !! PERSISTED !! current seed
	data         interface{}  // !! PERSISTED !! user data

	wrC chan *writeRequestFuture
}

var _ Machine = &machine{}

type applyResp struct {
	resp interface{}
	elap time.Duration
	err  error
}

func (m *machine) Apply(l *raft.Log) interface{} {
	packet, err := snappy.Decode(nil, l.Data)
	if err != nil {
		m.log.Panic(err)
	}
	m.mu.Lock()
	defer func() {
		m.appliedIndex = l.Index
		if m.firstIndex == 0 {
			m.firstIndex = m.appliedIndex
		}
		m.mu.Unlock()
	}()
	numReqs, n := binary.Uvarint(packet)
	if n <= 0 {
		m.log.Panic("invalid apply")
	}
	packet = packet[n:]
	resps := make([]applyResp, numReqs)
	for i := 0; i < int(numReqs); i++ {
		numArgs, n := binary.Uvarint(packet)
		if n <= 0 {
			m.log.Panic("invalid apply")
		}
		packet = packet[n:]
		args := make([]string, numArgs)
		for i := 0; i < len(args); i++ {
			argLen, n := binary.Uvarint(packet)
			if n <= 0 {
				m.log.Panic("invalid apply")
			}
			packet = packet[n:]
			args[i] = string(packet[:argLen])
			packet = packet[argLen:]
		}
		if len(args) == 0 {
			resps[i] = applyResp{nil, 0, nil}
		} else {
			cmdName := strings.ToLower(string(args[0]))
			cmd := m.commands[cmdName]
			if cmd.kind != 'w' {
				m.log.Panicf("invalid apply '%c', command: '%s'",
					cmd.kind, cmdName)
			}
			tick := cmdName == "tick"
			if m.start == 0 && !tick {
				// This is in fact the leader, but because the machine has yet
				// to receive a valid tick command, we'll treat this as if the
				// server *is not* the leader.
				resps[i] = applyResp{nil, 0, raft.ErrNotLeader}
			} else {
				start := time.Now()
				res, err := cmd.fn(m, nil, args)
				if tick {
					// return only the index and term
					res = raft.Log{Index: l.Index, Term: l.Term}
				}
				resps[i] = applyResp{res, time.Since(start), err}
			}
		}
	}
	return resps
}

func (m *machine) Data() interface{} {
	return m.data
}

func (m *machine) Log() Logger {
	return m.log
}

func (m *machine) Rand() Rand {
	return m
}

func (m *machine) Uint32() uint32 {
	seed := rincr(rincr(m.seed)) // twice called intentionally
	x := rgen(seed)
	if atomic.LoadInt32(&m.readers) == 0 {
		m.seed = seed
	}
	return x
}

func (m *machine) Uint64() uint64 {
	return (uint64(m.Uint32()) << 32) | uint64(m.Uint32())
}

const hexchars = "0123456789abcdef"

func (m *machine) UUID() string {
	var buf [36]byte
	m.Read(buf[:])
	for i := 0; i < len(buf); i++ {
		buf[i] = hexchars[buf[i]&15]
	}
	buf[8] = '-'
	buf[13] = '-'
	buf[14] = '4'
	buf[18] = '-'
	buf[23] = '-'
	return string(buf[:])
}

func (m *machine) Int() int {
	return int(m.Uint64() << 1 >> 1)
}

func (m *machine) Float64() float64 {
	return float64(m.Uint32()) / 4294967296.0
}

func (m *machine) Read(p []byte) (n int, err error) {
	seed := rincr(m.seed)
	for len(p) >= 4 {
		seed = rincr(seed)
		binary.LittleEndian.PutUint32(p, rgen(seed))
		p = p[4:]
	}
	if len(p) > 0 {
		var last [4]byte
		seed = rincr(seed)
		binary.LittleEndian.PutUint32(last[:], rgen(seed))
		for i := 0; i < len(p); i++ {
			p[i] = last[i]
		}
	}
	if atomic.LoadInt32(&m.readers) == 0 {
		m.seed = seed
	}
	return len(p), nil
}

// Rand is a random number interface used by Machine
type Rand interface {
	Int() int
	Uint64() uint64
	Uint32() uint32
	Float64() float64
	Read([]byte) (n int, err error)
	UUID() string
}

// #region -- pcg-family random number generator

func rincr(seed int64) int64 {
	return int64(uint64(seed)*6364136223846793005 + 1)
}

func rgen(seed int64) uint32 {
	state := uint64(seed)
	xorshifted := uint32(((state >> 18) ^ state) >> 27)
	rot := uint32(state >> 59)
	return (xorshifted >> rot) | (xorshifted << ((-rot) & 31))
}

// #endregion -- pcg-family random number generator

func (m *machine) Now() time.Time {
	ts := m.ts
	if atomic.LoadInt32(&m.readers) == 0 {
		m.ts++
	}
	return time.Unix(0, ts).UTC()
}

// contextMachine wraps the machine in a connection context.
type contextMachine struct {
	reader  bool
	context interface{}
	m       *machine
}

var _ Machine = contextMachine{}

func (m contextMachine) Now() time.Time {
	if m.reader {
		return m.m.Now()
	}
	return time.Time{}
}
func (m contextMachine) Rand() Rand {
	if m.reader {
		return m.m.Rand()
	}
	return nil
}
func (m contextMachine) Context() interface{} { return m.context }
func (m contextMachine) Log() Logger          { return m.m.Log() }
func (m contextMachine) Data() interface{}    { return m.m.Data() }

func getBaseMachine(m Machine) *machine {
	switch m := m.(type) {
	case contextMachine:
		return m.m
	case *machine:
		return m
	default:
		return nil
	}
}

// RawMachineInfo represents the raw components of the machine
type RawMachineInfo struct {
	TS   int64
	Seed int64
}

// ReadRawMachineInfo reads the raw machine components.
func ReadRawMachineInfo(m Machine, info *RawMachineInfo) {
	*info = RawMachineInfo{}
	if m := getBaseMachine(m); m != nil {
		info.TS = m.ts
		info.Seed = m.seed
	}
}

// WriteRawMachineInfo writes raw components to the machine. Use with care as
// this operation may destroy the consistency of your cluster.
func WriteRawMachineInfo(m Machine, info *RawMachineInfo) {
	if m := getBaseMachine(m); m != nil {
		m.ts = info.TS
		m.seed = info.Seed
	}
}

// BARRIER
// help: barrier is a noop that saves to the raft log. It can be used to
// ensure that the current server is the leader and that the cluster
// is working.
func cmdBARRIER(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrWrongNumArgs
	}
	return redcon.SimpleString("OK"), nil
}

// TICK timestamp-int64 random-int64
// help: updates the machine timestamp and random seed. It's not possible to
// directly call this from a client service. It can only be called by
// its own internal server instance.
func cmdTICK(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) != 3 {
		return nil, ErrWrongNumArgs
	}
	ts, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, err
	}
	if ts < 0 || ts <= m.ts {
		return nil, errors.New("timestamp is not monotonic")
	}
	seed, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, err
	}
	if seed == m.seed {
		return nil, errors.New("random number has not changed")
	}
	m.seed = seed
	m.ts = ts
	if m.start == 0 {
		m.start = m.ts
	}
	if m.tick != nil {
		// call the user defined tick function
		m.tick(m)
	}
	// Do not returns anything of value because it will be overwritten by the
	// Apply() function.
	return nil, nil
}

// CLUSTER subcommand args...
// help: calls a system-level cluster operation.
func cmdCLUSTER(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 2 {
		return nil, errWrongNumArgsCluster
	}
	args[1] = strings.ToLower(args[1])
	rcmd, ok := clusterCommands[args[1]]
	if !ok {
		return nil, errUnknownClusterCommand(args[:2])
	}
	return rcmd.fn(m, ra, args)
}

// RAFT subcommand args...
// help: calls a system-level raft operation.
func cmdRAFT(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 2 {
		return nil, errWrongNumArgsRaft
	}
	args[1] = strings.ToLower(args[1])
	rcmd, ok := raftCommands[args[1]]
	if !ok {
		return nil, errUnknownRaftCommand(args[:2])
	}
	return rcmd.fn(m, ra, args)
}

// RAFT LEADER
// help: returns the current leader; string
func cmdRAFTLEADER(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	return getLeaderAdvertiseAddr(ra), nil
}

// RAFT SERVER subcommand args...
func cmdRAFTSERVER(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 3 {
		return nil, errWrongNumArgsRaft
	}
	switch strings.ToLower(args[2]) {
	case "list":
		return cmdRAFTSERVERLIST(m, ra, args)
	case "add":
		return cmdRAFTSERVERADD(m, ra, args)
	case "remove":
		return cmdRAFTSERVERREMOVE(m, ra, args)
	default:
		return nil, fmt.Errorf("unknown raft command '%s', try RAFT HELP",
			args[1])
	}
}

// RAFT SERVER LIST
// help: returns a list of the servers in the cluster
func cmdRAFTSERVERLIST(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	servers, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var res [][]string
	for _, s := range servers {
		res = append(res, []string{
			"id", s.id,
			"address", s.address,
			"leader", fmt.Sprint(s.leader),
		})
	}
	return res, nil
}

// RAFT SERVER REMOVE id
// help: removes a server from the cluster; bool
func cmdRAFTSERVERREMOVE(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 4 {
		return nil, errWrongNumArgsRaft
	}
	f := ra.RemoveServer(raft.ServerID(string(args[3])), 0, 0)
	err := f.Error()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	return true, nil
}

// RAFT SERVER ADD id address
// help: Returns true if server added, or error; bool
func cmdRAFTSERVERADD(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 5 {
		return nil, errWrongNumArgsRaft
	}
	err := ra.AddVoter(raft.ServerID(args[3]), raft.ServerAddress(args[4]),
		0, 0).Error()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	return true, nil
}

// RAFT INFO [pattern]
// help: returns various raft related info; map[string]string
func cmdRAFTINFO(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	pattern := "*"
	switch len(args) {
	case 2:
	case 3:
		pattern = args[2]
	default:
		return nil, errWrongNumArgsRaft
	}
	if pattern == "state" {
		// Fast path to avoid locks. Under the hood there's only a single
		// atomic load
		return []string{"state", ra.State().String()}, nil
	}

	stats := ra.Stats()
	m.mu.RLock()
	behind := m.logRemain
	percent := m.logPercent
	m.mu.RUnlock()
	stats["logs_behind"] = fmt.Sprint(behind)
	stats["logs_loaded_percent"] = fmt.Sprintf("%0.1f", percent*100)
	final := make(map[string]string)
	for key, value := range stats {
		if match.Match(key, pattern) {
			final[key] = value
		}
	}
	return final, nil
}

// RAFT SNAPSHOT subcommand args...
func cmdRAFTSNAPSHOT(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 3 {
		return nil, errWrongNumArgsRaft
	}
	switch strings.ToLower(args[2]) {
	case "now":
		return cmdRAFTSNAPSHOTNOW(m, ra, args)
	case "list":
		return cmdRAFTSNAPSHOTLIST(m, ra, args)
	case "read":
		return cmdRAFTSNAPSHOTREAD(m, ra, args)
	case "file":
		return cmdRAFTSNAPSHOTFILE(m, ra, args)
	default:
		return nil, fmt.Errorf("unknown raft command '%s', try RAFT HELP",
			args[1])
	}
}

// RAFT SNAPSHOT NOW
// help: takes a snapshot of the data and returns information relating to the
// resulting snapshot; map[string]string
func cmdRAFTSNAPSHOTNOW(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	m.mu.Lock()
	if m.snap {
		m.mu.Unlock()
		return nil, errors.New("in progress")
	}
	m.snap = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.snap = false
		m.mu.Unlock()
	}()
	f := ra.Snapshot()
	err := f.Error()
	if err != nil {
		return nil, err
	}
	meta, rd, err := f.Open()
	if err != nil {
		return nil, err
	}
	if err := rd.Close(); err != nil {
		return nil, err
	}
	path := filepath.Join(m.dir, "snapshots", meta.ID, "state.bin")
	info, err := readSnapInfo(meta.ID, path)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// RAFT SNAPSHOT LIST
// help: returns a list of the current snapshots on disk. []map[string]string
func cmdRAFTSNAPSHOTLIST(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	list, err := m.snaps.List()
	if err != nil {
		return nil, err
	}
	var snaps []map[string]string
	for _, meta := range list {
		path := filepath.Join(m.dir, "snapshots", meta.ID, "state.bin")
		info, err := readSnapInfo(meta.ID, path)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, info)
	}
	return snaps, nil
}

// RAFT SNAPSHOT FILE id
// help: returns the path to the snapshot file; string
func cmdRAFTSNAPSHOTFILE(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 4 {
		return nil, errWrongNumArgsRaft
	}
	var err error
	path := filepath.Join(m.dir, "snapshots", args[3], "state.bin")
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}
	return path, nil
}

// RAFT SNAPSHOT READ id [RANGE offset limit]
// help: reads the contents of a snapshot file; []byte
func cmdRAFTSNAPSHOTREAD(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	var id string
	var offset, limit int64
	var allBytes bool
	switch len(args) {
	case 4:
		allBytes = true
	case 7:
		if strings.ToLower(args[4]) != "range" {
			return nil, ErrSyntax
		}
		var err error
		offset, err = strconv.ParseInt(args[5], 10, 64)
		if err != nil {
			return nil, ErrSyntax
		}
		limit, err = strconv.ParseInt(args[6], 10, 64)
		if err != nil {
			return nil, ErrSyntax
		}
		if offset < 0 || limit <= 0 {
			return nil, ErrSyntax
		}
	default:
		return nil, errWrongNumArgsRaft
	}
	id = args[3]
	path := filepath.Join(m.dir, "snapshots", id, "state.bin")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var bytes []byte
	if allBytes {
		bytes, err = io.ReadAll(f)
		if err != nil {
			return nil, err
		}
	} else {
		if _, err := f.Seek(offset, 0); err != nil {
			return nil, err
		}
		packet := make([]byte, 4096)
		for int64(len(bytes)) < limit {
			n, err := f.Read(packet)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			bytes = append(bytes, packet[:n]...)
		}
		if int64(len(bytes)) > limit {
			bytes = bytes[:limit]
		}
	}
	return bytes, nil
}

func readSnapInfo(id, path string) (map[string]string, error) {
	status := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	_, ts, _, err := readSnapHead(gr)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	status["timestamp"] = fmt.Sprint(ts)
	status["id"] = id
	status["size"] = fmt.Sprint(fi.Size())
	return status, nil
}

var clusterCommands = map[string]command{
	"help":  {'s', cmdCLUSTERHELP},
	"info":  {'s', cmdCLUSTERINFO},
	"slots": {'s', cmdCLUSTERSLOTS},
	"nodes": {'s', cmdCLUSTERNODES},
}

// CLUSTER HELP
// help: returns the valid RAFT related commands; []string
func cmdCLUSTERHELP(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	lines := []redcon.SimpleString{
		"CLUSTER INFO",
		"CLUSTER NODES",
		"CLUSTER SLOTS",
	}
	return lines, nil
}

// CLUSTER INFO
// help: returns various redis cluster info; string
func cmdCLUSTERINFO(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	size := len(slist)
	epoch := ra.LastIndex()
	return fmt.Sprintf(""+
		"cluster_state:ok\n"+
		"cluster_slots_assigned:16384\n"+
		"cluster_slots_ok:16384\n"+
		"cluster_slots_pfail:0\n"+
		"cluster_slots_fail:0\n"+
		"cluster_known_nodes:%d\n"+
		"cluster_size:%d\n"+
		"cluster_current_epoch:%d\n"+
		"cluster_my_epoch:%d\n"+
		"cluster_stats_messages_sent:0\n"+
		"cluster_stats_messages_received:0\n",
		size, size, epoch, epoch,
	), nil
}

// CLUSTER SLOTS
// help: returns the cluster slots, which is always all slots being assigned
// to the leader.
func cmdCLUSTERSLOTS(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var leader serverEntry
	for _, server := range slist {
		if server.leader {
			leader = server
			break
		}
	}
	if !leader.leader {
		return nil, errors.New("CLUSTERDOWN The cluster is down")
	}
	return []interface{}{
		[]interface{}{
			redcon.SimpleInt(0),
			redcon.SimpleInt(16383),
			[]interface{}{
				leader.host(),
				redcon.SimpleInt(leader.port()),
				leader.clusterID(),
			},
		},
	}, nil
}

// CLUSTER NODES
// help: returns the cluster nodes
func cmdCLUSTERNODES(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var leader serverEntry
	for _, server := range slist {
		if server.leader {
			leader = server
			break
		}
	}
	if !leader.leader {
		return nil, errors.New("CLUSTERDOWN The cluster is down")
	}
	leaderID := leader.clusterID()
	var result string
	for _, server := range slist {
		flags := "slave"
		followerOf := leaderID
		if server.leader {
			flags = "master"
			followerOf = "-"
		}
		result += fmt.Sprintf("%s %s:%d@%d %s %s 0 0 connected 0-16383\n",
			server.clusterID(),
			server.host(), server.port(), server.port(),
			flags, followerOf,
		)
	}
	return result, nil
}

var raftCommands = map[string]command{
	"help":     {'s', cmdRAFTHELP},
	"info":     {'s', cmdRAFTINFO},
	"leader":   {'s', cmdRAFTLEADER},
	"snapshot": {'s', cmdRAFTSNAPSHOT},
	"server":   {'s', cmdRAFTSERVER},
}

// RAFT HELP
// help: returns the valid RAFT related commands; []string
func cmdRAFTHELP(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	lines := []redcon.SimpleString{
		"RAFT LEADER",
		"RAFT INFO [pattern]",

		"RAFT SERVER LIST",
		"RAFT SERVER ADD id address",
		"RAFT SERVER REMOVE id",

		"RAFT SNAPSHOT NOW",
		"RAFT SNAPSHOT LIST",
		"RAFT SNAPSHOT FILE id",
		"RAFT SNAPSHOT READ id [RANGE start end]",
	}
	return lines, nil
}

// VERSION
func cmdVERSION(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrWrongNumArgs
	}
	return getBaseMachine(um).vers, nil
}

// MACHINE [HUMAN]
func cmdMACHINE(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	var human bool
	switch len(args) {
	case 1:
	case 2:
		arg := strings.ToLower(args[1])
		if arg == "human" || arg == "h" {
			human = true
		} else {
			return false, ErrSyntax
		}
	default:
		return false, ErrWrongNumArgs
	}
	status := make(map[string]string)
	now := m.Now().UnixNano()
	uptime := now - m.start
	boottime := m.start
	if human {
		status["now"] = time.Unix(0, now).UTC().Format(time.RFC3339Nano)
		status["uptime"] = time.Duration(uptime).String()
		status["boottime"] = time.Unix(0, boottime).UTC().Format(
			time.RFC3339Nano)
	} else {
		status["now"] = fmt.Sprint(now)
		status["uptime"] = fmt.Sprint(uptime)
		status["boottime"] = fmt.Sprint(boottime)
	}
	return status, nil
}

// Logger is the logger used by Uhaha for printing all console messages.
type Logger interface {
	Debugf(format string, args ...interface{})
	Debug(args ...interface{})
	Debugln(args ...interface{})
	Verbf(format string, args ...interface{})
	Verb(args ...interface{})
	Verbln(args ...interface{})
	Noticef(format string, args ...interface{})
	Notice(args ...interface{})
	Noticeln(args ...interface{})
	Printf(format string, args ...interface{})
	Print(args ...interface{})
	Println(args ...interface{})
	Warningf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningln(args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalln(args ...interface{})
	Panicf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicln(args ...interface{})
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
	Errorln(args ...interface{})
}

// ErrWrongNumArgs is returned when the arg count is wrong
var ErrWrongNumArgs = errors.New("wrong number of arguments")

// ErrUnauthorized is returned when a client connection has not been authorized
var ErrUnauthorized = errors.New("unauthorized")

// ErrUnknownCommand is returned when a command is not known
var ErrUnknownCommand = errors.New("unknown command")

// ErrInvalid is returned when an operation has invalid arguments or options
var ErrInvalid = errors.New("invalid")

// ErrCorrupt is returned when a data is invalid or corrupt
var ErrCorrupt = errors.New("corrupt")

// Receiver ...
type Receiver interface {
	Args() []string
	Recv() (interface{}, time.Duration, error)
}

// SendOptions ...
type SendOptions struct {
	Context        interface{}
	From           interface{}
	AllowOpenReads bool
	DenyOpenReads  bool
}

var defSendOpts = &SendOptions{}

// An Observer holds a channel that delivers the messages for all commands
// processed by a Service.
type Observer interface {
	Stop()
	C() <-chan Message
}

type observer struct {
	mon  *monitor
	msgC chan Message
}

func (o *observer) C() <-chan Message {
	return o.msgC
}

func (o *observer) Stop() {
	o.mon.obMu.Lock()
	defer o.mon.obMu.Unlock()
	if _, ok := o.mon.obs[o]; ok {
		delete(o.mon.obs, o)
		close(o.msgC)
	}
}

// A Message represents a command and is in a format that is consumed by
// an Observer.
type Message struct {
	// Args are the original command arguments.
	Args []string
	// Resp is the command reponse, if not an error.
	Resp interface{}
	// Err is the command error, if not successful.
	Err error
	// Elapsed is the amount of time that the command took to process.
	Elapsed time.Duration
	// Addr is the remote TCP address of the connection that generated
	// this message.
	Addr string
}

// Monitor represents an interface for sending and consuming command
// messages that are processed by a Service.
type Monitor interface {
	// Send a message to observers
	Send(msg Message)
	// NewObjser returns a new Observer containing a channel that will send the
	// messages for every command processed by the service.
	// Stop the observer to release associated resources.
	NewObserver() Observer
}

type monitor struct {
	s    *service
	obMu sync.Mutex
	obs  map[*observer]struct{}
}

func newMonitor(s *service) *monitor {
	m := &monitor{s: s}
	m.obs = make(map[*observer]struct{})
	return m
}

func (m *monitor) Send(msg Message) {
	if len(msg.Args) > 0 {
		// do not allow monitoring of certain system commands
		switch msg.Args[0] {
		case "raft", "machine", "auth", "cluster":
			return
		}
	}

	m.obMu.Lock()
	defer m.obMu.Unlock()
	for o := range m.obs {
		o.msgC <- msg
	}
}

func (m *monitor) NewObserver() Observer {
	o := new(observer)
	o.mon = m
	o.msgC = make(chan Message, 64)
	m.obMu.Lock()
	m.obs[o] = struct{}{}
	m.obMu.Unlock()
	return o
}

type ResponseFilter func(
	serviceName string, context interface{},
	args []string, v interface{},
) interface{}

// Service is a client facing service.
type Service interface {
	// The name of the service
	Name() string
	// Send a command with args from a client
	Send(args []string, opts *SendOptions) Receiver
	// Auth authorizes a client
	Auth(auth string) error
	// Log is shared logger
	Log() Logger
	// Monitor returns a service monitor for observing client commands.
	Monitor() Monitor
	// Opened
	Opened(addr string) (context interface{}, accept bool)
	// Closed
	Closed(context interface{}, addr string)
	// ResponseFilter
	ResponseFilter() ResponseFilter
}

type serviceEntry struct {
	name  string
	sniff func(rd io.Reader) bool
	serve func(s Service, ln net.Listener)
}

type service struct {
	m     *machine
	ra    *raftWrap
	auth  string
	name  string
	mon   *monitor
	rfilt ResponseFilter

	writeMu sync.Mutex
	write   map[interface{}]*writeRequestFuture
}

var _ Service = &service{}

func newService(m *machine, ra *raftWrap, auth, name string,
	rfilt ResponseFilter,
) *service {
	s := &service{m: m, ra: ra, auth: auth, name: name, rfilt: rfilt}
	s.write = make(map[interface{}]*writeRequestFuture)
	s.mon = newMonitor(s)
	return s
}

func (s *service) ResponseFilter() ResponseFilter {
	return s.rfilt
}

// Monitor allows for observing all incoming service commands from all clients.
// See an example in the examples/kvdb project.
func (s *service) Monitor() Monitor {
	return s.mon
}

func (s *service) Name() string {
	return s.name
}

func (s *service) Log() Logger {
	return s.m.log
}

func (s *service) Auth(auth string) error {
	if s.auth != auth {
		return ErrUnauthorized
	}
	return nil
}

// The Send function sends command args to the service and return a future
// receiver for getting the response.
// There are three type of commands: write, read, and system.
//
//   - Write commands always go though the raft log one at a time.
//   - Read commands do not go though the raft log but do need to be executed
//     on the leader. Many reads from multiple clients can execute at the same
//     time, but each read must wait until the leader has applied at least one
//     new tick (which acts as a barrier) and must wait for any pending writes
//     that the same client has issued to be fully applied before executing the
//     read.
//   - System commands run independently from the machine or user data space, and
//     are primarily used for executing lower level system operations such as
//     Raft functions, backups, server stats, etc.
//
// ** Open Reads **
// When the server has been started with the --openreads flag or when
// SendOptions.AllowOpenReads is true, followers can also accept reads.
// Using open reads runs the risk of returning stale data.
func (s *service) Send(args []string, opts *SendOptions) Receiver {
	if len(args) == 0 {
		// Empty command gets an empty response
		return Response(args, nil, 0, nil)
	}
	cmdName := strings.ToLower(args[0])
	cmd, ok := s.m.commands[cmdName]
	if !ok {
		if s.m.catchall.kind == 0 {
			return Response(args, nil, 0, ErrUnknownCommand)
		}
		cmd = s.m.catchall
	}
	if cmdName == "tick" {
		// The "tick" command is explicitly denied from being called by a
		// service. It must only be called from the runTicker function.
		// Let's just pretend like it's an unknown command.
		return Response(args, nil, 0, ErrUnknownCommand)
	}
	if opts == nil {
		// Use the default send options when the sender does not tell us what
		// they want.
		opts = defSendOpts
	}
	switch cmd.kind {
	case 'w': // write
		r := &writeRequestFuture{args: args, s: s, from: opts.From}
		r.wg.Add(1)
		s.m.wrC <- r
		s.addWrite(opts.From, r)
		return r
	case 'r': // read
		s.waitWrite(opts.From)
		start := time.Now()
		resp, err := s.execRead(cmd, args, opts)
		return Response(args, resp, time.Since(start),
			errRaftConvert(s.ra, err))
	case 's': // intermediate/system
		s.waitWrite(opts.From)
		start := time.Now()
		pm := contextMachine{m: s.m, context: opts.Context}
		resp, err := cmd.fn(pm, s.ra, args)
		return Response(args, resp, time.Since(start),
			errRaftConvert(s.ra, err))
	default:
		return Response(args, nil, 0, errors.New("invalid request"))
	}
}

func (s *service) Opened(addr string) (context interface{}, accept bool) {
	if s.m.connOpened != nil {
		return s.m.connOpened(addr)
	}
	return nil, true
}

func (s *service) Closed(context interface{}, addr string) {
	if s.m.connClosed != nil {
		s.m.connClosed(context, addr)
	}
}

func (s *service) execRead(cmd command, args []string, opts *SendOptions,
) (interface{}, error) {
	openReads := s.m.openReads
	if opts.AllowOpenReads {
		if opts.DenyOpenReads {
			return nil, ErrInvalid
		}
		openReads = true
	} else if opts.DenyOpenReads {
		openReads = false
	}
	var resp interface{}
	var err error
	if openReads {
		resp, err = s.execOpenRead(cmd, args, opts)
	} else {
		resp, err = s.execNonOpenRead(cmd, args, opts)
	}
	return resp, err
}

func (s *service) execOpenRead(cmd command, args []string, opts *SendOptions,
) (interface{}, error) {
	// open reads can be performed on the leaders and followers that have a log
	// which is reasonably loaded.
	if atomic.LoadInt32(&s.m.logLoaded) == 0 {
		return nil, raft.ErrNotLeader
	}
	// Set the machine to read access mode
	s.m.mu.RLock()
	atomic.AddInt32(&s.m.readers, 1)
	defer func() {
		// Return the machine to write access mode
		atomic.AddInt32(&s.m.readers, -1)
		s.m.mu.RUnlock()
	}()
	m := contextMachine{reader: true, context: opts.Context, m: s.m}
	return cmd.fn(m, s.ra, args)
}

func (s *service) execNonOpenRead(cmd command, args []string, opts *SendOptions,
) (interface{}, error) {
	// Non-open reads can only be performed on a leader that has received
	// a tick response. In this case a tick acts as a write barrier ensuring
	// that any read command will always follow the tick.
	s.m.mu.RLock()
	atomic.AddInt32(&s.m.readers, 1)
	defer func() {
		atomic.AddInt32(&s.m.readers, -1)
		s.m.mu.RUnlock()
	}()
	if s.ra.State() != raft.Leader || s.m.tickedIndex == 0 {
		return nil, raft.ErrNotLeader
	}
	// We are the leader and we have received a tick event.
	// Complete the read command.
	m := contextMachine{reader: true, context: opts.Context, m: s.m}
	return cmd.fn(m, s.ra, args)
}

func (s *service) addWrite(from interface{}, r *writeRequestFuture) {
	s.writeMu.Lock()
	s.write[from] = r
	s.writeMu.Unlock()
}

func (s *service) waitWrite(from interface{}) {
	s.writeMu.Lock()
	r := s.write[from]
	s.writeMu.Unlock()
	if r != nil {
		r.Recv()
	}
}

type simpleResponse struct {
	args []string
	v    interface{}
	elap time.Duration
	err  error
}

func (r *simpleResponse) Recv() (interface{}, time.Duration, error) {
	return r.v, r.elap, r.err
}

func (r *simpleResponse) Args() []string {
	return r.args
}

// Response ...
func Response(args []string, v interface{}, elapsed time.Duration, err error,
) Receiver {
	return &simpleResponse{args, v, elapsed, err}
}

// writeRequestFuture is the basic unity of communication from services to the
// raft log. It's a Future type that is sent through a channel, picked up by a
// background routine that then applies the `args` to the raft log. Upon
// successfully being applied, the `resp` is fill with the response, and
// `wg.Done` is called.
type writeRequestFuture struct {
	args []string
	resp interface{}
	err  error
	elap time.Duration
	wg   sync.WaitGroup
	s    *service
	from interface{}
}

func (r *writeRequestFuture) Args() []string {
	return r.args
}

// Recv received the response and time elapsed to process the write. Or, it
// returns an error.
func (r *writeRequestFuture) Recv() (interface{}, time.Duration, error) {
	r.wg.Wait()
	r.s.writeMu.Lock()
	if r.s.write[r.from] == r {
		delete(r.s.write, r.from)
	}
	r.s.writeMu.Unlock()
	return r.resp, r.elap, r.err
}

// redisService provides a service that is compatible with the Redis protocol.
func redisService() (
	name string,
	sniff func(io.Reader) bool,
	acceptor func(Service, net.Listener),
) {
	return "redis", nil, redisServiceHandler
}

type redisClient struct {
	authorized bool
	opts       SendOptions
}

func redisCommandToArgs(cmd redcon.Command) []string {
	args := make([]string, len(cmd.Args))
	args[0] = strings.ToLower(string(cmd.Args[0]))
	for i := 1; i < len(cmd.Args); i++ {
		args[i] = string(cmd.Args[i])
	}
	return args
}

type redisQuitClose struct{}

func redisServiceExecArgs(s Service, client *redisClient, conn redcon.Conn,
	args [][]string,
) {
	sname := s.Name()
	rfilt := s.ResponseFilter()
	recvs := make([]Receiver, len(args))
	var close bool
	for i, args := range args {
		var r Receiver
		switch args[0] {
		case "quit":
			r = Response(args, redisQuitClose{}, 0, nil)
			close = true
		case "auth":
			if len(args) != 2 {
				r = Response(args, nil, 0, ErrWrongNumArgs)
			} else if err := s.Auth(args[1]); err != nil {
				client.authorized = false
				r = Response(args, nil, 0, err)
			} else {
				client.authorized = true
				r = Response(args, redcon.SimpleString("OK"), 0, nil)
			}
		default:
			if !client.authorized {
				if err := s.Auth(""); err != nil {
					client.authorized = false
					r = Response(args, nil, 0, err)
				} else {
					client.authorized = true
				}
			}
			if client.authorized {
				switch args[0] {
				case "ping":
					if len(args) == 1 {
						r = Response(args, redcon.SimpleString("PONG"), 0,
							nil)
					} else if len(args) == 2 {
						r = Response(args, args[1], 0, nil)
					} else {
						r = Response(args, nil, 0, ErrWrongNumArgs)
					}
				case "shutdown":
					s.Log().Error("Shutting down")
					os.Exit(0)
				case "echo":
					if len(args) != 2 {
						r = Response(args, nil, 0, ErrWrongNumArgs)
					} else {
						r = Response(args, args[1], 0, nil)
					}
				default:
					r = s.Send(args, &client.opts)
				}
			}
		}
		recvs[i] = r
		if close {
			break
		}
	}
	// receive responses
	var filteredArgs [][]string
	for i, r := range recvs {
		resp, elapsed, err := r.Recv()
		if err != nil {
			if err == ErrUnknownCommand {
				err = fmt.Errorf("%s '%s'", err, args[i][0])
			}
			redisConnWriteAny(sname, rfilt, conn, client, r.Args(), err)
		} else {
			switch v := resp.(type) {
			case FilterArgs:
				filteredArgs = append(filteredArgs, v)
			case Hijack:
				conn := newRedisHijackedConn(conn.Detach())
				go v(s, conn)
			case redisQuitClose:
				v2 := redcon.SimpleString("OK")
				redisConnWriteAny(sname, rfilt, conn, client, r.Args(), v2)
				conn.Close()
			default:
				redisConnWriteAny(sname, rfilt, conn, client, r.Args(), v)
			}
		}
		// broadcast the request and response to all observers
		s.Monitor().Send(Message{
			Addr:    conn.RemoteAddr(),
			Args:    args[i],
			Resp:    resp,
			Err:     err,
			Elapsed: elapsed,
		})
	}
	if len(filteredArgs) > 0 {
		redisServiceExecArgs(s, client, conn, filteredArgs)
	}
}

func redisConnWriteAny(
	sname string,
	rfilt ResponseFilter,
	conn redcon.Conn,
	client *redisClient,
	args []string,
	v interface{},
) {
	if rfilt != nil {
		v = rfilt(sname, client.opts.Context, args, v)
	}
	conn.WriteAny(v)
}

func redisServiceHandler(s Service, ln net.Listener) {
	accept := func(conn redcon.Conn) bool {
		context, accept := s.Opened(conn.RemoteAddr())
		if !accept {
			return false
		}
		client := new(redisClient)
		client.opts.From = client
		client.opts.Context = context
		conn.SetContext(client)
		return true
	}
	closed := func(conn redcon.Conn, err error) {
		if conn.Context() == nil {
			return
		}
		client := conn.Context().(*redisClient)
		s.Closed(client.opts.Context, conn.RemoteAddr())
	}
	handle := func(conn redcon.Conn, cmd redcon.Command) {
		client := conn.Context().(*redisClient)
		var args [][]string
		args = append(args, redisCommandToArgs(cmd))
		for _, cmd := range conn.ReadPipeline() {
			args = append(args, redisCommandToArgs(cmd))
		}
		redisServiceExecArgs(s, client, conn, args)
	}

	if s, ok := s.(*service); ok {
		if s.ra.conf.LocalConnector != nil {
			s.ra.conf.LocalConnector(&localRedisConnector{
				auth:   s.ra.conf.Auth,
				accept: accept,
				closed: closed,
				handle: handle,
			})
		}
	}

	s.Log().Fatal(redcon.Serve(ln, handle, accept, closed))
}

// FilterArgs ...
type FilterArgs []string

// Hijack is a function type that can be used to "hijack" a service client
// connection and allowing to perform I/O operations outside the standard
// network loop. An example of it's usage can be found in the examples/kvdb
// project.
type Hijack func(s Service, conn HijackedConn)

// HijackedConn is a connection that has been detached from the main service
// network loop. It's entirely up to the hijacker to performs all I/O
// operations. The Write* functions buffer write data and the Flush must be
// called to do the actual sending of the data to the connection.
// Close the connection to when done.
type HijackedConn interface {
	// RemoteAddr is the connection remote tcp address.
	RemoteAddr() string
	// ReadCommands is an iterator function that reads pipelined commands.
	// Returns a error when the connection encountared and error.
	ReadCommands(func(args []string) bool) error
	// ReadCommand reads one command at a time.
	ReadCommand() (args []string, err error)
	// WriteAny writes any type to the write buffer using the format rules that
	// are defined by the original Service.
	WriteAny(v interface{})
	// WriteRaw writes raw data to the write buffer.
	WriteRaw(data []byte)
	// Flush the write write buffer and send data to the connection.
	Flush() error
	// Close the connection
	Close() error
}

type redisHijackConn struct {
	dconn redcon.DetachedConn
	cmds  []redcon.Command
	// anyWrap func(v interface{}) []byte
}

func newRedisHijackedConn(dconn redcon.DetachedConn) *redisHijackConn {
	return &redisHijackConn{dconn: dconn}
}

func (conn *redisHijackConn) ReadCommands(iter func(args []string) bool) error {
	if len(conn.cmds) == 0 {
		cmd, err := conn.dconn.ReadCommand()
		if err != nil {
			return err
		}
		if !iter(redisCommandToArgs(cmd)) {
			return nil
		}
		conn.cmds = conn.dconn.ReadPipeline()
	}
	for len(conn.cmds) > 0 {
		cmd := conn.cmds[0]
		conn.cmds = conn.cmds[1:]
		if !iter(redisCommandToArgs(cmd)) {
			return nil
		}
	}
	return nil
}

func (conn *redisHijackConn) WriteAny(v interface{}) {
	conn.dconn.WriteAny(v)
}

func (conn *redisHijackConn) WriteRaw(data []byte) {
	conn.dconn.WriteRaw(data)
}

func (conn *redisHijackConn) Flush() error {
	return conn.dconn.Flush()
}

func (conn *redisHijackConn) Close() error {
	return conn.dconn.Close()
}

func (conn *redisHijackConn) RemoteAddr() string {
	return conn.dconn.RemoteAddr()
}

func (conn *redisHijackConn) ReadCommand() (args []string, err error) {
	err = conn.ReadCommands(func(iargs []string) bool {
		args = iargs
		return false
	})
	return args, err
}

type LocalConnector interface {
	Open() (LocalConn, error)
}

type localRedisConnector struct {
	auth   string
	accept func(conn redcon.Conn) bool
	closed func(conn redcon.Conn, err error)
	handle func(conn redcon.Conn, cmd redcon.Command)
}

type LocalConn interface {
	Do(args ...string) redcon.RESP
	Close()
}

func (lc *localRedisConnector) Open() (LocalConn, error) {
	conn := &localRedisConn{lc: lc}
	conn.rc = &localRedconConn{conn: conn}
	if !conn.lc.accept(conn.rc) {
		return nil, errors.New("not accepted")
	}
	conn.Do("auth", lc.auth)
	return conn, nil
}

type localRedisConn struct {
	lc *localRedisConnector
	rc *localRedconConn
}

func (conn *localRedisConn) Close() {
	conn.lc.closed(conn.rc, nil)
}

func (conn *localRedisConn) Do(args ...string) redcon.RESP {
	var cmd redcon.Command
	cmd.Args = make([][]byte, len(args))
	raw := redcon.AppendArray(nil, len(args))
	for i := 0; i < len(args); i++ {
		raw = redcon.AppendBulkString(raw, args[i])
		cmd.Args[i] = []byte(args[i])
	}
	cmd.Raw = raw
	conn.lc.handle(conn.rc, cmd)
	var resp redcon.RESP
	if len(conn.rc.out) > 0 {
		_, resp = redcon.ReadNextRESP(conn.rc.out)
		conn.rc.out = nil
	}
	return resp
}

type localRedconConn struct {
	ctx  interface{}
	out  []byte
	conn *localRedisConn
}

func (c *localRedconConn) Close() error                   { return nil }
func (c *localRedconConn) Context() interface{}           { return c.ctx }
func (c *localRedconConn) SetContext(v interface{})       { c.ctx = v }
func (c *localRedconConn) SetReadBuffer(n int)            {}
func (c *localRedconConn) RemoteAddr() string             { return "" }
func (c *localRedconConn) ReadPipeline() []redcon.Command { return nil }
func (c *localRedconConn) PeekPipeline() []redcon.Command { return nil }
func (c *localRedconConn) NetConn() net.Conn              { return nil }
func (c *localRedconConn) Detach() redcon.DetachedConn    { return nil }

func (c *localRedconConn) WriteString(str string) {
	c.out = redcon.AppendString(c.out, str)
}
func (c *localRedconConn) WriteBulk(bulk []byte) {
	c.out = redcon.AppendBulk(c.out, bulk)
}
func (c *localRedconConn) WriteBulkString(bulk string) {
	c.out = redcon.AppendBulkString(c.out, bulk)
}
func (c *localRedconConn) WriteInt(num int) {
	c.out = redcon.AppendInt(c.out, int64(num))
}
func (c *localRedconConn) WriteInt64(num int64) {
	c.out = redcon.AppendInt(c.out, num)
}
func (c *localRedconConn) WriteUint64(num uint64) {
	c.out = redcon.AppendUint(c.out, num)
}
func (c *localRedconConn) WriteError(msg string) {
	c.out = redcon.AppendError(c.out, msg)
}
func (c *localRedconConn) WriteArray(count int) {
	c.out = redcon.AppendArray(c.out, count)
}
func (c *localRedconConn) WriteNull() {
	c.out = redcon.AppendNull(c.out)
}
func (c *localRedconConn) WriteRaw(data []byte) {
	c.out = append(c.out, data...)
}
func (c *localRedconConn) WriteAny(v interface{}) {
	c.out = redcon.AppendAny(c.out, v)
}
