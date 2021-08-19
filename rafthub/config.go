// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Config is the configuration for managing the behavior of the application.
// This must be fill out prior and then passed to the rafthub.Main() function.
type Config struct {
	cmds      map[string]command // appended by AddCommand
	catchall  command            // set by AddCatchallCommand
	services  []serviceEntry     // appended by AddService
	jsonType  reflect.Type       // used by UseJSONSnapshots
	jsonSnaps bool               // used by UseJSONSnapshots

	// Name gives the server application a name. Default "rafthub-app"
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
}

func (conf *Config) def() {
	if conf.Addr == "" {
		conf.Addr = "127.0.0.1:11001"
	}
	if conf.Version == "" {
		conf.Version = "0.0.0"
	}
	if conf.Name == "" {
		conf.Name = "rafthub-app"
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
	var vers bool
	flag.BoolVar(&vers, "v", false, "")
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
	if conf.Flag.PreParse != nil {
		conf.Flag.PreParse()
	}
	flag.Parse()
	if vers {
		fmt.Printf("%s\n", versline(*conf))
		os.Exit(0)
	}
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
func (conf *Config) AddService(sniff func(rd io.Reader) bool,
	acceptor func(s Service, ln net.Listener),
) {
	conf.services = append(conf.services, serviceEntry{sniff, acceptor})
}
