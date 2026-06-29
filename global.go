package main

import (
	"fmt"
	"sync/atomic"

	lediscfg "github.com/ledisdb/ledisdb/config"

	"github.com/ledisdb/ledisdb/ledis"
	rafthub "github.com/tidwall/uhaha"
)

// BuildDate: Binary file compilation time
// BuildVersion: Binary compiled GIT version
var (
	BuildDate    string
	BuildVersion string
)

var (
	le            *ledis.Ledis
	ldb           *ledis.DB
	ldsCfg        *lediscfg.Config
	serverInfo    *info
	respClientNum int64
	// storageReady reports whether the storage backend has finished
	// initializing (set true in DataDirReady). Read concurrently by the
	// observability readiness endpoint, so it must be atomic.
	storageReady atomic.Bool
)

var conf rafthub.Config // raft config

var banner string

func init() {
	banner = `  ____        _____         ___  ___ 
  /  _/______ / __(_)______ / _ \/ _ )
 _/ // __/ -_) _// / __/ -_) // / _  |
/___/\__/\__/_/ /_/_/  \__/____/____/ 
                                      
`

	fmt.Println(banner)
}
