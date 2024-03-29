package main

import (
	"fmt"

	lediscfg "github.com/ledisdb/ledisdb/config"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
	rafthub "github.com/tidwall/uhaha"
)

// BuildDate: Binary file compilation time
// BuildVersion: Binary compiled GIT version
var (
	BuildDate    string
	BuildVersion string
)

var (
	db            *leveldb.DB
	le            *ledis.Ledis
	ldb           *ledis.DB
	ldsCfg        *lediscfg.Config
	serverInfo    *info
	respClientNum int64
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
