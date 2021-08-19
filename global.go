/*
 * @Author: gitsrc
 * @Date: 2021-03-10 11:17:13
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-19 22:51:21
 * @FilePath: /IceFireDB/global.go
 */

package main

import (
	"fmt"

	"github.com/gitsrc/rafthub"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
)

//BuildDate: Binary file compilation time
//BuildVersion: Binary compiled GIT version
var (
	BuildDate    string
	BuildVersion string
)

var db *leveldb.DB
var le *ledis.Ledis
var ldb *ledis.DB

var conf rafthub.Config //raft config

var banner string

func init() {
	banner = `  ____        _____         ___  ___ 
  /  _/______ / __(_)______ / _ \/ _ )
 _/ // __/ -_) _// / __/ -_) // / _  |
/___/\__/\__/_/ /_/_/  \__/____/____/ 
                                      
`

	fmt.Println(banner)
}
