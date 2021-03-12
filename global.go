/*
 * @Author: gitsrc
 * @Date: 2021-03-10 11:17:13
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-12 14:16:42
 * @FilePath: /IceFireDB/global.go
 */

package main

import (
	"github.com/gitsrc/IceFireDB/rafthub"
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
	banner = `██╗ ██████╗███████╗███████╗██╗██████╗ ███████╗██████╗ ██████╗ 
██║██╔════╝██╔════╝██╔════╝██║██╔══██╗██╔════╝██╔══██╗██╔══██╗
██║██║     █████╗  █████╗  ██║██████╔╝█████╗  ██║  ██║██████╔╝
██║██║     ██╔══╝  ██╔══╝  ██║██╔══██╗██╔══╝  ██║  ██║██╔══██╗
██║╚██████╗███████╗██║     ██║██║  ██║███████╗██████╔╝██████╔╝
╚═╝ ╚═════╝╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝╚═════╝ ╚═════╝ 
                                                              `
}
