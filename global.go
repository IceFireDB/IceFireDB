/*
 * @Author: gitsrc
 * @Date: 2021-03-10 11:17:13
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-10 11:43:08
 * @FilePath: /IceFireDB/global.go
 */

package main

import (
	"github.com/gitsrc/IceFireDB/rafthub"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
)

var db *leveldb.DB
var le *ledis.Ledis
var ldb *ledis.DB

var conf rafthub.Config //raft config
