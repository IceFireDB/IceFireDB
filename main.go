/*
 * @Author: gitsrc
 * @Date: 2021-03-05 14:46:31
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-05 14:46:57
 * @FilePath: /IceFireDB/main.go
 */

package main

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"gitlab.com/gitsrc/rafthub"
)

type data struct {
	Count          int64
	LevelDBStorage *leveldb.DB
	sync.RWMutex
}

var localStoragePath string

func init() {
	localStoragePathTemp, err := os.Getwd()
	if err == nil {
		localStoragePathTemp += "/db_storage"
	}
	if _, err := os.Stat(localStoragePathTemp); os.IsNotExist(err) {
		log.Fatalf("The leveldb storage directory does not exist, please create the directory: (%s)\n", localStoragePathTemp)
	}

	localStoragePath = localStoragePathTemp
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)
}

func main() {
	// Set up a rafthub configuration
	var conf rafthub.Config

	conf.Name = "IceFireDB" // Hot and cold hybrid KV storage database, support redis protocol.
	conf.Version = "1.0.0"

	// Set the initial data. This is state of the data when first server in the
	// cluster starts for the first time ever.
	levelDBObj, err := leveldb.OpenFile(localStoragePath, nil)

	if err != nil {
		log.Fatalln(err)
	}

	defer levelDBObj.Close()

	conf.InitialData = &data{LevelDBStorage: levelDBObj}

	conf.UseJSONSnapshots = true

	// Add a command that will change the value of a Ticket.
	conf.AddWriteCommand("set", cmdKVSet)
	conf.AddReadCommand("get", cmdKVGet)
	conf.AddWriteCommand("del", cmdKVDel)
	// conf.AddReadCommand("count", cmdKVCount)
	// Finally, hand off all processing to rafthub.
	rafthub.Main(conf)
}

func cmdKVSet(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) != 3 {
		return nil, errors.New("The number of set parameters is illegal")
	}

	err := data.LevelDBStorage.Put([]byte(args[1]), []byte(args[2]), nil)

	return "OK", err
}

func cmdKVGet(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)
	var value interface{}

	if len(args) != 2 {
		return nil, errors.New("The number of get parameters is illegal")
	}

	value, err := data.LevelDBStorage.Get([]byte(args[1]), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return value, nil
}

func cmdKVDel(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)
	var value interface{}

	if len(args) != 2 {
		return nil, errors.New("The number of del parameters is illegal")
	}

	value = -1

	err := data.LevelDBStorage.Delete([]byte(args[1]), nil)
	if err != nil {
		return nil, err
	}

	value = 1

	return value, nil
}
