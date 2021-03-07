/*
 * @Author: gitsrc
 * @Date: 2021-03-05 14:46:31
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-07 12:02:16
 * @FilePath: /IceFireDB/main.go
 */

package main

import (
	"errors"
	"log"
	"sync"

	"context"

	"github.com/go-redis/redis/v8"
	"gitlab.com/gitsrc/rafthub"
)

type data struct {
	Count int64
	DB    *redis.Client
	sync.RWMutex
}

var remoteStoragePath string

func init() {
	remoteStoragePath = "/tmp/ledis.socks"

	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

}

var ctx = context.Background()

func main() {
	// Set up a rafthub configuration
	var conf rafthub.Config

	conf.Name = "IceFireDB" // Hot and cold hybrid KV storage database, support redis protocol.
	conf.Version = "1.0.0"

	// Set the initial data. This is state of the data when first server in the
	// cluster starts for the first time ever.
	rdb := redis.NewClient(&redis.Options{
		Network:  "unix",
		Addr:     remoteStoragePath,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	conf.InitialData = &data{DB: rdb}

	conf.UseJSONSnapshots = true

	conf.AddWriteCommand("set", cmdSet)
	conf.AddReadCommand("get", cmdGet)
	conf.AddWriteCommand("del", cmdDel)
	conf.AddWriteCommand("hset", cmdHSET)
	conf.AddWriteCommand("hget", cmdHGET)
	conf.AddWriteCommand("hdel", cmdHDEL)

	// conf.AddReadCommand("count", cmdKVCount)
	// Finally, hand off all processing to rafthub.
	rafthub.Main(conf)
}

func cmdSet(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) != 3 {
		return nil, errors.New("The number of set parameters is illegal")
	}

	cmdStatus := data.DB.Set(ctx, args[1], args[2], 0)

	return cmdStatus.Result()
}

func cmdGet(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) != 2 {
		return nil, errors.New("The number of get parameters is illegal")
	}

	cmdStatus := data.DB.Get(ctx, args[1])

	return cmdStatus.Result()
}

func cmdHSET(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) < 2 {
		return nil, errors.New("The number of hset parameters is illegal")
	}

	respArgs := make([]interface{}, len(args)-2)
	for i := 0; i < len(respArgs); i++ {
		respArgs[i] = args[i+2]
	}
	cmdStatus := data.DB.HSet(ctx, args[1], respArgs...)

	return cmdStatus.Result()
}

func cmdHGET(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) < 2 {
		return nil, errors.New("The number of hget parameters is illegal")
	}

	cmdStatus := data.DB.HGet(ctx, args[1], args[2])

	return cmdStatus.Result()
}

func cmdHDEL(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) < 2 {
		return nil, errors.New("The number of hdel parameters is illegal")
	}

	respArgs := args[2:]

	cmdStatus := data.DB.HDel(ctx, args[1], respArgs...)

	return cmdStatus.Result()
}

func cmdDel(m rafthub.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	if len(args) != 2 {
		return nil, errors.New("The number of del parameters is illegal")
	}

	cmdStatus := data.DB.Del(ctx, args[1])

	return cmdStatus.Result()
}
