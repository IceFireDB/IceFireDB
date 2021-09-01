package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gitsrc/IceFireDB/driver/badger"

	"github.com/ledisdb/ledisdb/ledis"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v8"
	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/tidwall/uhaha"
)

var (
	testConnOnce    sync.Once
	testRedisClient *redis.Client
)

func getTestConn() *redis.Client {
	f := func() {
		conf.DataDir = "/tmp/icefiredb"
		os.RemoveAll(conf.DataDir)
		conf.DataDirReady = func(dir string) {
			os.RemoveAll(filepath.Join(dir, "main.db"))

			ldsCfg = lediscfg.NewConfigDefault()
			ldsCfg.DataDir = filepath.Join(dir, "main.db")
			ldsCfg.Databases = 1
			ldsCfg.DBName = badger.StorageName
			var err error
			le, err = ledis.Open(ldsCfg)
			if err != nil {
				panic(err)
			}

			ldb, err = le.Select(0)
			if err != nil {
				panic(err)
			}
		}

		conf.Snapshot = snapshot
		conf.Restore = restore
		go uhaha.Main(conf)

		testRedisClient = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:11001",
		})

		// wait server starts
		backoff.Retry(func() error {
			_, err := testRedisClient.Set(context.Background(), "init", "1", 0).Result()
			return err
		}, backoff.NewConstantBackOff(1*time.Second))

		// clean all data
		testRedisClient.FlushAll(context.Background())
	}
	testConnOnce.Do(f)
	return testRedisClient
}
