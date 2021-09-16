package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"

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
	log.SetOutput(os.Stderr)
	f := func() {
		conf.DataDir = "/tmp/icefiredb"
		os.RemoveAll(conf.DataDir)
		conf.DataDirReady = func(dir string) {
			os.RemoveAll(filepath.Join(dir, "main.db"))

			ldsCfg = lediscfg.NewConfigDefault()
			ldsCfg.DataDir = filepath.Join(dir, "main.db")
			ldsCfg.Databases = 1
			ldsCfg.DBName = os.Getenv("DRIVER")
			ldsCfg.Databases = slotNum
			var err error
			le, err = ledis.Open(ldsCfg)
			if err != nil {
				panic(err)
			}

			ldb, err = NewLedisDBs(le, slotNum)
			if err != nil {
				panic(err)
			}
			db0, err := ldb.GetDB(0)
			// Obtain the leveldb object and handle it carefully
			driver := db0.GetSDB().GetDriver().GetStorageEngine()
			db = driver.(*leveldb.DB)
		}

		conf.Snapshot = snapshot
		conf.Restore = restore
		go uhaha.Main(conf)

		testRedisClient = redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:11001",
		})

		log.Println("waiting for DB bootstrap")
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
