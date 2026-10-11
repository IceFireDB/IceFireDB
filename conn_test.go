//go:build alltest
// +build alltest

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ledisdb/ledisdb/ledis"

	"github.com/cenkalti/backoff/v4"
	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/uhaha"

	"github.com/IceFireDB/IceFireDB/driver/oss"
)

var (
	testConnOnce    sync.Once
	testRedisClient *redis.Client
)

func getTestConn() *redis.Client {
	log.SetOutput(os.Stderr)
	f := func() {
		// Allow tests to redirect the OSS storage backend at an arbitrary
		// S3-compatible endpoint. The default in driver/oss/db.go is
		// "http://localhost:9000" (MinIO), but CI may run LocalStack or
		// MinIO on a different port. confInit() — which parses the
		// --oss-endpoint flag — is only invoked from main() and tests go
		// straight to uhaha.Main(), so we have to apply the override here.
		if endpoint := os.Getenv("OSS_TEST_ENDPOINT"); endpoint != "" {
			oss.OssDefaultConfig.EndPointConnection = endpoint
		}
		if accessKey := os.Getenv("OSS_TEST_ACCESS_KEY"); accessKey != "" {
			oss.OssDefaultConfig.AccessKey = accessKey
		}
		if secretKey := os.Getenv("OSS_TEST_SECRET_KEY"); secretKey != "" {
			oss.OssDefaultConfig.Secretkey = secretKey
		}
		conf.DataDir = "/tmp/icefiredb"
		os.RemoveAll(conf.DataDir)
		conf.DataDirReady = func(dir string) {
			os.RemoveAll(filepath.Join(dir, "main.db"))

			ldsCfg = lediscfg.NewConfigDefault()
			ldsCfg.DataDir = filepath.Join(dir, "main.db")
			ldsCfg.Databases = 1
			ldsCfg.DBName = os.Getenv("DRIVER")
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
		fmt.Printf("start with Storage Engine: %s\n", os.Getenv("DRIVER"))
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
