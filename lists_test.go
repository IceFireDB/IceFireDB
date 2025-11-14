//go:build alltest
// +build alltest

package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

func testListIndex(key []byte, index int64, v int) error {
	c := getTestConn()
	ctx := context.Background()

	n, err := c.Do(ctx, "lindex", key, index).Int()
	if err == redis.Nil && v != 0 {
		return fmt.Errorf("must nil")
	} else if err != nil && err != redis.Nil {
		r, _ := c.Do(ctx, "lindex", key, index).Result()
		log.Println("lindex err: ", spew.Sdump(r))
		return err
	} else if n != v {
		return fmt.Errorf("index err number %d != %d", n, v)
	}

	return nil
}

func testListRange(key []byte, start int64, stop int64, checkValues ...int) error {
	c := getTestConn()
	ctx := context.Background()

	res, err := c.Do(ctx, "lrange", key, start, stop).Result()
	if err != nil {
		return err
	}

	vs := cast.ToIntSlice(res)

	if len(vs) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(vs), len(checkValues))
	}

	var n int
	for i, v := range vs {
		if v != checkValues[i] {
			return fmt.Errorf("invalid data %d: %d != %d", i, n, checkValues[i])
		}
	}

	return nil
}

func TestList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	key := []byte("a")
	if n, err := c.Do(ctx, "lkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "lpush", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "lkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(1)
	}

	if n, err := c.Do(ctx, "rpush", key, 2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "rpush", key, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	//for ledis-cli a 1 2 3
	// 127.0.0.1:6379> lrange a 0 0
	// 1) "1"
	if err := testListRange(key, 0, 0, 1); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a 0 1
	// 1) "1"
	// 2) "2"
	if err := testListRange(key, 0, 1, 1, 2); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a 0 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(key, 0, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 5
	// 1) "3"
	if err := testListRange(key, -1, 5, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -5 -1
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(key, -5, -1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -2 -1
	// 1) "2"
	// 2) "3"
	if err := testListRange(key, -2, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 -2
	// (empty list or set)
	if err := testListRange(key, -1, -2); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 2
	// 1) "3"
	if err := testListRange(key, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -5 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	if err := testListRange(key, -5, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 0
	// (empty list or set)
	if err := testListRange(key, -1, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListRange([]byte("empty list"), 0, 100); err != nil {
		t.Fatal(err)
	}

	// 127.0.0.1:6379> lrange a -1 -1
	// 1) "3"
	if err := testListRange(key, -1, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 0, 1); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 1, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 5, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -2, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -3, 1); err != nil {
		t.Fatal(err)
	}
}

func TestListMPush(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	key := []byte("b")
	if n, err := c.Do(ctx, "rpush", key, 1, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 3, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "lpush", key, 1, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 6, 3, 2, 1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	key := []byte("c")
	if n, err := c.Do(ctx, "rpush", key, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "lpop", key).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "rpop", key).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if n, err := c.Do(ctx, "lpush", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 5, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if v, err := c.Do(ctx, "lpop", key).Int(); err != nil {
			t.Fatal(err)
		} else if v != i {
			t.Fatal(v)
		}
	}

	if n, err := c.Do(ctx, "llen", key).Int(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do(ctx, "rpush", key, 1, 2, 3, 4, 5)

	if n, err := c.Do(ctx, "lclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestRPopLPush(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	src := []byte("sr")
	des := []byte("de")

	c.Do(ctx, "lclear", src)
	c.Do(ctx, "lclear", des)

	if _, err := c.Do(ctx, "rpoplpush", src, des).Result(); err != redis.Nil {
		t.Fatal(err)
	}

	if v, err := c.Do(ctx, "llen", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal(v)
	}

	if n, err := c.Do(ctx, "rpush", src, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "rpoplpush", src, src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "llen", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if res, err := c.Do(ctx, "rpoplpush", src, des).Result(); err != nil {
		t.Fatal(err)
	} else if v := cast.ToInt(res); v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "llen", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "llen", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "lpop", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(ctx, "lpop", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}
}

func TestRPopLPushSingleElement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	src := []byte("sr")

	c.Do(ctx, "lclear", src)
	if n, err := c.Do(ctx, "rpush", src, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	ttl := time.Now().Unix() + 300
	if _, err := c.Do(ctx, "lexpireat", src, ttl).Int64(); err != nil {
		t.Log("lexpireat: ", err)
	}

	if v, err := c.Do(ctx, "rpoplpush", src, src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if tl, err := c.Do(ctx, "lttl", src).Int64(); err != nil {
		t.Fatal(err)
	} else if tl == -1 || tl > ttl {
		t.Fatal(tl)
	}
}

func TestTrim(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	key := []byte("d")
	if n, err := c.Do(ctx, "rpush", key, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "ltrim", key, 2, -1).Text(); err != nil {
		t.Fatal(err)
	} else if v != "OK" {
		t.Fatal(v)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "ltrim", key, 0, 1).Text(); err != nil {
		t.Fatal(err)
	} else if v != "OK" {
		t.Fatal(v)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func TestListErrorParams(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	c := getTestConn()
	ctx := context.Background()

	if _, err := c.Do(ctx, "lpush", "test_lpush").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "rpush", "test_rpush").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lpop", "test_lpop", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "rpop", "test_rpop", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "llen", "test_llen", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lindex", "test_lindex").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lrange", "test_lrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lttl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "lpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "ltrim_front", "test_ltrimfront", "-1").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "ltrim_back", "test_ltrimback", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
