//go:build alltest
// +build alltest

package main

import (
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

	n, err := c.Do(context.Background(), "lindex", key, index).Int()
	if err == redis.Nil && v != 0 {
		return fmt.Errorf("must nil")
	} else if err != nil && err != redis.Nil {
		r, _ := c.Do(context.Background(), "lindex", key, index).Result()
		log.Println("lindex err: ", spew.Sdump(r))
		return err
	} else if n != v {
		return fmt.Errorf("index err number %d != %d", n, v)
	}

	return nil
}

func testListRange(key []byte, start int64, stop int64, checkValues ...int) error {
	c := getTestConn()

	res, err := c.Do(context.Background(), "lrange", key, start, stop).Result()
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
	c := getTestConn()

	key := []byte("a")
	if n, err := c.Do(context.Background(), "lkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "lpush", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "lkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(1)
	}

	if n, err := c.Do(context.Background(), "rpush", key, 2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "rpush", key, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "llen", key).Int64(); err != nil {
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
	c := getTestConn()

	key := []byte("b")
	if n, err := c.Do(context.Background(), "rpush", key, 1, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 3, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(context.Background(), "lpush", key, 1, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 6, 3, 2, 1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPop(t *testing.T) {
	c := getTestConn()

	key := []byte("c")
	if n, err := c.Do(context.Background(), "rpush", key, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "lpop", key).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "rpop", key).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if n, err := c.Do(context.Background(), "lpush", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 5, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if v, err := c.Do(context.Background(), "lpop", key).Int(); err != nil {
			t.Fatal(err)
		} else if v != i {
			t.Fatal(v)
		}
	}

	if n, err := c.Do(context.Background(), "llen", key).Int(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do(context.Background(), "rpush", key, 1, 2, 3, 4, 5)

	if n, err := c.Do(context.Background(), "lclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestRPopLPush(t *testing.T) {
	c := getTestConn()

	src := []byte("sr")
	des := []byte("de")

	c.Do(context.Background(), "lclear", src)
	c.Do(context.Background(), "lclear", des)

	if _, err := c.Do(context.Background(), "rpoplpush", src, des).Result(); err != redis.Nil {
		t.Fatal(err)
	}

	if v, err := c.Do(context.Background(), "llen", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatal(v)
	}

	if n, err := c.Do(context.Background(), "rpush", src, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "rpoplpush", src, src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "llen", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "rpoplpush", src, des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "llen", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "llen", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "lpop", des).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 5 {
		t.Fatal(v)
	}

	if v, err := c.Do(context.Background(), "lpop", src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}
}

func TestRPopLPushSingleElement(t *testing.T) {
	c := getTestConn()

	src := []byte("sr")

	c.Do(context.Background(), "lclear", src)
	if n, err := c.Do(context.Background(), "rpush", src, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	ttl := time.Now().Unix() + 300
	if _, err := c.Do(context.Background(), "lexpireat", src, ttl).Int64(); err != nil {
		t.Log("lexpireat: ", err)
	}

	if v, err := c.Do(context.Background(), "rpoplpush", src, src).Int64(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if tl, err := c.Do(context.Background(), "lttl", src).Int64(); err != nil {
		t.Fatal(err)
	} else if tl == -1 || tl > ttl {
		t.Fatal(tl)
	}
}

func TestTrim(t *testing.T) {
	c := getTestConn()

	key := []byte("d")
	if n, err := c.Do(context.Background(), "rpush", key, 1, 2, 3, 4, 5, 6).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "ltrim", key, 2, -1).Text(); err != nil {
		t.Fatal(err)
	} else if v != "OK" {
		t.Fatal(v)
	}

	if n, err := c.Do(context.Background(), "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "ltrim", key, 0, 1).Text(); err != nil {
		t.Fatal(err)
	} else if v != "OK" {
		t.Fatal(v)
	}

	if n, err := c.Do(context.Background(), "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func TestListErrorParams(t *testing.T) {
	c := getTestConn()

	if _, err := c.Do(context.Background(), "lpush", "test_lpush").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "rpush", "test_rpush").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lpop", "test_lpop", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "rpop", "test_rpop", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "llen", "test_llen", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lindex", "test_lindex").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lrange", "test_lrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lttl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "lpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "ltrim_front", "test_ltrimfront", "-1").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "ltrim_back", "test_ltrimback", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
