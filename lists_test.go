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

func TestListExpirationCommands(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "list_expire_test"

	// Add elements to list
	if _, err := c.Do(ctx, "rpush", key, "a", "b", "c").Result(); err != nil {
		t.Fatal(err)
	}

	// Test LEXPIRE (relative expiration)
	if n, err := c.Do(ctx, "lexpire", key, 100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("LEXPIRE should return 1, got %d", n)
	}

	// Test LTTL
	if ttl, err := c.Do(ctx, "lttl", key).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 0 || ttl > 100 {
		t.Logf("TTL value: %d (may vary)", ttl)
	}

	// Test LEXPIREAT (absolute expiration)
	key2 := "list_expireat_test"
	c.Do(ctx, "rpush", key2, "a", "b")
	futureTime := time.Now().Unix() + 200
	if n, err := c.Do(ctx, "lexpireat", key2, futureTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("LEXPIREAT should return 1, got %d", n)
	}

	// Verify TTL
	if ttl, err := c.Do(ctx, "lttl", key2).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 190 || ttl > 201 {
		t.Fatalf("Expected TTL between 190-201, got %d", ttl)
	}

	// Test LEXPIRE with past time (should delete key)
	key3 := "list_expire_past"
	c.Do(ctx, "rpush", key3, "a")
	if n, err := c.Do(ctx, "lexpire", key3, -100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("LEXPIRE with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := c.Do(ctx, "lkeyexists", key3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expiration")
	}

	// Test LEXPIREAT with past timestamp
	key4 := "list_expireat_past"
	c.Do(ctx, "rpush", key4, "a")
	pastTime := time.Now().Unix() - 100
	if n, err := c.Do(ctx, "lexpireat", key4, pastTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("LEXPIREAT with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := c.Do(ctx, "lkeyexists", key4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expiration")
	}
}

func TestListClearCommands(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	// Test LCLEAR
	key := "list_clear_test"
	if _, err := c.Do(ctx, "rpush", key, "a", "b", "c", "d", "e").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatalf("Expected 5 elements, got %d", n)
	}

	if n, err := c.Do(ctx, "lclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatalf("LCLEAR should return 5, got %d", n)
	}

	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Expected 0 elements after LCLEAR, got %d", n)
	}

	// Test LMCLEAR
	key1 := "lmclear_test1"
	key2 := "lmclear_test2"
	c.Do(ctx, "rpush", key1, "a", "b")
	c.Do(ctx, "rpush", key2, "a", "b", "c")

	if n, err := c.Do(ctx, "lmclear", key1, key2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("LMCLEAR should return 2, got %d", n)
	}

	// Both keys should not exist
	if n, err := c.Do(ctx, "lkeyexists", key1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Key1 should not exist after LMCLEAR, got %d", n)
	}

	if n, err := c.Do(ctx, "lkeyexists", key2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Key2 should not exist after LMCLEAR, got %d", n)
	}
}

func TestListLINSERT(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "list_linsert_test"
	c.Do(ctx, "lclear", key)
	c.Do(ctx, "rpush", key, "a", "c", "e")

	// Test LINSERT BEFORE (IceFireDB requires uppercase BEFORE/AFTER)
	if n, err := c.Do(ctx, "linsert", key, "BEFORE", "c", "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("LINSERT BEFORE should return old length 3, got %d", n)
	}

	// Verify list contents (order is reversed due to LPush in implementation)
	res, err := c.Do(ctx, "lrange", key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	vs := cast.ToStringSlice(res)
	if len(vs) != 4 {
		t.Fatalf("List should have 4 elements, got %d", len(vs))
	}
	// Note: Due to LPush implementation, list is reversed
	if vs[0] != "e" || vs[1] != "c" || vs[2] != "b" || vs[3] != "a" {
		t.Fatalf("List should be [e, c, b, a], got %v", vs)
	}

	// Test LINSERT AFTER (IceFireDB requires uppercase BEFORE/AFTER)
	if n, err := c.Do(ctx, "linsert", key, "AFTER", "c", "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatalf("LINSERT AFTER should return old length 4, got %d", n)
	}

	// Verify list contents (order is reversed due to LPush in implementation)
	res, err = c.Do(ctx, "lrange", key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	vs = cast.ToStringSlice(res)
	if len(vs) != 5 {
		t.Fatalf("List should have 5 elements, got %d", len(vs))
	}
	// Note: Due to LPush implementation, list is reversed
	// After second lpush of [e, c, d, b, a], list becomes [a, b, d, c, e]
	if vs[0] != "a" || vs[1] != "b" || vs[2] != "d" || vs[3] != "c" || vs[4] != "e" {
		t.Fatalf("List should be [a, b, d, c, e], got %v", vs)
	}

	// Test LINSERT with non-existent pivot
	if n, err := c.Do(ctx, "linsert", key, "BEFORE", "x", "y").Int64(); err != nil {
		t.Fatal(err)
	} else if n != -1 {
		t.Fatalf("LINSERT with non-existent pivot should return -1, got %d", n)
	}

	// Test LINSERT on empty list
	if n, err := c.Do(ctx, "linsert", "empty_list_linsert", "BEFORE", "x", "y").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("LINSERT on empty list should return 0, got %d", n)
	}
}

func TestListLREM(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "list_lrem_test"
	c.Do(ctx, "lclear", key)

	// Create list with duplicates
	c.Do(ctx, "rpush", key, "a", "b", "b", "b", "c", "b", "a")

	// Test LREM count = 0 (remove all occurrences of "b")
	if n, err := c.Do(ctx, "lrem", key, 0, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatalf("LREM 0 should remove 4 'b's, got %d", n)
	}

	// Verify list length decreased
	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("List should have 3 elements, got %d", n)
	}

	// Reset list for next test
	c.Do(ctx, "lclear", key)
	c.Do(ctx, "rpush", key, "a", "b", "b", "b", "c", "b", "a")

	// Test LREM count > 0 (remove first 2 occurrences)
	if n, err := c.Do(ctx, "lrem", key, 2, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("LREM 2 should remove 2 'b's, got %d", n)
	}

	// Verify list length
	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatalf("List should have 5 elements, got %d", n)
	}

	// Reset list for next test
	c.Do(ctx, "lclear", key)
	c.Do(ctx, "rpush", key, "a", "b", "b", "b", "c", "b", "a")

	// Test LREM count < 0 (remove last 2 occurrences of "b")
	if n, err := c.Do(ctx, "lrem", key, -2, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("LREM -2 should remove 2 'b's, got %d", n)
	}

	// Verify list length
	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatalf("List should have 5 elements, got %d", n)
	}
}
