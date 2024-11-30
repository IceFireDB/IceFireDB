//go:build alltest
// +build alltest

package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestKV(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if ok, err := c.Set(ctx, "a", "1234", 0).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("set err")
	}

	if set, err := c.SetNX(ctx, "a", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != false {
		t.Fatalf("setnx err")
	}

	if set, err := c.SetNX(ctx, "b", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != true {
		t.Fatalf("setnx err")
	}

	if ok, err := c.SetEx(ctx, "mykey", "hello", 10*time.Second).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("setex err")
	}
	if v, err := c.Get(ctx, "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.GetSet(ctx, "a", "123").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.Get(ctx, "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "123" {
		t.Fatal(v)
	}

	if n, err := c.Exists(ctx, "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Exists(ctx, "a", "b").Result(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Exists(ctx, "a", "b", "c", "d").Result(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Exists(ctx, "c", "d").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Exists(ctx, "empty_key_test").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := c.Del(ctx, "a", "b").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Exists(ctx, "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Exists(ctx, "b").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	rangeKey := "range_key"
	if n, err := c.Append(ctx, rangeKey, "Hello ").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := c.SetRange(ctx, rangeKey, 6, "Redis").Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := c.StrLen(ctx, rangeKey).Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := c.GetRange(ctx, rangeKey, 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if v != "Hello Redis" {
		t.Fatal(v)
	}

	// Set up test data
	bitKey := "test:bitcount:key"

	// Set bits at positions 0, 7, and 14
	for _, pos := range []int{0, 7, 14} {
		if n, err := c.SetBit(context.Background(), bitKey, int64(pos), 1).Result(); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	// Verify the bits at positions 0, 7, and 14
	for _, pos := range []int{0, 7, 14} {
		if n, err := c.GetBit(context.Background(), bitKey, int64(pos)).Result(); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	// Test BITCOUNT with no start or end
	if n, err := c.BitCount(context.Background(), bitKey, nil).Result(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}

	// Test BITCOUNT with start and end in BYTE mode :first byte
	if n, err := c.BitCount(context.Background(), bitKey, &redis.BitCount{Start: 0, End: 0, Unit: "BYTE"}).Result(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}

	// Test BITCOUNT with start and end in BIT mode
	if n, err := c.BitCount(context.Background(), bitKey, &redis.BitCount{Start: 0, End: 15, Unit: "BIT"}).Result(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}

	// Test BITPOS with only bit provided
	if n, err := c.BitPos(context.Background(), bitKey, 1).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	// Test BITPOS with start provided
	if n, err := c.BitPos(context.Background(), bitKey, 1, 0).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	// Test BITPOS with start and end provided
	if n, err := c.BitPos(context.Background(), bitKey, 1, 0, 15).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	// Test BITPOS with start, end, and bitMode provided
	if n, err := c.BitPos(context.Background(), bitKey, 1, 0, 15, 8).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	c.Set(ctx, "key1", "foobar", 0)
	c.Set(ctx, "key2", "abcdef", 0)

	if n, err := c.BitOpAnd(ctx, "bit_dest_key", "key1", "key2").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Get(ctx, "bit_dest_key").Result(); err != nil {
		t.Fatal(err)
	} else if v != "`bc`ab" {
		t.Error(v)
	}
}

func TestKVM(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if ok, err := c.MSet(ctx, "a", "1", "b", "2").Result(); err != nil {
		t.Error(err)
	} else if ok != "OK" {
		t.Error(ok)
	}

	if v, err := c.MGet(ctx, "a", "b", "c").Result(); err != nil {
		t.Error(err)
	} else if len(v) != 3 {
		t.Error(len(v))
	} else {
		if vv, ok := v[0].(string); !ok || vv != "1" {
			t.Error("not 1")
		}

		if vv, ok := v[1].(string); !ok || vv != "2" {
			t.Error("not 2")
		}

		if v[2] != "" {
			t.Error("must nil")
		}
	}
}

func TestKVIncrDecr(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	if n, err := c.Incr(ctx, "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.Incr(ctx, "n").Result(); err != nil {
		t.Error(err)
	} else if n != 2 {
		t.Error(n)
	}

	if n, err := c.Decr(ctx, "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.IncrBy(ctx, "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 11 {
		t.Error(n)
	}

	if n, err := c.DecrBy(ctx, "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}
}

func TestKVErrorParams(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if _, err := c.Do(ctx, "get", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "set", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "getset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "setnx", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "incr", "a", "b").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "incrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "decrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(ctx, "del").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "mset").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "mset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "mget").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "expire").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "expire", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "expireat").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "expireat", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "ttl").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "persist").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "setex", "a", "blah", "hello world").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}
}
