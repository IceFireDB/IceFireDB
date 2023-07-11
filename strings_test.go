//go:build alltest
// +build alltest

package main

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestKV(t *testing.T) {
	c := getTestConn()

	if ok, err := c.Set(context.Background(), "a", "1234", 0).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("set err")
	}

	if set, err := c.SetNX(context.Background(), "a", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != false {
		t.Fatalf("setnx err")
	}

	if set, err := c.SetNX(context.Background(), "b", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != true {
		t.Fatalf("setnx err")
	}

	if ok, err := c.SetEX(context.Background(), "mykey", "hello", 10*time.Second).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("setex err")
	}
	if v, err := c.Get(context.Background(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.GetSet(context.Background(), "a", "123").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.Get(context.Background(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "123" {
		t.Fatal(v)
	}

	if n, err := c.Exists(context.Background(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Exists(context.Background(), "empty_key_test").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := c.Del(context.Background(), "a", "b").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Exists(context.Background(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Exists(context.Background(), "b").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	rangeKey := "range_key"
	if n, err := c.Append(context.Background(), rangeKey, "Hello ").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := c.SetRange(context.Background(), rangeKey, 6, "Redis").Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := c.StrLen(context.Background(), rangeKey).Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := c.GetRange(context.Background(), rangeKey, 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if v != "Hello Redis" {
		t.Fatal(v)
	}

	bitKey := "bit_key"
	if n, err := c.SetBit(context.Background(), bitKey, 7, 1).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.GetBit(context.Background(), bitKey, 7).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.BitCount(context.Background(), bitKey, &redis.BitCount{}).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.BitPos(context.Background(), bitKey, 1).Result(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	c.Set(context.Background(), "key1", "foobar", 0)
	c.Set(context.Background(), "key2", "abcdef", 0)

	if n, err := c.BitOpAnd(context.Background(), "bit_dest_key", "key1", "key2").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Get(context.Background(), "bit_dest_key").Result(); err != nil {
		t.Fatal(err)
	} else if v != "`bc`ab" {
		t.Error(v)
	}
}

func TestKVM(t *testing.T) {
	c := getTestConn()

	if ok, err := c.MSet(context.Background(), "a", "1", "b", "2").Result(); err != nil {
		t.Error(err)
	} else if ok != "OK" {
		t.Error(ok)
	}

	if v, err := c.MGet(context.Background(), "a", "b", "c").Result(); err != nil {
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

	if n, err := c.Incr(context.Background(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.Incr(context.Background(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 2 {
		t.Error(n)
	}

	if n, err := c.Decr(context.Background(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.IncrBy(context.Background(), "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 11 {
		t.Error(n)
	}

	if n, err := c.DecrBy(context.Background(), "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}
}

func TestKVErrorParams(t *testing.T) {
	c := getTestConn()

	if _, err := c.Do(context.Background(), "get", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "set", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "getset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "setnx", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "exists", "a", "b").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "incr", "a", "b").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "incrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "decrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(context.Background(), "del").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "mset").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "mset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "mget").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "expire").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "expire", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "expireat").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "expireat", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "ttl").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "persist").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "setex", "a", "blah", "hello world").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}
}
