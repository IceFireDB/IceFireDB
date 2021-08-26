package main

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestKV(t *testing.T) {
	c := getTestConn()

	if ok, err := c.Set(c.Context(), "a", "1234", 0).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("set err")
	}

	if set, err := c.SetNX(c.Context(), "a", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != false {
		t.Fatalf("setnx err")
	}

	if set, err := c.SetNX(c.Context(), "b", "123", 0).Result(); err != nil {
		t.Fatal(err)
	} else if set != true {
		t.Fatalf("setnx err")
	}

	if ok, err := c.SetEX(c.Context(), "mykey", "hello", 10).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatalf("setex err")
	}

	if v, err := c.Get(c.Context(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.GetSet(c.Context(), "a", "123").Result(); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	if v, err := c.Get(c.Context(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if v != "123" {
		t.Fatal(v)
	}

	if n, err := c.Exists(c.Context(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Exists(c.Context(), "empty_key_test").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := c.Del(c.Context(), "a", "b").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Exists(c.Context(), "a").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Exists(c.Context(), "b").Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	rangeKey := "range_key"
	if n, err := c.Append(c.Context(), rangeKey, "Hello ").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := c.SetRange(c.Context(), rangeKey, 6, "Redis").Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := c.StrLen(c.Context(), rangeKey).Result(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if v, err := c.GetRange(c.Context(), rangeKey, 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if v != "Hello Redis" {
		t.Fatal(v)
	}

	bitKey := "bit_key"
	if n, err := c.SetBit(c.Context(), bitKey, 7, 1).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.GetBit(c.Context(), bitKey, 7).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.BitCount(c.Context(), bitKey, &redis.BitCount{}).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.BitPos(c.Context(), bitKey, 1).Result(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	c.Set(c.Context(), "key1", "foobar", 0)
	c.Set(c.Context(), "key2", "abcdef", 0)

	if n, err := c.BitOpAnd(c.Context(), "bit_dest_key", "key1", "key2").Result(); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := c.Get(c.Context(), "bit_dest_key").Result(); err != nil {
		t.Fatal(err)
	} else if v != "`bc`ab" {
		t.Error(v)
	}
}

func TestKVM(t *testing.T) {
	c := getTestConn()

	if ok, err := c.MSet(c.Context(), "a", "1", "b", "2").Result(); err != nil {
		t.Error(err)
	} else if ok != "OK" {
		t.Error(ok)
	}

	if v, err := c.MGet(c.Context(), "a", "b", "c").Result(); err != nil {
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

	if n, err := c.Incr(c.Context(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.Incr(c.Context(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 2 {
		t.Error(n)
	}

	if n, err := c.Decr(c.Context(), "n").Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}

	if n, err := c.IncrBy(c.Context(), "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 11 {
		t.Error(n)
	}

	if n, err := c.DecrBy(c.Context(), "n", 10).Result(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error(n)
	}
}

func TestKVErrorParams(t *testing.T) {
	c := getTestConn()

	if _, err := c.Do(c.Context(), "get", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "set", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "getset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "setnx", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "exists", "a", "b").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "incr", "a", "b").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "incrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "decrby", "a").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

	if _, err := c.Do(c.Context(), "del").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "mset").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "mset", "a", "b", "c").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "mget").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "expire").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "expire", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "expireat").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "expireat", "a", "b").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "ttl").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "persist").Result(); err == nil {
		t.Errorf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "setex", "a", "blah", "hello world").Result(); err == nil {
		t.Errorf("invalid err %v", err)
	}

}
