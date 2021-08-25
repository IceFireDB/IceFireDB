package main

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestKV(t *testing.T) {
	c := getConnTest()
	defer c.Close()

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
		t.Fatal(v)
	}
}
