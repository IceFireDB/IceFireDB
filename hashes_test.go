//go:build alltest
// +build alltest

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cast"
)

func TestHash(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()

	key := []byte("a")
	if n, err := c.Do(ctx, "hkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hset", key, 1, 0).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := c.Do(ctx, "hkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hexists", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hexists", key, -1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hget", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hset", key, 1, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hget", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
}

func testHashArray(ay []interface{}, checkValues ...int) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		if ay[i] == nil && checkValues[i] != 0 {
			return fmt.Errorf("must nil")
		} else if ay[i] != nil {
			d := cast.ToInt(ay[i])

			if d != checkValues[i] {
				return fmt.Errorf("invalid data %d %v != %d", i, d, checkValues[i])
			}
		}
	}
	return nil
}

func TestHashM(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()

	key := []byte("b")
	if ok, err := c.Do(ctx, "hmset", key, 1, 1, 2, 2, 3, 3).Text(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "hmget", key, 1, 2, 3, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "hdel", key, 1, 2, 3, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "hmget", key, 1, 2, 3, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashIncr(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := []byte("c")
	if n, err := c.Do(ctx, "hincrby", key, 1, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hincrby", key, 1, 10).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hincrby", key, 1, -11).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(err)
	}
}

func TestHashGetAll(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := []byte("d")

	if ok, err := c.Do(ctx, "hmset", key, 1, 1, 2, 2, 3, 3).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if v, err := c.Do(ctx, "hgetall", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "hkeys", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "hvals", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "hclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashErrorParams(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if _, err := c.Do(ctx, "hset", "test_hset").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hget", "test_hget").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hexists", "test_hexists").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hdel", "test_hdel").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hlen", "test_hlen", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hincrby", "test_hincrby").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hmset", "test_hmset").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hmset", "test_hmset", "f1", "v1", "f2").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hmget", "test_hget").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hgetall").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hkeys").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hvals").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hclear", "test_hclear", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hexpire", "test_hexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hexpireat", "test_hexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "httl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "hpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
