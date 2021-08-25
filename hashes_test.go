package main

import (
	"fmt"
	"testing"

	"github.com/spf13/cast"
)

func TestHash(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("a")
	if n, err := c.Do(c.Context(), "hkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hset", key, 1, 0).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := c.Do(c.Context(), "hkeyexists", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hexists", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hexists", key, -1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hget", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hset", key, 1, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hget", key, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
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
	defer c.Close()

	key := []byte("b")
	if ok, err := c.Do(c.Context(), "hmset", key, 1, 1, 2, 2, 3, 3).Text(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if v, err := c.Do(c.Context(), "hmget", key, 1, 2, 3, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(c.Context(), "hdel", key, 1, 2, 3, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if v, err := c.Do(c.Context(), "hmget", key, 1, 2, 3, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashIncr(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("c")
	if n, err := c.Do(c.Context(), "hincrby", key, 1, 1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(err)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hincrby", key, 1, 10).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(err)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hincrby", key, 1, -11).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(err)
	}

}

func TestHashGetAll(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("d")

	if ok, err := c.Do(c.Context(), "hmset", key, 1, 1, 2, 2, 3, 3).Result(); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if v, err := c.Do(c.Context(), "hgetall", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(c.Context(), "hkeys", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(c.Context(), "hvals", key).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(cast.ToSlice(v), 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(c.Context(), "hclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(c.Context(), "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := c.Do(c.Context(), "hset", "test_hset").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hget", "test_hget").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hexists", "test_hexists").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hdel", "test_hdel").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hlen", "test_hlen", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hincrby", "test_hincrby").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hmset", "test_hmset").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hmset", "test_hmset", "f1", "v1", "f2").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hmget", "test_hget").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hgetall").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hkeys").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hvals").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hclear", "test_hclear", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hexpire", "test_hexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hexpireat", "test_hexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "httl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(c.Context(), "hpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
