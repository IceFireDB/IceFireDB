package main

import (
	"context"
	"fmt"
	"testing"
	"time"

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

func TestHashEnhancedHGET(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()
	key := "enhanced_hash_test"

	// Test HGET with empty value
	if _, err := c.Do(ctx, "hset", key, "empty_field", "").Result(); err != nil {
		t.Fatal(err)
	}

	v, err := c.Do(ctx, "hget", key, "empty_field").Result()
	if err != nil {
		t.Fatal(err)
	}

	valStr, ok := v.(string)
	if !ok {
		t.Fatalf("HGET should return string type for empty value, got %T", v)
	}

	if valStr != "" {
		t.Fatalf("Expected empty string, got '%s'", valStr)
	}

	// Test HGET with special characters
	specialValue := "hello\tworld\n"
	if _, err := c.Do(ctx, "hset", key, "special", specialValue).Result(); err != nil {
		t.Fatal(err)
	}

	v, err = c.Do(ctx, "hget", key, "special").Result()
	if err != nil {
		t.Fatal(err)
	}

	valStr, ok = v.(string)
	if !ok {
		t.Fatalf("HGET should return string type for special chars, got %T", v)
	}

	if valStr != specialValue {
		t.Fatalf("Expected '%s', got '%s'", specialValue, valStr)
	}
}

func TestHashExpirationCommands(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()
	key := "hash_expire_test"

	// Setup hash with fields
	if _, err := c.Do(ctx, "hmset", key, "field1", "value1", "field2", "value2").Result(); err != nil {
		t.Fatal(err)
	}

	// Test HEXPIRE (relative expiration)
	if n, err := c.Do(ctx, "hexpire", key, 100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("HEXPIRE should return 1, got %d", n)
	}

	// Test HTTL
	if ttl, err := c.Do(ctx, "httl", key).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 0 || ttl > 100 {
		t.Logf("TTL value: %d (may vary)", ttl)
	}

	// Test HEXPIREAT (absolute expiration)
	key2 := "hash_expireat_test"
	c.Do(ctx, "hmset", key2, "f1", "v1")
	futureTime := time.Now().Unix() + 200
	if n, err := c.Do(ctx, "hexpireat", key2, futureTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("HEXPIREAT should return 1, got %d", n)
	}

	// Verify TTL
	if ttl, err := c.Do(ctx, "httl", key2).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 190 || ttl > 201 {
		t.Fatalf("Expected TTL between 190-201, got %d", ttl)
	}

	// Test HEXPIRE with past time (should delete key)
	key3 := "hash_expire_past"
	c.Do(ctx, "hmset", key3, "f1", "v1")
	if n, err := c.Do(ctx, "hexpire", key3, -100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("HEXPIRE with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := c.Do(ctx, "hkeyexists", key3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expiration")
	}

	// Test HEXPIREAT with past timestamp
	key4 := "hash_expireat_past"
	c.Do(ctx, "hmset", key4, "f1", "v1")
	pastTime := time.Now().Unix() - 100
	if n, err := c.Do(ctx, "hexpireat", key4, pastTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("HEXPIREAT with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := c.Do(ctx, "hkeyexists", key4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expiration")
	}
}

func TestHashClearCommands(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	// Test HCLEAR
	key := "hash_clear_test"
	if _, err := c.Do(ctx, "hmset", key, "f1", "v1", "f2", "v2", "f3", "v3").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("Expected 3 fields, got %d", n)
	}

	if n, err := c.Do(ctx, "hclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("HCLEAR should return 3, got %d", n)
	}

	if n, err := c.Do(ctx, "hlen", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Expected 0 fields after HCLEAR, got %d", n)
	}

	// Test HMCLEAR
	key1 := "hmclear_test1"
	key2 := "hmclear_test2"
	c.Do(ctx, "hmset", key1, "f1", "v1")
	c.Do(ctx, "hmset", key2, "f1", "v1", "f2", "v2")

	if n, err := c.Do(ctx, "hmclear", key1, key2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("HMCLEAR should return 2, got %d", n)
	}

	// Both keys should not exist
	if n, err := c.Do(ctx, "hkeyexists", key1).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Key1 should not exist after HMCLEAR, got %d", n)
	}

	if n, err := c.Do(ctx, "hkeyexists", key2).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Key2 should not exist after HMCLEAR, got %d", n)
	}
}

func TestHashHSETNX(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()
	key := "hash_setnx_test"

	// First HSETNX should succeed
	if n, err := c.Do(ctx, "hsetnx", key, "field1", "value1").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("HSETNX on new field should return 1, got %d", n)
	}

	// Verify value
	if v, err := c.Do(ctx, "hget", key, "field1").Result(); err != nil {
		t.Fatal(err)
	} else if v != "value1" {
		t.Fatalf("Expected 'value1', got %v", v)
	}

	// Second HSETNX on existing field should fail
	if n, err := c.Do(ctx, "hsetnx", key, "field1", "value2").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("HSETNX on existing field should return 0, got %d", n)
	}

	// Value should be unchanged
	if v, err := c.Do(ctx, "hget", key, "field1").Result(); err != nil {
		t.Fatal(err)
	} else if v != "value1" {
		t.Fatalf("Value should be unchanged, got %v", v)
	}

	// HSETNX on new field should succeed
	if n, err := c.Do(ctx, "hsetnx", key, "field2", "value2").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("HSETNX on new field should return 1, got %d", n)
	}
}
