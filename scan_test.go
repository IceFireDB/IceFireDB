package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestScan(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()

	c.FlushAll(ctx)
	testKVScan(t, c)
	testHashKeyScan(t, c)
	testListKeyScan(t, c)
	testZSetKeyScan(t, c)
	testSetKeyScan(t, c)
	c.FlushAll(ctx)
}

func checkScanValues(t *testing.T, ay interface{}, values ...interface{}) {
	a, ok := ay.([]interface{})

	if !ok {
		t.Fatal("checkScanValues result not array", ay)
	}

	if len(a) != len(values) {
		t.Fatal(fmt.Sprintf("len %d != %d", len(a), len(values)))
	}

	for i, v := range a {
		if v, ok := v.(string); !ok || v != fmt.Sprintf("%v", values[i]) {
			t.Fatal(fmt.Sprintf("%d %s != %v", i, v, values[i]))
		}
	}
}

func checkScan(t *testing.T, c *redis.Client, tp string) {
	ctx := context.Background()
	if ay, err := c.Do(ctx, "XSCAN", tp, "", "count", 5).Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].(string); n != "4" {
		t.Fatal(n)
	} else {
		checkScanValues(t, ay[1], "0", "1", "2", "3", "4")
	}

	if ay, err := c.Do(ctx, "XSCAN", tp, "4", "count", 6).Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].(string); n != "" {
		t.Fatal(n)
	} else {
		checkScanValues(t, ay[1], 5, 6, 7, 8, 9)
	}
}

func testKVScan(t *testing.T, c *redis.Client) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := c.Do(ctx, "set", fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}
	checkScan(t, c, "KV")
}

func testHashKeyScan(t *testing.T, c *redis.Client) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := c.Do(ctx, "hset", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "HASH")
}

func testListKeyScan(t *testing.T, c *redis.Client) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := c.Do(ctx, "lpush", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "LIST")
}

func testZSetKeyScan(t *testing.T, c *redis.Client) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := c.Do(ctx, "zadd", fmt.Sprintf("%d", i), i, []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "ZSET")
}

func testSetKeyScan(t *testing.T, c *redis.Client) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := c.Do(ctx, "sadd", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "SET")
}

func TestXHashScan(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := "scan_hash"
	c.Do(ctx, "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(ctx, "XHSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestHashScan(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := "scan_hash"
	c.Do(ctx, "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(ctx, "HSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestXSetScan(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := "scan_set"
	c.Do(ctx, "SADD", key, "a", "b")

	if ay, err := c.Do(ctx, "XSSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}
}

func TestSetScan(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := "scan_set"
	c.Do(ctx, "SADD", key, "a", "b")

	if ay, err := c.Do(ctx, "SSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}
}

func TestXZSetScan(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "scan_zset"
	c.Do(ctx, "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(ctx, "XZSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestZSetScan(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "scan_zset"
	c.Do(ctx, "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(ctx, "ZSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestStandardSCAN(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	c.FlushAll(ctx)

	// Setup test data
	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("test_key_%d", i), "value", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	defer c.FlushAll(ctx)

	// Test basic SCAN
	ay, err := c.Do(ctx, "SCAN", "0").Result()
	if err != nil {
		t.Fatal(err)
	}

	// Result format: [cursor, [key1, key2, ...]]
	if result, ok := ay.([]interface{}); !ok || len(result) < 1 {
		t.Fatal("Expected array with at least cursor")
	} else if cursor, ok := result[0].(string); !ok {
		t.Fatal("Expected string cursor")
	} else if cursor == "" {
		t.Fatal("Expected non-empty cursor initially")
	} else if cursor == "0" && len(result) < 2 {
		t.Fatal("Expected at least one key when cursor is 0")
	}

	// Test SCAN with MATCH
	ay, err = c.Do(ctx, "SCAN", "0", "MATCH", "test_key_[01]").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok := ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("Expected array with cursor and at least one key")
	}

	// Get keys array from result[1]
	keys, ok := result[1].([]interface{})
	if !ok {
		t.Fatal("Expected array of keys as second element")
	}

	// Verify matched keys
	foundKeys := 0
	for _, keyIntf := range keys {
		if key, ok := keyIntf.(string); ok {
			t.Logf("Found key: %s", key)
			// Pattern "test_key_[01]" should match keys starting with "test_key_"
			// followed by '0' or '1' as the last character
			if len(key) == 10 && key[:9] == "test_key_" && (key[9] == '0' || key[9] == '1') {
				foundKeys++
			}
		}
	}

	if foundKeys < 1 {
		t.Fatal("Expected to find matched keys")
	}

	// Test SCAN with COUNT
	ay, err = c.Do(ctx, "SCAN", "0", "COUNT", "2").Result()
	if err != nil {
		t.Fatal(err)
	}

	result = ay.([]interface{})
	if len(result) >= 2 {
		keys, ok = result[1].([]interface{})
		if ok && len(keys) > 2 { // max 2 keys
			t.Fatalf("Expected at most 2 keys, got %d", len(keys))
		}
	}

	// Test SCAN with TYPE
	ay, err = c.Do(ctx, "SCAN", "0", "TYPE", "STRING").Result()
	if err != nil {
		t.Fatal(err)
	}

	result = ay.([]interface{})
	if len(result) < 1 {
		t.Fatal("Expected at least cursor")
	}
}

func TestScanArgsParsing(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	c.FlushAll(ctx)
	defer c.FlushAll(ctx)

	// Setup test data
	for i := 0; i < 5; i++ {
		if err := c.Set(ctx, fmt.Sprintf("arg_test_key_%d", i), "value", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}

	// Test SCAN with multiple options
	ay, err := c.Do(ctx, "SCAN", "0", "MATCH", "arg_test_key_*", "COUNT", "5", "TYPE", "STRING").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok := ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("Expected array with cursor and keys")
	}

	// Test SCAN with zero cursor (should work)
	ay, err = c.Do(ctx, "SCAN", "0").Result()
	if err != nil {
		t.Fatal(err)
	}

	// Test SCAN with non-zero cursor
	if result, ok := ay.([]interface{}); ok && len(result) >= 2 {
		if cursor, ok := result[0].(string); ok && cursor != "" {
			ay, err = c.Do(ctx, "SCAN", cursor).Result()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Test HSCAN with MATCH and COUNT
	key := "hscan_test_key"
	c.Do(ctx, "HMSET", key, "field1", "value1", "field2", "value2")
	ay, err = c.Do(ctx, "HSCAN", key, "0", "MATCH", "field*", "COUNT", "10").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("HSCAN should return array with cursor and field-value pairs")
	}

	// Test SSCAN with MATCH and COUNT
	skey := "sscan_test_key"
	c.Do(ctx, "SADD", skey, "member1", "member2")
	ay, err = c.Do(ctx, "SSCAN", skey, "0", "MATCH", "member*", "COUNT", "10").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("SSCAN should return array with cursor and members")
	}

	// Test ZSCAN with MATCH and COUNT
	zkey := "zscan_test_key"
	c.Do(ctx, "ZADD", zkey, 1, "member1", 2, "member2")
	ay, err = c.Do(ctx, "ZSCAN", zkey, "0", "MATCH", "member*", "COUNT", "10").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("ZSCAN should return array with cursor and member-score pairs")
	}

	// Test XSCAN with DESC option
	keyType := "KV"
	ay, err = c.Do(ctx, "XSCAN", keyType, "", "COUNT", "3", "DESC").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("XSCAN with DESC should return array")
	}

	// Test XSCAN with ASC option
	ay, err = c.Do(ctx, "XSCAN", keyType, "", "COUNT", "3", "ASC").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("XSCAN with ASC should return array")
	}

	// Test XHSCAN with DESC
	hxKey := "xhscan_test"
	c.Do(ctx, "HMSET", hxKey, "f1", "v1", "f2", "v2")
	ay, err = c.Do(ctx, "XHSCAN", hxKey, "", "COUNT", "5", "DESC").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("XHSCAN with DESC should return array")
	}

	// Test XSSCAN with DESC
	sxKey := "xsscan_test"
	c.Do(ctx, "SADD", sxKey, "m1", "m2", "m3")
	ay, err = c.Do(ctx, "XSSCAN", sxKey, "", "COUNT", "5", "DESC").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("XSSCAN with DESC should return array")
	}

	// Test XZSCAN with DESC
	zxKey := "xzscan_test"
	c.Do(ctx, "ZADD", zxKey, 1, "m1", 2, "m2", 3, "m3")
	ay, err = c.Do(ctx, "XZSCAN", zxKey, "", "COUNT", "5", "DESC").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok || len(result) < 2 {
		t.Fatal("XZSCAN with DESC should return array")
	}

	// Test XSCAN with empty MATCH (should match all)
	ay, err = c.Do(ctx, "XSCAN", keyType, "", "MATCH", "", "COUNT", "5").Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok = ay.([]interface{})
	if !ok {
		t.Fatal("XSCAN with empty MATCH should return array")
	}
}
