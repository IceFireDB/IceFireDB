//go:build alltest
// +build alltest

package main

import (
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestScan(t *testing.T) {
	c := getTestConn()
	c.FlushAll(context.Background())
	testKVScan(t, c)
	testHashKeyScan(t, c)
	testListKeyScan(t, c)
	testZSetKeyScan(t, c)
	testSetKeyScan(t, c)
	c.FlushAll(context.Background())
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
	if ay, err := c.Do(context.Background(), "XSCAN", tp, "", "count", 5).Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].(string); n != "4" {
		t.Fatal(n)
	} else {
		checkScanValues(t, ay[1], "0", "1", "2", "3", "4")
	}

	if ay, err := c.Do(context.Background(), "XSCAN", tp, "4", "count", 6).Result(); err != nil {
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
	for i := 0; i < 10; i++ {
		if err := c.Do(context.Background(), "set", fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}
	checkScan(t, c, "KV")
}

func testHashKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(context.Background(), "hset", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "HASH")
}

func testListKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(context.Background(), "lpush", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "LIST")
}

func testZSetKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(context.Background(), "zadd", fmt.Sprintf("%d", i), i, []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "ZSET")
}

func testSetKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(context.Background(), "sadd", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "SET")
}

func TestXHashScan(t *testing.T) {
	c := getTestConn()

	key := "scan_hash"
	c.Do(context.Background(), "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(context.Background(), "XHSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestHashScan(t *testing.T) {
	c := getTestConn()

	key := "scan_hash"
	c.Do(context.Background(), "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(context.Background(), "HSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestXSetScan(t *testing.T) {
	c := getTestConn()

	key := "scan_set"
	c.Do(context.Background(), "SADD", key, "a", "b")

	if ay, err := c.Do(context.Background(), "XSSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}
}

func TestSetScan(t *testing.T) {
	c := getTestConn()

	key := "scan_set"
	c.Do(context.Background(), "SADD", key, "a", "b")

	if ay, err := c.Do(context.Background(), "SSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "b")
	}
}

func TestXZSetScan(t *testing.T) {
	c := getTestConn()

	key := "scan_zset"
	c.Do(context.Background(), "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(context.Background(), "XZSCAN", key, "").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}

func TestZSetScan(t *testing.T) {
	c := getTestConn()

	key := "scan_zset"
	c.Do(context.Background(), "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(context.Background(), "XZSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}
