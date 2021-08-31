package main

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestScan(t *testing.T) {
	c := getTestConn()
	c.FlushAll(c.Context())
	testKVScan(t, c)
	testHashKeyScan(t, c)
	testListKeyScan(t, c)
	testZSetKeyScan(t, c)
	testSetKeyScan(t, c)
	c.FlushAll(c.Context())
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
	if ay, err := c.Do(c.Context(), "XSCAN", tp, "", "count", 5).Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else if n := ay[0].(string); n != "4" {
		t.Fatal(n)
	} else {
		checkScanValues(t, ay[1], "0", "1", "2", "3", "4")
	}

	if ay, err := c.Do(c.Context(), "XSCAN", tp, "4", "count", 6).Result(); err != nil {
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
		if err := c.Do(c.Context(), "set", fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}
	checkScan(t, c, "KV")
}

func testHashKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(c.Context(), "hset", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "HASH")
}

func testListKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(c.Context(), "lpush", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "LIST")
}

func testZSetKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(c.Context(), "zadd", fmt.Sprintf("%d", i), i, []byte("value")).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "ZSET")
}

func testSetKeyScan(t *testing.T, c *redis.Client) {
	for i := 0; i < 10; i++ {
		if err := c.Do(c.Context(), "sadd", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	checkScan(t, c, "SET")
}

func TestXHashScan(t *testing.T) {
	c := getTestConn()

	key := "scan_hash"
	c.Do(c.Context(), "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(c.Context(), "XHSCAN", key, "").Result(); err != nil {
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
	c.Do(c.Context(), "HMSET", key, "a", 1, "b", 2)

	if ay, err := c.Do(c.Context(), "HSCAN", key, "0").Result(); err != nil {
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
	c.Do(c.Context(), "SADD", key, "a", "b")

	if ay, err := c.Do(c.Context(), "XSSCAN", key, "").Result(); err != nil {
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
	c.Do(c.Context(), "SADD", key, "a", "b")

	if ay, err := c.Do(c.Context(), "SSCAN", key, "0").Result(); err != nil {
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
	c.Do(c.Context(), "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(c.Context(), "XZSCAN", key, "").Result(); err != nil {
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
	c.Do(c.Context(), "ZADD", key, 1, "a", 2, "b")

	if ay, err := c.Do(c.Context(), "XZSCAN", key, "0").Result(); err != nil {
		t.Fatal(err)
	} else if ay, ok := ay.([]interface{}); !ok || len(ay) != 2 {
		t.Fatal(len(ay))
	} else {
		checkScanValues(t, ay[1], "a", "1", "b", "2")
	}
}
