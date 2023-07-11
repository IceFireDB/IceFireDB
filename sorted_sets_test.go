//go:build alltest
// +build alltest

package main

import (
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

func TestZSet(t *testing.T) {
	c := getTestConn()

	key := []byte("myzset")

	if n, err := c.Do(context.Background(), "zadd", key, 3, "a", 4, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zadd", key, 1, "a", 2, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zadd", key, 3, "c", 4, "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := c.Do(context.Background(), "zscore", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if n, err := c.Do(context.Background(), "zrem", key, "d", "e").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zincrby", key, 4, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zincrby", key, -4, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zincrby", key, 4, "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zrem", key, "a", "b", "c", "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZSetCount(t *testing.T) {
	c := getTestConn()

	key := []byte("myzset")
	if _, err := c.Do(context.Background(), "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(context.Background(), "zcount", key, 2, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, 4, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, 4, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, "(2", 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, "2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, "(2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcount", key, "-inf", "+inf").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	c.Do(context.Background(), "zadd", key, 3, "e")

	if n, err := c.Do(context.Background(), "zcount", key, "(2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	c.Do(context.Background(), "zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetRank(t *testing.T) {
	c := getTestConn()

	key := []byte("myzset")
	if _, err := c.Do(context.Background(), "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(context.Background(), "zrank", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if _, err := c.Do(context.Background(), "zrank", key, "e").Result(); err != redis.Nil {
		t.Fatal(err)
	}

	if n, err := c.Do(context.Background(), "zrevrank", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := c.Do(context.Background(), "zrevrank", key, "e").Result(); err != redis.Nil {
		t.Fatal(err)
	}
}

func testZSetRange(ay []interface{}, checkValues ...interface{}) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		switch cv := checkValues[i].(type) {
		case string:
			v := cast.ToString(ay[i])
			if v != cv {
				return fmt.Errorf("not equal %s != %s", v, checkValues[i])
			}
		default:
			if s, _ := cast.ToIntE(ay[i]); s != checkValues[i] {
				return fmt.Errorf("not equal %v != %v", s, checkValues[i])
			}
		}

	}

	return nil
}

func TestZSetRangeScore(t *testing.T) {
	c := getTestConn()

	key := []byte("myzset_range")
	if _, err := c.Do(context.Background(), "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if v, err := c.Do(context.Background(), "zrangebyscore", key, 1, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrangebyscore", key, 1, 4, "withscores", "limit", 1, 2).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "c", 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrangebyscore", key, "-inf", "+inf", "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrangebyscore", key, "(1", "(4").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", "c"); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrangebyscore", key, 4, 1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrangebyscore", key, 4, 1, "withscores", "limit", 1, 2).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", 3, "b", 2); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrangebyscore", key, "+inf", "-inf", "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrangebyscore", key, "(4", "(1").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(context.Background(), "zremrangebyscore", key, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "zrangebyscore", key, 1, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", "d"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZSetRange(t *testing.T) {
	c := getTestConn()

	key := []byte("myzset_range_rank")
	if _, err := c.Do(context.Background(), "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if v, err := c.Do(context.Background(), "zrange", key, 0, 3, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrange", key, 1, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrange", key, -2, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrange", key, 0, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrange", key, -1, -2, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else if len(cast.ToSlice(v)) != 0 {
		t.Fatal(len(cast.ToSlice(v)))
	}

	if v, err := c.Do(context.Background(), "zrevrange", key, 0, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrange", key, 0, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrange", key, 2, 3, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(context.Background(), "zrevrange", key, -2, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(context.Background(), "zremrangebyrank", key, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := c.Do(context.Background(), "zrange", key, 0, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(context.Background(), "zclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(context.Background(), "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZsetErrorParams(t *testing.T) {
	c := getTestConn()

	//zadd
	if _, err := c.Do(context.Background(), "zadd", "test_zadd").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zadd", "test_zadd", "a", "b", "c").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zadd", "test_zadd", "-a", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zadd", "test_zad", "0.1", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcard
	if _, err := c.Do(context.Background(), "zcard").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zscore
	if _, err := c.Do(context.Background(), "zscore", "test_zscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrem
	if _, err := c.Do(context.Background(), "zrem", "test_zrem").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zincrby
	if _, err := c.Do(context.Background(), "zincrby", "test_zincrby").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zincrby", "test_zincrby", 0.1, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcount
	if _, err := c.Do(context.Background(), "zcount", "test_zcount").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zcount", "test_zcount", "-inf", "=inf").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zcount", "test_zcount", 0.1, 0.1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrank
	if _, err := c.Do(context.Background(), "zrank", "test_zrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevzrank
	if _, err := c.Do(context.Background(), "zrevrank", "test_zrevrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyrank
	if _, err := c.Do(context.Background(), "zremrangebyrank", "test_zremrangebyrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zremrangebyrank", "test_zremrangebyrank", 0.1, 0.1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyscore
	if _, err := c.Do(context.Background(), "zremrangebyscore", "test_zremrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zremrangebyscore", "test_zremrangebyscore", "-inf", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zremrangebyscore", "test_zremrangebyscore", 0, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrange
	if _, err := c.Do(context.Background(), "zrange", "test_zrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrange", "test_zrange", 0, 1, "withscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrange", "test_zrange", 0, 1, "withscores", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrange, almost same as zrange
	if _, err := c.Do(context.Background(), "zrevrange", "test_zrevrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrangebyscore
	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore", 0, 1, "withscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limi", 1, 1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", "a", 1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(context.Background(), "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", 1, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrangebyscore, almost same as zrangebyscore
	if _, err := c.Do(context.Background(), "zrevrangebyscore", "test_zrevrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zclear
	if _, err := c.Do(context.Background(), "zclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zmclear
	if _, err := c.Do(context.Background(), "zmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zexpire
	if _, err := c.Do(context.Background(), "zexpire", "test_zexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zexpireat
	if _, err := c.Do(context.Background(), "zexpireat", "test_zexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zttl
	if _, err := c.Do(context.Background(), "zttl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zpersist
	if _, err := c.Do(context.Background(), "zpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

}
