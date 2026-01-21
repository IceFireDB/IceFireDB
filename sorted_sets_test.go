package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

func TestZSet(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("myzset")

	if n, err := c.Do(ctx, "zadd", key, 3, "a", 4, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zadd", key, 3, "c", 4, "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := c.Do(ctx, "zscore", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if n, err := c.Do(ctx, "zrem", key, "d", "e").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zincrby", key, 4, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zincrby", key, -4, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zincrby", key, 4, "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zrem", key, "a", "b", "c", "d").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZSetCount(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("myzset")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "zcount", key, 2, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, 4, 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, 4, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, "(2", 4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, "2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, "(2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcount", key, "-inf", "+inf").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	c.Do(ctx, "zadd", key, 3, "e")

	if n, err := c.Do(ctx, "zcount", key, "(2", "(4").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	c.Do(ctx, "zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetRank(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("myzset")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "zrank", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if _, err := c.Do(ctx, "zrank", key, "e").Result(); err != redis.Nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "zrevrank", key, "c").Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := c.Do(ctx, "zrevrank", key, "e").Result(); err != redis.Nil {
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
	ctx := context.Background()

	key := []byte("myzset_range")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if v, err := c.Do(ctx, "zrangebyscore", key, 1, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrangebyscore", key, 1, 4, "withscores", "limit", 1, 2).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "c", 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrangebyscore", key, "-inf", "+inf", "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrangebyscore", key, "(1", "(4").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", "c"); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrangebyscore", key, 4, 1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrangebyscore", key, 4, 1, "withscores", "limit", 1, 2).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", 3, "b", 2); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrangebyscore", key, "+inf", "-inf", "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrangebyscore", key, "(4", "(1").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "zremrangebyscore", key, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "zrangebyscore", key, 1, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", "d"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZSetRange(t *testing.T) {
	c := getTestConn()

	ctx := context.Background()
	key := []byte("myzset_range_rank")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d").Result(); err != nil {
		t.Fatal(err)
	}

	if v, err := c.Do(ctx, "zrange", key, 0, 3, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrange", key, 1, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrange", key, -2, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrange", key, 0, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrange", key, -1, -2, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else if len(cast.ToSlice(v)) != 0 {
		t.Fatal(len(cast.ToSlice(v)))
	}

	if v, err := c.Do(ctx, "zrevrange", key, 0, 4, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrange", key, 0, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrange", key, 2, 3, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := c.Do(ctx, "zrevrange", key, -2, -1, "withscores").Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "zremrangebyrank", key, 2, 3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := c.Do(ctx, "zrange", key, 0, 4).Result(); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(cast.ToSlice(v), "a", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := c.Do(ctx, "zclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZsetErrorParams(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	//zadd
	if _, err := c.Do(ctx, "zadd", "test_zadd").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zadd", "test_zadd", "a", "b", "c").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zadd", "test_zadd", "-a", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zadd", "test_zad", "0.1", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcard
	if _, err := c.Do(ctx, "zcard").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zscore
	if _, err := c.Do(ctx, "zscore", "test_zscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrem
	if _, err := c.Do(ctx, "zrem", "test_zrem").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zincrby
	if _, err := c.Do(ctx, "zincrby", "test_zincrby").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zincrby", "test_zincrby", 0.1, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zcount
	if _, err := c.Do(ctx, "zcount", "test_zcount").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zcount", "test_zcount", "-inf", "=inf").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zcount", "test_zcount", 0.1, 0.1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrank
	if _, err := c.Do(ctx, "zrank", "test_zrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevzrank
	if _, err := c.Do(ctx, "zrevrank", "test_zrevrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyrank
	if _, err := c.Do(ctx, "zremrangebyrank", "test_zremrangebyrank").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zremrangebyrank", "test_zremrangebyrank", 0.1, 0.1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zremrangebyscore
	if _, err := c.Do(ctx, "zremrangebyscore", "test_zremrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zremrangebyscore", "test_zremrangebyscore", "-inf", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zremrangebyscore", "test_zremrangebyscore", 0, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrange
	if _, err := c.Do(ctx, "zrange", "test_zrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrange", "test_zrange", 0, 1, "withscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrange", "test_zrange", 0, 1, "withscores", "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrange, almost same as zrange
	if _, err := c.Do(ctx, "zrevrange", "test_zrevrange").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrangebyscore
	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore", 0, 1, "withscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limi", 1, 1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", "a", 1).Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do(ctx, "zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", 1, "a").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zrevrangebyscore, almost same as zrangebyscore
	if _, err := c.Do(ctx, "zrevrangebyscore", "test_zrevrangebyscore").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zclear
	if _, err := c.Do(ctx, "zclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zmclear
	if _, err := c.Do(ctx, "zmclear").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zexpire
	if _, err := c.Do(ctx, "zexpire", "test_zexpire").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zexpireat
	if _, err := c.Do(ctx, "zexpireat", "test_zexpireat").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zttl
	if _, err := c.Do(ctx, "zttl").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	//zpersist
	if _, err := c.Do(ctx, "zpersist").Result(); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

}

func TestZScoreReturnValue(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("zscore_test")
	if _, err := c.Do(ctx, "zadd", key, 1, "member1", 2, "member2", 3, "member3").Result(); err != nil {
		t.Fatal(err)
	}

	// Test ZSCORE returns score as bulk string
	score, err := c.Do(ctx, "zscore", key, "member2").Result()
	if err != nil {
		t.Fatal(err)
	}

	// Verify score is returned as string
	scoreStr, ok := score.(string)
	if !ok {
		t.Fatalf("ZSCORE should return string, got %T", score)
	}

	if scoreStr != "2" {
		t.Fatalf("Expected score '2', got '%s'", scoreStr)
	}

	// Test ZSCORE on non-existent member
	nilScore, err := c.Do(ctx, "zscore", key, "nonexistent").Result()
	// ZSCORE on non-existent member returns nil, which may be an error or nil value
	if err != nil && err.Error() != "redis: nil" {
		t.Fatalf("ZSCORE on non-existent member should return nil or redis:nil error, got %v", err)
	}
	if nilScore != nil {
		t.Fatalf("ZSCORE on non-existent member should return nil, got %v", nilScore)
	}
}

func TestZSetRangeByRank(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("zset_range_rank_test")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c", 4, "d", 5, "e").Result(); err != nil {
		t.Fatal(err)
	}

	// Test ZRANGE with different index ranges
	testCases := []struct {
		start    int64
		stop     int64
		expected []string
	}{
		{0, -1, []string{"a", "b", "c", "d", "e"}},
		{0, 2, []string{"a", "b", "c"}},
		{2, 4, []string{"c", "d", "e"}},
		{-3, -1, []string{"c", "d", "e"}},
		{1, 3, []string{"b", "c", "d"}},
	}

	for _, tc := range testCases {
		result, err := c.Do(ctx, "zrange", key, tc.start, tc.stop).Result()
		if err != nil {
			t.Fatalf("ZRANGE %d %d failed: %v", tc.start, tc.stop, err)
		}

		resultArr, ok := result.([]interface{})
		if !ok {
			t.Fatalf("ZRANGE should return array, got %T", result)
		}

		if len(resultArr) != len(tc.expected) {
			t.Fatalf("ZRANGE %d %d: expected %d elements, got %d", tc.start, tc.stop, len(tc.expected), len(resultArr))
		}
	}

	// Test ZREVRANGE
	revResult, err := c.Do(ctx, "zrevrange", key, 0, 2).Result()
	if err != nil {
		t.Fatal(err)
	}

	revArr, ok := revResult.([]interface{})
	if !ok {
		t.Fatalf("ZREVRANGE should return array, got %T", revResult)
	}

	// ZREVRANGE should return elements in reverse order
	if len(revArr) != 3 {
		t.Fatalf("ZREVRANGE should return 3 elements, got %d", len(revArr))
	}
}

func TestZSetIncrBy(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("zincrby_test")

	// First ZINCRBY creates the member
	newScore, err := c.Do(ctx, "zincrby", key, 5, "member").Result()
	if err != nil {
		t.Fatal(err)
	}

	newScoreInt, ok := newScore.(int64)
	if !ok {
		t.Fatalf("ZINCRBY should return int64, got %T", newScore)
	}

	if newScoreInt != 5 {
		t.Fatalf("Expected score 5, got %d", newScoreInt)
	}

	// Second ZINCRBY increments the score
	incrScore, err := c.Do(ctx, "zincrby", key, 3, "member").Result()
	if err != nil {
		t.Fatal(err)
	}

	incrScoreInt, ok := incrScore.(int64)
	if !ok {
		t.Fatalf("ZINCRBY should return int64, got %T", incrScore)
	}

	if incrScoreInt != 8 {
		t.Fatalf("Expected score 8, got %d", incrScoreInt)
	}

	// ZINCRBY with negative increment
	negScore, err := c.Do(ctx, "zincrby", key, -2, "member").Result()
	if err != nil {
		t.Fatal(err)
	}

	negScoreInt, ok := negScore.(int64)
	if !ok {
		t.Fatalf("ZINCRBY should return int64, got %T", negScore)
	}

	if negScoreInt != 6 {
		t.Fatalf("Expected score 6, got %d", negScoreInt)
	}
}

func TestZSetClear(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := []byte("zclear_test")
	if _, err := c.Do(ctx, "zadd", key, 1, "a", 2, "b", 3, "c").Result(); err != nil {
		t.Fatal(err)
	}

	// Verify members exist
	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("Expected 3 members, got %d", n)
	}

	// Clear the sorted set
	if n, err := c.Do(ctx, "zclear", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatalf("ZCLEAR should return 3, got %d", n)
	}

	// Verify set is empty
	if n, err := c.Do(ctx, "zcard", key).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Expected 0 members after ZCLEAR, got %d", n)
	}
}
