//go:build alltest
// +build alltest

package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDBSet(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()

	key := "testdb_set_a"
	member := "member"
	key1 := "testdb_set_a1"
	key2 := "testdb_set_a2"
	member1 := "testdb_set_m1"
	member2 := "testdb_set_m2"

	if n, err := db.SAdd(ctx, key, member).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if cnt, err := db.SCard(ctx, key).Result(); err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatal(cnt)
	}

	if n, err := db.SIsMember(ctx, key, member).Result(); err != nil {
		t.Fatal(err)
	} else if !n {
		t.Fatal(n)
	}

	if v, err := db.SMembers(ctx, key).Result(); err != nil {
		t.Fatal(err)
	} else if v[0] != "member" {
		t.Fatal(v[0])
	}

	if n, err := db.SRem(ctx, key, member).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	db.SAdd(ctx, key1, member1, member2)

	if n, err := db.Do(ctx, "SClear", key1).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 2 {
		t.Fatal(n)
	}

	db.SAdd(ctx, key1, member1, member2)
	db.SAdd(ctx, key2, member1, member2, "xxx")

	if n, _ := db.SCard(ctx, key2).Result(); n != 3 {
		t.Fatal(n)
	}
	if n, err := db.Do(ctx, "SMclear", key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 2 {
		t.Fatal(n)
	}

	db.SAdd(ctx, key2, member1, member2)
	if n, err := db.Do(ctx, "SExpire", key2, 3600).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}

	if n, err := db.Do(ctx, "SExpireAt", key2, time.Now().Unix()+3600).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}

	if n, err := db.Do(ctx, "STTL", key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n < 0 {
		t.Fatal(n)
	}

	if n, err := db.Do(ctx, "SPersist", key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}
}

func TestSetOperation(t *testing.T) {
	db := getTestConn()
	testUnion(db, t)
	testInter(db, t)
	testDiff(db, t)
}

func testUnion(db *redis.Client, t *testing.T) {
	ctx := context.Background()
	key := "testdb_set_union_1"
	key1 := "testdb_set_union_2"
	key2 := "testdb_set_union_2"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	db.SAdd(ctx, key, m1, m2)
	db.SAdd(ctx, key1, m1, m2, m3)
	db.SAdd(ctx, key2, m2, m3)

	if _, err := db.SUnion(ctx, key, key2).Result(); err != nil {
		t.Fatal(err)
	}

	dstkey := "union_dsk"
	db.SAdd(ctx, dstkey, "x")
	if num, err := db.SUnionStore(ctx, dstkey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if num != 3 {
		t.Fatal(num)
	}

	if _, err := db.SMembers(ctx, dstkey).Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := db.SCard(ctx, dstkey).Result(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	v1, _ := db.SUnion(ctx, key1, key2).Result()
	v2, _ := db.SUnion(ctx, key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	v1, _ = db.SUnion(ctx, key, key1, key2).Result()
	v2, _ = db.SUnion(ctx, key, key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	if v, err := db.SUnion(ctx, key, key).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	empKey := "0"
	if v, err := db.SUnion(ctx, key, empKey).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}
}

func testInter(db *redis.Client, t *testing.T) {
	ctx := context.Background()
	key1 := "testdb_set_inter_1"
	key2 := "testdb_set_inter_2"
	key3 := "testdb_set_inter_3"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	m4 := "m4"

	db.SAdd(ctx, key1, m1, m2)
	db.SAdd(ctx, key2, m2, m3, m4)
	db.SAdd(ctx, key3, m2, m4)

	if v, err := db.SInter(ctx, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v)
	}

	dstKey := "inter_dsk"
	if n, err := db.SInterStore(ctx, dstKey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	k1 := "set_k1"
	k2 := "set_k2"

	db.SAdd(ctx, k1, m1, m3, m4)
	db.SAdd(ctx, k2, m2, m3)
	if n, err := db.SInterStore(ctx, "set_xxx", k1, k2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	v1, _ := db.SInter(ctx, key1, key2).Result()
	v2, _ := db.SInter(ctx, key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	v1, _ = db.SInter(ctx, key1, key2, key3).Result()
	v2, _ = db.SInter(ctx, key2, key3, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	if v, err := db.SInter(ctx, key1, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	empKey := "0"
	if v, err := db.SInter(ctx, key1, empKey).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SInter(ctx, empKey, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}
}

func testDiff(db *redis.Client, t *testing.T) {
	ctx := context.Background()
	key0 := "testdb_set_diff_0"
	key1 := "testdb_set_diff_1"
	key2 := "testdb_set_diff_2"
	key3 := "testdb_set_diff_3"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	m4 := "m4"

	db.SAdd(ctx, key1, m1, m2)
	db.SAdd(ctx, key2, m2, m3, m4)
	db.SAdd(ctx, key3, m3)

	if v, err := db.SDiff(ctx, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v)
	}

	dstKey := "diff_dsk"
	if n, err := db.SDiffStore(ctx, dstKey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := db.SDiff(ctx, key2, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(ctx, key1, key2, key3).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v) // return 1
	}

	if v, err := db.SDiff(ctx, key2, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(ctx, key0, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(ctx, key1, key0).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}
}

func TestSKeyExists(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()
	key := "skeyexists_test"
	if n, err := db.Do(ctx, "SKeyExists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.SAdd(ctx, key, "hello", "world")

	if n, err := db.Do(ctx, "SKeyExists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal("invalid value ", n)
	}
}
