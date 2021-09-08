package main

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestDBSet(t *testing.T) {
	db := getTestConn()

	key := "testdb_set_a"
	member := "member"
	key1 := "testdb_set_a1"
	key2 := "testdb_set_a2"
	member1 := "testdb_set_m1"
	member2 := "testdb_set_m2"

	if n, err := db.SAdd(db.Context(), key, member).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if cnt, err := db.SCard(db.Context(), key).Result(); err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatal(cnt)
	}

	if n, err := db.SIsMember(db.Context(), key, member).Result(); err != nil {
		t.Fatal(err)
	} else if !n {
		t.Fatal(n)
	}

	if v, err := db.SMembers(db.Context(), key).Result(); err != nil {
		t.Fatal(err)
	} else if v[0] != "member" {
		t.Fatal(v[0])
	}

	if n, err := db.SRem(db.Context(), key, member).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	db.SAdd(db.Context(), key1, member1, member2)

	if n, err := db.Do(db.Context(), "SClear", key1).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 2 {
		t.Fatal(n)
	}

	db.SAdd(db.Context(), key1, member1, member2)
	db.SAdd(db.Context(), key2, member1, member2, "xxx")

	if n, _ := db.SCard(db.Context(), key2).Result(); n != 3 {
		t.Fatal(n)
	}
	if n, err := db.Do(db.Context(), "SMclear", key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 2 {
		t.Fatal(n)
	}

	db.SAdd(db.Context(), key2, member1, member2)
	if n, err := db.Do(db.Context(), "SExpire", key2, 3600).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}

	if n, err := db.Do(db.Context(), "SExpireAt", key2, time.Now().Unix()+3600).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}

	if n, err := db.Do(db.Context(), "STTL", key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n < 0 {
		t.Fatal(n)
	}

	if n, err := db.Do(db.Context(), "SPersist", key2).Result(); err != nil {
		t.Fatal(err)
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal(n)
	}
}

//func TestSetOperation(t *testing.T) {
//	db := getTestConn()
//	testUnion(db, t)
//	testInter(db, t)
//	testDiff(db, t)
//}

func testUnion(db *redis.Client, t *testing.T) {
	key := "testdb_set_union_1"
	key1 := "testdb_set_union_2"
	key2 := "testdb_set_union_2"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	db.SAdd(db.Context(), key, m1, m2)
	db.SAdd(db.Context(), key1, m1, m2, m3)
	db.SAdd(db.Context(), key2, m2, m3)

	if _, err := db.SUnion(db.Context(), key, key2).Result(); err != nil {
		t.Fatal(err)
	}

	dstkey := "union_dsk"
	db.SAdd(db.Context(), dstkey, "x")
	if num, err := db.SUnionStore(db.Context(), dstkey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if num != 3 {
		t.Fatal(num)
	}

	if _, err := db.SMembers(db.Context(), dstkey).Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := db.SCard(db.Context(), dstkey).Result(); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	v1, _ := db.SUnion(db.Context(), key1, key2).Result()
	v2, _ := db.SUnion(db.Context(), key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	v1, _ = db.SUnion(db.Context(), key, key1, key2).Result()
	v2, _ = db.SUnion(db.Context(), key, key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	if v, err := db.SUnion(db.Context(), key, key).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	empKey := "0"
	if v, err := db.SUnion(db.Context(), key, empKey).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}
}

func testInter(db *redis.Client, t *testing.T) {
	key1 := "testdb_set_inter_1"
	key2 := "testdb_set_inter_2"
	key3 := "testdb_set_inter_3"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	m4 := "m4"

	db.SAdd(db.Context(), key1, m1, m2)
	db.SAdd(db.Context(), key2, m2, m3, m4)
	db.SAdd(db.Context(), key3, m2, m4)

	if v, err := db.SInter(db.Context(), key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v)
	}

	dstKey := "inter_dsk"
	if n, err := db.SInterStore(db.Context(), dstKey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	k1 := "set_k1"
	k2 := "set_k2"

	db.SAdd(db.Context(), k1, m1, m3, m4)
	db.SAdd(db.Context(), k2, m2, m3)
	if n, err := db.SInterStore(db.Context(), "set_xxx", k1, k2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	v1, _ := db.SInter(db.Context(), key1, key2).Result()
	v2, _ := db.SInter(db.Context(), key2, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	v1, _ = db.SInter(db.Context(), key1, key2, key3).Result()
	v2, _ = db.SInter(db.Context(), key2, key3, key1).Result()
	if len(v1) != len(v2) {
		t.Fatal(v1, v2)
	}

	if v, err := db.SInter(db.Context(), key1, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	empKey := "0"
	if v, err := db.SInter(db.Context(), key1, empKey).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SInter(db.Context(), empKey, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}
}

func testDiff(db *redis.Client, t *testing.T) {
	key0 := "testdb_set_diff_0"
	key1 := "testdb_set_diff_1"
	key2 := "testdb_set_diff_2"
	key3 := "testdb_set_diff_3"

	m1 := "m1"
	m2 := "m2"
	m3 := "m3"
	m4 := "m4"

	db.SAdd(db.Context(), key1, m1, m2)
	db.SAdd(db.Context(), key2, m2, m3, m4)
	db.SAdd(db.Context(), key3, m3)

	if v, err := db.SDiff(db.Context(), key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v)
	}

	dstKey := "diff_dsk"
	if n, err := db.SDiffStore(db.Context(), dstKey, key1, key2).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := db.SDiff(db.Context(), key2, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(db.Context(), key1, key2, key3).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 1 {
		t.Fatal(v) // return 1
	}

	if v, err := db.SDiff(db.Context(), key2, key2).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(db.Context(), key0, key1).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(v)
	}

	if v, err := db.SDiff(db.Context(), key1, key0).Result(); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(v)
	}
}

func TestSKeyExists(t *testing.T) {
	db := getTestConn()
	key := "skeyexists_test"
	if n, err := db.Do(db.Context(), "SKeyExists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.SAdd(db.Context(), key, "hello", "world")

	if n, err := db.Do(db.Context(), "SKeyExists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal("invalid value ", n)
	}
}
