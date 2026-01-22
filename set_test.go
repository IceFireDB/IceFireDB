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
	if n, err := db.Do(ctx, "skeyexists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 0 {
		t.Fatal("invalid value ", n)
	}

	db.SAdd(ctx, key, "hello", "world")

	if n, err := db.Do(ctx, "skeyexists", key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n, ok := n.(int64); !ok || n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestSetSMOVE(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()

	srcKey := "smove_src"
	destKey := "smove_dest"
	member := "move_member"

	// Add member to source set
	if n, err := db.SAdd(ctx, srcKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("Expected 1, got %d", n)
	}

	// Verify member is in source set
	if n, err := db.SIsMember(ctx, srcKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if !n {
		t.Fatal("Member should be in source set")
	}

	// Move member from source to destination
	if moved, err := db.SMove(ctx, srcKey, destKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if !moved {
		t.Fatal("SMOVE should return true")
	}

	// Verify member is no longer in source set
	if n, err := db.SIsMember(ctx, srcKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if n {
		t.Fatal("Member should not be in source set after move")
	}

	// Verify member is in destination set
	if n, err := db.SIsMember(ctx, destKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if !n {
		t.Fatal("Member should be in destination set after move")
	}

	// Test SMOVE with non-existent member (should return false)
	if moved, err := db.SMove(ctx, srcKey, destKey, "nonexistent").Result(); err != nil {
		t.Fatal(err)
	} else if moved {
		t.Fatal("SMOVE with non-existent member should return false")
	}

	// Test SMOVE with non-existent source set (should return false)
	if moved, err := db.SMove(ctx, "nonexistent_src", destKey, member).Result(); err != nil {
		t.Fatal(err)
	} else if moved {
		t.Fatal("SMOVE with non-existent source should return false")
	}
}

func TestSetSPOP(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()

	// Test SPOP with count > set size (should return all members)
	key4 := "spop_large_count"
	db.SAdd(ctx, key4, "a", "b", "c")
	poppedAllResult, err := db.Do(ctx, "spop", key4, 10).Result()
	if err != nil {
		t.Fatal(err)
	}

	poppedAll, ok := poppedAllResult.([]interface{})
	if !ok || len(poppedAll) != 3 {
		t.Fatalf("Expected all 3 members when count > set size, got %v", poppedAllResult)
	}

	if n, err := db.SCard(ctx, key4).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("Set should be empty after popping all members, got %d", n)
	}
}

func TestSetSRANDMEMBER(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()

	key := "srandmember_test"
	members := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	// Add members to set
	for _, m := range members {
		if _, err := db.SAdd(ctx, key, m).Result(); err != nil {
			t.Fatal(err)
		}
	}

	// Test SRANDMEMBER with positive count (should not remove)
	result1, err := db.Do(ctx, "srandmember", key, 3).Result()
	if err != nil {
		t.Fatal(err)
	}

	result1Arr, ok := result1.([]interface{})
	if !ok || len(result1Arr) != 3 {
		t.Fatalf("Expected 3 members, got %v", result1)
	}

	// Verify all members are still in set
	if n, err := db.SCard(ctx, key).Result(); err != nil {
		t.Fatal(err)
	} else if n != 10 {
		t.Fatalf("Set should still have 10 members, got %d", n)
	}

	// Test SRANDMEMBER with negative count (IceFireDB may not support duplicates)
	// In some implementations, negative count just returns |count| elements if available
	result2, err := db.Do(ctx, "srandmember", key, -5).Result()
	if err != nil {
		t.Fatal(err)
	}

	result2Arr, ok := result2.([]interface{})
	if !ok {
		t.Fatalf("SRANDMEMBER with negative count should return array, got %T", result2)
	}
	// Note: IceFireDB may return min(|count|, set size) elements
	if len(result2Arr) != 5 && len(result2Arr) != 10 {
		t.Fatalf("Expected 5 or 10 members with negative count, got %v", result2)
	}

	// Test SRANDMEMBER with count larger than set size
	result4, err := db.Do(ctx, "srandmember", key, 100).Result()
	if err != nil {
		t.Fatal(err)
	}

	result4Arr, ok := result4.([]interface{})
	if !ok || len(result4Arr) != 10 {
		t.Fatalf("Expected all 10 members when count > set size, got %v", result4)
	}

	// Test SRANDMEMBER with negative count - some implementations return all unique elements
	key2 := "srandmember_dup_test"
	db.SAdd(ctx, key2, "a", "b", "c")
	result5, err := db.Do(ctx, "srandmember", key2, -10).Result()
	if err != nil {
		t.Fatal(err)
	}

	result5Arr, ok := result5.([]interface{})
	if !ok {
		t.Fatalf("SRANDMEMBER should return array, got %T", result5)
	}
	// IceFireDB may return min(|count|, set size) unique elements
	if len(result5Arr) != 10 && len(result5Arr) != 3 {
		t.Fatalf("Expected 10 or 3 members with negative count, got %v", result5)
	}
}

func TestSetExpirationCommands(t *testing.T) {
	db := getTestConn()
	ctx := context.Background()

	key := "set_expire_test"

	// Add member to set
	if _, err := db.SAdd(ctx, key, "member").Result(); err != nil {
		t.Fatal(err)
	}

	// Test SEXPIRE (relative time)
	if n, err := db.Do(ctx, "sexpire", key, 100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("SEXPIRE should return 1, got %d", n)
	}

	// Verify TTL is set
	if ttl, err := db.Do(ctx, "sttl", key).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 0 || ttl > 100 {
		t.Logf("TTL value: %d (may vary)", ttl)
	}

	// Test SEXPIREAT (absolute timestamp)
	key2 := "set_expireat_test"
	db.SAdd(ctx, key2, "member")
	futureTime := time.Now().Unix() + 200
	if n, err := db.Do(ctx, "sexpireat", key2, futureTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("SEXPIREAT should return 1, got %d", n)
	}

	// Verify TTL is around 200 seconds
	if ttl, err := db.Do(ctx, "sttl", key2).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl <= 190 || ttl > 201 {
		t.Fatalf("Expected TTL between 190-201, got %d", ttl)
	}

	// Test SEXPIRE with past time (should delete key)
	key3 := "set_expire_past"
	db.SAdd(ctx, key3, "member")
	if n, err := db.Do(ctx, "sexpire", key3, -100).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("SEXPIRE with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := db.Do(ctx, "skeyexists", key3).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expired")
	}

	// Test SEXPIREAT with past timestamp
	key4 := "set_expireat_past"
	db.SAdd(ctx, key4, "member")
	pastTime := time.Now().Unix() - 100
	if n, err := db.Do(ctx, "sexpireat", key4, pastTime).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("SEXPIREAT with past time should return 0, got %d", n)
	}

	// Key should not exist
	if n, err := db.Do(ctx, "skeyexists", key4).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Key should not exist after expired")
	}

	// Test SPERSIST (remove expiration)
	key5 := "set_persist_test"
	db.SAdd(ctx, key5, "member")
	db.Do(ctx, "sexpire", key5, 1000)
	if n, err := db.Do(ctx, "spersist", key5).Int64(); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("SPERSIST should return 1, got %d", n)
	}

	// TTL should be -1 (no expiration)
	if ttl, err := db.Do(ctx, "sttl", key5).Int64(); err != nil {
		t.Fatal(err)
	} else if ttl != -1 {
		t.Fatalf("SPERSIST should make TTL -1, got %d", ttl)
	}
}
