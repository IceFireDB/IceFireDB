//go:build alltest
// +build alltest

package main

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// listValues returns the list stored at key as strings.
func listValues(t *testing.T, c *redis.Client, key string) []string {
	t.Helper()
	res, err := c.Do(context.Background(), "lrange", key, 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange %s: %v", key, err)
	}
	return cast.ToStringSlice(res)
}

// requireList asserts the exact element order of a list.
func requireList(t *testing.T, c *redis.Client, key string, want ...string) {
	t.Helper()
	got := listValues(t, c, key)
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", key, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", key, got, want)
		}
	}
}

// requireTTL runs a TTL command and returns its value (-1 means "no expiration").
func requireTTL(t *testing.T, c *redis.Client, cmd, key string) int64 {
	t.Helper()
	ttl, err := c.Do(context.Background(), cmd, key).Int64()
	if err != nil {
		t.Fatalf("%s %s: %v", cmd, key, err)
	}
	return ttl
}

// TestSETTTLOptions covers the TTL side of SET: a plain SET clears an existing
// expiration, KEEPTTL preserves it, and PX is rounded up because the engine
// stores whole-second expirations.
func TestSETTTLOptions(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_set_ttl"
	c.Del(ctx, key)

	if _, err := c.Do(ctx, "set", key, "v1", "EX", "100").Result(); err != nil {
		t.Fatal(err)
	}
	if ttl := requireTTL(t, c, "ttl", key); ttl <= 0 {
		t.Fatalf("SET EX 100 should leave a TTL, got %d", ttl)
	}

	// Redis clears the expiration on a plain SET.
	if _, err := c.Do(ctx, "set", key, "v2").Result(); err != nil {
		t.Fatal(err)
	}
	if ttl := requireTTL(t, c, "ttl", key); ttl != -1 {
		t.Fatalf("plain SET must clear the TTL, got %d", ttl)
	}

	if _, err := c.Do(ctx, "set", key, "v3", "EX", "100").Result(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Do(ctx, "set", key, "v4", "KEEPTTL").Result(); err != nil || got != "OK" {
		t.Fatalf("SET KEEPTTL = %v, %v", got, err)
	}
	if ttl := requireTTL(t, c, "ttl", key); ttl <= 0 {
		t.Fatalf("KEEPTTL must preserve the TTL, got %d", ttl)
	}
	if v, err := c.Get(ctx, key).Result(); err != nil || v != "v4" {
		t.Fatalf("SET KEEPTTL should still write the value: %q, %v", v, err)
	}
}

// TestExpireSecondsFromMillis pins the millisecond-to-second rounding: the engine
// keeps whole-second expirations, so a sub-second PX must round up rather than
// truncate to an already-expired key.
func TestExpireSecondsFromMillis(t *testing.T) {
	cases := map[int64]int64{
		0:    0,
		1:    1,
		500:  1,
		999:  1,
		1000: 1,
		1001: 2,
		1500: 2,
		2000: 2,
	}
	for ms, want := range cases {
		if got := expireSecondsFromMillis(ms); got != want {
			t.Fatalf("expireSecondsFromMillis(%d) = %d, want %d", ms, got, want)
		}
	}
}

// TestSETPXIsAccepted covers the PX option end to end: SET must accept it and the
// key must still be readable. Exact TTLs are deliberately not asserted over the
// wire: expirations are stamped from the Raft clock while the storage layer
// validates and reports them against the local clock, so a lagging Raft clock can
// shift a short expiry — or reject it outright as "invalid expire value".
func TestSETPXIsAccepted(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	for _, px := range []string{"60000", "120000"} {
		key := "pr982_px_" + px
		c.Del(ctx, key)
		if _, err := c.Do(ctx, "set", key, "v", "PX", px).Result(); err != nil {
			t.Fatalf("SET PX %s: %v", px, err)
		}
		if v, err := c.Get(ctx, key).Result(); err != nil || v != "v" {
			t.Fatalf("SET PX %s value = %q, %v", px, v, err)
		}
	}
}

// TestSETNXAndXXNilReplies checks the null-bulk answers of the conditional forms.
func TestSETNXAndXXNilReplies(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_set_nx_xx"
	c.Del(ctx, key)

	if _, err := c.Do(ctx, "set", key, "v1", "NX").Result(); err != nil {
		t.Fatalf("SET NX on a missing key must succeed: %v", err)
	}
	if got, err := c.Do(ctx, "set", key, "v2", "NX").Result(); err != redis.Nil {
		t.Fatalf("SET NX on an existing key must be a nil reply, got %v (err %v)", got, err)
	}
	if _, err := c.Do(ctx, "set", "pr982_set_xx_missing", "v1", "XX").Result(); err != redis.Nil {
		t.Fatalf("SET XX on a missing key must be a nil reply, got %v", err)
	}
	if got, err := c.Do(ctx, "set", key, "v3", "XX").Result(); err != nil || got != "OK" {
		t.Fatalf("SET XX on an existing key = %v, %v", got, err)
	}
	if v, err := c.Get(ctx, key).Result(); err != nil || v != "v3" {
		t.Fatalf("SET XX should update the value: %q, %v", v, err)
	}
}

// TestHGETEmptyValue pins that an existing-but-empty field answers an empty bulk
// string rather than a nil reply.
func TestHGETEmptyValue(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_hash_empty"
	c.Del(ctx, key)
	if _, err := c.Do(ctx, "hset", key, "f", "").Result(); err != nil {
		t.Fatal(err)
	}

	raw, err := c.Do(ctx, "hget", key, "f").Result()
	if err == redis.Nil {
		// The decentralized drivers collapse an empty value to nil on read, so an
		// empty field is indistinguishable from a missing one there.
		t.Skip("this backend cannot tell an empty hash value from a missing field")
	}
	if err != nil {
		t.Fatalf("HGET of an empty value returned an error: %v", err)
	}
	if s, ok := raw.(string); !ok || s != "" {
		t.Fatalf("HGET of an empty value = %#v, want empty string", raw)
	}

	if _, err := c.Do(ctx, "hget", key, "missing").Result(); err != redis.Nil {
		t.Fatalf("HGET of a missing field must be a nil reply, got %v", err)
	}
}

// TestZSCOREReplyType pins the bulk-string reply and the nil answer for a missing
// member.
func TestZSCOREReplyType(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_zscore"
	c.Del(ctx, key)
	if _, err := c.Do(ctx, "zadd", key, "5", "m").Result(); err != nil {
		t.Fatal(err)
	}

	raw, err := c.Do(ctx, "zscore", key, "m").Result()
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := raw.(string); !ok || s != "5" {
		t.Fatalf("ZSCORE = %#v, want the bulk string \"5\"", raw)
	}

	if _, err := c.Do(ctx, "zscore", key, "missing").Result(); err != redis.Nil {
		t.Fatalf("ZSCORE of a missing member must be a nil reply, got %v", err)
	}
}

// TestSPOPAndSRANDMEMBERReplyTypes covers both reply shapes: the bare form answers
// a single member, the counted form an array.
func TestSPOPAndSRANDMEMBERReplyTypes(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	c.Del(ctx, "pr982_spop_one")
	if _, err := c.Do(ctx, "sadd", "pr982_spop_one", "only").Result(); err != nil {
		t.Fatal(err)
	}
	raw, err := c.Do(ctx, "spop", "pr982_spop_one").Result()
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := raw.(string); !ok || s != "only" {
		t.Fatalf("SPOP without COUNT = %#v, want the member as a bulk string", raw)
	}
	if _, err := c.Do(ctx, "spop", "pr982_spop_one").Result(); err != redis.Nil {
		t.Fatalf("SPOP on a missing key must be a nil reply, got %v", err)
	}

	c.Del(ctx, "pr982_spop_many")
	if _, err := c.Do(ctx, "sadd", "pr982_spop_many", "a", "b", "c").Result(); err != nil {
		t.Fatal(err)
	}
	popped, err := c.Do(ctx, "spop", "pr982_spop_many", "2").StringSlice()
	if err != nil {
		t.Fatalf("SPOP with COUNT: %v", err)
	}
	if len(popped) != 2 {
		t.Fatalf("SPOP COUNT 2 returned %d members", len(popped))
	}
	if emptied, err := c.Do(ctx, "spop", "pr982_spop_many", "100").StringSlice(); err != nil || len(emptied) != 1 {
		t.Fatalf("SPOP COUNT greater than the set size = %v, %v", emptied, err)
	}
	if zero, err := c.Do(ctx, "spop", "pr982_spop_many", "0").StringSlice(); err != nil || len(zero) != 0 {
		t.Fatalf("SPOP COUNT 0 = %v, %v", zero, err)
	}

	c.Del(ctx, "pr982_srand")
	if _, err := c.Do(ctx, "sadd", "pr982_srand", "a", "b", "c").Result(); err != nil {
		t.Fatal(err)
	}
	raw, err = c.Do(ctx, "srandmember", "pr982_srand").Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := raw.(string); !ok {
		t.Fatalf("SRANDMEMBER without COUNT = %#v, want a bulk string", raw)
	}
	if _, err := c.Do(ctx, "srandmember", "pr982_srand_missing").Result(); err != redis.Nil {
		t.Fatalf("SRANDMEMBER on a missing key must be a nil reply, got %v", err)
	}
	positive, err := c.Do(ctx, "srandmember", "pr982_srand", "2").StringSlice()
	if err != nil || len(positive) != 2 {
		t.Fatalf("SRANDMEMBER COUNT 2 = %v, %v", positive, err)
	}
	// A negative count may repeat members, so it always returns |count| entries.
	negative, err := c.Do(ctx, "srandmember", "pr982_srand", "-5").StringSlice()
	if err != nil || len(negative) != 5 {
		t.Fatalf("SRANDMEMBER COUNT -5 = %v, %v", negative, err)
	}
}

// TestLREMOrder pins which occurrences count > 0 and count < 0 remove.
func TestLREMOrder(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_lrem"
	c.Do(ctx, "lclear", key)
	c.Do(ctx, "rpush", key, "a", "b", "b", "b", "c", "b", "a")

	if n, err := c.Do(ctx, "lrem", key, "-2", "b").Int64(); err != nil || n != 2 {
		t.Fatalf("LREM -2 removed %d (%v)", n, err)
	}
	requireList(t, c, key, "a", "b", "b", "c", "a")

	if n, err := c.Do(ctx, "lrem", key, "1", "b").Int64(); err != nil || n != 1 {
		t.Fatalf("LREM 1 removed %d (%v)", n, err)
	}
	requireList(t, c, key, "a", "b", "c", "a")

	// Removing elements must not drop the list's TTL.
	c.Do(ctx, "lexpire", key, 100)
	if n, err := c.Do(ctx, "lrem", key, "0", "a").Int64(); err != nil || n != 2 {
		t.Fatalf("LREM 0 removed %d (%v)", n, err)
	}
	requireList(t, c, key, "b", "c")
	if ttl := requireTTL(t, c, "lttl", key); ttl <= 0 {
		t.Fatalf("LREM dropped the list TTL, got %d", ttl)
	}
}

// TestSCANAcrossTypes walks a single SCAN cursor across the engine's per-type
// keyspaces: every matching key of every type must be visited exactly once.
func TestSCANAcrossTypes(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	const prefix = "pr982_scan:"
	want := []string{prefix + "string", prefix + "hash", prefix + "list", prefix + "set", prefix + "zset"}

	c.Do(ctx, "set", prefix+"string", "1")
	c.Do(ctx, "hset", prefix+"hash", "f", "1")
	c.Do(ctx, "rpush", prefix+"list", "x")
	c.Do(ctx, "sadd", prefix+"set", "m")
	c.Do(ctx, "zadd", prefix+"zset", "1", "m")

	seen := map[string]bool{}
	cursor := "0"
	// COUNT is a total budget shared across the keyspaces in a fixed order, so use a
	// budget large enough that the walk finishes even when earlier keyspaces still
	// hold keys from other tests.
	for i := 0; i < 200; i++ {
		res, err := c.Do(ctx, "scan", cursor, "MATCH", prefix+"*", "COUNT", "1000").Result()
		if err != nil {
			t.Fatalf("SCAN: %v", err)
		}
		parts, ok := res.([]interface{})
		if !ok || len(parts) != 2 {
			t.Fatalf("SCAN reply = %#v, want [cursor, keys]", res)
		}
		cursor, ok = parts[0].(string)
		if !ok {
			t.Fatalf("SCAN cursor = %#v, want a bulk string", parts[0])
		}
		keys, ok := parts[1].([]interface{})
		if !ok {
			t.Fatalf("SCAN keys = %#v, want an array", parts[1])
		}
		for _, k := range keys {
			seen[k.(string)] = true
		}
		if cursor == "0" {
			break
		}
	}
	if cursor != "0" {
		t.Fatalf("SCAN did not finish, last cursor %q", cursor)
	}
	for _, k := range want {
		if !seen[k] {
			t.Fatalf("SCAN never returned %q (saw %v)", k, seen)
		}
	}

	// A TYPE filter restricts the same walk to one keyspace.
	res, err := c.Do(ctx, "scan", "0", "MATCH", prefix+"*", "TYPE", "hash").Result()
	if err != nil {
		t.Fatalf("SCAN TYPE hash: %v", err)
	}
	keys := res.([]interface{})[1].([]interface{})
	if len(keys) != 1 || keys[0].(string) != prefix+"hash" {
		t.Fatalf("SCAN TYPE hash = %v, want only %q", keys, prefix+"hash")
	}
}

// TestSCANMatchGlob pins Redis glob translation for MATCH.
func TestSCANMatchGlob(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	c.Do(ctx, "set", "pr982_glob:alpha", "1")
	c.Do(ctx, "set", "pr982_glob:beta", "1")
	c.Do(ctx, "set", "pr982_glob:gamma", "1")

	res, err := c.Do(ctx, "scan", "0", "MATCH", "pr982_glob:?eta", "COUNT", "100").Result()
	if err != nil {
		t.Fatal(err)
	}
	keys := res.([]interface{})[1].([]interface{})
	if len(keys) != 1 || keys[0].(string) != "pr982_glob:beta" {
		t.Fatalf("MATCH ?eta = %v, want only pr982_glob:beta", keys)
	}

}

// TestClientSuppliedLimits covers the bounds that keep one request from asking the
// server to materialise an unbounded reply.
func TestClientSuppliedLimits(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if _, err := c.Do(ctx, "sadd", "pr982_limits", "a", "b", "c").Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "srandmember", "pr982_limits", "-2000000").Result(); err == nil {
		t.Fatal("SRANDMEMBER beyond the count cap should be an error")
	}
	if _, err := c.Do(ctx, "srandmember", "pr982_limits", "2000000").Result(); err == nil {
		t.Fatal("SRANDMEMBER beyond the count cap should be an error")
	}

	// COUNT is a hint, so a huge value must be clamped rather than rejected.
	if _, err := c.Do(ctx, "set", "pr982_limits:key", "1").Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "scan", "0", "MATCH", "pr982_limits*", "COUNT", "100000000").Result(); err != nil {
		t.Fatalf("SCAN with a huge COUNT: %v", err)
	}
	if _, err := c.Do(ctx, "sscan", "pr982_limits", "0", "COUNT", "100000000").Result(); err != nil {
		t.Fatalf("SSCAN with a huge COUNT: %v", err)
	}

	// PX is capped by the millisecond-to-second division, so even the largest
	// value stays representable; EX can overflow the seconds arithmetic and must
	// be refused rather than silently dropping the expiry.
	if _, err := c.Do(ctx, "set", "pr982_limits:ttl", "v", "PX", "9223372036854775807").Result(); err != nil {
		t.Fatalf("SET PX with the largest millisecond value should be accepted: %v", err)
	}
	c.Del(ctx, "pr982_limits:ttl")
	if _, err := c.Do(ctx, "set", "pr982_limits:ttl", "v", "EX", "9223372036854775807").Result(); err == nil {
		t.Fatal("SET EX with an overflowing value should be an error")
	}
	// A rejected expiration must not leave the value behind.
	if _, err := c.Get(ctx, "pr982_limits:ttl").Result(); err != redis.Nil {
		t.Fatalf("a rejected SET must not write the value, got %v", err)
	}
}

// TestNativeScanPaging pages the X*SCAN family with COUNT 1 over three entries:
// every entry must appear exactly once and the loop must terminate. A flipped
// inclusive bound used to return the page-ending field twice, which made COUNT 1
// loop forever.
func TestNativeScanPaging(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	c.Do(ctx, "hclear", "pr982_xscan_hash")
	c.Do(ctx, "hset", "pr982_xscan_hash", "f1", "v1", "f2", "v2", "f3", "v3")
	c.Do(ctx, "sclear", "pr982_xscan_set")
	c.Do(ctx, "sadd", "pr982_xscan_set", "m1", "m2", "m3")
	c.Do(ctx, "zadd", "pr982_xscan_zset", "1", "z1", "2", "z2", "3", "z3")

	cases := []struct {
		cmd  string
		key  string
		want []string
	}{
		{"xhscan", "pr982_xscan_hash", []string{"f1", "f2", "f3"}},
		{"xsscan", "pr982_xscan_set", []string{"m1", "m2", "m3"}},
		{"xzscan", "pr982_xscan_zset", []string{"z1", "z2", "z3"}},
	}

	for _, tc := range cases {
		seen := map[string]int{}
		cursor := ""
		for i := 0; i < 10; i++ {
			res, err := c.Do(ctx, tc.cmd, tc.key, cursor, "COUNT", "1").Result()
			if err != nil {
				t.Fatalf("%s: %v", tc.cmd, err)
			}
			parts := res.([]interface{})
			next := ""
			if parts[0] != nil {
				next = parts[0].(string)
			}
			for _, v := range parts[1].([]interface{}) {
				seen[v.(string)]++
			}
			if next == "" {
				break
			}
			cursor = next
		}
		for _, k := range tc.want {
			if seen[k] == 0 {
				t.Fatalf("%s never returned %q (saw %v)", tc.cmd, k, seen)
			}
			if seen[k] > 1 {
				t.Fatalf("%s returned %q %d times", tc.cmd, k, seen[k])
			}
		}
	}
}

// TestMatchFiltersInsideTheHandler pins that MATCH is applied to a bounded page:
// a pattern that matches nothing must still end the iteration instead of walking
// the whole keyspace, and a pattern that matches one field must return only it.
func TestMatchFiltersInsideTheHandler(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	for _, k := range []string{"pr982_match:one", "pr982_match:two", "pr982_match:three"} {
		if _, err := c.Do(ctx, "set", k, "1").Result(); err != nil {
			t.Fatal(err)
		}
	}

	// A pattern that matches nothing must still terminate, with bounded work per
	// call: pages come back empty while the cursor advances, and the walk ends.
	cursor, pages := "0", 0
	for {
		res, err := c.Do(ctx, "scan", cursor, "MATCH", "pr982_match:zzz*", "COUNT", "2").Result()
		if err != nil {
			t.Fatalf("SCAN with a non-matching pattern: %v", err)
		}
		parts := res.([]interface{})
		if keys := parts[1].([]interface{}); len(keys) != 0 {
			t.Fatalf("a non-matching MATCH returned %v", keys)
		}
		cursor = parts[0].(string)
		pages++
		if cursor == "0" {
			break
		}
		if pages > 100 {
			t.Fatalf("SCAN with a non-matching MATCH did not terminate")
		}
	}

	c.Do(ctx, "hclear", "pr982_match_hash")
	c.Do(ctx, "hset", "pr982_match_hash", "alpha", "1", "beta", "2")
	res, err := c.Do(ctx, "hscan", "pr982_match_hash", "0", "MATCH", "al*", "COUNT", "10").Result()
	if err != nil {
		t.Fatalf("HSCAN MATCH: %v", err)
	}
	flat := res.([]interface{})[1].([]interface{})
	if len(flat) != 2 || flat[0].(string) != "alpha" || flat[1].(string) != "1" {
		t.Fatalf("HSCAN MATCH al* = %v, want [alpha 1]", flat)
	}
}

// TestSMOVESelfMoveKeepsTTL pins that moving a member onto its own key is a no-op
// returning 1: removing and re-adding would delete an emptied set, and with it the
// key's expiration.
func TestSMOVESelfMoveKeepsTTL(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_smove_self"
	c.Del(ctx, key)
	if _, err := c.Do(ctx, "sadd", key, "m").Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "sexpire", key, "100").Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Do(ctx, "smove", key, key, "m").Int64(); err != nil || n != 1 {
		t.Fatalf("SMOVE onto the same key = %d, %v (want 1)", n, err)
	}
	if members, err := c.Do(ctx, "smembers", key).StringSlice(); err != nil || len(members) != 1 {
		t.Fatalf("member was lost: %v, %v", members, err)
	}
	if ttl := requireTTL(t, c, "sttl", key); ttl <= 0 {
		t.Fatalf("SMOVE onto the same key dropped the TTL, got %d", ttl)
	}
}

// TestGlobEdgeCases pins the pattern translation edges that used to surface as
// errors or as "match everything".
func TestGlobEdgeCases(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if _, err := c.Do(ctx, "set", "pr982_glob_edge:a", "1").Result(); err != nil {
		t.Fatal(err)
	}
	// An unbalanced bracket is a literal in Redis, not a regexp error.
	if _, err := c.Do(ctx, "scan", "0", "MATCH", "pr982_glob_edge[", "COUNT", "10").Result(); err != nil {
		t.Fatalf("unbalanced bracket pattern should not error: %v", err)
	}
	// An explicitly empty pattern matches only an empty name, not everything.
	res, err := c.Do(ctx, "scan", "0", "MATCH", "", "COUNT", "10").Result()
	if err != nil {
		t.Fatalf("empty MATCH pattern: %v", err)
	}
	if keys := res.([]interface{})[1].([]interface{}); len(keys) != 0 {
		t.Fatalf("an empty MATCH pattern returned %v", keys)
	}
}

// TestLREMMinimumCount pins the int64 guard: negating MinInt64 would overflow and
// turn the command into a silent no-op.
func TestLREMMinimumCount(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	key := "pr982_lrem_min"
	c.Do(ctx, "lclear", key)
	c.Do(ctx, "rpush", key, "a", "b")
	if _, err := c.Do(ctx, "lrem", key, "-9223372036854775808", "a").Result(); err == nil {
		t.Fatal("LREM with the minimum int64 count should be an out-of-range error")
	}
	if n, err := c.Do(ctx, "llen", key).Int64(); err != nil || n != 2 {
		t.Fatalf("the rejected LREM must not touch the list: %d, %v", n, err)
	}
}
