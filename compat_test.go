//go:build alltest
// +build alltest

package main

import (
	"context"
	"testing"
)

// TestCompatCrossTypeKeyspaces documents and locks a known divergence from
// Redis: IceFireDB (via ledis) uses SEPARATE per-type keyspaces, so the same
// key name can simultaneously hold values of different types, and operations
// of one type against a key of another type do NOT return WRONGTYPE.
//
// Real Redis would return "WRONGTYPE Operation against a key holding the wrong
// kind of value" on the second operation below. See COMPATIBILITY.md.
func TestCompatCrossTypeKeyspaces(t *testing.T) {
	c := getTestConn()
	ctx := context.Background()

	if err := c.Del(ctx, "compat:ct").Err(); err != nil {
		t.Fatalf("del: %v", err)
	}
	if err := c.Set(ctx, "compat:ct", "iamstring", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}

	// In Redis this would error with WRONGTYPE; here it succeeds.
	if err := c.HSet(ctx, "compat:ct", "f", "v").Err(); err != nil {
		t.Fatalf("HSET on string key unexpectedly errored: %v (behavior changed — update COMPATIBILITY.md)", err)
	}

	// Both type-views of the key coexist.
	if got, err := c.Get(ctx, "compat:ct").Result(); err != nil || got != "iamstring" {
		t.Fatalf("GET after HSET = %q, err=%v; want \"iamstring\", nil (divergence changed)", got, err)
	}
	if got, err := c.HGet(ctx, "compat:ct", "f").Result(); err != nil || got != "v" {
		t.Fatalf("HGET after SET = %q, err=%v; want \"v\", nil (divergence changed)", got, err)
	}
}
