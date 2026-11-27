package client

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func noopLog(string, ...any) {}

// newUnreachablePool builds a pool pointed at a port nothing listens on: connection
// creation always fails, which is exactly the state we want to poke at.
func newUnreachablePool(t *testing.T) *Pool {
	t.Helper()
	pool, err := NewPool(noopLog, 0, 4, 2, "127.0.0.1:1", "user", "pass", "db")
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return pool
}

// TestPoolCloseStopsBackgroundGoroutines is the regression guard for the leak that
// made a proxy restart leave its maintenance goroutines behind forever.
func TestPoolCloseStopsBackgroundGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	pool := newUnreachablePool(t)
	pool.Close()
	pool.Close() // idempotent

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines still running after Close: %d, started with %d", runtime.NumGoroutine(), before)
}

// TestPoolCloseIsIdempotent guards double shutdown, which happens when the signal path
// and the deferred cleanup both run.
func TestPoolCloseIsIdempotent(t *testing.T) {
	pool := newUnreachablePool(t)
	pool.Close()
	pool.Close()

	stats := ConnectionStats{}
	pool.GetStats(&stats)
	if stats.TotalCount < 0 {
		t.Fatalf("TotalCount = %d, want >= 0", stats.TotalCount)
	}
}

// TestGetConnHonoursContext checks that a caller waiting for a connection can give up:
// without it, a shutdown would hang on every blocked proxy session.
func TestGetConnHonoursContext(t *testing.T) {
	pool := newUnreachablePool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := pool.GetConn(ctx); err == nil {
		t.Fatal("GetConn returned a connection from an unreachable server")
	}
}
