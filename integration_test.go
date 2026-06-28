//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These integration tests launch the real IceFireDB binary as OS subprocesses
// so they can exercise crash recovery (SIGKILL) and multi-node Raft behavior
// that the in-process alltest harness cannot. They are gated behind the
// `integration` build tag and require the binary path in IFDB_BIN.
//
//	go build -o /tmp/ifdb-it . && IFDB_BIN=/tmp/ifdb-it go test -tags integration -run TestIntegration -v ./

func itBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("IFDB_BIN")
	if bin == "" {
		t.Skip("IFDB_BIN not set; build the binary and set IFDB_BIN to run integration tests")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("IFDB_BIN=%q not usable: %v", bin, err)
	}
	return bin
}

// startNode launches a single IceFireDB node bound to addr with data dir dir.
// --localtime avoids uhaha's internet time synchronization (which blocks startup
// when offline). Returns the running command; caller must stop it.
func startNode(t *testing.T, bin, addr, nodeID, dir string, extra ...string) *exec.Cmd {
	t.Helper()
	args := append([]string{"-a", addr, "-n", nodeID, "-d", dir, "--localtime"}, extra...)
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start node %s: %v", nodeID, err)
	}
	return cmd
}

// waitWritable polls until the node at addr accepts a write (i.e. it is up and
// is the Raft leader), or fails after timeout.
func waitWritable(t *testing.T, addr string, timeout time.Duration) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := c.Set(ctx, "__ready__", "1", 0).Err(); err == nil {
			return c
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node at %s not writable within %s: %v", addr, timeout, lastErr)
	return nil
}

func waitReadable(t *testing.T, addr string, timeout time.Duration) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := c.Ping(ctx).Err(); err == nil {
			return c
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node at %s not reachable within %s: %v", addr, timeout, lastErr)
	return nil
}

func kill9(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill() // SIGKILL on Linux — no graceful shutdown
	_, _ = cmd.Process.Wait()
}

// nodeRole classifies a node by attempting a write: "leader" if the write is
// accepted, "follower" if it is redirected (MOVED/TRY — meaning the node has
// joined the cluster and knows the leader), or "down" otherwise.
func nodeRole(addr string) string {
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	err := c.Set(context.Background(), "__role_probe__", "1", 0).Err()
	if err == nil {
		return "leader"
	}
	if msg := err.Error(); contains(msg, "MOVED") || contains(msg, "TRY") {
		return "follower"
	}
	return "down"
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// waitLeaderAmong polls the given addresses until one reports as leader,
// returning that leader's address. Used to observe Raft failover.
func waitLeaderAmong(t *testing.T, addrs []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, a := range addrs {
			if nodeRole(a) == "leader" {
				return a
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("no leader elected among %v within %s", addrs, timeout)
	return ""
}

// isRedirect reports whether an error is a Raft not-leader redirect (MOVED/TRY),
// which is transient while leadership settles after a failover.
func isRedirect(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return contains(m, "MOVED") || contains(m, "TRY")
}

// clusterGet reads key from whichever survivor currently serves it, retrying
// across leadership changes/redirects until timeout. A genuine missing key
// (redis.Nil) is decisive and returned as ("", true) so the caller can detect
// data loss. Returns ok=false only if no node served the read before timeout.
func clusterGet(survivors []string, key string, timeout time.Duration) (string, bool) {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, a := range survivors {
			c := redis.NewClient(&redis.Options{Addr: a})
			got, err := c.Get(ctx, key).Result()
			c.Close()
			if err == nil {
				return got, true
			}
			if err == redis.Nil {
				return "", true // key genuinely absent — decisive
			}
			// redirect or connection error: try next survivor / retry
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", false
}

// clusterSet writes key=val to whichever survivor is the leader, retrying across
// redirects until timeout.
func clusterSet(survivors []string, key, val string, timeout time.Duration) bool {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, a := range survivors {
			c := redis.NewClient(&redis.Options{Addr: a})
			err := c.Set(ctx, key, val, 0).Err()
			c.Close()
			if err == nil {
				return true
			}
			if !isRedirect(err) {
				// connection error while node restarts/elects: keep trying
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// TestIntegrationClusterFailover forms a 3-node cluster, writes to the leader,
// hard-kills the leader, and asserts a new leader is elected, all data
// replicated before the kill survives, and the cluster accepts new writes.
func TestIntegrationClusterFailover(t *testing.T) {
	bin := itBinary(t)
	addrs := []string{"127.0.0.1:11081", "127.0.0.1:11082", "127.0.0.1:11083"}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

	// Start the bootstrap leader (node 1), then join nodes 2 and 3.
	n1 := startNode(t, bin, addrs[0], "1", dirs[0])
	defer kill9(t, n1)
	_ = waitWritable(t, addrs[0], 30*time.Second)

	n2 := startNode(t, bin, addrs[1], "2", dirs[1], "-j", addrs[0])
	defer kill9(t, n2)
	n3 := startNode(t, bin, addrs[2], "3", dirs[2], "-j", addrs[0])
	defer kill9(t, n3)

	// Wait for cluster formation: nodes 2 and 3 must report as followers
	// (joined and aware of the leader).
	formDeadline := time.Now().Add(45 * time.Second)
	for {
		if nodeRole(addrs[1]) == "follower" && nodeRole(addrs[2]) == "follower" {
			break
		}
		if time.Now().After(formDeadline) {
			t.Fatalf("cluster did not form (n2=%s n3=%s)", nodeRole(addrs[1]), nodeRole(addrs[2]))
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Write data to the leader. Raft commits require a majority (2/3), so each
	// committed write is guaranteed to be on at least one follower.
	ctx := context.Background()
	lc := redis.NewClient(&redis.Options{Addr: addrs[0]})
	const n = 200
	for i := 0; i < n; i++ {
		if err := lc.Set(ctx, fmt.Sprintf("clus:%d", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
			lc.Close()
			t.Fatalf("write %d to leader: %v", i, err)
		}
	}
	lc.Close()

	// Chaos: hard-kill the leader (node 1).
	kill9(t, n1)

	// A new leader must be elected among the survivors.
	survivors := []string{addrs[1], addrs[2]}
	newLeader := waitLeaderAmong(t, survivors, 45*time.Second)
	t.Logf("new leader after failover: %s", newLeader)

	// All data written before the kill must survive (read via whichever survivor
	// currently serves reads; retries across any residual leadership settling).
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("clus:%d", i)
		got, ok := clusterGet(survivors, key, 45*time.Second)
		if !ok {
			t.Fatalf("could not read %s from any survivor after failover", key)
		}
		if want := fmt.Sprintf("v%d", i); got != want {
			t.Fatalf("%s = %q after failover, want %q (data lost)", key, got, want)
		}
	}

	// The cluster must accept new writes after failover.
	if !clusterSet(survivors, "clus:postfailover", "ok", 45*time.Second) {
		t.Fatalf("cluster did not accept writes after failover")
	}
	if got, ok := clusterGet(survivors, "clus:postfailover", 30*time.Second); !ok || got != "ok" {
		t.Fatalf("read-back after failover = %q, ok=%v; want \"ok\"", got, ok)
	}
}

// TestIntegrationCrashRecovery writes data, hard-kills the node (SIGKILL), then
// restarts it on the same data dir and asserts the data survived.
func TestIntegrationCrashRecovery(t *testing.T) {
	bin := itBinary(t)
	dir := t.TempDir()
	const addr = "127.0.0.1:11071"

	// Phase 1: start, write, hard-kill.
	cmd := startNode(t, bin, addr, "1", dir)
	c := waitWritable(t, addr, 30*time.Second)
	ctx := context.Background()

	const n = 200
	for i := 0; i < n; i++ {
		if err := c.Set(ctx, fmt.Sprintf("crash:%d", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
			kill9(t, cmd)
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = c.Close()
	kill9(t, cmd)

	// Phase 2: restart on the same data dir, verify durability.
	cmd2 := startNode(t, bin, addr, "1", dir)
	defer kill9(t, cmd2)
	c2 := waitWritable(t, addr, 30*time.Second)
	c2.Close()

	// Reads go through clusterGet so they tolerate the brief MOVED-to-self window
	// while the freshly-restarted node re-establishes leadership for reads.
	single := []string{addr}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("crash:%d", i)
		got, ok := clusterGet(single, key, 30*time.Second)
		if !ok {
			t.Fatalf("could not read %s after restart", key)
		}
		if want := fmt.Sprintf("v%d", i); got != want {
			t.Fatalf("%s = %q after restart, want %q (data lost)", key, got, want)
		}
	}
}
