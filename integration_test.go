//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
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

// clusterRoles returns the current role of each address (for diagnostics).
func clusterRoles(addrs []string) []string {
	r := make([]string, len(addrs))
	for i, a := range addrs {
		r[i] = nodeRole(a)
	}
	return r
}

// startCluster3 brings up a 3-node cluster (node1 bootstraps, nodes 2 and 3
// join) on ports base, base+1, base+2, and waits for a healthy 1-leader/
// 2-follower formation. Returns the addresses, data dirs, and commands. The
// caller is responsible for killing the commands.
func startCluster3(t *testing.T, bin string, base int) (addrs, dirs []string, cmds []*exec.Cmd) {
	t.Helper()
	for i := 0; i < 3; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", base+i))
		dirs = append(dirs, t.TempDir())
	}
	cmds = make([]*exec.Cmd, 3)
	cmds[0] = startNode(t, bin, addrs[0], "1", dirs[0])
	_ = waitWritable(t, addrs[0], 30*time.Second)
	cmds[1] = startNode(t, bin, addrs[1], "2", dirs[1], "-j", addrs[0])
	cmds[2] = startNode(t, bin, addrs[2], "3", dirs[2], "-j", addrs[0])

	deadline := time.Now().Add(60 * time.Second)
	for {
		leaders, followers := 0, 0
		for _, a := range addrs {
			switch nodeRole(a) {
			case "leader":
				leaders++
			case "follower":
				followers++
			}
		}
		if leaders == 1 && followers == 2 {
			return addrs, dirs, cmds
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster did not form: roles=%v", clusterRoles(addrs))
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitNodeUp waits until a node responds (role != "down") after a restart.
func waitNodeUp(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nodeRole(addr) != "down" {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("node %s did not come back within %s", addr, timeout)
}

// TestIntegrationRollingRestart restarts every node one at a time while writing
// continuously, asserting the cluster stays available throughout (quorum holds
// with 2/3 up) and that every acknowledged write survives the full rotation.
func TestIntegrationRollingRestart(t *testing.T) {
	bin := itBinary(t)
	addrs, dirs, cmds := startCluster3(t, bin, 11111)
	defer func() {
		for _, c := range cmds {
			kill9(t, c)
		}
	}()
	ids := []string{"1", "2", "3"}

	written := 0
	writeBatch := func(n int) {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("roll:%d", written)
			if !clusterSet(addrs, key, fmt.Sprintf("v%d", written), 30*time.Second) {
				t.Fatalf("write %s failed — cluster unavailable during rolling restart", key)
			}
			written++
		}
	}

	writeBatch(30)
	for i := 0; i < 3; i++ {
		kill9(t, cmds[i])
		// With one node down (2/3 up) the cluster must remain writable.
		writeBatch(20)
		// Restart on the same data dir; it recovers and rejoins.
		cmds[i] = startNode(t, bin, addrs[i], ids[i], dirs[i])
		waitNodeUp(t, addrs[i], 45*time.Second)
		writeBatch(20)
	}

	// Every acknowledged write must still be present.
	for i := 0; i < written; i++ {
		key := fmt.Sprintf("roll:%d", i)
		got, ok := clusterGet(addrs, key, 30*time.Second)
		if !ok || got != fmt.Sprintf("v%d", i) {
			t.Fatalf("%s lost/mismatch after rolling restart: got=%q ok=%v", key, got, ok)
		}
	}
	t.Logf("rolling restart complete; all %d acknowledged writes survived", written)
}

// TestIntegrationLeaderChurn repeatedly kills the current leader. Each round a
// new leader must be elected and all previously-acknowledged data must persist.
// The killed node is restarted each round to keep quorum capacity for the next.
func TestIntegrationLeaderChurn(t *testing.T) {
	bin := itBinary(t)
	addrs, dirs, cmds := startCluster3(t, bin, 11121)
	defer func() {
		for _, c := range cmds {
			kill9(t, c)
		}
	}()
	ids := []string{"1", "2", "3"}
	idxByAddr := map[string]int{addrs[0]: 0, addrs[1]: 1, addrs[2]: 2}

	type kv struct{ k, v string }
	var acked []kv
	put := func(k, v string) {
		if !clusterSet(addrs, k, v, 30*time.Second) {
			t.Fatalf("write %s failed during leader churn", k)
		}
		acked = append(acked, kv{k, v})
	}

	put("churn:seed", "s")

	const rounds = 3
	for r := 0; r < rounds; r++ {
		leader := waitLeaderAmong(t, addrs, 30*time.Second)
		put(fmt.Sprintf("churn:r%d", r), fmt.Sprintf("v%d", r))

		li := idxByAddr[leader]
		t.Logf("round %d: killing leader %s", r, leader)
		kill9(t, cmds[li])

		// Survivors must elect a new leader.
		survivors := []string{}
		for j, a := range addrs {
			if j != li {
				survivors = append(survivors, a)
			}
		}
		newLeader := waitLeaderAmong(t, survivors, 45*time.Second)
		if newLeader == leader {
			t.Fatalf("round %d: leader did not change", r)
		}

		// Restart the killed node so it rejoins (restores 3-node quorum capacity).
		cmds[li] = startNode(t, bin, addrs[li], ids[li], dirs[li])
		waitNodeUp(t, addrs[li], 45*time.Second)
	}

	// All acknowledged data must have survived the churn.
	for _, e := range acked {
		got, ok := clusterGet(addrs, e.k, 30*time.Second)
		if !ok || got != e.v {
			t.Fatalf("%s lost/mismatch after leader churn: got=%q ok=%v", e.k, got, ok)
		}
	}
	t.Logf("leader churn complete over %d rounds; all %d acknowledged writes survived", rounds, len(acked))
}

// TestIntegrationSoak drives sustained concurrent write load against a 3-node
// cluster for a configurable duration, then verifies a sample of acknowledged
// writes survived. With SOAK_CHAOS=1 it also kills+restarts the leader on a
// timer during the run (soak-under-chaos). Tunables:
//
//	SOAK_DURATION (default 30s), SOAK_WORKERS (default 4), SOAK_CHAOS=1
func TestIntegrationSoak(t *testing.T) {
	bin := itBinary(t)

	dur := 30 * time.Second
	if s := os.Getenv("SOAK_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			dur = d
		}
	}
	workers := 4
	if s := os.Getenv("SOAK_WORKERS"); s != "" {
		if n, err := fmt.Sscanf(s, "%d", &workers); err != nil || n != 1 || workers < 1 {
			workers = 4
		}
	}
	chaos := os.Getenv("SOAK_CHAOS") == "1"

	addrs, dirs, cmds := startCluster3(t, bin, 11131)
	ids := []string{"1", "2", "3"}
	var cmdMu sync.Mutex // guards cmds during chaos restarts
	defer func() {
		cmdMu.Lock()
		defer cmdMu.Unlock()
		for _, c := range cmds {
			kill9(t, c)
		}
	}()

	deadline := time.Now().Add(dur)
	var totalOK, totalErr int64
	ackedPerWorker := make([][]string, workers)
	var wg sync.WaitGroup

	// Optional chaos: kill + restart the current leader on a timer.
	if chaos {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				time.Sleep(8 * time.Second)
				if !time.Now().Before(deadline) {
					return
				}
				leader := ""
				for _, a := range addrs {
					if nodeRole(a) == "leader" {
						leader = a
						break
					}
				}
				if leader == "" {
					continue
				}
				li := 0
				for j, a := range addrs {
					if a == leader {
						li = j
					}
				}
				cmdMu.Lock()
				kill9(t, cmds[li])
				cmds[li] = nil
				cmdMu.Unlock()
				// Let the majority elect a new leader, then restart the node.
				time.Sleep(3 * time.Second)
				c := startNode(t, bin, addrs[li], ids[li], dirs[li])
				cmdMu.Lock()
				cmds[li] = c
				cmdMu.Unlock()
				t.Logf("soak-chaos: cycled leader %s", leader)
			}
		}()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clients := make([]*redis.Client, len(addrs))
			for i, a := range addrs {
				clients[i] = redis.NewClient(&redis.Options{Addr: a, DialTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
			}
			defer func() {
				for _, c := range clients {
					c.Close()
				}
			}()
			li, n := 0, 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("soak:%d:%d", id, n)
				ok := false
				for try := 0; try < len(addrs)+1; try++ {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					err := clients[li].Set(ctx, key, key, 0).Err()
					cancel()
					if err == nil {
						ok = true
						break
					}
					li = (li + 1) % len(addrs) // rotate to find the current leader
				}
				if ok {
					atomic.AddInt64(&totalOK, 1)
					ackedPerWorker[id] = append(ackedPerWorker[id], key)
				} else {
					atomic.AddInt64(&totalErr, 1)
				}
				n++
			}
		}(w)
	}

	wg.Wait()

	ok := atomic.LoadInt64(&totalOK)
	errs := atomic.LoadInt64(&totalErr)
	t.Logf("soak: duration=%s workers=%d chaos=%v acked=%d failed=%d (%.0f ok-ops/sec)",
		dur, workers, chaos, ok, errs, float64(ok)/dur.Seconds())

	if ok == 0 {
		t.Fatalf("soak made no successful writes")
	}
	// GA guard: refuse to pass if throughput collapsed to near-nothing. This
	// catches degraded-but-nonzero regressions that the ok==0 check alone misses.
	// The 1 ops/s floor is deliberately conservative (typical runs are hundreds
	// of ops/s) so it does not flake under chaos or on slow CI.
	throughput := float64(ok) / dur.Seconds()
	if throughput < 1.0 {
		t.Fatalf("soak throughput %.1f ops/s below 1 ops/s GA floor (acked=%d, duration=%s)", throughput, ok, dur)
	}
	// Without chaos the cluster should be continuously available — failures
	// should be negligible. With chaos, brief unavailability during elections
	// is expected, so we only require that progress was made.
	if !chaos && errs > ok/100+5 {
		t.Fatalf("soak: too many write failures without chaos: acked=%d failed=%d", ok, errs)
	}

	// Verify a strided sample of acknowledged writes survived (value == key).
	checked := 0
	for id := 0; id < workers; id++ {
		keys := ackedPerWorker[id]
		step := 1
		if len(keys) > 100 {
			step = len(keys) / 100
		}
		for i := 0; i < len(keys); i += step {
			got, found := clusterGet(addrs, keys[i], 30*time.Second)
			if !found || got != keys[i] {
				t.Fatalf("soak: acknowledged key %s lost/mismatch: got=%q found=%v", keys[i], got, found)
			}
			checked++
		}
	}
	t.Logf("soak: verified %d sampled acknowledged writes intact", checked)
}

// TestIntegrationFollowerRejoin exercises join/leave churn: it kills a follower,
// restarts it (which must rejoin from its persisted Raft state), then kills the
// original leader. A new leader can only be elected if the restarted follower
// successfully rejoined to restore quorum (2 of 3). This proves rejoin plus
// data durability across the churn.
func TestIntegrationFollowerRejoin(t *testing.T) {
	bin := itBinary(t)
	addrs := []string{"127.0.0.1:11091", "127.0.0.1:11092", "127.0.0.1:11093"}
	ids := []string{"1", "2", "3"}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cmds := make([]*exec.Cmd, 3)

	// Bring up the cluster: node 1 bootstraps, nodes 2 and 3 join.
	cmds[0] = startNode(t, bin, addrs[0], ids[0], dirs[0])
	defer func() { kill9(t, cmds[0]) }()
	_ = waitWritable(t, addrs[0], 30*time.Second)
	cmds[1] = startNode(t, bin, addrs[1], ids[1], dirs[1], "-j", addrs[0])
	defer func() { kill9(t, cmds[1]) }()
	cmds[2] = startNode(t, bin, addrs[2], ids[2], dirs[2], "-j", addrs[0])
	defer func() { kill9(t, cmds[2]) }()

	// Wait for formation (both 2 and 3 report as followers).
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

	// Write data to the current leader.
	const n = 150
	if !clusterSet(addrs, "rejoin:seed", "1", 30*time.Second) {
		t.Fatalf("seed write failed")
	}
	ctx := context.Background()
	leaderAddr := waitLeaderAmong(t, addrs, 30*time.Second)
	lc := redis.NewClient(&redis.Options{Addr: leaderAddr})
	for i := 0; i < n; i++ {
		if err := lc.Set(ctx, fmt.Sprintf("rejoin:%d", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
			lc.Close()
			t.Fatalf("write %d: %v", i, err)
		}
	}
	lc.Close()

	// Identify a follower (not the leader), find its index, kill and restart it.
	followerIdx := -1
	for i, a := range addrs {
		if a != leaderAddr && nodeRole(a) == "follower" {
			followerIdx = i
			break
		}
	}
	if followerIdx == -1 {
		t.Fatalf("no follower found to restart (leader=%s)", leaderAddr)
	}
	t.Logf("restarting follower %s (node %s)", addrs[followerIdx], ids[followerIdx])
	kill9(t, cmds[followerIdx])
	// Restart WITHOUT -j: it must rejoin from persisted Raft state.
	cmds[followerIdx] = startNode(t, bin, addrs[followerIdx], ids[followerIdx], dirs[followerIdx])
	// Give it a moment to come back and rejoin.
	rejoinDeadline := time.Now().Add(30 * time.Second)
	for nodeRole(addrs[followerIdx]) == "down" && time.Now().Before(rejoinDeadline) {
		time.Sleep(300 * time.Millisecond)
	}

	// Now kill the original leader. Quorum (2/3) only holds if the restarted
	// follower rejoined; otherwise no new leader can be elected.
	for i, a := range addrs {
		if a == leaderAddr {
			kill9(t, cmds[i])
		}
	}
	survivors := []string{}
	for _, a := range addrs {
		if a != leaderAddr {
			survivors = append(survivors, a)
		}
	}
	newLeader := waitLeaderAmong(t, survivors, 45*time.Second)
	t.Logf("new leader after follower-rejoin + leader-kill: %s", newLeader)

	// Data must be intact and the cluster writable.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("rejoin:%d", i)
		got, ok := clusterGet(survivors, key, 45*time.Second)
		if !ok {
			t.Fatalf("could not read %s after churn", key)
		}
		if want := fmt.Sprintf("v%d", i); got != want {
			t.Fatalf("%s = %q after churn, want %q (data lost)", key, got, want)
		}
	}
	if !clusterSet(survivors, "rejoin:postchurn", "ok", 30*time.Second) {
		t.Fatalf("cluster not writable after churn")
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
