//go:build partition
// +build partition

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestPartitionSplitBrain runs a 3-node cluster in Docker containers and induces
// a real network partition by disconnecting the leader from the inter-node
// (raft) network. It verifies the canonical Raft no-split-brain guarantee:
//   - the isolated former leader loses quorum and can no longer commit writes;
//   - the surviving majority elects a new leader and stays available;
//   - after the partition heals, the rejoined node converges (no data loss).
//
// Two docker networks are used: a raft plane ("...-net", which we cut) and a
// control plane ("...-ctl", which stays up so a redis-cli client can reach every
// node — including the isolated one — to observe behavior). Clients run as
// throwaway redis:alpine containers ON the control network, because the test
// process itself is in a different network namespace than the docker host and
// cannot reach published ports.
//
// Requires docker and a static linux binary path in IFDB_STATIC:
//
//	CGO_ENABLED=0 go build -o /tmp/ifdb-static . && \
//	  IFDB_STATIC=/tmp/ifdb-static go test -tags partition -run TestPartition -v ./
func TestPartitionSplitBrain(t *testing.T) {
	bin := os.Getenv("IFDB_STATIC")
	if bin == "" {
		t.Skip("IFDB_STATIC not set; build a static linux binary to run the partition test")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("IFDB_STATIC=%q not usable: %v", bin, err)
	}
	mustDocker(t)
	mustImage(t, cliImage)

	const raftNet = "ifdb-pt-net"
	const ctlNet = "ifdb-pt-ctl"
	names := []string{"ifdb-pt-n1", "ifdb-pt-n2", "ifdb-pt-n3"}
	raftAlias := []string{"ptnode1", "ptnode2", "ptnode3"} // raft-plane (advertised)
	ctlAlias := []string{"ctl1", "ctl2", "ctl3"}           // control-plane (client)

	cleanup := func() {
		for _, n := range names {
			_ = exec.Command("docker", "rm", "-f", n).Run()
		}
		_ = exec.Command("docker", "network", "rm", raftNet).Run()
		_ = exec.Command("docker", "network", "rm", ctlNet).Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	mustRun(t, "docker", "network", "create", raftNet)
	mustRun(t, "docker", "network", "create", ctlNet)

	// Create each node on the control network, attach the raft network, so both
	// NICs exist before the process boots. The binary is wrapped in a retry loop
	// because uhaha exits on a transient join failure; retrying lets the joiner
	// succeed once the leader is stable.
	for i := range names {
		ifdbCmd := fmt.Sprintf(
			"/ifdb -a 0.0.0.0:11001 -n %d -d /data --localtime --advertise %s:11001",
			i+1, raftAlias[i],
		)
		if i > 0 {
			ifdbCmd += " -j " + raftAlias[0] + ":11001"
		}
		loop := "while true; do " + ifdbCmd + "; echo '[restart]'; sleep 1; done"
		mustRun(t, "docker",
			"create", "--name", names[i],
			"--network", ctlNet, "--network-alias", ctlAlias[i],
			"-v", bin+":/ifdb:ro",
			"alpine:latest", "sh", "-c", loop,
		)
		mustRun(t, "docker", "network", "connect", "--alias", raftAlias[i], raftNet, names[i])
	}

	// Start the bootstrap leader, wait until it is a stable leader.
	mustRun(t, "docker", "start", names[0])
	if !waitRole(ctlNet, ctlAlias[0], "leader", 45*time.Second) {
		dumpDiag(t, names)
		t.Fatalf("node1 did not become leader")
	}
	// Join nodes one at a time (serialize membership changes).
	mustRun(t, "docker", "start", names[1])
	if !waitRole(ctlNet, ctlAlias[1], "follower", 60*time.Second) {
		dumpDiag(t, names)
		t.Fatalf("node2 did not become follower: role=%s", role(ctlNet, ctlAlias[1]))
	}
	mustRun(t, "docker", "start", names[2])
	if !waitRole(ctlNet, ctlAlias[2], "follower", 60*time.Second) {
		dumpDiag(t, names)
		t.Fatalf("node3 did not become follower: role=%s", role(ctlNet, ctlAlias[2]))
	}

	// Seed data via the leader.
	if !partSet(ctlNet, ctlAlias, "pt:seed", "1", 30*time.Second) {
		t.Fatalf("seed write failed")
	}

	// Identify the leader index.
	leaderIdx := -1
	for i := range ctlAlias {
		if role(ctlNet, ctlAlias[i]) == "leader" {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatalf("no leader found before partition")
	}
	t.Logf("leader before partition: %s", names[leaderIdx])

	// PARTITION: cut the leader off the raft network. It keeps its control-plane
	// NIC so the client can still observe it.
	mustRun(t, "docker", "network", "disconnect", raftNet, names[leaderIdx])

	majority := []string{}
	for i := range ctlAlias {
		if i != leaderIdx {
			majority = append(majority, ctlAlias[i])
		}
	}
	isolated := ctlAlias[leaderIdx]

	// (a) The majority must elect a new leader and accept writes.
	if !partSet(ctlNet, majority, "pt:during", "ok", 60*time.Second) {
		dumpDiag(t, names)
		t.Fatalf("majority did not accept writes after partition")
	}
	t.Logf("majority accepted a write after partition")

	// (b) The isolated former leader must NOT accept writes (no quorum).
	if !eventuallyRejectsWrites(ctlNet, isolated, 40*time.Second) {
		dumpDiag(t, names)
		t.Fatalf("isolated former leader %s still accepted a write — split-brain!", names[leaderIdx])
	}
	t.Logf("isolated former leader correctly rejects writes (no quorum)")

	// HEAL.
	mustRun(t, "docker", "network", "connect", "--alias", raftAlias[leaderIdx], raftNet, names[leaderIdx])

	// (c) Convergence: partition-era and seed data readable cluster-wide, no loss.
	if got, ok := partGet(ctlNet, ctlAlias, "pt:during", 60*time.Second); !ok || got != "ok" {
		dumpDiag(t, names)
		t.Fatalf("partition-era key not converged: got=%q ok=%v", got, ok)
	}
	if got, ok := partGet(ctlNet, ctlAlias, "pt:seed", 30*time.Second); !ok || got != "1" {
		t.Fatalf("seed key lost after heal: got=%q ok=%v", got, ok)
	}
	t.Logf("cluster converged after heal; no data loss")
}

// --- helpers ---

const cliImage = "redis:alpine"

func mustDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not usable: %v", err)
	}
}

func mustImage(t *testing.T, image string) {
	t.Helper()
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		if out, perr := exec.Command("docker", "pull", image).CombinedOutput(); perr != nil {
			t.Skipf("cannot obtain %s image: %v\n%s", image, perr, out)
		}
	}
}

func mustRun(t *testing.T, name string, args ...string) {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

func dumpDiag(t *testing.T, names []string) {
	t.Helper()
	ps, _ := exec.Command("docker", "ps", "-a", "--filter", "name=ifdb-pt", "--format", "{{.Names}} {{.Status}}").CombinedOutput()
	t.Logf("DIAG docker ps:\n%s", ps)
	for _, n := range names {
		logs, _ := exec.Command("docker", "logs", "--tail", "10", n).CombinedOutput()
		t.Logf("DIAG logs %s:\n%s", n, logs)
	}
}

// rcli runs `redis-cli -h <alias> -p 11001 <args...>` from a throwaway container
// on the control network, bounded by timeout. Returns trimmed combined output.
func rcli(ctlNet, alias string, timeout time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"run", "--rm", "--network", ctlNet, cliImage,
		"redis-cli", "-h", alias, "-p", "11001"}, args...)
	out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// role classifies a node by attempting a write:
//   - "leader": the write is accepted ("OK");
//   - "follower": the node is up but not the leader — uhaha replies with a
//     redirect ("MOVED"/"TRY") when it knows the leader, or "CLUSTERDOWN node is
//     not the leader" when the leader address is not yet known;
//   - "down": unreachable or the write blocked (no quorum).
func isNotLeaderReply(out string) bool {
	return strings.Contains(out, "MOVED") ||
		strings.Contains(out, "TRY") ||
		strings.Contains(out, "not the leader") ||
		strings.Contains(out, "CLUSTERDOWN")
}

func role(ctlNet, alias string) string {
	out, err := rcli(ctlNet, alias, 6*time.Second, "SET", "__role__", "1")
	switch {
	case err == nil && out == "OK":
		return "leader"
	case isNotLeaderReply(out):
		return "follower"
	default:
		return "down"
	}
}

func waitRole(ctlNet, alias, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if role(ctlNet, alias) == want {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// partSet writes key=val via whichever alias is the leader, retrying until timeout.
func partSet(ctlNet string, aliases []string, key, val string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, a := range aliases {
			out, err := rcli(ctlNet, a, 6*time.Second, "SET", key, val)
			if err == nil && out == "OK" {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}

// partGet reads key via whichever alias is the leader, retrying until timeout.
func partGet(ctlNet string, aliases []string, key string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, a := range aliases {
			out, err := rcli(ctlNet, a, 6*time.Second, "GET", key)
			// redis-cli exits 0 even for error replies, so filter non-leader
			// replies by text; only the leader returns the actual value.
			if err == nil && out != "" && !isNotLeaderReply(out) {
				return out, true
			}
		}
		time.Sleep(time.Second)
	}
	return "", false
}

// eventuallyRejectsWrites returns true once a write to the isolated node fails
// and stays failing (the node lost quorum / stepped down). A successful "OK"
// at any point means it committed without quorum — a split-brain violation.
func eventuallyRejectsWrites(ctlNet, alias string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out1, err1 := rcli(ctlNet, alias, 6*time.Second, "SET", "__iso1__", "x")
		rejected1 := !(err1 == nil && out1 == "OK")
		if rejected1 {
			// Confirm it stays rejecting (not a transient blip mid-stepdown).
			time.Sleep(2 * time.Second)
			out2, err2 := rcli(ctlNet, alias, 6*time.Second, "SET", "__iso2__", "x")
			if !(err2 == nil && out2 == "OK") {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}
