//go:build sqlproxe2e

// Package e2e exercises IceFireDB-SQLProxy end to end: a real MySQL server in Docker, two
// proxy processes replicating to each other over p2p, and a MySQL client going through the
// proxy. Unit tests cannot prove that a write actually reaches a peer; this can.
//
// Run with: make e2e   (needs docker; skipped when docker is unavailable)
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
)

const (
	rootUser      = "root"
	rootPassword  = "rootpass"
	mysqlUser     = "sqlproxy"
	mysqlPassword = "sqlproxypass"
	readerUser    = "reader"
	readerPass    = "readerpass"
	p2pToken      = "e2e-shared-secret"

	schemaA = "e2e_a"
	schemaB = "e2e_b"

	startupTimeout  = 90 * time.Second
	repliceTimeout  = 30 * time.Second
	pollInterval    = 250 * time.Millisecond
	proxyUser       = "host1"
	proxyPass       = "host1"
	readonlyTenant  = "reader"
	readonlyTenantP = "readerpass"
)

func TestReplicationEndToEnd(t *testing.T) {
	requireDocker(t)

	mysqlAddr := startMySQL(t)
	seedSchema(t, mysqlAddr)

	binary := buildProxy(t)

	nodeA := startNode(t, nodeSpec{
		name:   "a",
		schema: schemaA,
		peers:  nil,
		binary: binary,
	})
	t.Logf("node A: proxy=%s debug=%d peer=%s", nodeA.proxyAddr, nodeA.debugPort, nodeA.peerID())

	peerAddr := nodeA.dialableAddr(t)
	nodeB := startNode(t, nodeSpec{
		name:   "b",
		schema: schemaB,
		peers:  []string{peerAddr},
		binary: binary,
	})
	t.Logf("node B: proxy=%s debug=%d", nodeB.proxyAddr, nodeB.debugPort)

	waitFor(t, repliceTimeout, "nodes to connect", func() bool {
		return nodeB.peers(t) >= 1
	})
	waitForReplication(t, nodeA.proxyAddr, mysqlAddr, schemaB)

	t.Run("autocommit write reaches the peer", func(t *testing.T) {
		execSQL(t, nodeA.proxyAddr, "INSERT INTO t (id, v) VALUES (1, 'autocommit')")
		waitForRow(t, mysqlAddr, schemaB, 1, "autocommit")
	})

	t.Run("prepared statement write reaches the peer", func(t *testing.T) {
		runPrepared(t, nodeA.proxyAddr, "INSERT INTO t (id, v) VALUES (?, ?)", 2, "prepared")
		waitForRow(t, mysqlAddr, schemaB, 2, "prepared")
	})

	t.Run("transaction reaches the peer as one batch", func(t *testing.T) {
		conn := dial(t, nodeA.proxyAddr)
		defer conn.Close()

		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO t (id, v) VALUES (10, 'txn-10')")
		mustExec(t, conn, "INSERT INTO t (id, v) VALUES (11, 'txn-11')")
		mustExec(t, conn, "COMMIT")

		waitForRow(t, mysqlAddr, schemaB, 10, "txn-10")
		waitForRow(t, mysqlAddr, schemaB, 11, "txn-11")
	})

	t.Run("rolled back transaction does not reach the peer", func(t *testing.T) {
		conn := dial(t, nodeA.proxyAddr)
		defer conn.Close()

		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO t (id, v) VALUES (12, 'rolled-back')")
		mustExec(t, conn, "ROLLBACK")

		// Give replication a chance to (incorrectly) deliver it.
		time.Sleep(2 * time.Second)
		if rowExists(t, mysqlAddr, schemaB, 12) {
			t.Fatal("a rolled back transaction was replicated")
		}
	})

	t.Run("readonly tenant cannot write", func(t *testing.T) {
		conn := dialAs(t, nodeA.proxyAddr, readonlyTenant, readonlyTenantP)
		defer conn.Close()

		if _, err := conn.Execute("INSERT INTO t (id, v) VALUES (13, 'nope')"); err == nil {
			t.Fatal("readonly tenant was allowed to write")
		}
		if _, err := conn.Execute("SELECT id FROM t WHERE id = 1"); err != nil {
			t.Fatalf("readonly tenant cannot read: %v", err)
		}
	})

	t.Run("metrics report replication", func(t *testing.T) {
		applied := nodeB.metric(t, `sqlproxy_replication_applied_total{kind="txn"}`)
		if applied < 1 {
			t.Fatalf("node B applied no txn message: %s", nodeB.metrics(t))
		}
		if got := nodeB.metric(t, `sqlproxy_replication_rejected_total{reason="signature"}`); got != 0 {
			t.Fatalf("node B rejected %v signed messages", got)
		}
	})
}

// ---------------------------------------------------------------- MySQL

func startMySQL(t *testing.T) string {
	t.Helper()

	port := freePort(t)
	name := fmt.Sprintf("icefiredb-sqlproxy-e2e-%d", time.Now().UnixNano())

	// mysql_native_password keeps the test independent of the caching_sha2 RSA exchange.
	args := []string{
		"run", "-d", "--name", name,
		"-e", "MYSQL_ROOT_PASSWORD=" + rootPassword,
		// Without MYSQL_ROOT_HOST the image creates root@localhost only, which the
		// seeding connection below (from the host, over TCP) cannot use.
		"-e", "MYSQL_ROOT_HOST=%",
		"-e", "MYSQL_DATABASE=" + schemaA,
		"-e", "MYSQL_USER=" + mysqlUser,
		"-e", "MYSQL_PASSWORD=" + mysqlPassword,
		"-p", fmt.Sprintf("127.0.0.1:%d:3306", port),
		"mysql:8.0",
		"--default-authentication-plugin=mysql_native_password",
		"--sql-mode=STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("start mysql container: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		// The container is removed below, so its log has to be captured here to be useful.
		if t.Failed() {
			if out, err := exec.Command("docker", "logs", "--tail", "100", name).CombinedOutput(); err == nil {
				t.Logf("mysql log:\n%s", out)
			}
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	mysqlAddrForTest = addr
	waitFor(t, startupTimeout, "MySQL to accept connections", func() bool {
		conn, err := client.Connect(addr, mysqlUser, mysqlPassword, schemaA)
		if err != nil {
			return false
		}
		defer conn.Close()
		_, err = conn.Execute("SELECT 1")
		return err == nil
	})
	return addr
}

func seedSchema(t *testing.T, mysqlAddr string) {
	t.Helper()

	// The image grants MYSQL_USER rights on MYSQL_DATABASE only, so creating the peer
	// schema and the read-only user needs the root account.
	conn := dialAsMySQL(t, mysqlAddr, rootUser, rootPassword, schemaA)
	defer conn.Close()

	for _, stmt := range []string{
		"CREATE DATABASE IF NOT EXISTS " + schemaB,
		"GRANT ALL PRIVILEGES ON " + schemaB + ".* TO '" + mysqlUser + "'@'%'",
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s'", readerUser, readerPass),
		"GRANT SELECT ON " + schemaA + ".* TO '" + readerUser + "'@'%'",
		"GRANT SELECT ON " + schemaB + ".* TO '" + readerUser + "'@'%'",
		"CREATE TABLE IF NOT EXISTS " + schemaA + ".t (id INT PRIMARY KEY, v VARCHAR(64))",
		"CREATE TABLE IF NOT EXISTS " + schemaB + ".t (id INT PRIMARY KEY, v VARCHAR(64))",
	} {
		mustExec(t, conn, stmt)
	}
}

// ---------------------------------------------------------------- proxy

type nodeSpec struct {
	name   string
	schema string
	peers  []string
	binary string
}

type node struct {
	proxyAddr string
	debugPort int
	peerIDVal string
	cmd       *exec.Cmd
}

func (n *node) peerID() string { return n.peerIDVal }

// renderConfig produces a node's configuration file. It is separate from startNode so a
// test can prove the YAML this suite ships parses, without needing a container.
func renderConfig(spec nodeSpec, proxyPort, debugPort int, mysqlAddr string) string {
	peers := "[]"
	if len(spec.peers) > 0 {
		quoted := make([]string, 0, len(spec.peers))
		for _, p := range spec.peers {
			quoted = append(quoted, fmt.Sprintf("%q", p))
		}
		peers = "[" + strings.Join(quoted, ", ") + "]"
	}

	return fmt.Sprintf(`server:
  addr: "127.0.0.1:%d"
  idleTimeout: 60
  writeTimeout: 60
debug:
  enable: true
  port: %d
mysql:
  admin:
    addr: "%s"
    user: "%s"
    password: "%s"
    dbname: "%s"
    minAlive: 1
    maxAlive: 8
    maxIdle: 2
  readonly:
    addr: "%s"
    user: "%s"
    password: "%s"
    dbname: "%s"
    minAlive: 1
    maxAlive: 4
    maxIdle: 2
userlist:
  - user: "%s"
    password: "%s"
  - user: "%s"
    password: "%s"
    role: readonly
p2p:
  enable: true
  serviceDiscoveryId: "icefiredb-sqlproxy-e2e"
  serviceCommandTopic: "icefiredb-sqlproxy-e2e-command"
  adminTopic: "icefiredb-sqlproxy-e2e-admin"
  readonlyTopic: "icefiredb-sqlproxy-e2e-readonly"
  serviceDiscoverMode: "advertise"
  nodeHostIp: "127.0.0.1"
  nodeHostPort: 0
  authToken: "%s"
  bootstrapPeers: %s
`,
		proxyPort, debugPort,
		mysqlAddr, mysqlUser, mysqlPassword, spec.schema,
		mysqlAddr, readerUser, readerPass, spec.schema,
		proxyUser, proxyPass, readonlyTenant, readonlyTenantP,
		p2pToken, peers,
	)
}

func startNode(t *testing.T, spec nodeSpec) *node {
	t.Helper()

	proxyPort := freePort(t)
	debugPort := freePort(t)
	dir := t.TempDir()

	conf := renderConfig(spec, proxyPort, debugPort, mysqlHostPort(t))

	confPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	logFile, err := os.Create(filepath.Join(dir, "node.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}

	cmd := exec.Command(spec.binary, "-c", confPath, "-l", "debug")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start node %s: %v", spec.name, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		logFile.Close()
		if t.Failed() {
			if out, err := os.ReadFile(filepath.Join(dir, "node.log")); err == nil {
				t.Logf("node %s log:\n%s", spec.name, out)
			}
		}
	})

	n := &node{
		proxyAddr: fmt.Sprintf("127.0.0.1:%d", proxyPort),
		debugPort: debugPort,
		cmd:       cmd,
	}

	waitFor(t, startupTimeout, "node "+spec.name+" to report healthy", func() bool {
		report, err := n.health()
		return err == nil && report.Status == "ok"
	})

	report, err := n.health()
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	n.peerIDVal = report.P2P.PeerID
	return n
}

func buildProxy(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "IceFireDB-SQLProxy")
	cmd := exec.Command("go", "build", "-o", out, "../..")
	cmd.Dir = mustGetwd(t)
	if buildOut, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build proxy: %v\n%s", err, buildOut)
	}
	return out
}

// ---------------------------------------------------------------- health/metrics

type health struct {
	Status string `json:"status"`
	P2P    struct {
		Enabled bool     `json:"enabled"`
		Peers   int      `json:"peers"`
		PeerID  string   `json:"peerId"`
		Addrs   []string `json:"addrs"`
	} `json:"p2p"`
}

func (n *node) health() (*health, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", n.debugPort))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var report health
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return nil, err
	}
	return &report, nil
}

func (n *node) peers(t *testing.T) int {
	t.Helper()
	report, err := n.health()
	if err != nil {
		return 0
	}
	return report.P2P.Peers
}

func (n *node) metrics(t *testing.T) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", n.debugPort))
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}

// metric returns the value of an exact "name{labels} value" sample line.
func (n *node) metric(t *testing.T, sample string) float64 {
	t.Helper()
	for _, line := range strings.Split(n.metrics(t), "\n") {
		if !strings.HasPrefix(line, sample+" ") {
			continue
		}
		var value float64
		if _, err := fmt.Sscanf(strings.TrimPrefix(line, sample+" "), "%g", &value); err != nil {
			t.Fatalf("parse metric %q: %v", line, err)
		}
		return value
	}
	return 0
}

// dialableAddr picks the TCP multiaddr another node can dial.
func (n *node) dialableAddr(t *testing.T) string {
	t.Helper()

	var lastErr error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(pollInterval) {
		report, err := n.health()
		if err != nil {
			lastErr = err
			continue
		}
		for _, addr := range report.P2P.Addrs {
			if strings.Contains(addr, "/tcp/") && strings.Contains(addr, "/ip4/127.0.0.1/") {
				return addr
			}
		}
		lastErr = fmt.Errorf("no dialable tcp address in %v", report.P2P.Addrs)
	}
	t.Fatalf("node has no dialable address: %v", lastErr)
	return ""
}

// ---------------------------------------------------------------- SQL helpers

func dial(t *testing.T, addr string) *client.Conn {
	t.Helper()
	return dialAs(t, addr, proxyUser, proxyPass)
}

func dialAs(t *testing.T, addr, user, password string) *client.Conn {
	t.Helper()
	conn, err := client.Connect(addr, user, password, schemaA)
	if err != nil {
		t.Fatalf("connect to proxy %s as %s: %v", addr, user, err)
	}
	return conn
}

func dialAsMySQL(t *testing.T, addr, user, password, db string) *client.Conn {
	t.Helper()
	conn, err := client.Connect(addr, user, password, db)
	if err != nil {
		t.Fatalf("connect to mysql as %s: %v", user, err)
	}
	return conn
}

func execSQL(t *testing.T, proxyAddr, statement string) {
	t.Helper()
	conn := dial(t, proxyAddr)
	defer conn.Close()
	mustExec(t, conn, statement)
}

func runPrepared(t *testing.T, proxyAddr, statement string, args ...any) {
	t.Helper()
	conn := dial(t, proxyAddr)
	defer conn.Close()

	stmt, err := conn.Prepare(statement)
	if err != nil {
		t.Fatalf("prepare %q: %v", statement, err)
	}
	defer stmt.Close()

	if _, err := stmt.Execute(args...); err != nil {
		t.Fatalf("execute %q: %v", statement, err)
	}
}

func mustExec(t *testing.T, conn *client.Conn, statement string) {
	t.Helper()
	if _, err := conn.Execute(statement); err != nil {
		t.Fatalf("execute %q: %v", statement, err)
	}
}

// rowMatches reports whether the peer's schema already holds want for id.
func rowMatches(mysqlAddr, db string, id int, want string) bool {
	conn, err := client.Connect(mysqlAddr, mysqlUser, mysqlPassword, db)
	if err != nil {
		return false
	}
	defer conn.Close()

	result, err := conn.Execute(fmt.Sprintf("SELECT v FROM t WHERE id = %d", id))
	if err != nil || len(result.Values) == 0 {
		return false
	}
	return string(result.Values[0][0].AsString()) == want
}

// waitForReplication publishes a warm-up write until the peer applies it. Gossipsub grafts
// a peer a heartbeat or two after it connects, and a write published before that is lost —
// best-effort replication makes no delivery guarantee — so the test waits for the channel
// to be live rather than racing it.
func waitForReplication(t *testing.T, proxyAddr, mysqlAddr, peerSchema string) {
	t.Helper()

	deadline := time.Now().Add(repliceTimeout)
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		want := fmt.Sprintf("warmup-%d", attempt)
		execSQL(t, proxyAddr, fmt.Sprintf("REPLACE INTO t (id, v) VALUES (100, '%s')", want))

		probeUntil := time.Now().Add(3 * time.Second)
		for time.Now().Before(probeUntil) {
			if rowMatches(mysqlAddr, peerSchema, 100, want) {
				t.Logf("replication became ready after %d warm-up write(s)", attempt)
				return
			}
			time.Sleep(pollInterval)
		}
	}
	t.Fatalf("replication never became ready: no warm-up write reached %s", peerSchema)
}

func rowExists(t *testing.T, mysqlAddr, db string, id int) bool {
	t.Helper()
	conn := dialAsMySQL(t, mysqlAddr, mysqlUser, mysqlPassword, db)
	defer conn.Close()

	result, err := conn.Execute(fmt.Sprintf("SELECT id FROM t WHERE id = %d", id))
	if err != nil {
		t.Fatalf("select id=%d in %s: %v", id, db, err)
	}
	return len(result.Values) > 0
}

func waitForRow(t *testing.T, mysqlAddr, db string, id int, want string) {
	t.Helper()
	waitFor(t, repliceTimeout, fmt.Sprintf("row %d to appear in %s", id, db), func() bool {
		return rowMatches(mysqlAddr, db, id, want)
	})
}

// ---------------------------------------------------------------- plumbing

// mysqlAddrForTest is set by startMySQL so every node config points at the same
// container.
var mysqlAddrForTest string

func mysqlHostPort(t *testing.T) string {
	t.Helper()
	if mysqlAddrForTest == "" {
		t.Fatal("mysql address not initialised")
	}
	return mysqlAddrForTest
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func requireDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_E2E") != "" {
		t.Skip("SKIP_E2E is set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not available")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon is not reachable")
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func mustGetwd(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return dir
}
