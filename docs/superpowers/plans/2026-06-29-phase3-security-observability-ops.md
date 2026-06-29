# Phase 3 — Security, Observability & Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for the code task (Task 4). The doc/hygiene tasks are low-risk and may be applied directly. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close the Phase 3 gaps that gate a credible 1.0.0-rc for production/Kubernetes: clean the repo of a tracked env file, make docs accurate, let the auth secret come from the environment, add liveness/readiness/metrics HTTP endpoints, and write an operations runbook.

**Architecture:** Mostly additive. The one code feature is an opt-in observability HTTP server (separate from pprof) exposing `/healthz`, `/readyz`, and Prometheus `/metrics`. Auth gains an environment-variable fallback. Everything else is documentation/repo hygiene.

**Grounding facts (verified):**
- Only `.env` is git-tracked; the large binary, `*.test`, `*.out`, `*.log` are already gitignored (present in the working tree, never committed). `.env` contains only `IPFS_LOG_DB_NAME` (which is NOT read anywhere — the ipfs-log db name comes from the `--ipfs-log-dbname` flag). `godotenv.Load()` logs and continues if `.env` is absent, so untracking it is safe.
- `github.com/prometheus/client_golang/prometheus` is already vendored — `/metrics` needs no new dependency.
- Auth is wired only via the `--auth` flag → `conf.Auth` (`flags.go:118`). No env fallback today.
- `respClientNum` (global, `global.go`) is an atomic connected-client counter maintained by `connOpened`/`connClosed`.
- `ldb` (global) is non-nil once storage init succeeds in `DataDirReady`; it is the natural readiness signal.

**Scope note on ACL:** Full per-user Redis ACL is a large feature with real security surface and is out of scope for this RC pass. This plan instead (a) removes the secret-on-cmdline footgun via an env fallback and (b) documents the single-shared-token limitation and recommended network controls in `SECURITY.md`. Per-user ACL is recorded as a future roadmap item.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `.gitignore` | Ignore `.env` | Modify |
| `.env.example` | Document expected env vars without committing real values | Create |
| `AGENTS.md` | Correct the `IDB` interface description; note orbitdb is disabled | Modify |
| `flags.go` | `--auth` env fallback; register `--metrics`/`--metrics-addr` flags | Modify |
| `main.go` | Start the observability server when enabled | Modify |
| `observability.go` | `/healthz`, `/readyz`, `/metrics` HTTP server | Create |
| `observability_test.go` | Tests for the three endpoints | Create |
| `SECURITY.md` | Auth model, limitations, hardening guidance | Create |
| `OPERATIONS.md` | Backup/restore, rolling upgrade, config compatibility runbook | Create |

---

## Task 1: Repo hygiene — untrack `.env`

**Files:** `.gitignore` (modify), `.env.example` (create)

- [ ] **Step 1: Stop tracking `.env` (keep the local file)**

```bash
git rm --cached .env
```

- [ ] **Step 2: Add `.env` to `.gitignore`**

Append under a new comment near the top-level ignore list:

```
# Local environment file (never commit secrets/config)
.env
```

- [ ] **Step 3: Create `.env.example`** documenting the variable shape without real values:

```
# Copy to .env for local development. Values here are examples only.
# NOTE: IPFS_LOG_DB_NAME below is loaded by godotenv but the effective value is
# taken from the --ipfs-log-dbname flag; this entry is illustrative.
IPFS_LOG_DB_NAME="ifdb-ipfs-log-db-name-channel1"
```

- [ ] **Step 4: Verify and commit**

Run: `git status --porcelain` — should show `.env` removed from the index, `.env.example` and `.gitignore` staged. `git ls-files | grep -c '^\.env$'` → `0`.

```bash
git add .gitignore .env.example
git commit -s -m "chore(repo): stop tracking .env; add .env.example"
```

---

## Task 2: Docs accuracy — fix the `IDB` interface and orbitdb note

**Files:** `AGENTS.md` (modify)

The `AGENTS.md` "Storage Driver Interface" block omits methods. The real interface is `github.com/ledisdb/ledisdb/store/driver.IDB`.

- [ ] **Step 1: Replace the interface block** in `AGENTS.md` section 8 with the accurate definition:

```go
type IDB interface {
    Close() error

    Get(key []byte) ([]byte, error)
    Put(key []byte, value []byte) error
    Delete(key []byte) error

    SyncPut(key []byte, value []byte) error
    SyncDelete(key []byte) error

    NewIterator() IIterator
    NewWriteBatch() IWriteBatch
    NewSnapshot() (ISnapshot, error)

    Compact() error
    GetStorageEngine() interface{}
}
```

- [ ] **Step 2: Add an orbitdb note.** In section 6 (Project Structure Reference), append a line after the `orbitdb/` entry clarifying status:

```
│   │                            # NOTE: orbitdb is currently disabled in main.go/flags.go (commented out)
```

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -s -m "docs(agents): correct IDB interface definition; note orbitdb is disabled"
```

---

## Task 3: Auth secret from the environment

**Files:** `flags.go` (modify), `SECURITY.md` (create)

- [ ] **Step 1: Add an env fallback for `--auth` in `flags.go`.** After `flag.Parse()` (so the flag wins if both are set), add:

```go
	// Allow the cluster auth token to come from the environment so it is not
	// exposed on the command line / process list. The --auth flag takes
	// precedence when both are set.
	if conf.Auth == "" {
		conf.Auth = os.Getenv("ICEFIREDB_AUTH")
	}
```

(`os` is already imported in flags.go.)

- [ ] **Step 2: Create `SECURITY.md`:**

```markdown
# Security Model

## Authentication

IceFireDB inherits a single shared-secret authentication model from the uhaha
Raft layer: one token, shared by all clients and all cluster nodes.

- Set it with `--auth <token>` **or** the `ICEFIREDB_AUTH` environment variable
  (preferred — avoids exposing the secret in the process list / shell history).
  The flag takes precedence if both are set.
- There is **no per-user ACL** today. Every authenticated client has full access.

## Recommended hardening

- Always set an auth token in any non-loopback deployment.
- Prefer `ICEFIREDB_AUTH` (or a secrets manager that injects it) over `--auth`.
- Enable TLS with `--tls-cert` / `--tls-key` for client and inter-node traffic.
- Restrict network exposure (firewall / security groups / network policies) to
  trusted clients and peer nodes; do not expose the port publicly.
- Keep the pprof server off in production (it is off by default; only `--pprof`
  enables it, bound to loopback).

## Roadmap

Per-user ACLs (multiple identities with scoped permissions) are a planned future
enhancement and are not part of 1.0.0.
```

- [ ] **Step 3: Verify and commit**

Run: `go build ./...`. Then verify the env fallback compiles and is reachable (it is plain code after flag.Parse).

```bash
git add flags.go SECURITY.md
git commit -s -m "feat(security): allow auth token via ICEFIREDB_AUTH env; add SECURITY.md"
```

---

## Task 4: Observability server (`/healthz`, `/readyz`, `/metrics`)

**Files:** `observability.go` (create), `observability_test.go` (create), `flags.go` (modify), `main.go` (modify)

**REQUIRED SUB-SKILL:** Use superpowers:subagent-driven-development (implement → spec review → quality review).

Opt-in, separate from pprof, bound to loopback by default.

- [ ] **Step 1: Write `observability_test.go`** (drives the handlers directly via httptest, no real server/port needed):

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzAlwaysOK(t *testing.T) {
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return false }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rr.Code)
	}
}

func TestReadyzReflectsReadiness(t *testing.T) {
	// Not ready.
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return false }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz (not ready) = %d, want 503", rr.Code)
	}
	// Ready.
	rr = httptest.NewRecorder()
	observabilityMux(func() bool { return true }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/readyz (ready) = %d, want 200", rr.Code)
	}
}

func TestMetricsServed(t *testing.T) {
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return true }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct == "" {
		t.Fatalf("/metrics missing Content-Type")
	}
}
```

- [ ] **Step 2: Run the test — expect compile failure** (`undefined: observabilityMux`).

Run: `go test ./ -run 'TestHealthz|TestReadyz|TestMetrics' -v`

- [ ] **Step 3: Implement `observability.go`:**

```go
package main

import (
	"log"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// observabilityMux builds the HTTP handler for the observability endpoints.
// readyFn reports whether the node is ready to serve (storage initialized).
func observabilityMux(readyFn func() bool) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "icefiredb_connected_clients",
			Help: "Number of currently connected RESP clients.",
		}, func() float64 { return float64(atomic.LoadInt64(&respClientNum)) }),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readyFn() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// startObservabilityServer runs the observability endpoints on addr in a
// goroutine. ready reports node readiness (storage initialized).
func startObservabilityServer(addr string, ready func() bool) {
	go func() {
		log.Printf("observability server listening on %s (/healthz /readyz /metrics)", addr)
		if err := http.ListenAndServe(addr, observabilityMux(ready)); err != nil {
			log.Printf("observability server stopped: %v", err)
		}
	}()
}
```

- [ ] **Step 4: Wire flags in `flags.go`.** Add near the pprof flags:

```go
	flag.BoolVar(&enableMetrics, "metrics", false, "enable the observability HTTP server (/healthz /readyz /metrics)")
	flag.StringVar(&metricsAddr, "metrics-addr", "127.0.0.1:11002", "")
```

And add to the `usage` const Advanced options:

```
  --metrics        : enable the observability server (/healthz /readyz /metrics) (default: off)
  --metrics-addr addr : observability bind address (default: 127.0.0.1:11002)
```

- [ ] **Step 5: Wire globals + startup in `main.go`.** Add to the `var (...)` block:

```go
	// enableMetrics gates the observability HTTP server (off by default).
	enableMetrics bool
	// metricsAddr is the listen address for the observability server.
	metricsAddr string
```

And in `main()`, alongside the pprof block (before `rafthub.Main(conf)`):

```go
	if enableMetrics {
		startObservabilityServer(metricsAddr, func() bool { return ldb != nil })
	}
```

- [ ] **Step 6: Run tests and build**

Run: `go test ./ -run 'TestHealthz|TestReadyz|TestMetrics' -v` (all PASS), `go build ./...`, `gofmt -l observability.go observability_test.go main.go flags.go`, `go vet ./`.

- [ ] **Step 7: Commit**

```bash
git add observability.go observability_test.go flags.go main.go
git commit -s -m "feat(observability): add opt-in /healthz, /readyz, and Prometheus /metrics"
```

---

## Task 5: Operations runbook

**Files:** `OPERATIONS.md` (create)

- [ ] **Step 1: Create `OPERATIONS.md`:**

```markdown
# Operations Runbook

## Backup

IceFireDB persists data through the Raft snapshot mechanism. To capture a backup:

1. A snapshot is produced on demand by the Raft layer and stored under the node's
   data directory (`-d <dir>`). Copy the data directory of a healthy node (ideally
   the leader, or a quiesced follower) to your backup location.
2. For a consistent copy, stop writes or snapshot a follower that is caught up.

## Restore

Use the `--restore <path>` flag to start a brand-new single-node cluster from a
snapshot file:

    icefiredb -d <fresh-data-dir> --restore <snapshot-path>

This bootstraps a single node from the snapshot. Other nodes must then re-join the
new cluster with `-j <leader-addr>`. `--restore` is ignored if the data directory
already contains state, and cannot be combined with `-j`.

## Rolling upgrade

A multi-node cluster tolerates losing a minority of nodes, so upgrade one node at
a time:

1. Stop one follower; replace its binary; restart it on the same data directory.
   It rejoins from persisted Raft state and catches up (verified by
   `TestIntegrationFollowerRejoin`).
2. Wait until it reports as a healthy follower (e.g. `/readyz` returns 200) before
   moving to the next node.
3. Upgrade the leader last; it will step down and a follower will be elected.

Never take down a majority simultaneously — that loses quorum and halts writes.

## Configuration compatibility

- Do not change `--storage-backend` for an existing data directory; the on-disk
  format is backend-specific.
- Keep `--auth` / `ICEFIREDB_AUTH` consistent across all nodes — it is shared.
- Node identity (`-n`) and bind address (`-a`) must remain stable for a given
  data directory across restarts.

## Health checks

Enable the observability server with `--metrics` and point your orchestrator at:

- `/healthz` — liveness (process up).
- `/readyz` — readiness (storage initialized); use to gate traffic/rollout.
- `/metrics` — Prometheus exposition (Go/process metrics + connected clients).
```

- [ ] **Step 2: Commit**

```bash
git add OPERATIONS.md
git commit -s -m "docs(ops): add operations runbook (backup/restore/rolling upgrade)"
```

---

## Final Verification

- [ ] `go build ./...` clean.
- [ ] `go test ./ -run 'TestHealthz|TestReadyz|TestMetrics'` passes.
- [ ] `git ls-files | grep -c '^\.env$'` → 0.
- [ ] `go vet ./` clean; `gofmt -l` clean on changed Go files.
- [ ] New docs present: `SECURITY.md`, `OPERATIONS.md`, `.env.example`.
