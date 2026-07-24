# Backend Support Matrix + Redis Compatibility — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Define an explicit per-backend support tier for the 1.0.0 NoSQL release, and document + lock down the known Redis-protocol semantic divergences so users migrating from Redis are not surprised.

**Architecture:** This is documentation + test work, no behavior changes. The Redis command suite already runs in CI against every backend (per-driver jobs in `.github/workflows/test.yml`, all `-tags alltest`). What's missing is (a) a clear statement of which backends are production-supported at 1.0.0 vs beta/experimental, and (b) explicit, tested documentation of where IceFireDB's RESP semantics differ from real Redis.

**Tech Stack:** Go 1.25 (vendored), ledisdb command layer, `go-redis/v9` test client driving a real uhaha server (the existing `//go:build alltest` harness in `conn_test.go`).

**Grounding facts (verified against the code/runtime):**
- CI runs `-tags alltest` per backend: goleveldb, badger, hybriddb, crdt, ipfs, ipfs-log, ipfs-synckv, oss. Local `make test` does NOT (`Makefile:69` lacks `-tags alltest`).
- Pure-local backends (no external service): `goleveldb` (ledis built-in, default), `badger`, `hybriddb`. All three pass the full command suite locally.
- External-service backends: `ipfs`, `ipfs-log`, `ipfs-synckv` (IPFS daemon @ :5001), `oss` (S3/MinIO @ :9000), `crdt` (libp2p networking).
- `hybriddb` also has dedicated unit tests (`driver/hybriddb/*_test.go`, run by the `hybriddb-unit-test` CI job).
- **Verified RESP divergence:** with `DRIVER=goleveldb`, `SET k v` then `HSET k f x` returns NO error; afterwards `GET k`→`"v"` and `HGET k f`→`"x"` both succeed (separate per-type keyspaces), and `TYPE k`→`""`. Real Redis returns `WRONGTYPE` on the second op. This is inherited ledis behavior, not a bug to fix here — but it MUST be documented.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `README.md` | Add a "Backend Support Matrix" section with the 1.0.0 support tiers | Modify |
| `COMPATIBILITY.md` | New top-level doc: Redis-compatibility statement and the known divergences | Create |
| `compat_test.go` | New `//go:build alltest` test that locks the documented divergence behaviors so they can't change silently | Create |
| `Makefile` | Add a `test-compat` target that runs the `-tags alltest` suite locally against `$(DRIVER)` | Modify |
| `AGENTS.md` | Document the new `make test-compat` target in the commands table | Modify |
| `docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md` | Tick the "supported-backend matrix" Phase 2 item as addressed by this plan | Modify |

---

## Task 1: Backend Support Matrix (decision + README)

**Files:**
- Modify: `README.md`

This records the product decision for 1.0.0. The recommended tiers (rationale: GA = pure-local + mature/dedicated tests; Beta = works and CI-tested but needs hardening or external infra; Experimental = decentralized/external, still maturing per the README's own status badges).

- [x] **Step 1: Add the support-matrix section to README.md**

Insert a new section (place it just after the "System Design" table, before "Quick Start"):

```markdown
## 🧱 Backend Support Matrix (1.0.0)

Storage backends are classified by support tier for the 1.0.0 release. "GA" backends are
recommended for production; "Beta" are usable and CI-tested but may need tuning or carry
caveats; "Experimental" are decentralized/external-service backends still maturing.

| Backend       | Tier         | Storage              | External dependency      | Notes |
|---------------|--------------|----------------------|--------------------------|-------|
| `goleveldb`   | **GA**       | Local LSM (default)  | none                     | Default engine; mature ledis storage. |
| `hybriddb`    | **GA**       | Local hot/cold tier  | none                     | ristretto cache over leveldb; has dedicated unit tests. |
| `badger`      | Beta         | Local LSM            | none                     | CI-tested; default open options are memory-heavy — tune before heavy production use. |
| `ipfs-synckv` | Beta         | IPFS + local mirror  | IPFS daemon (:5001)      | Encrypted (AES-GCM); CI-tested against a real IPFS node. |
| `ipfs`        | Experimental | IPFS                 | IPFS daemon (:5001)      | Decentralized storage; beta maturity. |
| `ipfs-log`    | Experimental | IPFS append-only log | IPFS daemon (:5001)      | Decentralized log; multi-node identifier via `--ipfs-log-dbname`. |
| `oss`         | Experimental | S3 / object storage  | S3 endpoint + credentials| Object-storage backend. |
| `crdt`        | Experimental | P2P CRDT             | libp2p networking        | Conflict-free cross-site sync; beta maturity. |

> All backends are exercised by the per-backend CI jobs in `.github/workflows/test.yml`.
> Tier reflects production-readiness and operational complexity, not just test coverage.
> RESP semantics are identical across backends — see [COMPATIBILITY.md](COMPATIBILITY.md).
```

- [x] **Step 2: Verify the README renders (no broken table) and links resolve**

Run: `grep -n "Backend Support Matrix" README.md` (confirms the section exists) and visually check the table columns are aligned and the `COMPATIBILITY.md` link target will exist after Task 2.

- [x] **Step 3: Commit**

```bash
git add README.md
git commit -s -m "docs(readme): add 1.0.0 backend support matrix (GA/Beta/Experimental tiers)"
```

---

## Task 2: Redis Compatibility doc + lock-in test

**Files:**
- Create: `COMPATIBILITY.md`
- Create: `compat_test.go`

Document the known RESP divergences and add a test that pins the *current* behavior so it can't drift silently. The test ASSERTS the documented (divergent) behavior — its purpose is regression-locking + executable documentation, not asserting Redis-correctness.

- [x] **Step 1: Write the lock-in test (it should pass against current behavior)**

Create `compat_test.go`:

```go
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
```

- [x] **Step 2: Run the lock-in test**

Run: `DRIVER=goleveldb go test -tags alltest -run TestCompatCrossTypeKeyspaces -v ./`
Expected: PASS. (The server takes a few seconds to boot in the harness — uhaha syncs time first; this is normal.)

- [x] **Step 3: Create COMPATIBILITY.md**

```markdown
# Redis Protocol Compatibility

IceFireDB speaks the Redis RESP protocol via the ledisdb command layer. Most common
commands behave as in Redis, but there are intentional divergences inherited from
ledisdb's storage model. This document lists the ones most likely to affect users
migrating from Redis. Behaviors marked "locked by test" are pinned by `compat_test.go`
(run with `-tags alltest`) so they cannot change without a deliberate update here.

## Separate per-type keyspaces (no WRONGTYPE)  — locked by test

In Redis, a key holds exactly one value of one type, and operating on it with a
command for a different type returns:

    WRONGTYPE Operation against a key holding the wrong kind of value

IceFireDB instead maintains a SEPARATE keyspace per data type (string, hash, list,
set, sorted set). Consequences:

- The same key name can simultaneously hold, e.g., a string AND a hash.
- A type-mismatched operation does NOT return WRONGTYPE; it operates on that type's
  own (possibly empty) view of the key.
- `TYPE <key>` may return an empty string for keys created via type-specific commands.

Example (verified):

    SET k v        -> OK
    HSET k f x     -> 1        (Redis: WRONGTYPE error)
    GET k          -> "v"
    HGET k f       -> "x"
    TYPE k         -> ""

If your application relies on WRONGTYPE errors for type safety, add that check at the
application layer.

## Expiration is per-type

IceFireDB exposes per-type expiration commands (e.g. `HEXPIRE`, `LEXPIRE`, `SEXPIRE`,
`ZEXPIRE`, and `*TTL`/`*PERSIST` variants) rather than only the single Redis `EXPIRE`/
`TTL` on a key. See the command list in `README.md`.

## Scope of this document

This is not an exhaustive compatibility report. It captures the divergences confirmed
to date. Additional differences may exist; contributions documenting (and where
appropriate, locking via `compat_test.go`) further divergences are welcome.
```

- [x] **Step 4: Commit**

```bash
git add compat_test.go COMPATIBILITY.md
git commit -s -m "docs(compat): document and lock per-type-keyspace Redis divergence"
```

---

## Task 3: Local `make test-compat` target

**Files:**
- Modify: `Makefile`
- Modify: `AGENTS.md`

Local `make test` skips the `-tags alltest` command suite that CI runs. Add a convenience target so developers can run the same suite locally against a chosen backend.

- [x] **Step 1: Add the target to the Makefile**

After the existing `test:` target (`Makefile:68-69`), add:

```makefile
test-compat:
	DRIVER=$(DRIVER) go test -v -count=1 -tags alltest ./
```

(`DRIVER` already defaults to `badger` at the top of the Makefile; override with e.g. `DRIVER=goleveldb make test-compat`.)

- [x] **Step 2: Verify the target runs the suite**

Run: `DRIVER=goleveldb make test-compat 2>&1 | tail -5`
Expected: the alltest command suite (TestKV, TestHash, TestList, TestZSet, TestDBSet, TestScan, TestCompatCrossTypeKeyspaces, …) runs and ends with `ok  github.com/IceFireDB/IceFireDB`.

- [x] **Step 3: Document it in AGENTS.md**

In the "Useful Commands Recap" table in `AGENTS.md` (section 5), add a row:

```
| `DRIVER=goleveldb make test-compat` | Run the RESP command-compatibility suite (`-tags alltest`). |
```

- [x] **Step 4: Commit**

```bash
git add Makefile AGENTS.md
git commit -s -m "build(make): add test-compat target to run the alltest RESP suite locally"
```

---

## Task 4: Mark the plan item addressed

**Files:**
- Modify: `docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md`

- [x] **Step 1: Update the Phase 2 bullet**

In the Phase 2 list, change the "supported-backend matrix" bullet to note it is now delivered:

Replace:
```
- Decide and document the **supported-backend matrix** for 1.0.0 (recommend: ship goleveldb + hybriddb as GA, mark ipfs/oss/crdt as beta).
```
with:
```
- ~~Decide and document the **supported-backend matrix** for 1.0.0~~ — DONE: see `README.md` "Backend Support Matrix" and `COMPATIBILITY.md` (delivered by docs/superpowers/plans/2026-06-29-backend-matrix-redis-compat.md). GA: goleveldb, hybriddb; Beta: badger, ipfs-synckv; Experimental: ipfs, ipfs-log, oss, crdt.
```

- [x] **Step 2: Commit**

```bash
git add docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md
git commit -s -m "docs(plan): mark backend-support-matrix item delivered"
```

---

## Final Verification (after all tasks)

- [x] `DRIVER=goleveldb make test-compat` passes (includes the new compat test).
- [x] `DRIVER=badger go test -tags alltest -run TestCompatCrossTypeKeyspaces ./` passes (divergence is backend-independent).
- [x] `README.md` has the Backend Support Matrix; `COMPATIBILITY.md` exists and is linked from it.
- [x] `go build ./...` still clean (no source behavior changed).
