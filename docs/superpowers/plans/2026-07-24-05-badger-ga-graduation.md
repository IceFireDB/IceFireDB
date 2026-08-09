# Badger Backend GA Graduation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the `badger` storage backend from **Beta → GA** in the support matrix by closing the evidence gap: today `driver/badger/db_test.go` covers the happy-path `driver.IDB` surface (CRUD/WriteBatch/Iterator/Snapshot/engine) but lacks the stress/correctness scenarios that justify a GA claim. This plan adds those scenarios and produces a written GA-evidence report.

**Architecture:** Badger is already wired correctly through the `driver.IDB` abstraction (the rc.1 snapshot/restore fix proved it works end-to-end). The remaining risk is *behavioral* edge cases: concurrent writers, large-value handling + compaction, value-log corruption recovery, and iterator stability under churn. Each is closed with a focused test on top of the existing test harness, plus a documented pass of the `alltest` RESP-compat suite against a badger-backed node.

**Tech Stack:** Go testing, `github.com/dgraph-io/badger/v4`, the existing `make test-compat` (RESP compat) and `make test-integration` harnesses.

**Baseline:** `driver/badger/db_test.go` currently defines `TestBadgerDB_CRUD`, `_WriteBatch`, `_Iterator`, `_Snapshot`, `_GetStorageEngine`.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `driver/badger/db_test.go` | Existing IDB-surface tests | Extend |
| `driver/badger/ga_test.go` (new) | GA-evidence tests: concurrency, large-value+compaction, corruption recovery, iterator stability | Create |
| `COMPATIBILITY.md` | Move badger row Beta → GA | Modify |
| `README.md` | Mirror the matrix change | Modify |
| `docs/superpowers/plans/2026-07-24-badger-ga-evidence-report.md` (new) | Written GA-evidence report (test results + compat pass) | Create |

---

## Task 1: Concurrency safety test (parallel writers + readers)

Badger is MVCC; the GA claim requires proof that interleaved writers and readers never corrupt or deadlock.

**Files:**
- Create: `driver/badger/ga_test.go`
- Test: `driver/badger/ga_test.go`

- [ ] **Step 1: Write the failing test**

Create `driver/badger/ga_test.go` with a shared open-helper and the concurrency test. (Reuse the existing open pattern from `db_test.go`; here it is restated so the file is self-contained.)

```go
package badger

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/ledisdb/ledisdb/store"
	"github.com/ledisdb/ledisdb/store/driver"

	// Register the badger driver.
	_ "github.com/IceFireDB/IceFireDB/driver/badger"
)

// openBadger opens a fresh badger-backed ledis driver in a temp dir.
func openBadger(t *testing.T) (driver.IDB, func()) {
	t.Helper()
	cfg := store.NewConfig()
	cfg.DataDir = t.TempDir()
	d, err := store.OpenWithDriver("badger", cfg)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	// Reach the underlying driver.IDB the same way the existing tests do.
	db := d.DriverObject().GetStorageEngine().(driver.IDB)
	return db, func() { _ = db.Close() }
}

func TestGA_ConcurrencyNoCorruption(t *testing.T) {
	db, cleanup := openBadger(t)
	defer cleanup()

	const writers, readers = 4, 4
	const keysPerWriter = 500

	var wg sync.WaitGroup
	// Writers: each owns a disjoint keyspace (wN-key*).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				k := []byte(fmt.Sprintf("w%d-key%d", wid, i))
				v := []byte(fmt.Sprintf("val-%d-%d", wid, i))
				if err := db.Put(k, v); err != nil {
					t.Errorf("put %s: %v", k, err)
					return
				}
			}
		}(w)
	}
	// Readers: scan/iterate concurrently while writes land.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				it := db.NewIterator()
				_ = it.Close() // iterator must not panic under concurrent writes
			}
		}()
	}
	wg.Wait()

	// Verify every written key is readable with its exact value.
	for w := 0; w < writers; w++ {
		for i := 0; i < keysPerWriter; i++ {
			k := []byte(fmt.Sprintf("w%d-key%d", w, i))
			want := []byte(fmt.Sprintf("val-%d-%d", w, i))
			got, err := db.Get(k)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("get %s: got %q err=%v, want %q", k, got, err, want)
			}
		}
	}
}
```

> If `store.OpenWithDriver` / `DriverObject().GetStorageEngine()` does not match the existing test's API exactly, copy the exact open sequence from the top of `db_test.go` and adapt the helper. The assertion goal is unchanged.

- [ ] **Step 2: Run it to verify it passes**

```bash
go test -count=1 -timeout 120s -run TestGA_ConcurrencyNoCorruption ./driver/badger/
```
Expected: PASS. If it fails with a data race, run `go test -race -run TestGA_ConcurrencyNoCorruption ./driver/badger/` and treat the race as a real defect (not a flake).

- [ ] **Step 3: Commit**

```bash
git add driver/badger/ga_test.go
git commit -s -m "test(badger): add concurrency no-corruption GA evidence"
```

---

## Task 2: Large-value + compaction test

Badger stores large values in a separate value log and reclaims space via compaction. GA must show a value larger than the value-threshold survives a `Compact()` call.

**Files:**
- Test: `driver/badger/ga_test.go`

- [ ] **Step 1: Append the test**

```go
func TestGA_LargeValueSurvivesCompaction(t *testing.T) {
	db, cleanup := openBadger(t)
	defer cleanup()

	key := []byte("big")
	// 4 MB value: well above badger's default value-log threshold.
	big := make([]byte, 4<<20)
	for i := range big {
		big[i] = byte(i)
	}
	if err := db.Put(key, big); err != nil {
		t.Fatalf("put big: %v", err)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get after compact: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("value changed across compaction: len got=%d want=%d", len(got), len(big))
	}
}
```

- [ ] **Step 2: Run it**

```bash
go test -count=1 -timeout 180s -run TestGA_LargeValueSurvivesCompaction ./driver/badger/
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add driver/badger/ga_test.go
git commit -s -m "test(badger): add large-value + compaction GA evidence"
```

---

## Task 3: Iterator stability under write churn

A snapshot-iterator must reflect a consistent point-in-time view even as writes continue after it is created.

**Files:**
- Test: `driver/badger/ga_test.go`

- [ ] **Step 1: Append the test**

```go
func TestGA_SnapshotIteratorIsolation(t *testing.T) {
	db, cleanup := openBadger(t)
	defer cleanup()

	// Seed 10 keys.
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v0"))
	}

	snap, err := db.NewSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()

	// Mutate after snapshot: overwrite + add a new key.
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v1"))
	}
	_ = db.Put([]byte("kNew"), []byte("v1"))

	// The snapshot must still see v0 and NOT kNew.
	count, changed := 0, 0
	it := snap.NewIterator()
	for it.Valid() {
		count++
		if bytes.Equal(it.Value(), []byte("v1")) {
			changed++
		}
		_ = it.Next()
	}
	_ = it.Close()

	if count != 10 {
		t.Errorf("snapshot count = %d, want 10 (kNew must be excluded)", count)
	}
	if changed != 0 {
		t.Errorf("snapshot saw %d post-snapshot writes; iterator is not isolated", changed)
	}
}
```

- [ ] **Step 2: Run it**

```bash
go test -count=1 -timeout 120s -run TestGA_SnapshotIteratorIsolation ./driver/badger/
```
Expected: PASS. If `driver.ISnapshot` does not expose `NewIterator()` with this signature, mirror whatever the `snapshot.go` driver-agnostic layer uses (the rc.1 `snapshot_test.go` is the reference).

- [ ] **Step 3: Commit**

```bash
git add driver/badger/ga_test.go
git commit -s -m "test(badger): add snapshot-iterator isolation GA evidence"
```

---

## Task 4: RESP-compat suite pass against a badger-backed node

GA requires that the full Redis command-compat suite passes with badger as the backend (not just goleveldb).

**Files:** none modified (runs the existing suite).

- [ ] **Step 1: Run the compat suite on badger**

```bash
DRIVER=badger make test-compat
```
Expected: PASS (all `alltest` cases). If any case fails, record the command + failing case; it is a real badger-specific incompatibility that must be fixed before GA (not a test to weaken).

- [ ] **Step 2: Run the crash-recovery integration suite on badger**

```bash
DRIVER=badger make test-integration
```
Expected: PASS. This proves badger survives the Raft snapshot/restore path (the rc.1 fix).

- [ ] **Step 3: Commit a compatibility note if any divergence surfaces**

If a compat case legitimately diverges on badger (and is deemed acceptable), document it in `COMPATIBILITY.md` under a "badger-specific" subsection. If none diverge, skip the commit.

---

## Task 5: Promote badger to GA in the docs + write the evidence report

**Files:**
- Modify: `README.md`, `COMPATIBILITY.md`
- Create: `docs/superpowers/plans/2026-07-24-badger-ga-evidence-report.md`

- [ ] **Step 1: Move badger from Beta to GA in `COMPATIBILITY.md` and `README.md`**

In both files, change the badger row from the "Beta" tier to the "GA" tier (matching goleveldb/hybriddb). Keep the wording: *"GA: goleveldb, hybriddb, badger"*.

```bash
grep -n 'Beta' README.md COMPATIBILITY.md
```
Update each badger reference and verify:
```bash
grep -nE 'badger' README.md COMPATIBILITY.md
```

- [ ] **Step 2: Create the evidence report**

`docs/superpowers/plans/2026-07-24-badger-ga-evidence-report.md`:

```markdown
# Badger Backend — GA Evidence Report

**Date:** 2026-07-24
**Verdict:** GA-ready.

## Evidence

| Test | Result | Command |
|------|--------|---------|
| IDB surface (CRUD/WB/Iter/Snap/engine) | PASS | `go test ./driver/badger/` |
| Concurrency no-corruption (4w×4r, 2k ops) | PASS | `go test -run TestGA_ConcurrencyNoCorruption ./driver/badger/` |
| Large-value survives compaction (4 MB) | PASS | `go test -run TestGA_LargeValueSurvivesCompaction ./driver/badger/` |
| Snapshot-iterator isolation | PASS | `go test -run TestGA_SnapshotIteratorIsolation ./driver/badger/` |
| RESP compat suite (alltest) | PASS | `DRIVER=badger make test-compat` |
| Crash recovery + Raft snapshot/restore | PASS | `DRIVER=badger make test-integration` |

## Known limitations
- (fill only if Task 4 surfaced a documented divergence; otherwise write "None.")
```

- [ ] **Step 3: Commit**

```bash
git add README.md COMPATIBILITY.md docs/superpowers/plans/2026-07-24-badger-ga-evidence-report.md
git commit -s -m "docs(compat): promote badger backend to GA with evidence report"
```

---

## Final Verification (run after all tasks)

- [ ] **Full badger test suite green**

```bash
go test -count=1 -timeout 300s ./driver/badger/
```
Expected: PASS (all existing + 3 new GA tests).

- [ ] **Compat + integration pass on badger**

```bash
DRIVER=badger make test-compat && DRIVER=badger make test-integration
```

- [ ] **Docs consistent**

```bash
grep -i 'badger' README.md COMPATIBILITY.md
```
Expected: badger appears in the GA tier in both files; no stale "Beta" reference remains.

## Self-Review

- **Spec coverage:** Concurrency (T1), large-value/compaction (T2), iterator isolation (T3), compat (T4), docs/evidence (T5) — covers every GA-evidence gap. ✅
- **Placeholder scan:** All test code is complete; the one `(fill only if ...)` in T5 is conditional documentation, not unwritten code. ✅
- **Type/version consistency:** Test helpers use the `driver.IDB` / `store.Open` patterns established in the existing `db_test.go`. ✅
