# Plan Checklist Reconciliation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]` syntax for tracking).

**Goal:** Reconcile the checkboxes in the three `2026-06-2x-*.md` plans with what the code actually delivers, so the planning docs are auditable. Today all **76** checkboxes read `- [ ]` (unchecked) even though the corresponding work shipped on `fix/nosql-1.0.0-readiness`. No production code changes.

**Architecture:** For each plan, cross-reference every task step against the commits it claims produced it (each step's "Commit" line names a `git commit -m` subject). A step is `[x]` if that commit exists in history and its test/verification passed; it stays `[ ]` only if the work genuinely did not land. Output is a single docs-only commit per plan.

**Tech Stack:** git log, plain markdown edits.

**Baseline (2026-07-24):**

| Plan | Tasks | Checkboxes | Currently `[x]` |
|------|-------|-----------|-----------------|
| `2026-06-28-nosql-1.0.0-readiness.md` | 6 + final verification | 35 | 0 |
| `2026-06-29-backend-matrix-redis-compat.md` | 4 + final verification | 17 | 0 |
| `2026-06-29-phase3-security-observability-ops.md` | 5 + final verification | 24 | 0 |

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md` | P0 plan | Flip implemented steps to `[x]` |
| `docs/superpowers/plans/2026-06-29-backend-matrix-redis-compat.md` | Phase 2 plan | Flip implemented steps to `[x]` |
| `docs/superpowers/plans/2026-07-24-08-plan-checklist-reconciliation.md` | This plan | Update checkboxes |

---

## Task 1: Reconcile the P0 plan (`2026-06-28`)

**Files:**
- Modify: `docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md`

- [ ] **Step 1: List the commits that implement this plan**

```bash
git log fix/nosql-1.0.0-readiness --oneline | grep -iE 'snapshot|crypto|pprof|version|ipfs|singularity|leveldb' | head
```
Expected hits: `feat(snapshot): add backend-agnostic...`, `fix(snapshot): route Raft snapshot...`, `fix(ipfs-synckv): return errors on decrypt...`, `fix(ipfs): harden encrypt/decrypt...`, `fix(server): make pprof opt-in...`, `chore(version): set honest pre-release version 1.0.0-rc.1`.

- [ ] **Step 2: Verify each task's evidence exists, then flip its boxes**

For each task in the plan, run its verification command and flip `- [ ]` → `- [x]`:

- Task 1 (snapshot extraction): `ls snapshot.go snapshot_test.go && go test -run TestSnapshotRestoreRoundTrip .` → pass → flip Task 1's 5 steps.
- Task 2 (wire main.go / drop leveldb global): `grep -c 'func snapshot\|func restore' snapshot.go main.go && ! grep -q 'leveldb' global.go` → flip Task 2's 7 steps.
- Task 3 (crypto returns errors): `go test -run 'TestDecryptCorrupt|TestDecryptTampered|TestNonceUniqueness' ./driver/ipfs-synckv/` → pass → flip Task 3's 6 steps.
- Task 4 (ipfs driver hardening): `grep -q 'return' driver/ipfs/singularity.go` and build → flip Task 4's 3 steps.
- Task 5 (pprof opt-in): `grep -q 'enablePprof\|pprof-addr' flags.go` → flip Task 5's 6 steps.
- Task 6 (version string): `grep -q '1.0.0-rc.1' main.go` → flip Task 6's 3 steps.
- Final verification: `go build ./... && go test . && go vet ./...` → flip the 4 final-verification boxes.

Leave a box `[ ]` **only** if its verification command fails.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-28-nosql-1.0.0-readiness.md
git commit -s -m "docs(plan): reconcile P0 readiness checklist with shipped commits"
```

---

## Task 2: Reconcile the Phase 2 plan (`2026-06-29 backend-matrix`)

**Files:**
- Modify: `docs/superpowers/plans/2026-06-29-backend-matrix-redis-compat.md`

- [ ] **Step 1: List the commits**

```bash
git log fix/nosql-1.0.0-readiness --oneline | grep -iE 'backend|matrix|compat|test-compat|badger|crash|failover|rejoin'
```
Expected: backend-matrix README, compat doc + lock test, `make test-compat`, badger unit tests, crash-recovery, failover, follower-rejoin.

- [ ] **Step 2: Flip the implemented boxes**

- Task 1 (matrix in README): `grep -q 'Backend Support Matrix' README.md` → flip.
- Task 2 (compat doc + lock test): `test -f COMPATIBILITY.md && grep -q 'compat_test\|TestCompat' compat_test.go` → flip.
- Task 3 (`make test-compat`): `grep -q 'test-compat' Makefile` → flip.
- Task 4 (mark delivered): already reflected → flip.
- Final verification boxes: run `DRIVER=goleveldb make test-compat` → flip if pass.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-29-backend-matrix-redis-compat.md
git commit -s -m "docs(plan): reconcile backend-matrix/compat checklist with shipped commits"
```

---

## Task 3: Reconcile the Phase 3 plan (`2026-06-29 phase3`)

**Files:**
- Modify: `docs/superpowers/plans/2026-06-29-phase3-security-observability-ops.md`

- [ ] **Step 1: List the commits**

```bash
git log fix/nosql-1.0.0-readiness --oneline | grep -iE 'env|\.env|AGENTS|auth|ICEFIREDB_AUTH|healthz|readyz|metrics|observ|runbook|OPERATIONS|partition|soak|rolling|leader-churn'
```

- [ ] **Step 2: Flip the implemented boxes**

- Task 1 (untrack `.env`): `git ls-files .env | wc -l` → `0` → flip; `test -f .env.example` → flip.
- Task 2 (docs accuracy `AGENTS.md`): `grep -q 'Close\|SyncPut' AGENTS.md` → flip if present.
- Task 3 (auth from env): `grep -q 'ICEFIREDB_AUTH' flags.go` → flip.
- Task 4 (observability): `test -f observability.go && grep -q '/healthz\|/readyz\|/metrics' observability.go` → flip.
- Task 5 (operations runbook): `test -f OPERATIONS.md` → flip.
- Final verification boxes: run the observability test → flip if pass.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-29-phase3-security-observability-ops.md
git commit -s -m "docs(plan): reconcile phase3 security/observability/ops checklist with shipped commits"
```

---

## Final Verification (run after all tasks)

- [ ] **Checkbox counts are now truthful**

```bash
for f in 2026-06-28-nosql-1.0.0-readiness \
         2026-06-29-backend-matrix-redis-compat \
         2026-06-29-phase3-security-observability-ops ; do
  done=$(grep -cE '^\s*- \[x\]' "docs/superpowers/plans/$f.md")
  open=$(grep -cE '^\s*- \[ \]' "docs/superpowers/plans/$f.md")
  echo "$f: done=$done open=$open"
done
```
Expected: `done` is the large majority; `open` is small and each remaining open box is genuinely unshipped work (cross-reference its commit subject).

- [ ] **No production code was touched**

```bash
git diff --name-only fix/nosql-1.0.0-readiness..HEAD | grep -v '^docs/superpowers/plans/' | grep -v '^scripts/'
```
Expected: empty (only plan docs + the triage script changed).

## Self-Review

- **Spec coverage:** All three stale plans reconciled (Tasks 1–3). ✅
- **Placeholder scan:** Concrete verification commands per box; the only conditional is "flip if pass / leave if fail", which is the correct disposition logic, not a placeholder. ✅
- **Type/version consistency:** Commit subjects referenced match the actual `git log` output for the branch. ✅
