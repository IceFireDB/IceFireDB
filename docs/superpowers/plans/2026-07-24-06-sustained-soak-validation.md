# Sustained Soak Validation (GA Evidence) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce the **durability evidence** required for 1.0.0 GA: a sustained soak run of **≥ 1 hour with `SOAK_CHAOS=1`** that shows zero acknowledged-write loss under continuous load plus leader churn, on the **final** dependency stack (i.e. run *after* plan 03's conservative upgrade lands).

**Architecture:** The harness already exists — `make soak` drives `TestIntegrationSoak` (`integration_test.go`) against a 3-node Raft cluster with configurable `SOAK_DURATION`, `SOAK_WORKERS`, and `SOAK_CHAOS`. The rc.1 validation only ran 25–30s. GA needs a structured matrix (baseline → long → chaos → long+chaos), each producing a captured report. This plan also adds lightweight throughput/durability assertions so a regression fails the test rather than merely printing numbers.

**Tech Stack:** Go testing, `make soak`, the existing 3-node integration test harness, `tidwall/uhaha` Raft.

**Baseline:** `TestIntegrationSoak` validated at 30s/4-workers (~10k writes, 0 failures) and 25s under chaos (leader cycled twice, all acknowledged writes intact).

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `integration_test.go` | `TestIntegrationSoak` — add throughput + durability assertions | Modify |
| `Makefile` | `soak` target — document the GA matrix env vars | Modify (comments) |
| `docs/superpowers/plans/2026-07-24-06-soak-ga-report.md` (new) | Captured results + verdict | Create |

---

## Task 1: Strengthen the soak test with throughput + durability assertions

Today the soak logs counts; GA requires it to *fail* on silent data loss or throughput collapse.

**Files:**
- Modify: `integration_test.go` (the `TestIntegrationSoak` body)

- [ ] **Step 1: Locate the current soak body**

```bash
grep -n 'func TestIntegrationSoak\|acknowledged\|SOAK_DURATION\|SOAK_WORKERS\|SOAK_CHAOS' integration_test.go
```
Record the line numbers of the write loop, the acknowledged-writes set, and the final verification loop.

- [ ] **Step 2: Add a minimum-throughput guard**

Inside `TestIntegrationSoak`, after the timed load window, before the final durability check, add (adapting variable names to the existing code):

```go
// GA guard: refuse to pass if throughput collapsed to nothing.
elapsed := loadEnd.Sub(loadStart).Seconds()
if elapsed <= 0 {
    t.Fatalf("soak elapsed time non-positive: %v", loadEnd.Sub(loadStart))
}
throughput := float64(ackedCount) / elapsed
if throughput < 1.0 {
    t.Fatalf("soak throughput %.1f ops/s below 1 ops/s GA floor (acked=%d, elapsed=%.1fs)", throughput, ackedCount, elapsed)
}
t.Logf("soak throughput: %.1f ops/s over %.1fs (acked=%d)", throughput, elapsed, ackedCount)
```

- [ ] **Step 3: Make the durability assertion hard**

Replace any soft "log if mismatch" with a hard failure at the final verification loop:

```go
if verifiedCount != ackedCount {
    t.Fatalf("DATA LOSS: acknowledged %d writes but only %d survived restart", ackedCount, verifiedCount)
}
t.Logf("durability OK: %d/%d acknowledged writes survived", verifiedCount, ackedCount)
```

- [ ] **Step 4: Run the short soak to confirm the assertions compile and behave**

```bash
SOAK_DURATION=10s SOAK_WORKERS=4 make soak
```
Expected: PASS, logs include a `throughput:` line and a `durability OK:` line.

- [ ] **Step 5: Commit**

```bash
git add integration_test.go
git commit -s -m "test(soak): add throughput floor and hard durability assertions for GA"
```

---

## Task 2: Run the GA soak matrix

Run on the final dependency stack (after plan 03 merges). Each run is captured to a file for the report.

**Files:** none modified (test runs).

- [ ] **Step 1: Baseline — 5 minutes, no chaos**

```bash
SOAK_DURATION=5m SOAK_WORKERS=8 make soak 2>&1 | tee /tmp/soak-01-baseline.log
```
Expected: PASS; record the throughput line.

- [ ] **Step 2: Long — 1 hour, no chaos**

```bash
SOAK_DURATION=60m SOAK_WORKERS=8 make soak 2>&1 | tee /tmp/soak-02-long.log
```
Expected: PASS. This is the headline GA evidence.

- [ ] **Step 3: Chaos — 10 minutes with leader cycling**

```bash
SOAK_DURATION=10m SOAK_WORKERS=8 SOAK_CHAOS=1 make soak 2>&1 | tee /tmp/soak-03-chaos.log
```
Expected: PASS; leader cycled ≥1 time, all acknowledged writes intact.

- [ ] **Step 4: Long + chaos — 1 hour with leader cycling**

```bash
SOAK_DURATION=60m SOAK_WORKERS=8 SOAK_CHAOS=1 make soak 2>&1 | tee /tmp/soak-04-long-chaos.log
```
Expected: PASS. This is the strongest GA evidence.

> Each run may be scheduled separately; runs 2 and 4 take a full hour. If they flake on environment (port conflict, leftover data dir), rerun — but a *data-loss* failure is never a flake.

---

## Task 3: Write the GA soak report

**Files:**
- Create: `docs/superpowers/plans/2026-07-24-06-soak-ga-report.md`

- [ ] **Step 1: Extract the headline numbers from each log**

```bash
for f in /tmp/soak-0*.log; do
  echo "=== $f ==="
  grep -E 'throughput:|durability OK:|DATA LOSS|FAIL' "$f" | tail -5
done
```

- [ ] **Step 2: Create the report**

`docs/superpowers/plans/2026-07-24-06-soak-ga-report.md`:

```markdown
# Sustained Soak — GA Evidence Report

**Date:** <run date>
**Stack:** 1.0.0-rc.1 + plan-03 conservative dependency upgrade
**Verdict:** <GA-ready | regression found>

## Matrix

| Run | Duration | Workers | Chaos | Throughput | Acked | Survived | Result |
|-----|----------|---------|-------|------------|-------|----------|--------|
| baseline | 5m | 8 | off | <op/s> | <n> | <n> | PASS |
| long | 60m | 8 | off | <op/s> | <n> | <n> | PASS |
| chaos | 10m | 8 | on | <op/s> | <n> | <n> | PASS |
| long+chaos | 60m | 8 | on | <op/s> | <n> | <n> | PASS |

## Logs
- baseline: /tmp/soak-01-baseline.log
- long: /tmp/soak-02-long.log
- chaos: /tmp/soak-03-chaos.log
- long+chaos: /tmp/soak-04-long-chaos.log

## Verdict
The 60m + chaos run completed with 0 acknowledged-write loss and throughput above the GA floor. This satisfies the GA soak exit criterion in `2026-07-24-00-ga-roadmap-index.md`.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-07-24-06-soak-ga-report.md
git commit -s -m "docs(soak): record GA sustained-load evidence report"
```

---

## Final Verification (run after all tasks)

- [ ] **All 4 matrix runs PASS with no DATA LOSS line**

```bash
grep -L 'durability OK' /tmp/soak-0*.log   # lists logs MISSING the success line
```
Expected: empty output (every log contains `durability OK`).

- [ ] **Report committed and referenced from the roadmap index**

```bash
grep -q 'soak-ga-report' docs/superpowers/plans/2026-07-24-00-ga-roadmap-index.md && echo "linked"
```
Expected: `linked`.

## Self-Review

- **Spec coverage:** GA exit criterion "≥1h soak with SOAK_CHAOS=1, 0 data loss" is exactly run 4 (Task 2 Step 4); the others are supporting evidence. Hard assertions (Task 1) prevent silent passes. ✅
- **Placeholder scan:** `<run date>`/`<op/s>`/`<n>` in Task 3 are outputs captured in Task 2, not unwritten code. ✅
- **Type/version consistency:** Env var names (`SOAK_DURATION`/`SOAK_WORKERS`/`SOAK_CHAOS`) match the existing Makefile target. ✅
