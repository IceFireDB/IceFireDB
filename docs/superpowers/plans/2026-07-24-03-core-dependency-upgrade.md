# Core Dependency Upgrade (Conservative) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the 3 conservative core-dependency bumps that Dependabot has already validated (`libp2p-kad-dht` 0.40.0, `kubo` 0.41.0, `go-redis` 9.20.0), on an **isolated branch** off `1.0.0-rc.1`, each verified by build + test + a `govulncheck` re-run. This also re-tests whether GO-2024-3218 (kad-dht) is resolved by 0.40.0.

**Architecture:** `libp2p`/`kubo` are the **networking core** of the distributed drivers — a bad bump silently corrupts cluster behavior. So each bump lands in its own commit and is followed by build + the full test suite + govulncheck, not just a tidy. The branch is created off `fix/nosql-1.0.0-readiness` (rc.1) so it does not entangle with the rc.1 PR review. Only Dependabot-validated target versions are used (the "conservative" scope).

**Tech Stack:** Go 1.26.x, vendored modules, `govulncheck`, the existing `make test` / `make test-integration` / `make soak` harness.

**Constraints / known traps:**
- **Do NOT touch `github.com/syndtr/goleveldb`.** It is pinned down with a `replace` and a comment: *"Fixed goleveldb version, new version fails unit test on SET instruction."* Leave it.
- **`driver/ipfs-synckv` iterator tests require a live IPFS daemon.** On a daemon-less machine `TestIteratorPrefix` fails with "already have a datastore named badgerds" — that is an **environment** failure, not a regression. Distinguish it from real failures by running the crypto-only tests separately (`-run 'Singularity|Encrypt|Decrypt|Nonce|Tamper'`).
- These 3 modules pull a large transitive libp2p/ipfs subtree; expect `vendor/` churn.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `go.mod` / `go.sum` | Version bumps | Modify per task |
| `vendor/` | Regenerated | `go mod vendor` per task |
| `SECURITY.md` | Update GO-2024-3218 disposition after Task 1's re-test | Modify |

---

## Task 0: Create the isolated upgrade branch

**Files:** none.

- [ ] **Step 1: Ensure a clean tree on rc.1**

```bash
git status --porcelain
```
Expected: empty output (clean). If not, commit or stash first.

- [ ] **Step 2: Create the branch**

```bash
git checkout fix/nosql-1.0.0-readiness
git checkout -b chore/deps-conservative-upgrade
```

- [ ] **Step 3: Record the pre-upgrade baseline**

```bash
go build ./... && echo "baseline build ok"
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep 'affected by' | tee /tmp/pre-upgrade-vulns.txt
```
Expected: build ok; the "affected by N" line saved for comparison.

---

## Task 1: Bump `libp2p-kad-dht` 0.38.0 → 0.40.0

The priority bump — it is the only one tied to an open advisory (GO-2024-3218).

**Files:** `go.mod`, `go.sum`, `vendor/**`.

- [ ] **Step 1: Bump + tidy + re-vendor**

```bash
go get github.com/libp2p/go-libp2p-kad-dht@v0.40.0
go mod tidy
go mod vendor
```

- [ ] **Step 2: Build**

```bash
go build ./...
```
Expected: exit 0. If there are compile errors, they are almost certainly libp2p API drift in the ipfs/ipfs-synckv/ipfs-log drivers — record them; do not force. (See "If the bump breaks the build" below.)

- [ ] **Step 3: Run core + crypto tests (daemon-independent)**

```bash
go test -count=1 -timeout 180s .
go test -count=1 -timeout 120s ./driver/ipfs-synckv/ -run 'Singularity|Encrypt|Decrypt|Nonce|Tamper'
```
Expected: both PASS. (Do not run the full ipfs-synckv package here — `TestIteratorPrefix` needs a live daemon.)

- [ ] **Step 4: Re-run govulncheck to check GO-2024-3218**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep -E 'GO-2024-3218|affected by'
```
Expected: either GO-2024-3218 disappears (fixed in 0.40.0 — best case), or it persists with `Fixed in: N/A`. Record the outcome for Task 4.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum vendor/
git commit -s -m "chore(deps): bump libp2p-kad-dht to v0.40.0"
```

---

## Task 2: Bump `kubo` 0.40.1 → 0.41.0

**Files:** `go.mod`, `go.sum`, `vendor/**`.

- [ ] **Step 1: Bump + tidy + re-vendor**

```bash
go get github.com/ipfs/kubo@v0.41.0
go mod tidy
go mod vendor
```

- [ ] **Step 2: Build**

```bash
go build ./...
```
Expected: exit 0. kubo is a heavy dependency; transitive churn is expected but the public API IceFireDB uses (`go-ipfs-api`, `boxo`) is stable across 0.40→0.41.

- [ ] **Step 3: Core tests**

```bash
go test -count=1 -timeout 180s .
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum vendor/
git commit -s -m "chore(deps): bump kubo to v0.41.0"
```

---

## Task 3: Bump `go-redis` 9.18.0 → 9.20.0

**Files:** `go.mod`, `go.sum`, `vendor/**`.

- [ ] **Step 1: Bump + tidy + re-vendor**

```bash
go get github.com/redis/go-redis/v9@v9.20.0
go mod tidy
go mod vendor
```

- [ ] **Step 2: Build**

```bash
go build ./...
```
Expected: exit 0. `go-redis` is used by the proxy subprojects; within the root module it is mostly transitive.

- [ ] **Step 3: Core tests**

```bash
go test -count=1 -timeout 180s .
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum vendor/
git commit -s -m "chore(deps): bump go-redis to v9.20.0"
```

---

## Task 4: Full verification + update GO-2024-3218 disposition

**Files:** `SECURITY.md` (conditional on Task 1 Step 4 outcome).

- [ ] **Step 1: Whole-module build + vet**

```bash
go build ./... && go vet ./...
```
Expected: exit 0, no vet findings in changed packages.

- [ ] **Step 2: Run the full available test matrix**

```bash
go test -count=1 -timeout 180s .
go test -count=1 -timeout 180s ./driver/badger/
go test -count=1 -timeout 120s ./driver/ipfs-synckv/ -run 'Singularity|Encrypt|Decrypt|Nonce|Tamper'
```
Expected: all PASS. (Integration/soak suites that need a live cluster are exercised in plan 06, not here.)

- [ ] **Step 3: Final govulncheck comparison**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep 'affected by' | tee /tmp/post-upgrade-vulns.txt
diff /tmp/pre-upgrade-vulns.txt /tmp/post-upgrade-vulns.txt || true
```
Expected: count is ≤ the pre-upgrade count; ideally GO-2024-3218 is gone.

- [ ] **Step 4: Update `SECURITY.md` based on the GO-2024-3218 outcome**

If Task 1 Step 4 showed GO-2024-3218 resolved, replace the "tracked in plan 03" entry (added by plan 01 Task 5) with:

```markdown
### GO-2024-3218 — github.com/libp2p/go-libp2p-kad-dht

- **Status:** RESOLVED. Bumped to v0.40.0 (commit <sha>); govulncheck no longer reports it.
```

If it persists (upstream still `Fixed in: N/A`), update the entry to:

```markdown
### GO-2024-3218 — github.com/libp2p/go-libp2p-kad-dht@v0.40.0

- **Status:** Persisting; no upstream fix (Fixed in: N/A). Accepted risk for 1.0.0 GA. Owner sign-off: <name/date>.
```

```bash
git add SECURITY.md
git commit -s -m "docs(security): update GO-2024-3218 disposition after kad-dht 0.40.0 bump"
```

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin chore/deps-conservative-upgrade
```
Then open the PR against `fix/nosql-1.0.0-readiness` with body summarizing the 3 bumps + govulncheck before/after. (If `gh` is unavailable, use the `pull/new/...` link GitHub prints.)

---

## If the bump breaks the build

`libp2p`/`kubo` occasionally make breaking API changes. If `go build ./...` fails after a bump:

1. Capture the errors: `go build ./... 2>&1 | tee /tmp/build-fail.txt`.
2. Identify the affected call sites (they will be in `driver/ipfs*`).
3. **Do not** widen scope mid-task. Record the failure in this plan, revert that single bump (`git checkout -- go.mod go.sum vendor/ && go mod tidy && go mod vendor`), and proceed with the other bumps. Re-attempt the breaking bump in a follow-up plan with the actual API-migration code.

This keeps the conservative branch mergeable even if one bump needs migration work.

## Final Verification (run after all tasks)

- [ ] **`govulncheck` count did not increase**

```bash
cat /tmp/post-upgrade-vulns.txt
```
Expected: count ≤ the rc.1 baseline; GO-2024-3218 dispositioned either way.

- [ ] **goleveldb pin untouched**

```bash
grep -n 'syndtr/goleveldb' go.mod
```
Expected: the `replace` line pinning it to the older commit is still present, unchanged.

- [ ] **All three bumps present in `go.mod`**

```bash
grep -E 'libp2p-kad-dht v0\.40\.0|kubo v0\.41\.0|go-redis/v9 v9\.20\.0' go.mod
```
Expected: three matches.

## Self-Review

- **Spec coverage:** All 3 conservative targets (kad-dht 0.40.0, kubo 0.41.0, go-redis 9.20.0) covered in Tasks 1–3; GO-2024-3218 re-test in Task 1 Step 4 + Task 4 Step 4. ✅
- **Placeholder scan:** Concrete commands throughout; the `<sha>`/`<name/date>` in Task 4 are outputs of earlier steps, not unwritten code. ✅
- **Type/version consistency:** Targets match plan 02's actionable set and plan 01's baseline table. ✅
