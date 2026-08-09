# Fork Dependency Coordination — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the **IceFireDB-owned fork dependencies** (`ledisdb`, `uhaha`, `go-ipfs-log`, and the smaller `go-dag-jose`/`redis-go-cluster`/`golibs` forks) so the root module tracks recent upstream security/feature fixes that cannot be pulled directly because of the `replace` directives.

**Architecture:** Seven `replace` directives in `go.mod` pin forks/pins. Three are load-bearing forks we maintain: `ledisdb` (storage core), `uhaha` (Raft framework), `go-ipfs-log` (IPFS log driver). Upgrading them is **blocked on the fork repositories tagging new versions**, so this plan is **asynchronous**: it starts the upstream coordination (issues/PRs) immediately and lands the bumps as each fork release becomes available. `goleveldb` is explicitly **out of scope** — it is pinned *down* because newer versions fail the SET unit test (see the `replace` comment).

**Tech Stack:** Go modules, the IceFireDB fork repos on GitHub, `go list -m -versions`.

**Current `replace` block (from `go.mod`):**

```
berty.tech/go-ipfs-log v1.10.2            => github.com/IceFireDB/berty-go-ipfs-log v1.22.0
ceramicnetwork/go-dag-jose v0.1.0         => github.com/IceFireDB/go-dag-jose v1.0.2
chasex/redis-go-cluster v1.0.0            => github.com/gitsrc/redis-go-cluster v1.0.1
ledisdb/ledisdb (pinned old)              => github.com/IceFireDB/ledisdb v0.8.3
siddontang/go (pinned old)                => github.com/IceFireDB/golibs v0.1.0
syndtr/goleveldb (PINNED DOWN — do not touch; new version fails SET unit test)
tidwall/uhaha v0.11.3                     => github.com/IceFireDB/uhaha v0.12.1
```

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `go.mod` / `go.sum` | `replace` target versions | Modify per fork release |
| `vendor/` | Regenerated | `go mod vendor` per fork release |
| `docs/superpowers/plans/2026-07-24-04-fork-coordination.md` | This plan + the tracking table | Update as releases land |

---

## Task 1: Inventory each fork's available versions and upstream delta

This is the evidence-gathering step that decides what to request from each fork repo.

**Files:** none modified.

- [ ] **Step 1: List available tagged versions for each fork module**

```bash
for m in \
  github.com/IceFireDB/ledisdb \
  github.com/IceFireDB/uhaha \
  github.com/IceFireDB/berty-go-ipfs-log \
  github.com/IceFireDB/go-dag-jose \
  github.com/gitsrc/redis-go-cluster \
  github.com/IceFireDB/golibs ; do
  echo "=== $m ==="
  go list -m -versions "$m" 2>/dev/null | tr ' ' '\n' | tail -8
done
```
Record the latest tag for each. (These queries hit the module proxy; they will return the set of published versions.)

- [ ] **Step 2: Compare each fork to its upstream base**

For the three load-bearing forks, capture the upstream's latest tag too:

```bash
go list -m -versions github.com/tidwall/uhaha 2>/dev/null | tail -1   # uhaha upstream
go list -m -versions github.com/ledisdb/ledisdb 2>/dev/null | tail -1 # ledisdb upstream
```
The IceFireDB forks exist specifically to carry patches on top of these. The gap between the fork's latest tag and upstream's latest tag is the coordination backlog.

- [ ] **Step 3: Populate the tracking table in this plan**

Append a "Fork upgrade tracking" subsection at the bottom of this file (see Final section) with: fork, current replace version, fork latest tag, upstream latest tag, owner, issue link, status.

- [ ] **Step 4: Commit the plan update**

```bash
git add docs/superpowers/plans/2026-07-24-04-fork-dependency-coordination.md
git commit -s -m "docs(plan): record fork dependency inventory for coordination"
```

---

## Task 2: Open coordination issues on each load-bearing fork

The forks are maintained in the IceFireDB org, so these are internal requests, not external blockers — but they still need an owner and a target version.

**Files:** none modified (GitHub issues).

- [ ] **Step 1: Open an issue on `IceFireDB/ledisdb`**

Title: `Rebase on upstream ledisdb and tag for IceFireDB 1.0.0 GA`
Body should state: the current `v0.8.3` replace target, the upstream version it should track, and that this gates the root module's GA. Link this plan.

```bash
gh issue create --repo IceFireDB/ledisdb --title "Rebase on upstream ledisdb and tag for IceFireDB 1.0.0 GA" --body "See docs/superpowers/plans/2026-07-24-04-fork-dependency-coordination.md (Task 2). Current replace target: v0.8.3." || echo "open manually: https://github.com/IceFireDB/ledisdb/issues/new"
```

- [ ] **Step 2: Open an issue on `IceFireDB/uhaha`**

```bash
gh issue create --repo IceFireDB/uhaha --title "Rebase on upstream uhaha and tag for IceFireDB 1.0.0 GA" --body "See docs/superpowers/plans/2026-07-24-04-fork-dependency-coordination.md (Task 2). Current replace target: v0.12.1." || echo "open manually"
```

- [ ] **Step 3: Open an issue on `IceFireDB/berty-go-ipfs-log`**

```bash
gh issue create --repo IceFireDB/berty-go-ipfs-log --title "Rebase and tag for IceFireDB 1.0.0 GA" --body "See docs/superpowers/plans/2026-07-24-04-fork-dependency-coordination.md. Current replace target: v1.22.0." || echo "open manually"
```

- [ ] **Step 4: Paste the issue links into the tracking table (Task 1 Step 3).**

---

## Task 3: Land a fork bump as a release becomes available

Repeat this task once per fork as each tags a new version. This task is **identical for every fork** — change `<FORK>`, `<MODULE>`, and `<VERSION>` from the tracking table.

**Files:** `go.mod`, `go.sum`, `vendor/**`.

- [ ] **Step 1: Update the replace target**

Using `ledisdb` → `<VERSION>` as the worked example:

```bash
go get github.com/IceFireDB/ledisdb@<VERSION>
go mod tidy
go mod vendor
```
For forks reached only via the `replace` left-hand side (e.g. `tidwall/uhaha`), edit `go.mod` directly so the `replace ... => github.com/IceFireDB/uhaha <VERSION>` line reflects the new tag, then:

```bash
go mod tidy && go mod vendor
```

- [ ] **Step 2: Build + full test suite**

```bash
go build ./... && go test -count=1 -timeout 180s . && go test -count=1 -timeout 180s ./driver/badger/
```
Expected: PASS. `ledisdb`/`uhaha` touch the storage and Raft core, so a regression here is likely to surface in these suites.

- [ ] **Step 3: Commit per fork**

```bash
git add go.mod go.sum vendor/
git commit -s -m "chore(deps): bump IceFireDB/<FORK> fork to <VERSION>"
```

- [ ] **Step 4: Update the tracking table status to "shipped @ <VERSION>"**

---

## Out of scope

- **`github.com/syndtr/goleveldb`** — pinned *down* on purpose. The `replace` comment reads: *"Fixed goleveldb version, new version fails unit test on SET instruction."* Re-evaluating this pin is a separate investigation (why does the newer version fail SET?) and does **not** belong in the GA path. File a spike if desired, but do not change the pin here.
- **Re-vendoring the entire tree by hand.** Always use `go mod vendor`; never edit `vendor/` by hand.

---

## Final Verification (run after each fork bump ships)

- [ ] **Build + core + badger tests green**

```bash
go build ./... && go test -count=1 -timeout 180s . && go test -count=1 -timeout 180s ./driver/badger/
```

- [ ] **`govulncheck` did not regress**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep 'affected by'
```
Expected: count no greater than before the fork bump.

- [ ] **goleveldb pin still intact**

```bash
grep -n 'syndtr/goleveldb' go.mod
```
Expected: unchanged pin.

## Fork upgrade tracking

> Fill in during Task 1, update through Task 3.

| Fork | Replace LHS | Current target | Fork latest tag | Upstream latest | Owner | Issue | Status |
|------|-------------|----------------|-----------------|-----------------|-------|-------|--------|
| IceFireDB/ledisdb | ledisdb/ledisdb | v0.8.3 | _TBD_ | _TBD_ | | | |
| IceFireDB/uhaha | tidwall/uhaha | v0.12.1 | _TBD_ | _TBD_ | | | |
| IceFireDB/berty-go-ipfs-log | berty.tech/go-ipfs-log | v1.22.0 | _TBD_ | _TBD_ | | | |
| IceFireDB/go-dag-jose | ceramicnetwork/go-dag-jose | v1.0.2 | _TBD_ | — | | | low priority |
| gitsrc/redis-go-cluster | chasex/redis-go-cluster | v1.0.1 | _TBD_ | — | | | low priority |
| IceFireDB/golibs | siddontang/go | v0.1.0 | _TBD_ | — | | | low priority |
| ~~syndtr/goleveldb~~ | (pinned down) | — | — | — | — | — | **OUT OF SCOPE** |

## Self-Review

- **Spec coverage:** All 6 active forks covered (3 load-bearing in Task 2, 3 low-priority via the same Task 3 pattern); goleveldb explicitly excluded. ✅
- **Placeholder scan:** `<FORK>/<MODULE>/<VERSION>` in Task 3 are explicit fill-from-table markers bound to the tracking table (real values produced in Task 1), not vague TODOs. ✅
- **Type/version consistency:** Replace-target versions match the `go.mod` block quoted at the top. ✅
