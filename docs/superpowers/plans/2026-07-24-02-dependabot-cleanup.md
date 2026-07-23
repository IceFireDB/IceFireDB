# Dependabot Branch Cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the accumulated dependabot branch backlog (~30 remote + several local) to **0 open branches below the current dependency baseline**, by deleting superseded bumps and handing the 3 newer-than-current bumps (kubo 0.41.0, kad-dht 0.40.0, go-redis 9.20.0) to plan 03.

**Architecture:** Every `dependabot/*` branch encodes `<module>-<version>`. A branch is **superseded** if its version is ≤ the version currently in `go.mod`; **actionable** if greater. The triage is automated by a script that parses each branch name against `go.mod`, so it stays correct as the baseline moves. Superseded branches are deleted (local + remote). Actionable branches are *not* merged here — they are inputs to plan 03's conservative upgrade.

**Tech Stack:** git, bash, `go list -m`.

**Baseline (2026-07-24), key current versions:**

| Module | Current (go.mod) |
|--------|------------------|
| `github.com/ipfs/kubo` | v0.40.1 |
| `github.com/libp2p/go-libp2p-kad-dht` | v0.38.0 |
| `github.com/redis/go-redis/v9` | v9.18.0 |
| `github.com/multiformats/go-multiaddr` | v0.16.1 |
| `github.com/ipfs/go-datastore` | v0.9.1 |
| `github.com/dgraph-io/badger/v4` | v4.9.2 |

**Dependabot remote branches known to be actionable (hand to plan 03):**
- `kubo-0.41.0`, `go-libp2p-kad-dht-0.40.0`, `go-redis/v9-9.20.0`

Everything else in the `dependabot/*` set is ≤ current and therefore superseded.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `scripts/triage-dependabot-branches.sh` | Reproducible triage: parse branch names vs `go.mod`, emit classify CSV | Create |
| `docs/superpowers/plans/2026-07-24-02-dependabot-cleanup.md` | This plan | Update checkboxes |

No production code is touched by this plan.

---

## Task 1: Write the triage script

**Files:**
- Create: `scripts/triage-dependabot-branches.sh`

- [ ] **Step 1: Create the script**

Write `scripts/triage-dependabot-branches.sh`:

```bash
#!/usr/bin/env bash
# Classify every dependabot/* branch as SUPERSEDED (<= go.mod) or ACTIONABLE (> go.mod).
# Output TSV: <status>\t<branch>\t<branch-version>\t<go.mod-version>
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Build "module@version" baseline from go.mod (direct + indirect).
baseline() {
  go list -m all 2>/dev/null
}

for branch in $(git branch -r --list 'origin/dependabot/*' | sed 's#^[[:space:]]*##'); do
  name="${branch#origin/dependabot/go_modules/}"
  # name looks like: github.com/ipfs/kubo-0.41.0  or  github.com/redis/go-redis/v9-9.20.0
  ver="${name##*-}"                       # 0.41.0 / 9.20.0
  mod="${name%-*}"                         # github.com/ipfs/kubo
  # Map branch module path to go.mod path (handle vN suffix for go-redis).
  current="$(baseline | awk -v m="$mod" '$1==m{print $2; found=1} END{}' | head -1)"
  current="${current#v}"
  if [ -z "$current" ]; then
    printf 'UNKNOWN\t%s\t%s\t(not in module graph)\n' "$branch" "$ver"
  elif printf '%s\n%s\n' "$current" "$ver" | sort -V -C; then
    printf 'SUPERSEDED\t%s\t%s\t%s\n' "$branch" "$ver" "$current"
  else
    printf 'ACTIONABLE\t%s\t%s\t%s\n' "$branch" "$ver" "$current"
  fi
done
```

- [ ] **Step 2: Make it executable and run**

```bash
chmod +x scripts/triage-dependabot-branches.sh
scripts/triage-dependabot-branches.sh | sort | tee /tmp/dependabot-triage.tsv
```
Expected: one line per dependabot branch, each tagged `SUPERSEDED`, `ACTIONABLE`, or `UNKNOWN`.

- [ ] **Step 3: Sanity-check the actionable set**

```bash
grep '^ACTIONABLE' /tmp/dependabot-triage.tsv
```
Expected: exactly these three (modulo churn introduced since the baseline):
```
ACTIONABLE	origin/dependabot/go_modules/github.com/ipfs/kubo-0.41.0	0.41.0	0.40.1
ACTIONABLE	origin/dependabot/go_modules/github.com/libp2p/go-libp2p-kad-dht-0.40.0	0.40.0	0.38.0
ACTIONABLE	origin/dependabot/go_modules/github.com/redis/go-redis/v9-9.20.0	9.20.0	9.18.0
```

- [ ] **Step 4: Commit the script**

```bash
git add scripts/triage-dependabot-branches.sh
git commit -s -m "chore(scripts): add dependabot branch triage script"
```

---

## Task 2: Delete superseded dependabot branches (remote)

Superseded branches add no value and bloat the branch list. Each is a Dependabot PR; closing the PR auto-deletes the branch if Dependabot config has `open-pull-requests-limit` cleanup, but to be deterministic we delete via git.

**Files:** none (git operations only).

- [ ] **Step 1: Capture the superseded list to a file**

```bash
grep '^SUPERSEDED' /tmp/dependabot-triage.tsv | awk '{print $2}' > /tmp/superseded-branches.txt
wc -l /tmp/superseded-branches.txt
```
Expected: a count ≥ 20 (the kubo 0.35–0.39, kad-dht 0.29–0.37, go-redis 9.12–9.17, multiaddr, datastore families).

- [ ] **Step 2: Review the list before deletion**

```bash
cat /tmp/superseded-branches.txt
```
Confirm every line is `origin/dependabot/...`. If any non-dependabot line appears, stop and fix the script.

- [ ] **Step 3: Delete the remote branches**

```bash
xargs -a /tmp/superseded-branches.txt -I{} git push origin --delete {}
```
This closes the corresponding Dependabot PRs as well. Expected: each prints ` - [deleted]         (none) -> ...`.

- [ ] **Step 4: Delete any matching local dependabot branches**

```bash
git branch --list 'dependabot/*' | xargs -r -n1 git branch -D
```
Expected: deletes the 5 local dependabot branches that have no value beyond their remote.

- [ ] **Step 5: Prune and confirm**

```bash
git fetch --prune origin
git branch -r | grep -c dependabot
```
Expected: the remaining dependabot count equals the actionable set (3) plus any newly opened since.

No commit — this task is pure branch hygiene.

---

## Task 3: Hand the actionable set to plan 03

The 3 actionable bumps are *not* merged blindly; they go through plan 03's build + test + govulncheck verification.

**Files:** none modified here.

- [ ] **Step 1: Record the handoff in this plan's roadmap**

Confirm the actionable list from Task 1 Step 3 matches plan 03's targets. They must agree:

```bash
grep -E 'kubo.*0\.41|kad-dht.*0\.40|go-redis.*9\.20' docs/superpowers/plans/2026-07-24-03-core-dependency-upgrade.md
```
Expected: three matches, one per module. If plan 03 was edited to use different versions, reconcile the two plans before proceeding.

- [ ] **Step 2: Note the open Dependabot PR numbers for tracking**

```bash
gh pr list --label dependencies --state open 2>/dev/null || echo "(gh not available; track manually in the project board)"
```

---

## Final Verification (run after all tasks)

- [ ] **No superseded dependabot branch remains**

```bash
scripts/triage-dependabot-branches.sh | grep -c '^SUPERSEDED'
```
Expected: `0`.

- [ ] **Only actionable branches remain open**

```bash
scripts/triage-dependabot-branches.sh | grep '^ACTIONABLE' | wc -l
```
Expected: `3` (kubo 0.41.0, kad-dht 0.40.0, go-redis 9.20.0), each tracked in plan 03.

- [ ] **Triage script is idempotent and re-runnable**

```bash
scripts/triage-dependabot-branches.sh >/dev/null && echo "ok"
```
Expected: `ok`, exit 0. Re-running as new Dependabot PRs open keeps the workflow self-maintaining.

## Self-Review

- **Spec coverage:** Every dependabot branch is dispositioned (delete-if-superseded in Task 2, hand-off-if-actionable in Task 3). ✅
- **Placeholder scan:** Concrete commands throughout; the script is complete and executable. ✅
- **Type/version consistency:** The actionable set (kubo 0.41.0, kad-dht 0.40.0, go-redis 9.20.0) matches plan 03's targets exactly. ✅
