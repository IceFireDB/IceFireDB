# Security Vulnerability Triage (1.0.0 GA) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `govulncheck` affecting-vulnerability count from **8 to ≤ 2** (the two no-fix advisories), by landing the trivially-safe patches (Go toolchain, `quic-go`, `x/text`, `x/net`) and producing a documented decision for the two advisories that have no upstream fix.

**Architecture:** `govulncheck` is the source of truth. Of the 8 affecting vulnerabilities found on `1.0.0-rc.1`, 6 are closable with low-risk version bumps (3 stdlib → Go toolchain bump; 3 module patches). The remaining 2 (`pion/dtls/v2` and `libp2p-kad-dht`) have `Fixed in: N/A` and are dispositioned explicitly here; the kad-dht one is re-tested under plan 03.

**Tech Stack:** Go 1.26.x (toolchain), `golang.org/x/vuln/cmd/govulncheck`, Go modules.

**Baseline evidence (2026-07-24, branch `fix/nosql-1.0.0-readiness`):**

```
Your code is affected by 8 vulnerabilities from 5 modules and the Go standard library.
```

| # | Advisory | Module | Found in | Fixed in | Disposition |
|---|----------|--------|----------|----------|-------------|
| 1 | GO-2026-5970 | `golang.org/x/text` | v0.36.0 | v0.39.0 | **Bump** (Task 2) |
| 2 | GO-2026-5856 | stdlib `crypto/tls` | go1.26.3 | go1.26.5 | **Go toolchain bump** (Task 1) |
| 3 | GO-2026-5676 | `github.com/quic-go/quic-go` | v0.59.0 | v0.59.1 | **Patch bump** (Task 3) |
| 4 | GO-2026-5039 | stdlib `net/textproto` | go1.26.3 | go1.26.4 | **Go toolchain bump** (Task 1) |
| 5 | GO-2026-5037 | stdlib `crypto/x509` | go1.26.3 | go1.26.4 | **Go toolchain bump** (Task 1) |
| 6 | GO-2026-5026 | `golang.org/x/net` | v0.52.0 | v0.55.0 | **Bump** (Task 2) |
| 7 | GO-2026-4479 | `github.com/pion/dtls/v2` | v2.2.12 | N/A | **Decision** (Task 4) |
| 8 | GO-2024-3218 | `github.com/libp2p/go-libp2p-kad-dht` | v0.38.0 | N/A | **Re-test in plan 03** (Task 5) |

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `go.mod` / `go.sum` | Module versions + toolchain directive | Modify |
| `vendor/` | Vendored dependency tree | Regenerate via `go mod vendor` |
| `SECURITY.md` | Document accepted-risk advisories + mitigations | Append section |
| `docs/superpowers/plans/2026-07-24-01-security-vuln-triage.md` | This plan | Update checkboxes as work lands |

---

## Task 1: Pin the Go toolchain to fix the 3 stdlib vulnerabilities

Closes GO-2026-5037, GO-2026-5039, GO-2026-5856 (all fixed in go1.26.4/1.26.5).

**Files:**
- Modify: `go.mod` (`toolchain` and/or `go` directive)
- Modify: `go.sum`

- [ ] **Step 1: Confirm the toolchain directive**

Run:
```bash
grep -nE '^(go |toolchain )' go.mod
```
Expected output (current):
```
go 1.25.0
```

- [ ] **Step 2: Bump the toolchain to go1.26.5**

Run:
```bash
go get toolchain@go1.26.5
```
This adds/updates the `toolchain` directive. Verify:
```bash
grep -nE '^(go |toolchain )' go.mod
```
Expected: a `toolchain go1.26.5` line is present (or the `go` directive is raised). The local `go` binary is `go1.26.3` (per `go version`); the `toolchain` directive makes the build download and use `go1.26.5`.

- [ ] **Step 3: Rebuild and confirm the toolchain is in effect**

```bash
go version      # still reports the system go1.26.3, that's fine
go build ./...  # triggers toolchain switch to 1.26.5 if needed
```
Expected: build succeeds, exit 0.

- [ ] **Step 4: Verify the 3 stdlib vulns are gone**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep -E 'GO-2026-(5037|5039|5856)|No vulnerabilities|affected by'
```
Expected: none of GO-2026-5037/5039/5856 appear; the "affected by N" count drops by 3.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum
git commit -s -m "fix(security): pin toolchain go1.26.5 to close stdlib CVEs (5037/5039/5856)"
```

---

## Task 2: Bump `golang.org/x/text` and `golang.org/x/net`

Closes GO-2026-5970 (x/text v0.36.0 → v0.39.0) and GO-2026-5026 (x/net v0.52.0 → v0.55.0).

**Files:**
- Modify: `go.mod`, `go.sum`, `vendor/golang.org/x/text/**`, `vendor/golang.org/x/net/**`

- [ ] **Step 1: Bump both modules**

```bash
go get golang.org/x/text@v0.39.0 golang.org/x/net@v0.55.0
go mod tidy
```

- [ ] **Step 2: Re-vendor**

```bash
go mod vendor
```

- [ ] **Step 3: Build and run the core test suite**

```bash
go build ./... && go test -count=1 -timeout 180s .
```
Expected: build exit 0; tests PASS.

- [ ] **Step 4: Verify both vulns are gone**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep -E 'GO-2026-(5970|5026)|affected by'
```
Expected: GO-2026-5970 and GO-2026-5026 no longer appear; count drops by 2 more (cumulative drop: 5).

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum vendor/
git commit -s -m "fix(security): bump x/text to v0.39.0 and x/net to v0.55.0 (GO-2026-5970, GO-2026-5026)"
```

---

## Task 3: Patch `quic-go` v0.59.0 → v0.59.1

Closes GO-2026-5676. This is a patch-level bump (zero breaking-change risk).

**Files:**
- Modify: `go.mod`, `go.sum`, `vendor/github.com/quic-go/quic-go/**`

- [ ] **Step 1: Bump and re-vendor**

```bash
go get github.com/quic-go/quic-go@v0.59.1
go mod tidy
go mod vendor
```

- [ ] **Step 2: Build and test**

```bash
go build ./... && go test -count=1 -timeout 180s .
```
Expected: PASS.

- [ ] **Step 3: Verify the vuln is gone**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep -E 'GO-2026-5676|affected by'
```
Expected: GO-2026-5676 no longer appears; count drops by 1 more (cumulative drop: 6; from 8 → 2).

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum vendor/
git commit -s -m "fix(security): bump quic-go to v0.59.1 (GO-2026-5676)"
```

---

## Task 4: Disposition GO-2026-4479 (`pion/dtls/v2`, no upstream fix)

This advisory has `Fixed in: N/A`. `pion/dtls` is pulled in transitively by the WebRTC/libp2p transport stack. We cannot simply bump it away.

**Files:**
- Modify: `SECURITY.md`

- [ ] **Step 1: Read the advisory and map the reachable code path**

Open https://pkg.go.dev/vuln/GO-2026-4479 in a browser. Then locate where `pion/dtls` is actually reached from IceFireDB code:

```bash
grep -rnE 'pion/dtls|webrtc' --include='*.go' . | grep -v '^./vendor' | head
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | grep -A6 'GO-2026-4479'
```
Record the symbol(s) and call site(s) reported by govulncheck.

- [ ] **Step 2: Decide the disposition (one of)**

- **(a) Not reachable in default config** — if the vulnerable symbol is only reached via the opt-in WebRTC transport (`webrtc-network-mode`), document it as "exposure gated behind a non-default flag".
- **(b) Reachable** — open an upstream issue at https://github.com/pion/dtls and/or pin a patched fork via a `replace` directive (coordinate with plan 04 / F3). Until then, treat as accepted risk.

- [ ] **Step 3: Document the decision in `SECURITY.md`**

Append to `SECURITY.md` (after the existing "Roadmap" section):

```markdown
## Known accepted vulnerabilities

### GO-2026-4479 — github.com/pion/dtls/v2 (no upstream fix as of 2026-07-24)

- **Reachability:** <fill from Step 1 — e.g. "Only via the opt-in WebRTC transport (`webrtc-network-mode`), not the default deployment.">
- **Disposition:** <"Accepted risk for 1.0.0 GA" | "Mitigated by ..." | "Tracking upstream fix at <link>">
- **Owner sign-off:** <name/date>
```

- [ ] **Step 4: Commit**

```bash
git add SECURITY.md
git commit -s -m "docs(security): document GO-2026-4479 (pion/dtls) disposition for GA"
```

---

## Task 5: Hand GO-2024-3218 (kad-dht) to plan 03

`govulncheck` reports `Fixed in: N/A`, but a newer `libp2p-kad-dht` (0.40.0) exists and may carry an untagged fix. This is handled in [`2026-07-24-03-core-dependency-upgrade.md`](03-core-dependency-upgrade.md) Task 1 (bump to 0.40.0) + Task 4 (re-run govulncheck to confirm). Do **not** act on it here.

**Files:** none modified in this task.

- [ ] **Step 1: Record the handoff note in `SECURITY.md`**

Append:
```markdown
### GO-2024-3218 — github.com/libp2p/go-libp2p-kad-dht@v0.38.0

- **Disposit­ion:** Tracked in plan `2026-07-24-03-core-dependency-upgrade.md`. A conservative bump to v0.40.0 is applied there, followed by a govulncheck re-test to confirm resolution.
```

- [ ] **Step 2: Commit**

```bash
git add SECURITY.md
git commit -s -m "docs(security): track GO-2024-3218 (kad-dht) via dependency-upgrade plan"
```

---

## Final Verification (run after all tasks)

- [ ] **Full govulncheck sweep on the core**

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest . ./driver/... 2>&1 | tee /tmp/govuln-final.txt | tail -5
```
Expected: "Your code is affected by 2 vulnerabilities" (GO-2026-4479 + GO-2024-3218), both documented in `SECURITY.md`. If count > 2, a new regression was introduced — investigate before proceeding.

- [ ] **Build + core tests still green**

```bash
go build ./... && go test -count=1 -timeout 180s . && go vet ./...
```
Expected: all PASS / exit 0.

- [ ] **`SECURITY.md` contains both accepted-risk entries**

```bash
grep -c 'GO-2026-4479\|GO-2024-3218' SECURITY.md
```
Expected: `2` (or more, if both appear in multiple sections).

## Self-Review

- **Spec coverage:** All 8 advisories dispositioned (6 fixed in Tasks 1–3, 1 documented in Task 4, 1 handed off in Task 5). ✅
- **Placeholder scan:** The two `<fill ...>` markers in Task 3 Step 3 are the *output* of an investigation step (Step 1 produces the value to paste), not placeholders for unwritten code. ✅
- **Type/version consistency:** All version targets match the baseline evidence table at the top. ✅
