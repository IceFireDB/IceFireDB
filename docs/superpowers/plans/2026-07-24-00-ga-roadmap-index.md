# IceFireDB NoSQL — Path to 1.0.0 GA (Roadmap Index)

> **For agentic workers:** This is an index, not an executable plan. Each linked file is a standalone implementation plan. Execute them in the order below; honor the stated dependencies.

**Goal:** Take the current `1.0.0-rc.1` (branch `fix/nosql-1.0.0-readiness`) to a defensible **1.0.0 GA** by closing the confirmed gaps: security vulnerabilities, dependency staleness, Beta-backend graduation evidence, soak validation, and the single-shared-token auth model.

**Status snapshot (2026-07-24):**

| Check | Result |
|-------|--------|
| `go build ./...` | ✅ pass |
| `go test .` (core) | ✅ pass |
| `go vet` (changed pkgs) | ✅ clean |
| P0 blockers (snapshot nil ptr, crypto panic, pprof exposure) | ✅ fixed |
| `govulncheck ./...` affecting vulns | ❌ **8** (6 stdlib/module patches, 2 no-fix) |
| Auth model | ❌ single shared token, no ACL |
| Backend tiers | goleveldb/hybriddb = GA; badger/ipfs-synckv = Beta; rest = Experimental |
| Version string | `1.0.0-rc.1` (honest) |

---

## Execution order

| Seq | Plan | Workstream | Depends on | Estimated effort |
|-----|------|-----------|------------|------------------|
| 1 | [`2026-07-24-01-security-vuln-triage.md`](01-security-vuln-triage.md) | C | — | 🟢 S |
| 2 | [`2026-07-24-02-dependabot-cleanup.md`](02-dependabot-cleanup.md) | F1 | C | 🟢 S–M |
| 3 | [`2026-07-24-03-core-dependency-upgrade.md`](03-core-dependency-upgrade.md) | F2 | C, F1 | 🟡 M–L |
| 4 | [`2026-07-24-04-fork-dependency-coordination.md`](04-fork-dependency-coordination.md) | F3 | async (parallel) | 🔴 L (blocked on fork releases) |
| 5 | [`2026-07-24-05-badger-ga-graduation.md`](05-badger-ga-graduation.md) | B | F2 | 🟡 M |
| 6 | [`2026-07-24-06-sustained-soak-validation.md`](06-sustained-soak-validation.md) | D | F2, B | 🟡 M |
| 7 | [`2026-07-24-07-acl-security-model.md`](07-acl-security-model.md) | A | (after rc.1 field feedback) | 🔴 L |
| 8 | [`2026-07-24-08-plan-checklist-reconciliation.md`](08-plan-checklist-reconciliation.md) | E | anytime | 🟢 S |

## Why this order

1. **C → F1 → F2** are one continuous thread: `govulncheck` (C) tells us exactly which bumps are security-required; F1 clears the dependabot backlog; F2 lands the conservative core bumps that close most of C's findings. Doing them together avoids redundant `go.mod` churn.
2. **F2 unblocks B and D**: badger GA evidence and a credible soak must run on the *final* dependency stack, not the rc.1 stack.
3. **F3 is asynchronous**: three dependencies are IceFireDB forks (`ledisdb`, `uhaha`, `go-ipfs-log`). Their upgrades depend on fork maintainers tagging new versions; start the requests early but don't block the main line.
4. **A (ACL) waits for rc.1 field feedback**: a full per-user ACL is a large, potentially breaking change. Shipping rc.1 first lets real deployments tell us whether a shared-token + hardening is sufficient or a full ACL is required.
5. **E (checklist reconciliation) is mechanical** and can be slotted in anytime to keep the existing `2026-06-2x-*.md` plans auditable.

## GA exit criteria (all must be true before tagging `1.0.0`)

- [ ] `govulncheck ./...` reports **0 affecting** vulnerabilities (or every remaining one is documented as accepted/mitigated with owner sign-off).
- [ ] Go toolchain pinned to a fixed stdlib release (≥ the version that closes GO-2026-5037/5039/5856).
- [ ] No dependabot branch older than the current dependency baseline remains open.
- [ ] badger graduated from Beta → GA (plan 05 exit criteria met).
- [ ] A soak run of **≥ 1 hour** with `SOAK_CHAOS=1` passes with 0 data loss (plan 06).
- [ ] Auth gap is either (a) ACL implemented (plan 07) or (b) explicitly accepted with a documented deployment constraint.
- [ ] All `2026-06-2x-*.md` plan checkboxes reconciled with the actual code (plan 08).
