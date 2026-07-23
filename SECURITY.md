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

## Known accepted vulnerabilities

Vulnerabilities that remain open because there is no upstream fix, or because
the vulnerable code path is not reached by IceFireDB. Each entry records the
reachability analysis and the disposition.

### GO-2026-4479 — github.com/pion/dtls/v2@v2.2.12 (no upstream fix as of 2026-07-24)

- **Reachability:** Not reached by IceFireDB application code. IceFireDB does not
  import `pion/dtls` or `pion/webrtc` directly; the package is present only as an
  *indirect* transitive dependency of the libp2p/kubo transport stack
  (`go.mod` marks it `// indirect`), and no `webrtc` transport is enabled by any
  flag in `flags.go`/`main.go`. The `govulncheck` traces are limited to package
  `init()` initialization and `fmt.Sprintf` reflection into `String()` methods —
  i.e. linkage reachability, not an actual DTLS handshake or authentication-key
  operation. The vulnerable functionality (DTLS authentication) is therefore not
  exercised.
- **Disposition:** Accepted risk for 1.0.0 GA. If an upstream fix is published,
  pick it up via plan `2026-07-24-04-fork-dependency-coordination.md` or a direct
  bump once the transitive consumer (libp2p/kubo) advances.

### GO-2024-3218 — github.com/libp2p/go-libp2p-kad-dht@v0.38.0

- **Disposition:** Tracked in plan `2026-07-24-03-core-dependency-upgrade.md`.
  `govulncheck` reports `Fixed in: N/A`, but a newer `libp2p-kad-dht` (v0.40.0)
  exists and may carry an untagged fix. A conservative bump to v0.40.0 is applied
  in that plan, followed by a `govulncheck` re-test to confirm resolution.

## Roadmap

Per-user ACLs (multiple identities with scoped permissions) are a planned future
enhancement and are not part of 1.0.0.
