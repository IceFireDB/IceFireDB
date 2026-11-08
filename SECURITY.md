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

## Known vulnerabilities and accepted risk

The dependency set is checked two ways: GitHub Dependabot alerts (every advisory that touches a module
in `go.mod`) and `govulncheck` (only the vulnerabilities reachable from the code that is actually
built). Only the second one describes risk.

After the dependency upgrade that raised the toolchain to Go 1.27.3, `govulncheck .` on the engine
reports **two** reachable vulnerabilities. Both are accepted, with the reason and the reachability
condition recorded here rather than dismissed:

| ID | Module | Why it is not fixed | Where it is reachable |
| --- | --- | --- | --- |
| `GO-2026-4479` | `github.com/pion/dtls/v2@v2.2.12` | No fixed release exists for `v2`. Upstream moved the WebRTC transport to `pion/dtls/v3`, but `v2` is still required by `kubo@v0.40.1`, `boxo`, `hsanjuan/ipfs-lite@v1.8.6` and every IceFireDB fork in `go.mod` (`components-go`, `icefiredb-crdt-kv`, `icefiredb-ipfs-log`, `berty.tech/go-ipfs-log`), so it cannot be removed by a version bump alone. | Only libp2p's WebRTC transport, which runs when a decentralized backend (`crdt`, `ipfs`, `ipfs-log`, `ipfs-synckv`) is selected or when a bridge binary's P2P layer is enabled. |
| `GO-2024-3218` | `github.com/libp2p/go-libp2p-kad-dht@v0.38.0` | The fix requires `kad-dht >= 0.40`, which changed the `dht.New` / `dualdht.New` signatures (the `context.Context` argument was dropped). Bumping it breaks `kubo@v0.40.1`, `hsanjuan/ipfs-lite@v1.8.6` and `icefiredb-crdt-kv@v0.3.0`, which still call the old signature, so it needs a coordinated upstream and fork release. | Same libp2p/DHT path as above. |

Mitigations in force today:

- Keep the libp2p port of those backends and of the bridge binaries off untrusted networks. This is
  what `OPERATIONS.md` already requires ("restrict network exposure … to trusted clients and peer
  nodes").
- Both entries sit on Experimental-tier backends; the GA backends (`goleveldb`, `hybriddb`) never enter
  that code at runtime.

Both entries are expected to disappear with a single coordinated upgrade of `kubo`, `ipfs-lite` and
the IceFireDB forks — re-check them with `govulncheck .` at that point.

## Roadmap

Per-user ACLs (multiple identities with scoped permissions) are a planned future
enhancement and are not part of 1.0.0.
