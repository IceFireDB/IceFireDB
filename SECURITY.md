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

## Roadmap

Per-user ACLs (multiple identities with scoped permissions) are a planned future
enhancement and are not part of 1.0.0.
