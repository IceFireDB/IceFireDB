# Operations Runbook

## Backup

IceFireDB persists data through the Raft snapshot mechanism. To capture a backup:

1. A snapshot is produced on demand by the Raft layer and stored under the node's
   data directory (`-d <dir>`). Copy the data directory of a healthy node (ideally
   the leader, or a quiesced follower) to your backup location.
2. For a consistent copy, stop writes or snapshot a follower that is caught up.

## Restore

Use the `--restore <path>` flag to start a brand-new single-node cluster from a
snapshot file:

    icefiredb -d <fresh-data-dir> --restore <snapshot-path>

This bootstraps a single node from the snapshot. Other nodes must then re-join the
new cluster with `-j <leader-addr>`. `--restore` is ignored if the data directory
already contains state, and cannot be combined with `-j`.

## Rolling upgrade

A multi-node cluster tolerates losing a minority of nodes, so upgrade one node at
a time:

1. Stop one follower; replace its binary; restart it on the same data directory.
   It rejoins from persisted Raft state and catches up (verified by
   `TestIntegrationFollowerRejoin`).
2. Wait until it reports as a healthy follower (e.g. `/readyz` returns 200) before
   moving to the next node.
3. Upgrade the leader last; it will step down and a follower will be elected.

Never take down a majority simultaneously — that loses quorum and halts writes.

## Configuration compatibility

- Do not change `--storage-backend` for an existing data directory; the on-disk
  format is backend-specific.
- Keep `--auth` / `ICEFIREDB_AUTH` consistent across all nodes — it is shared.
- Node identity (`-n`) and bind address (`-a`) must remain stable for a given
  data directory across restarts.

## Health checks

Enable the observability server with `--metrics` and point your orchestrator at:

- `/healthz` — liveness (process up).
- `/readyz` — readiness (storage initialized); use to gate traffic/rollout.
- `/metrics` — Prometheus exposition (Go/process metrics + connected clients).
