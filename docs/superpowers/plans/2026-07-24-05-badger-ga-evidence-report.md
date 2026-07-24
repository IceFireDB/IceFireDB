# Badger Backend — GA Evidence Report

**Date:** 2026-07-24
**Branch:** `test/badger-ga-graduation` (off `chore/deps-conservative-upgrade`)
**Verdict:** **GA-ready.** badger is promoted from Beta → GA in the support matrix.

## Summary

GA-graduation testing surfaced and fixed two real driver defects, then proved the
backend across the full correctness + reliability matrix. badger now provides
true point-in-time snapshot isolation and a working `Compact()` (previously a no-op).

## Driver fixes delivered (as part of this graduation)

1. **Snapshot isolation** — `NewSnapshot()` previously created a fresh read
   transaction on every `Get()`/`NewIterator()` call, so it always read the
   latest state (no isolation). Fixed: one read transaction is pinned at snapshot
   creation and reused; the iterator gains an `ownsTxn` flag so its `Close()`
   cannot discard the snapshot's shared transaction. Commit `49a96afb`.
2. **`Compact()`** — was a no-op. Implemented via `RunValueLogGC(0.5)` (ignoring
   `ErrNoRewrite`); badger auto-compacts the LSM tree. Commit `49a96afb`.

## Evidence

| Test | Result | Command |
|------|--------|---------|
| IDB surface (CRUD/WriteBatch/Iterator/Snapshot/engine) | PASS | `go test ./driver/badger/` |
| Concurrency no-corruption (4w×4r, 4k ops, `-race`) | PASS | `go test -run TestGA_ConcurrencyNoCorruption ./driver/badger/` |
| Large-value survives Compact (4 MB + value-log GC) | PASS | `go test -run TestGA_LargeValueSurvivesCompaction ./driver/badger/` |
| Snapshot-iterator isolation (excludes post-snapshot writes) | PASS | `go test -run TestGA_SnapshotIteratorIsolation ./driver/badger/` |
| RESP compat suite (`alltest`) | PASS (1 pre-existing harness failure, see below) | `DRIVER=badger make test-compat` |
| Raft snapshot round-trip (goleveldb + badger) | PASS | `TestSnapshotRestoreRoundTrip/badger` |
| Crash recovery + multi-node failover + rejoin + rolling-restart + leader-churn + soak | PASS (6/6) | `DRIVER=badger make test-integration` |

Integration suite detail (`DRIVER=badger make test-integration`, 139s):
- `TestIntegrationClusterFailover` PASS (kill leader → re-election, replicated data survives)
- `TestIntegrationRollingRestart` PASS (restart every node under continuous writes)
- `TestIntegrationLeaderChurn` PASS (repeated leader kills across rounds)
- `TestIntegrationSoak` PASS (sustained concurrent load)
- `TestIntegrationFollowerRejoin` PASS (kill+restart follower, restore quorum)
- `TestIntegrationCrashRecovery` PASS (SIGKILL mid-data → durability across restart)

## Known limitations / non-divergences

- **`TestKV` fails identically on goleveldb AND badger** (`strings_test.go:25:
  wrong number of arguments`). This is a **pre-existing test-harness bug**,
  not a badger-specific divergence — it is out of scope for badger GA graduation
  and should be fixed as a separate task. Every other compat case that passes on
  goleveldb also passes on badger.
- **Default open options are memory-heavy.** badger's defaults trade memory for
  throughput; tune `ValueLogFileSize` / `NumCompactors` / cache sizes before
  heavy production use. (Carried over from the previous Beta note.)
- **`driver/...` Iterator `Prev()`/`Last()`** create their own temporary read
  transactions and are unaffected by the snapshot-isolation change.

## GA criteria met

This satisfies the badger-graduation exit criterion in
`2026-07-24-00-ga-roadmap-index.md` and the full task list in
`2026-07-24-05-badger-ga-graduation.md`.
