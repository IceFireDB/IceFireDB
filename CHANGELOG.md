# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **RESP compatibility**: `SET` accepts `EX`, `PX`, `NX`, `XX` and `KEEPTTL`; new `LINSERT`, `LREM`,
  `SMOVE`, `SPOP`, `SRANDMEMBER` and the top-level `SCAN`; `HSCAN`/`SSCAN`/`ZSCAN` now follow the Redis
  cursor protocol, while the native `XSCAN`/`XHSCAN`/`XSSCAN`/`XZSCAN` family keeps its contract.
- **Docs**: [`COMMAND-REFERENCE.md`](COMMAND-REFERENCE.md) documents all 110 registered commands
  (syntax, arguments, replies, examples, English + 中文); [`COMMAND-AUDIT.md`](COMMAND-AUDIT.md) records
  the rules every command must satisfy (replication class, determinism, limits, tests).

### Changed
- **Scanning (breaking)**: `HSCAN`/`SSCAN`/`ZSCAN` answer Redis-style cursors (`0` starts, `0` finishes)
  instead of the ledis-style cursor they used before. The cursor is the last field/member name, not an
  integer, so client helpers that parse it as a number are not supported — use the generic command API.
  The `X*SCAN` family keeps the previous behaviour.
- **Limits**: `COUNT` on any scan is capped at 10000 (`COUNT` is a hint), and `SRANDMEMBER` rejects an
  absolute count above 1048576 instead of materialising an unbounded reply. A scan page can be empty
  while the cursor still advances, so loop until the cursor reports the end.

### Fixed
- **RESP compatibility**: `HGET` returns an empty bulk string for an existing empty field on the local
  backends; `ZSCORE` returns a bulk string instead of an integer reply; `SPOP`/`SRANDMEMBER` answer a
  single member for their bare form; `SMOVE` answers `1` when the destination already held the member.
- **Semantics**: `SET ... NX/XX` answers a null bulk when the condition is not met, `KEEPTTL` keeps the
  TTL it names, `PX` rounds up instead of asking for an expiry in the past, `LINSERT` answers the new
  length and reports `0`/`-1` for a missing pivot/key, and `LINSERT`/`LREM` no longer reverse the list
  or drop its TTL.
- **Stability**: `SPOP` draws from the Raft machine's generator so every replica pops the same member;
  `XHSCAN` no longer returns the page-ending field twice (it looped forever at `COUNT 1`); a negative
  `SRANDMEMBER` count of `MinInt64` no longer panics the process; an unbounded scan `COUNT` no longer
  allocates gigabytes; `go test ./...` compiles again (the `alltest` build tag is restored).

### Fixed
- **hybriddb**: Fix cache invalidation after Delete operations
  - Added tombstone marker (nil value cacheItem) in Delete to prevent cache repopulation
  - Updated Get to detect nil values and return nil without repopulating cache
  - Fixes TestSnapshot failure where deleted keys were returned from stale cache entries
  - Fixes TestCacheConsistency_ConcurrentOverwrite race condition between cache and storage
- **hybriddb**: Fix cache data corruption caused by storing byte slice references instead of copies
  - Cache now stores independent copies of keys and values to prevent external mutation
  - Changed Ristretto cache key type from `[]byte` to `string` for proper key comparison
  - Fixed WriteBatch cache invalidation to use string keys
  - Resolves snapshot test failures where modified values were not reflected correctly
- **hybriddb**: Fix list metadata cache inconsistency
  - Added `LMetaType` constant (value: 1) for identifying LedisDB list metadata keys
  - Introduced `InvalidateListCache()` method to invalidate both key and list metadata cache entries
  - Updated `Put()`, `Delete()`, `SyncPut()`, and `SyncDelete()` to use proper cache invalidation
  - Ensures consistency between hot tier (cache) and cold tier (leveldb) storage

### Changed
- **hybriddb**: Improve cache key tracking in WriteBatch operations by storing key copies

### Added
- **hybriddb**: Comprehensive cache consistency test suite (25 test cases)
  - Basic operation consistency tests (PutThenGet, PutOverwrite, Delete, SyncOperations)
  - List cache invalidation tests for metadata key handling
  - Concurrency tests for multi-goroutine Put/Get/Delete operations
  - Edge case tests (cache expiry, close/reopen, snapshot isolation)
  - All tests verify consistency between cache and underlying storage
