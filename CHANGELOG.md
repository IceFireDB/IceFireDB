# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
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
