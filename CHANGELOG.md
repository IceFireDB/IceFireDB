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

### Changed
- **hybriddb**: Improve cache key tracking in WriteBatch operations by storing key copies
