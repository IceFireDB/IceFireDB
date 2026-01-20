# AGENTS Guidelines for IceFireDB

This repository contains IceFireDB, a decentralized database infrastructure bridging Web2 and Web3 ecosystems. When working on this project interactively with an agent, please follow guidelines below so that development experience continues to work smoothly.

## 1. Use Development Mode, Not Production Build

* **Always use `make run` or `make run_dev` while iterating** on the application. This starts IceFireDB in development mode with proper debugging and logging enabled.
* **Do _not_ run `make all` or `make release` inside agent session.** Running production build commands creates cross-platform binaries and can leave the development workflow in an inconsistent state. If a production build is required, do it outside of the interactive agent workflow.

## 2. Keep Dependencies in Sync

If you add or update dependencies, remember to:

1. Run `go mod tidy` to update `go.mod` and `go.sum`.
2. Update `vendor/` directory if vendor mode is used: `go mod vendor`.
3. Re-build and re-test to ensure dependencies are properly integrated.

## 3. Coding Conventions

* Prefer Go 1.25+ syntax for new code.
* Use `sync.RWMutex` for thread-safe operations.
* Follow write-through caching pattern for hybrid storage.
* Follow existing naming patterns (e.g., `db`, `cfg`, `err`).

## 4. Git Commit and PR Guidelines

* **Always use `git commit -s -m`** to add the Signed-off-by line.
* All commit messages and PR descriptions must be in **English**.
* Follow conventional commits format: `<type>(<scope>): <subject>`
* Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
* All code comments must be in **English**.

Example:
```bash
git commit -s -m "feat(hybriddb): add configurable cache TTL support"
```

## 5. Useful Commands Recap

| Command                        | Purpose                                            |
| ------------------------------ | -------------------------------------------------- |
| `make run`                     | Start IceFireDB in development mode.              |
| `make run_dev`                 | Run with development configuration.                 |
| `make localbuild`               | Build binary for local environment.                 |
| `make test`                    | Run test suite.                                   |
| `DRIVER=badger make test`      | Run tests with specific storage driver.            |
| `go test ./...`                | Execute all tests.                                 |
| `go mod tidy`                  | Clean up dependencies.                             |
| `make race`                    | Run tests with race detection.                      |

## 6. Project Structure Reference

```
IceFireDB/
├── driver/                      # Storage driver implementations
│   ├── badger/                  # BadgerDB driver (LSM tree based)
│   ├── crdt/                    # CRDT KV driver (conflict-free replicated)
│   ├── hybriddb/                # Hybrid cache driver (hot/cold tier)
│   ├── ipfs/                    # IPFS storage driver
│   ├── ipfs-log/                # IPFS Log driver (append-only log)
│   ├── ipfs-synckv/             # IPFS SyncKV driver
│   ├── orbitdb/                 # OrbitDB driver
│   └── oss/                     # Object Storage Service driver
├── IceFireDB-SQLite/            # MySQL protocol to SQLite backend
├── IceFireDB-SQLProxy/          # MySQL protocol proxy for traditional DBs
├── IceFireDB-Redis-Proxy/       # Redis protocol proxy
├── IceFireDB-PubSub/            # Decentralized PubSub system
├── *.go                         # Core NoSQL database (RESP protocol)
└── Makefile                     # Build and test scripts
```

## 7. Key Development Patterns

### Adding a New Storage Driver

1. Create a new directory under `driver/`
2. Implement the `driver.IDB` interface with required methods
3. Register the driver in the `init()` function
4. Add configuration flags in `flags.go`
5. Add a switch case in `main.go` for initialization
6. Add unit tests

### Adding a New Redis Command

1. Implement the command handler in the appropriate file (e.g., `strings.go` for string commands)
2. Register the command in the ledis command registry
3. Add tests in `*_test.go` file
4. Update README.md with command documentation

## 8. Storage Driver Interface

All storage drivers implement the `driver.IDB` interface:

```go
type IDB interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    NewWriteBatch() IWriteBatch
    NewIterator() IIterator
    NewSnapshot() (ISnapshot, error)
    GetStorageEngine() any
}
```

## 9. CLI Flags Reference

| Flag                    | Description                        | Default                |
| ---------------------- | ---------------------------------- | ---------------------- |
| `-a addr`             | Bind address                        | `127.0.0.1:11001`    |
| `-n id`               | Node ID                            | `1`                    |
| `-d dir`              | Data directory                      | `data`                 |
| `-j addr`             | Join cluster at address              | -                      |
| `--storage-backend`    | Storage driver                     | `goleveldb`            |
| `--hot-cache-size`    | Hot cache size (MB)                | `1024`                 |
| `--ipfs-endpoint`     | IPFS API endpoint                 | -                      |
| `--oss-endpoint`      | OSS endpoint                      | -                      |
| `--oss-ak`            | OSS access key                    | -                      |
| `--oss-sk`            | OSS secret key                    | -                      |
| `--servicename`       | P2P service name                  | -                      |
| `--datatopic`         | P2P data sync channel             | -                      |
| `--nettopic`          | P2P net discovery channel         | -                      |

## 10. Common Issues

1. **IPFS Connection**: Ensure IPFS daemon is running when using the `ipfs` driver.
2. **Port Conflicts**: Default port `11001` may conflict with other services.
3. **Cache Size**: Adjust `--hot-cache-size` based on available memory.
4. **Encryption**: `ipfs-synckv` driver requires `--ipfs-synckv-key` for encryption.

---

Following these practices ensures that the agent-assisted development workflow stays fast and dependable. When in doubt, run tests before making changes, and use development mode instead of production builds.
