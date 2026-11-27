# Changelog

All notable changes to IceFireDB-SQLProxy are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows the version reported by `IceFireDB-SQLProxy --version`. The proxy is part of the
IceFireDB repository's single Go module, so there is one release per repository version.

## [1.0.0]

The first release that can be trusted with data. Previous versions replicated SQL text on a
best-effort channel but silently lost prepared-statement writes, split transactions across nodes,
had a read-only mode that never took effect, and could leak or crash on a failed upstream
connection.

### Added

- `/healthz` and `/metrics` on `127.0.0.1:<debug.port>`, behind the existing `debug.enable`
  switch: upstream pool health, replication published/applied/dropped/failed counters, rejection
  reasons by label, p2p peer count, and this node's peer ID and multiaddrs.
- `p2p.authToken` (HMAC-SHA256 message signing) and `p2p.allowedPeers` (peer ID allow list).
  Unset values keep the old permissive behaviour and log a warning.
- `p2p.bootstrapPeers`: explicit peer multiaddrs for private clusters, which disables public DHT
  discovery and makes startup deterministic.
- `userlist[].role` (`admin` by default, or `readonly`), which routes a tenant to the readonly
  upstream pool.
- `server.idleTimeout` and `server.writeTimeout` to drop stalled clients.
- Graceful shutdown: SIGTERM/SIGINT stop accepting, drain in-flight connections for 5s, close
  upstream pools, and exit 0. A second signal exits immediately.
- `make vet`, `make test`, `make e2e`, `make docker`, and a Dockerfile
  (build context: the repository root).
- End-to-end suite against a real MySQL 8 instance with two proxy nodes.
- `docs/replication.md`: message envelope, bounds, failure modes, divergence detection.

### Fixed

- **Prepared statements were never replicated correctly**: the published text still contained
  `?` placeholders, so a peer either failed the statement or inserted a literal `?`. Arguments
  are now inlined as MySQL literals (binary, numeric, temporal and `NULL` values included).
- **Transactions were replicated statement by statement**, handing peers a partial transaction
  that committed there independently. They are now buffered and published as one batch at
  `COMMIT`, and discarded on `ROLLBACK`.
- **`ALTER` was never replicated**: the keyword list contained `ALERT`. Classification is now
  lexical, ignores comments and whitespace, splits multi-statement batches, resolves `WITH`
  CTEs, and covers `REPLACE`, `TRUNCATE`, `RENAME`, `GRANT`, `REVOKE` and `LOAD`.
- **Read-only mode never worked**: connections always took a connection from the admin pool, and
  the check compared the upstream connection's user name against `mysql.readonly.user`, which
  never matched. Role-based pool routing makes it effective, and the classifier refuses every
  statement that is not provably a read or a session statement.
- **Inbound messages were executed unauthenticated**: any peer that joined the topic could run
  arbitrary SQL, including `DROP TABLE`, on the local MySQL. Messages are now authenticated,
  bounded, de-duplicated, and restricted to data-changing statements.
- **A failed `popConn` returned a nil connection with a nil error**, so a proxy whose MySQL was
  down dereferenced nil on the next client connection. Retries now return the error, and the
  failure is reported to the client as `ERROR 1040` per statement instead of a protocol desync.
- **`popConn` could block forever**: the pool was asked for a connection with the process
  context, which never expires. Each attempt now has a deadline.
- **`Handle.CloseConn` committed connections instead of rolling back**: it called `Commit` when
  the session was *not* in a transaction and never rolled back an open one.
- **Connection leaks**: connections left in a transaction were neither returned to the pool nor
  closed, the incoming per-sender connection map dropped connections on the floor, and a peer
  that never sent `COMMIT` held a connection and its locks forever.
- **The pool's background goroutines never stopped**, so every restart leaked two goroutines and
  its idle connections. `Pool.Close` now stops them.
- **`asyncSQL` panicked in a loop** when a topic subscription closed; closed channels are now
  handled and the per-sender map is not resurrected in a broken state.
- **`Accept` errors panicked the process**; they are now logged and retried with a backoff.
- **SIGTERM called `os.Exit(0)`**, skipping every defer: pools were never closed and in-flight
  queries were killed mid-response.
- **The publish path blocked client writes** on an unbuffered channel; publishing is now
  non-blocking and reports drops.
- **The server never reported `SERVER_STATUS_AUTOCOMMIT`**, so clients that track the session
  state believed autocommit was off.
- `go vet ./...` failures (unreachable code) and the dead `NewMySQLProxy` constructor.
- A configuration file that could not be read panicked with a stack trace; it now produces a
  readable error and exit code 1.

### Changed

- `config/config.yaml` gained commented examples for the new options. Existing configuration
  files keep working unchanged: every new field is optional and defaults to the previous
  behaviour, except that `debug.enable: true` now actually serves the debug endpoints.
- Batch classification is conservative: a statement whose leading keyword is not recognised is
  refused for read-only tenants and is not replicated.
- Logs no longer print the full statement text at info level on the replication path; statements
  are logged at debug level, and failures are logged with the message id.
- The `pkg/mysql` fork was left structurally intact; only the specific defects above were fixed.

### Known limitations

- Replication remains best-effort: no consensus, no conflict resolution, no catch-up for a peer
  that was offline. See `docs/replication.md`.
- Pre-1.0.0 nodes cannot replicate to 1.0.0 nodes when `p2p.authToken` is configured.
