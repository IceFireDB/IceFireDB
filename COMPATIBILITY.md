# Redis Protocol Compatibility

IceFireDB speaks the Redis RESP protocol via the ledisdb command layer. Most common
commands behave as in Redis, but there are intentional divergences inherited from
ledisdb's storage model. This document lists the ones most likely to affect users
migrating from Redis. Behaviors marked "locked by test" are pinned by `compat_test.go`
(run with `-tags alltest`) so they cannot change without a deliberate update here.

## Separate per-type keyspaces (no WRONGTYPE)  — locked by test

In Redis, a key holds exactly one value of one type, and operating on it with a
command for a different type returns:

    WRONGTYPE Operation against a key holding the wrong kind of value

IceFireDB instead maintains a SEPARATE keyspace per data type (string, hash, list,
set, sorted set). Consequences:

- The same key name can simultaneously hold, e.g., a string AND a hash.
- A type-mismatched operation does NOT return WRONGTYPE; it operates on that type's
  own (possibly empty) view of the key.
- `TYPE <key>` may return an empty string for keys created via type-specific commands.

Example (verified):

    SET k v        -> OK
    HSET k f x     -> 1        (Redis: WRONGTYPE error)
    GET k          -> "v"
    HGET k f       -> "x"
    TYPE k         -> ""

If your application relies on WRONGTYPE errors for type safety, add that check at the
application layer.

## Expiration is per-type

IceFireDB exposes per-type expiration commands (e.g. `HEXPIRE`, `LEXPIRE`, `SEXPIRE`,
`ZEXPIRE`, and `*TTL`/`*PERSIST` variants) rather than only the single Redis `EXPIRE`/
`TTL` on a key. See the command list in `README.md`.

## Scope of this document

This is not an exhaustive compatibility report. It captures the divergences confirmed
to date. Additional differences may exist; contributions documenting (and where
appropriate, locking via `compat_test.go`) further divergences are welcome.
