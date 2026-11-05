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

IceFireDB exposes per-type expiration commands rather than only the single Redis `EXPIRE`/`TTL` on a
key: hashes have `HEXPIRE`/`HEXPIREAT`/`HTTL`, lists `LEXPIRE`/`LEXPIREAT`/`LTTL`, sets
`SEXPIRE`/`SEXPIREAT`/`STTL`/`SPERSIST`. Strings, hashes and lists have **no** `PERSIST` command
(those handlers are not registered), and sorted sets have no expiration command at all — clear and
recreate the key instead. See the command list in `README.md`.

## Expiration has one-second resolution

IceFireDB stores expirations as whole seconds. `SET key value PX <ms>` is therefore rounded **up** to
the next second: `PX 500` keeps the key for 1 second and `PX 1500` for 2, where Redis would use 0.5
and 1.5 seconds. A key never expires earlier than the caller asked for. Every `*TTL` command reports
whole seconds for the same reason.

## Empty values on the decentralized backends

`HSET key field ""` followed by `HGET key field` answers an empty bulk string on the local backends
(`goleveldb`, `badger`, `hybriddb`), as Redis does. The decentralized drivers report an empty value as
missing, so there `HGET` answers a nil reply instead — an empty field and an absent field look the
same. `GET` follows the same rule for string values.

## Expiration timestamps use the Raft clock

Every expiration is computed from the Raft machine clock (`m.Now()`), which is what keeps the
expiry identical on every replica, while the storage layer validates timestamps and reports `TTL`
against the local wall clock. When the Raft clock lags the local clock, a short-lived key can
therefore report `-1` (no TTL) although it still holds its value, and an expiry shorter than the
skew is rejected with `invalid expire value`. Long TTLs are unaffected in practice; for sub-second
precision, do not rely on it. `SET ... KEEPTTL` and the list rebuilds in `LINSERT`/`LREM` re-apply the remaining TTL at apply
time, so the stored expiry — not merely the reported TTL — can differ between replicas by the apply
latency.

## Scan cursors are not integers

`SCAN` answers an opaque base64 cursor, and `HSCAN`/`SSCAN`/`ZSCAN` answer the last field or member
name they returned. That means a field literally named `0` collides with the "iteration finished"
marker, and client helpers that parse the cursor as a number (for example go-redis' typed `HScan`)
reject it. Use the generic command API and treat the cursor as opaque.

## Conditional writes see each replica's own expiry check

Expired keys are reaped asynchronously against the local clock, and `SET ... NX`/`XX` tests existence
through that same view. A key that has logically expired can therefore still be visible to one replica
but not another, and the two can disagree about whether the write happens. The same caveat applies to
`SETNX` and to any existence test.

## List rebuilds are not transactional

`LINSERT` and `LREM` have no primitive in the storage layer, so they rebuild the whole list
(`LRange` → `LClear` → `RPush`, then the TTL is restored). The rebuild happens inside one Raft apply,
so no other client observes a partial list, but it is not rolled back: if the backend fails a write
half-way through, the list is left truncated. The operation is also O(n) in the list length.

## Safety limits

Two client-supplied numbers are bounded so that a single request cannot make the server materialise an
unbounded reply:

- `COUNT` on `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` is capped at 10000. Redis treats `COUNT` as a hint, so
  this only bounds the page size — iteration still visits every key.
- `|count|` on `SRANDMEMBER` is capped at 1048576; beyond that the command answers an error instead
  of an array.

A seconds-based expiration that cannot be represented (`SET ... EX` near the int64 limit) is refused
with an error instead of being silently dropped. Millisecond values cannot overflow, because they are
divided down to seconds first.

## Scope of this document

This is not an exhaustive compatibility report. It captures the divergences confirmed
to date. Additional differences may exist; contributions documenting (and where
appropriate, locking via `compat_test.go`) further divergences are welcome.
