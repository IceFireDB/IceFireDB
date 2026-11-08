# Command Audit

Every command the engine registers answers to two contracts: the Redis behaviour a client expects,
and the engine's own rules for being replicated and staying inside its resource budget. This file is
the audit trail for both. Read it before changing a command handler, and update it in the same change.

Two companion documents split the rest of the ground:

- [`COMMAND-REFERENCE.md`](COMMAND-REFERENCE.md) — the client-facing reference: syntax, arguments,
  replies and a production example for every command.
- `COMPATIBILITY.md` — the divergences from Redis that a client will actually notice.
- The engine and storage specs — why the layers are shaped the way they are.

## How to audit a command

Nine checks. Each one has caught a real defect at least once.

1. **Which replication class?** `conf.AddWriteCommand` means the handler is replayed on **every**
   replica; `AddReadCommand` runs locally (leader after a write barrier, or a follower under
   `--openreads`). Getting this wrong is silent corruption, not a crash: `ZINCRBY`,
   `ZREMRANGEBYSCORE` and `ZREMRANGEBYRANK` are still registered as reads and therefore never
   replicate.
2. **Is a write command deterministic?** Inside a write handler: no `math/rand`, no `time.Now()`, no
   map iteration order, no float formatting. Use `m.Now()` for time and `m.Rand()` for randomness —
   uhaha seeds both identically across the cluster and replays them in order after a restart. A read
   command may use `math/rand` freely; `m.Rand()` would hand it a frozen series.
3. **Does it respect the storage layer's actual behaviour?** See the ledger below — the ledis
   semantics here are not obvious and several are load-bearing.
4. **Are the reply types right?** `nil` → null bulk, `[]byte` → bulk string, `[][]byte`/slices →
   array, `redcon.SimpleString` only for `+OK`-style replies (never for values: a simple string
   cannot carry `\r\n` and corrupts the stream).
5. **Is every client-controlled size bounded?** `COUNT`, `|count|`, cursor strings and `MATCH`
   patterns all arrive from the network. Bound them before allocating (`maxScanCount`,
   `maxRandMemberCount`).
6. **Is validation done before the write?** A rejected argument must not leave a half-applied
   mutation — `SET ... EX <unrepresentable>` validates the expiry before storing the value for
   exactly this reason.
7. **Is the expiry arithmetic clock-correct and overflow-safe?** Compute with `m.Now()`, never the
   local clock, and guard `now + ttl` against int64 overflow.
8. **Is a cursor opaque and monotonic?** Clients echo it back verbatim, so it must never be
   interpreted as a number by the server, and the resume point must advance past the last entry that
   was *scanned* — not the last one that happened to match.
9. **Do the tests pin the semantics?** Every rule above needs a test that fails when the rule is
   broken. An assertion that accepts two outcomes (or `err == nil || err == redis.Nil`) is not a test.

## Storage-layer ledger

Verified against `vendor/github.com/ledisdb/ledisdb`; these are the behaviours that decide whether a
handler is correct.

| Storage behaviour | Consequence for handlers |
| --- | --- |
| `DB.Set` calls `rmExpire` — a plain write **drops the key's TTL** | `SET ... KEEPTTL` must read the remaining TTL first and re-apply it after the write |
| `LClear` / `SClear` / `HClear` delete the key and its expiry | Any rebuild via `LClear` must restore the TTL explicitly |
| `Get`/`HGet` answer `(nil, nil)` for a missing key on the pure-Go drivers, `leveldb.ErrNotFound` on others; an empty-but-present value is an empty non-nil slice on the local backends | Distinguish "missing" from "empty" by `val == nil`, never by `len(val) == 0`. The decentralized drivers collapse the two — documented in `COMPATIBILITY.md` |
| `Scan`/`HScan`/`SScan`/`ZScan` apply `MATCH` **before** counting; an empty pattern means "no filter" | Fewer results than `COUNT` reliably means "keyspace exhausted". Filtering inside the storage layer can walk an entire keyspace per call, so the scan handlers filter in the handler over a bounded page |
| `TTL`/`LTTL`/`STTL` compute `expiry - time.Now()` (local clock) and return `-1` both for "no expiry" and "already expired" | A TTL read inside a write command is not replica-stable — see the known gaps |
| `ExpireAt` rejects a timestamp `<= time.Now().Unix()` with `invalid expire value` | Expiry arithmetic must never land in the past; that is also why a lagging Raft clock can reject a short TTL |
| `SMembers` iterates a range iterator (encoded-key order on the LSM backends) | A write command that picks members must not depend on iteration order — `SPOP` sorts first |

## Commands covered by this audit

`r` = read command, `w` = write command (replicated).

| Command | Class | One-line contract | Limits |
| --- | --- | --- | --- |
| `SET` | w | `[EX s \| PX ms \| NX \| XX \| KEEPTTL]`; NX/XX miss → null bulk | expiry must be representable; contradictory options are syntax errors |
| `LINSERT` | w | `BEFORE\|AFTER` (case-insensitive) → new length; `0` = no pivot, `-1` = no key | O(n) rebuild |
| `LREM` | w | `count>0` from head, `<0` from tail, `0` all → removed count | O(n) rebuild; `MinInt64` rejected |
| `SMOVE` | w | `1` moved, `0` not a member; self-move is a no-op | — |
| `SPOP` | w | bare form → member (bulk), `COUNT` form → array | `COUNT<0` error, clamped to set size |
| `SRANDMEMBER` | r | bare → member, positive count → distinct, negative → repeats | `\|count\| ≤ 1048576` |
| `SCAN` | r | one opaque cursor across all five keyspaces; `MATCH`/`COUNT`/`TYPE` | `COUNT ≤ 10000` |
| `HSCAN` `SSCAN` `ZSCAN` | r | Redis cursor protocol, cursor is the last field/member name | `COUNT ≤ 10000` |
| `XSCAN` `XHSCAN` `XSSCAN` `XZSCAN` | r | native family: empty cursor, `MATCH` is a **regex**, `ASC`/`DESC` | `COUNT ≤ 10000` |
| `HGET` | r | bulk string; empty-but-present value survives on the local backends | — |
| `ZSCORE` | r | bulk string (not an integer reply); missing member → null bulk | — |

### `SET` — `strings.go`

Parses options case-insensitively, rejects contradictory ones (`NX`+`XX`, `KEEPTTL` with `EX`/`PX`,
`EX` with `PX`, non-positive expiry), then checks existence for `NX`/`XX`. Expiry is computed as
whole seconds (`expireSecondsFromMillis` rounds **up**), validated against int64 overflow **before**
the value is stored, and applied with `m.Now()`. A plain `SET` intentionally clears an existing TTL —
that is Redis behaviour and it is what the storage layer already does.

Open point: `NX`/`XX` test existence through the storage layer's view, which does not consult the
expiry; a key that has logically expired can still be visible to one replica and not another.

### `LINSERT` / `LREM` — `lists.go`, `replaceList`

No primitive exists for either, so both rebuild the list: read it, compute the new contents, then
`replaceList` (`LClear` → `RPush` in order → restore the TTL). Three things make this correct rather
than plausible: `RPush` keeps element order (the original implementation used `LPush` and reversed
the list), the TTL is captured with `LTTL` and re-applied, and `LREM` skips the rebuild entirely when
nothing matched, so an untouched list keeps its expiry bit-for-bit.

Open points: the rebuild is O(n) in memory and storage writes, it is not transactional (a backend
failure after `LClear` leaves a truncated list), and the TTL is re-derived from the local clock.

### `SPOP` — `set.go`

A write command, so the picked member must be identical on every replica. Two measures make that
true: members are sorted before the draw (so the outcome cannot depend on the backend's iteration
order), and the index comes from `m.Rand()` — never `math/rand`, whose source is per-process and
would silently diverge the state machines. Selection is a partial Fisher–Yates over the sorted slice,
so only the returned slots are shuffled. The bare form answers a bulk member; the `COUNT` form answers
an array, clamped to the set size, with `COUNT 0` → empty array and a negative count rejected.

Open point: no test replays the same log on two machines to prove the cluster-identical outcome; the
guarantee currently rests on uhaha's documented `Rand()` contract.

### `SRANDMEMBER` — `set.go`

A read command: it is not replayed, so plain `math/rand` is both safe and *required* (`m.Rand()`
returns a frozen series to read commands). Positive counts are distinct members, negative counts
repeat, `0` answers an empty array, and a missing key answers null bulk in the bare form. `|count|` is
capped so one request cannot ask the server to materialise an unbounded reply.

### `SCAN` — `scan.go`

The engine keeps a separate keyspace per type with no global key order, so one Redis cursor cannot be
a single last-key. The cursor is therefore five sub-cursors (not started / exhausted / last raw key),
base64-encoded and opaque; `0` ends the walk. Each call spends a single `COUNT` budget across the
keyspaces in a fixed order, marks a keyspace exhausted when a scan returns fewer entries than it
asked for, and advances the resume point past the last **raw** entry — `MATCH` is applied afterwards
in the handler, so a pattern that matches little cannot turn one call into a full-keyspace walk.

### `HSCAN` / `SSCAN` / `ZSCAN` — `scan.go`

Redis-style protocol (`0` to start, `MATCH`, `COUNT`), but the cursor they return is the last
field/member name rather than a numeric offset — see `COMPATIBILITY.md`. `HSCAN`/`ZSCAN` flatten to
field/value and member/score pairs. Same bounded `MATCH` handling as `SCAN`.

### `XSCAN` family — `scan.go`

Deliberately left on its original contract: an empty cursor starts, the returned cursor is the last
key/field/member, `MATCH` is a **regular expression** passed to the storage layer (not a Redis glob),
and `ASC`/`DESC` select the direction. This change only adds the `COUNT` bound and repairs an
inclusive-bound flip that made `XHSCAN` return the page-ending field twice (and loop forever at
`COUNT 1`).

## Known gaps

- **No cross-replica determinism test.** `SPOP`'s cluster-identical outcome is guaranteed by the
  `m.Rand()` contract and by sorting, but no test replays one log on two machines. A two-machine
  replay harness is the obvious next step.
- **No mechanical guard.** A one-line check (a file that calls `AddWriteCommand` must not import
  `math/rand`) would have caught the `SPOP` defect without review. Worth adding to CI.
- **Expiry is not replica-stable.** Expirations are stamped from the Raft clock but validated and
  reported against the local clock, and `KEEPTTL`/`LINSERT`/`LREM` re-derive a remaining TTL from the
  local clock. Replicas can therefore store expirations that differ by the apply latency. Fixing it
  needs a storage-layer accessor for the absolute expiry.
- **`SPERSIST` leaves the expiry behind on the `ipfs` backend** (observed on CI; cause unconfirmed);
  it has no test because the fix does not belong in this change.
- **List rebuilds are not transactional** and are O(n) — an in-place `LSet`/`LTrim` rewrite would fix
  both, at a higher risk of off-by-one bugs than the current shape.

## Change log

- Added with the RESP compatibility change (`SET` options, `LINSERT`/`LREM`, `SMOVE`/`SPOP`/
  `SRANDMEMBER`, the Redis scan family, `HGET`/`ZSCORE` reply types). The audit found and fixed:
  `SPOP` on `math/rand` (replication), an unbounded `SCAN COUNT` and a `SRANDMEMBER` negative-count
  panic (resource), the `XHSCAN` inclusive flip (correctness), and a dropped `alltest` build tag that
  broke `make test`.
