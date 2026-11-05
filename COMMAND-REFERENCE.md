# Command Reference · 命令参考

Complete reference for every command the IceFireDB engine registers: syntax, arguments, return
values, replies, limits and a production example. Commands are grouped by data type. English first;
中文见每节下半部分。

IceFireDB 引擎注册的全部命令参考：语法、参数、返回值、限额与生产示例。按数据类型分组；每节前半为英文，
后半为中文。

Related documents:

- [`COMPATIBILITY.md`](COMPATIBILITY.md) — where behaviour intentionally differs from Redis.
- [`COMMAND-AUDIT.md`](COMMAND_AUDIT.md) — how each command is audited (replication class, limits, tests).
- [`OPERATIONS.md`](OPERATIONS.md) — running, backing up and upgrading a deployment.

相关文档：`COMPATIBILITY.md`（与 Redis 的偏差）、`COMMAND-AUDIT.md`（每条命令的审计口径）、
`OPERATIONS.md`（部署与运维）。

---

## How to read this document · 如何阅读

| Notation | Meaning | 记法 | 含义 |
| --- | --- | --- | --- |
| `KEY` | Binary-safe key name, up to 1 MiB | `KEY` | 二进制安全的键名，最大 1 MiB |
| `VALUE` | Binary-safe value, up to 64 MiB on the local backends | `VALUE` | 二进制安全的值，本地后端上限 64 MiB |
| `[ARG]` | Optional argument | `[ARG]` | 可选参数 |
| `ARG...` | One or more repetitions | `ARG...` | 一个或多个 |
| `A \| B` | Alternative | `A \| B` | 二选一 |
| **R** / **W** | Read command (runs locally) / Write command (replicated through the Raft log) | **R** / **W** | 读命令（本地执行）/ 写命令（经 Raft 日志复制） |
| `integer` | RESP integer reply, `:1` | `integer` | RESP 整数回复 `:1` |
| `bulk` | RESP bulk string, `$3 foo` | `bulk` | RESP bulk string |
| `null` | RESP null bulk, `$-1` — "no such value" | `null` | RESP 空批量 `$-1`，表示"不存在" |
| `array` | RESP array, `*2 ...` | `array` | RESP 数组 |
| `+OK` | RESP simple string | `+OK` | RESP 简单字符串 |

### Rules that apply to every command · 全局约定

**English**

- **One keyspace per data type.** A key name can hold a string *and* a hash *and* a list at the same
  time, and a command for the wrong type operates on that type's own (possibly empty) view instead of
  answering `WRONGTYPE`. See `COMPATIBILITY.md`.
- **Expiration is per type.** `EXPIRE`/`TTL`/`PERSIST` apply to the string at that key;
  `HEXPIRE`/`HTTL`/`HPERSIST` to the hash, `LEXPIRE`/`LTTL`, `SEXPIRE`/`STTL`, and the sorted set has
  no TTL command of its own (use `ZCLEAR` and re-create). All TTL values are whole seconds; `-1`
  means "no expiration" and is also what an expired key reports.
- **Scores are 64-bit integers.** `ZADD`/`ZINCRBY`/`ZSCORE` use `int64`, not floating point.
- **Counts and paging are bounded** — see [Limits](#limits--限额).
- **Write commands are replayed on every replica**, so their result must be identical cluster-wide;
  this is why e.g. `SPOP` picks deterministically. Nothing in the command surface exposes that.
- Error replies are RESP errors: `-ERR ...`, `-WRONGTYPE ...` is never produced (per-type keyspaces),
  `-MOVED`/`-TRY` may be returned by a follower that is not allowed to serve the read.

**中文**

- **每种数据类型一个独立键空间。** 同名 key 可以同时是字符串、哈希、列表；对类型不符的命令会在该类型
  自己的（可能为空的）视图上执行，而不是返回 `WRONGTYPE`。详见 `COMPATIBILITY.md`。
- **过期时间是按类型分开的。** `EXPIRE`/`TTL`/`PERSIST` 作用于该键的字符串；哈希用
  `HEXPIRE`/`HTTL`/`HPERSIST`，列表用 `LEXPIRE`/`LTTL`，集合用 `SEXPIRE`/`STTL`；有序集合没有
  独立 TTL 命令（用 `ZCLEAR` 清掉重建）。所有 TTL 为整秒，`-1` 表示"无过期"，已过期的键也报 `-1`。
- **分值是 64 位整数**（`int64`），不是浮点数。
- **数量与分页都有上限**，见[限额](#limits--限额)。
- **写命令会在每个副本上重放**，结果必须全集群一致（例如 `SPOP` 的抽取是确定性的）；命令面不暴露这些细节。
- 错误以 RESP 错误返回：`-ERR ...`；不会出现 `-WRONGTYPE`（因为按类型分键空间）；从节点不允许读时可能返回
  `-MOVED` / `-TRY`。

---

## Strings · 字符串

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `SET` | W | `SET KEY VALUE [EX seconds] [PX milliseconds] [NX] [XX] [KEEPTTL]` | `+OK`, or `null` when `NX`/`XX` blocks the write | `SET user:1 '{"n":"ada"}' EX 60` → `OK` |
| `SETNX` | W | `SETNX KEY VALUE` | `integer` 1 written, 0 already exists | `SETNX job:42 1` → `1` |
| `SETEX` | W | `SETEX KEY seconds VALUE` | `+OK` | `SETEX session:9 60 tok` → `OK` |
| `SETEXAT` | W | `SETEXAT KEY unixTime VALUE` | `+OK` | `SETEXAT session:9 1767225600 tok` → `OK` |
| `GETSET` | W | `GETSET KEY VALUE` | `bulk` old value, `null` if absent | `GETSET counter 0` → `"7"` |
| `GET` | R | `GET KEY` | `bulk`, `null` if absent | `GET user:1` → `{"n":"ada"}` |
| `MGET` | R | `MGET KEY [KEY ...]` | `array` of bulk/null | `MGET a b` → `[val, null]` |
| `MSET` | W | `MSET KEY VALUE [KEY VALUE ...]` | `+OK` | `MSET a 1 b 2` → `OK` |
| `APPEND` | W | `APPEND KEY VALUE` | `integer` new length | `APPEND log "x\n"` → `5` |
| `STRLEN` | R | `STRLEN KEY` | `integer` length in bytes | `STRLEN user:1` → `13` |
| `INCR` | W | `INCR KEY` | `integer` new value | `INCR hits` → `1` |
| `INCRBY` | W | `INCRBY KEY increment` | `integer` new value | `INCRBY hits 5` → `6` |
| `DECR` | W | `DECR KEY` | `integer` new value | `DECR hits` → `5` |
| `DECRBY` | W | `DECRBY KEY decrement` | `integer` new value | `DECRBY hits 5` → `0` |
| `GETRANGE` | R | `GETRANGE KEY start end` | `bulk` substring (inclusive, negative offsets from the end) | `GETRANGE user:1 0 3` → `{"n"` |
| `SETRANGE` | W | `SETRANGE KEY offset VALUE` | `integer` new length | `SETRANGE user:1 0 '{"m"'` → `13` |
| `GETBIT` | R | `GETBIT KEY offset` | `integer` 0/1 | `GETBIT bitmap 7` → `0` |
| `SETBIT` | W | `SETBIT KEY offset value` | `integer` previous bit | `SETBIT bitmap 7 1` → `0` |
| `BITCOUNT` | R | `BITCOUNT KEY [start end [BYTE\|BIT]]` | `integer` number of set bits | `BITCOUNT bitmap 0 0` → `1` |
| `BITOP` | W | `BITOP AND\|OR\|XOR\|NOT destKey KEY [KEY ...]` | `integer` length of the result | `BITOP AND out a b` → `2` |
| `BITPOS` | R | `BITPOS KEY bit [start [end [BYTE\|BIT]]]` | `integer` position, `-1` if not found | `BITPOS bitmap 1` → `7` |
| `DEL` | W | `DEL KEY [KEY ...]` | `integer` keys removed | `DEL a b` → `2` |
| `EXISTS` | R | `EXISTS KEY [KEY ...]` | `integer` number of existing keys | `EXISTS a b` → `1` |
| `EXPIRE` | W | `EXPIRE KEY seconds` | `integer` 1 set, 0 key absent | `EXPIRE session:9 60` → `1` |
| `EXPIREAT` | W | `EXPIREAT KEY unixTime` | `integer` 1 set, 0 key absent | `EXPIREAT session:9 1767225600` → `1` |
| `TTL` | R | `TTL KEY` | `integer` seconds left, `-1` no expiration | `TTL session:9` → `59` |

**Notes.** `SET` clears an existing expiration unless `KEEPTTL` is given; `PX` is rounded **up** to the
next second because expirations are stored in whole seconds. There is no string `PERSIST` command in
this engine (the handler is commented out) — `EXPIRE` again, or `SET ... KEEPTTL`, is the way to keep a
value and change its lifetime. `BITCOUNT`/`BITPOS` accept the optional
`BYTE`/`BIT` unit as in Redis 7. `MGET` answers `null` per missing key.

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `SET` | 写 | `SET KEY VALUE [EX 秒] [PX 毫秒] [NX] [XX] [KEEPTTL]` | `+OK`；`NX`/`XX` 条件不满足时 `null` | `SET user:1 '{"n":"ada"}' EX 60` → `OK` |
| `SETNX` | 写 | `SETNX KEY VALUE` | `integer` 1 写入，0 已存在 | `SETNX job:42 1` → `1` |
| `SETEX` | 写 | `SETEX KEY 秒 VALUE` | `+OK` | `SETEX session:9 60 tok` → `OK` |
| `SETEXAT` | 写 | `SETEXAT KEY 时间戳 VALUE` | `+OK` | `SETEXAT session:9 1767225600 tok` → `OK` |
| `GETSET` | 写 | `GETSET KEY VALUE` | `bulk` 旧值，不存在为 `null` | `GETSET counter 0` → `"7"` |
| `GET` | 读 | `GET KEY` | `bulk`，不存在为 `null` | `GET user:1` → `{"n":"ada"}` |
| `MGET` | 读 | `MGET KEY [KEY ...]` | `array`，逐项为 bulk 或 `null` | `MGET a b` → `[val, null]` |
| `MSET` | 写 | `MSET KEY VALUE [KEY VALUE ...]` | `+OK` | `MSET a 1 b 2` → `OK` |
| `APPEND` | 写 | `APPEND KEY VALUE` | `integer` 新长度 | `APPEND log "x\n"` → `5` |
| `STRLEN` | 读 | `STRLEN KEY` | `integer` 字节长度 | `STRLEN user:1` → `13` |
| `INCR` | 写 | `INCR KEY` | `integer` 新值 | `INCR hits` → `1` |
| `INCRBY` | 写 | `INCRBY KEY 增量` | `integer` 新值 | `INCRBY hits 5` → `6` |
| `DECR` | 写 | `DECR KEY` | `integer` 新值 | `DECR hits` → `5` |
| `DECRBY` | 写 | `DECRBY KEY 减量` | `integer` 新值 | `DECRBY hits 5` → `0` |
| `GETRANGE` | 读 | `GETRANGE KEY start end` | `bulk` 子串（闭区间，负偏移从尾部计） | `GETRANGE user:1 0 3` → `{"n"` |
| `SETRANGE` | 写 | `SETRANGE KEY offset VALUE` | `integer` 新长度 | `SETRANGE user:1 0 '{"m"'` → `13` |
| `GETBIT` | 读 | `GETBIT KEY offset` | `integer` 0/1 | `GETBIT bitmap 7` → `0` |
| `SETBIT` | 写 | `SETBIT KEY offset 值` | `integer` 原比特 | `SETBIT bitmap 7 1` → `0` |
| `BITCOUNT` | 读 | `BITCOUNT KEY [start end [BYTE\|BIT]]` | `integer` 置位数 | `BITCOUNT bitmap 0 0` → `1` |
| `BITOP` | 写 | `BITOP AND\|OR\|XOR\|NOT 目标 KEY [KEY ...]` | `integer` 结果长度 | `BITOP AND out a b` → `2` |
| `BITPOS` | 读 | `BITPOS KEY bit [start [end [BYTE\|BIT]]]` | `integer` 位置，未找到为 `-1` | `BITPOS bitmap 1` → `7` |
| `DEL` | 写 | `DEL KEY [KEY ...]` | `integer` 删除数量 | `DEL a b` → `2` |
| `EXISTS` | 读 | `EXISTS KEY [KEY ...]` | `integer` 存在数量 | `EXISTS a b` → `1` |
| `EXPIRE` | 写 | `EXPIRE KEY 秒` | `integer` 1 设置成功，0 键不存在 | `EXPIRE session:9 60` → `1` |
| `EXPIREAT` | 写 | `EXPIREAT KEY 时间戳` | `integer` 1/0 | `EXPIREAT session:9 1767225600` → `1` |
| `TTL` | 读 | `TTL KEY` | `integer` 剩余秒数，无过期为 `-1` | `TTL session:9` → `59` |

**说明。** 普通 `SET` 会清掉已有的过期时间，`KEEPTTL` 才会保留；`PX` 会**向上取整**到秒（过期时间按整
秒存储）。本引擎**没有字符串的 `PERSIST`**（处理器存在但被注释掉了）——想保留值又改生命周期，请再次
`EXPIRE`，或用 `SET ... KEEPTTL`。`BITCOUNT`/`BITPOS` 支持 Redis 7 的 `BYTE`/`BIT` 单位参数。`MGET` 对不存在的键逐项返回 `null`。

---

## Hashes · 哈希

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `HSET` | W | `HSET KEY FIELD VALUE [FIELD VALUE ...]` | `integer` fields added | `HSET user:1 name ada age 36` → `2` |
| `HSETNX` | W | `HSETNX KEY FIELD VALUE` | `integer` 1 written, 0 field exists | `HSETNX user:1 name ada` → `0` |
| `HGET` | R | `HGET KEY FIELD` | `bulk`, `null` if the field is absent | `HGET user:1 name` → `ada` |
| `HMGET` | R | `HMGET KEY FIELD [FIELD ...]` | `array` of bulk/null | `HMGET user:1 name x` → `[ada, null]` |
| `HMSET` | W | `HMSET KEY FIELD VALUE [FIELD VALUE ...]` | `+OK` | `HMSET user:2 name bob` → `OK` |
| `HDEL` | W | `HDEL KEY FIELD [FIELD ...]` | `integer` fields removed | `HDEL user:1 age` → `1` |
| `HEXISTS` | R | `HEXISTS KEY FIELD` | `integer` 0/1 | `HEXISTS user:1 name` → `1` |
| `HGETALL` | R | `HGETALL KEY` | `array` field/value pairs | `HGETALL user:1` → `[name, ada]` |
| `HKEYS` | R | `HKEYS KEY` | `array` of fields | `HKEYS user:1` → `[name]` |
| `HVALS` | R | `HVALS KEY` | `array` of values | `HVALS user:1` → `[ada]` |
| `HLEN` | R | `HLEN KEY` | `integer` field count | `HLEN user:1` → `1` |
| `HSTRLEN` | R | `HSTRLEN KEY FIELD` | `integer` value length, 0 if absent | `HSTRLEN user:1 name` → `3` |
| `HINCRBY` | W | `HINCRBY KEY FIELD increment` | `integer` new value | `HINCRBY stats hits 1` → `1` |
| `HKEYEXISTS` | R | `HKEYEXISTS KEY` | `integer` 0/1 — does the hash exist | `HKEYEXISTS user:1` → `1` |
| `HCLEAR` | W | `HCLEAR KEY` | `integer` fields removed | `HCLEAR user:1` → `2` |
| `HMCLEAR` | W | `HMCLEAR KEY [KEY ...]` | `integer` hashes removed | `HMCLEAR a b` → `2` |
| `HEXPIRE` | W | `HEXPIRE KEY seconds` | `integer` 1 set, 0 key absent | `HEXPIRE user:1 300` → `1` |
| `HEXPIREAT` | W | `HEXPIREAT KEY unixTime` | `integer` 1/0 | `HEXPIREAT user:1 1767225600` → `1` |
| `HTTL` | R | `HTTL KEY` | `integer` seconds left, `-1` none | `HTTL user:1` → `299` |

**Notes.** `HCLEAR`/`HMCLEAR` delete the hash (like `DEL` for that type) and with it the hash's
expiration; `HDEL` removes individual fields. Hashes expose `HEXPIRE`/`HEXPIREAT`/`HTTL` but no
`HPERSIST` (the handler is not registered). `HGET` of an existing empty value answers an empty bulk
string on the local backends and `null` on the decentralized ones (`COMPATIBILITY.md`).

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `HSET` | 写 | `HSET KEY FIELD VALUE [FIELD VALUE ...]` | `integer` 新增字段数 | `HSET user:1 name ada age 36` → `2` |
| `HSETNX` | 写 | `HSETNX KEY FIELD VALUE` | `integer` 1 写入，0 字段已存在 | `HSETNX user:1 name ada` → `0` |
| `HGET` | 读 | `HGET KEY FIELD` | `bulk`，字段不存在为 `null` | `HGET user:1 name` → `ada` |
| `HMGET` | 读 | `HMGET KEY FIELD [FIELD ...]` | `array`，逐项 bulk 或 `null` | `HMGET user:1 name x` → `[ada, null]` |
| `HMSET` | 写 | `HMSET KEY FIELD VALUE [FIELD VALUE ...]` | `+OK` | `HMSET user:2 name bob` → `OK` |
| `HDEL` | 写 | `HDEL KEY FIELD [FIELD ...]` | `integer` 删除字段数 | `HDEL user:1 age` → `1` |
| `HEXISTS` | 读 | `HEXISTS KEY FIELD` | `integer` 0/1 | `HEXISTS user:1 name` → `1` |
| `HGETALL` | 读 | `HGETALL KEY` | `array` 字段/值交替 | `HGETALL user:1` → `[name, ada]` |
| `HKEYS` | 读 | `HKEYS KEY` | `array` 字段列表 | `HKEYS user:1` → `[name]` |
| `HVALS` | 读 | `HVALS KEY` | `array` 值列表 | `HVALS user:1` → `[ada]` |
| `HLEN` | 读 | `HLEN KEY` | `integer` 字段数 | `HLEN user:1` → `1` |
| `HSTRLEN` | 读 | `HSTRLEN KEY FIELD` | `integer` 值长度，缺失为 0 | `HSTRLEN user:1 name` → `3` |
| `HINCRBY` | 写 | `HINCRBY KEY FIELD 增量` | `integer` 新值 | `HINCRBY stats hits 1` → `1` |
| `HKEYEXISTS` | 读 | `HKEYEXISTS KEY` | `integer` 0/1，哈希是否存在 | `HKEYEXISTS user:1` → `1` |
| `HCLEAR` | 写 | `HCLEAR KEY` | `integer` 删除字段数 | `HCLEAR user:1` → `2` |
| `HMCLEAR` | 写 | `HMCLEAR KEY [KEY ...]` | `integer` 删除哈希数 | `HMCLEAR a b` → `2` |
| `HEXPIRE` | 写 | `HEXPIRE KEY 秒` | `integer` 1/0 | `HEXPIRE user:1 300` → `1` |
| `HEXPIREAT` | 写 | `HEXPIREAT KEY 时间戳` | `integer` 1/0 | `HEXPIREAT user:1 1767225600` → `1` |
| `HTTL` | 读 | `HTTL KEY` | `integer` 剩余秒，无过期为 `-1` | `HTTL user:1` → `299` |

**说明。** `HCLEAR`/`HMCLEAR` 是"整条哈希删除"（等价于该类型的 `DEL`），会连带删掉哈希的过期时间；`HDEL`
只删字段。哈希只有 `HEXPIRE`/`HEXPIREAT`/`HTTL`，**没有 `HPERSIST`**（处理器未注册）。`HGET` 命中"存在但值为空"的字段时，本地后端返回空 bulk，去中心化后端返回 `null`（见
`COMPATIBILITY.md`）。

---

## Lists · 列表

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `LPUSH` | W | `LPUSH KEY VALUE [VALUE ...]` | `integer` new length | `LPUSH q a b` → `2` |
| `RPUSH` | W | `RPUSH KEY VALUE [VALUE ...]` | `integer` new length | `RPUSH q c` → `3` |
| `LPOP` | W | `LPOP KEY` | `bulk` head element, `null` if empty | `LPOP q` → `b` |
| `RPOP` | W | `RPOP KEY` | `bulk` tail element, `null` if empty | `RPOP q` → `c` |
| `LLEN` | R | `LLEN KEY` | `integer` element count | `LLEN q` → `1` |
| `LINDEX` | R | `LINDEX KEY index` | `bulk`, `null` if out of range | `LINDEX q 0` → `a` |
| `LRANGE` | R | `LRANGE KEY start stop` | `array` (inclusive, negative offsets from the end) | `LRANGE q 0 -1` → `[a]` |
| `LSET` | W | `LSET KEY index VALUE` | `+OK` | `LSET q 0 z` → `OK` |
| `LTRIM` | W | `LTRIM KEY start stop` | `+OK` | `LTRIM q 0 0` → `OK` |
| `RPOPLPUSH` | W | `RPOPLPUSH source destination` | `bulk` moved element, `null` if source empty | `RPOPLPUSH q backup` → `z` |
| `LINSERT` | W | `LINSERT KEY BEFORE\|AFTER pivot VALUE` | `integer` new length; `0` pivot absent; `-1` key absent | `LINSERT q BEFORE z y` → `2` |
| `LREM` | W | `LREM KEY count VALUE` | `integer` elements removed | `LREM q 0 z` → `1` |
| `LKEYEXISTS` | R | `LKEYEXISTS KEY` | `integer` 0/1 — does the list exist | `LKEYEXISTS q` → `1` |
| `LCLEAR` | W | `LCLEAR KEY` | `integer` elements removed | `LCLEAR q` → `1` |
| `LMCLEAR` | W | `LMCLEAR KEY [KEY ...]` | `integer` lists removed | `LMCLEAR a b` → `2` |
| `LEXPIRE` | W | `LEXPIRE KEY seconds` | `integer` 1/0 | `LEXPIRE q 600` → `1` |
| `LEXPIREAT` | W | `LEXPIREAT KEY unixTime` | `integer` 1/0 | `LEXPIREAT q 1767225600` → `1` |
| `LTTL` | R | `LTTL KEY` | `integer` seconds left, `-1` none | `LTTL q` → `599` |

**Notes.** `LPUSH a b` produces `[b, a]` (each element is pushed to the head, Redis order). `LREM` with
`count > 0` removes from the head, `count < 0` from the tail, `count = 0` every occurrence. `LINSERT`
and `LREM` have no primitive in the storage layer, so they rebuild the whole list: they are **O(n)**,
they preserve the list's TTL, and a backend failure mid-rebuild leaves the list truncated — see
[`COMMAND-AUDIT.md`](COMMAND_AUDIT.md). Blocking commands (`BLPOP`) are deliberately not available.

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `LPUSH` | 写 | `LPUSH KEY VALUE [VALUE ...]` | `integer` 新长度 | `LPUSH q a b` → `2` |
| `RPUSH` | 写 | `RPUSH KEY VALUE [VALUE ...]` | `integer` 新长度 | `RPUSH q c` → `3` |
| `LPOP` | 写 | `LPOP KEY` | `bulk` 头部元素，空列表为 `null` | `LPOP q` → `b` |
| `RPOP` | 写 | `RPOP KEY` | `bulk` 尾部元素，空列表为 `null` | `RPOP q` → `c` |
| `LLEN` | 读 | `LLEN KEY` | `integer` 元素数量 | `LLEN q` → `1` |
| `LINDEX` | 读 | `LINDEX KEY 下标` | `bulk`，越界为 `null` | `LINDEX q 0` → `a` |
| `LRANGE` | 读 | `LRANGE KEY start stop` | `array`（闭区间，负下标从尾部计） | `LRANGE q 0 -1` → `[a]` |
| `LSET` | 写 | `LSET KEY 下标 VALUE` | `+OK` | `LSET q 0 z` → `OK` |
| `LTRIM` | 写 | `LTRIM KEY start stop` | `+OK` | `LTRIM q 0 0` → `OK` |
| `RPOPLPUSH` | 写 | `RPOPLPUSH 源 目标` | `bulk` 被移动的元素，源为空为 `null` | `RPOPLPUSH q backup` → `z` |
| `LINSERT` | 写 | `LINSERT KEY BEFORE\|AFTER 基准值 VALUE` | `integer` 新长度；`0` 基准值不存在；`-1` 键不存在 | `LINSERT q BEFORE z y` → `2` |
| `LREM` | 写 | `LREM KEY count VALUE` | `integer` 删除数量 | `LREM q 0 z` → `1` |
| `LKEYEXISTS` | 读 | `LKEYEXISTS KEY` | `integer` 0/1，列表是否存在 | `LKEYEXISTS q` → `1` |
| `LCLEAR` | 写 | `LCLEAR KEY` | `integer` 删除元素数 | `LCLEAR q` → `1` |
| `LMCLEAR` | 写 | `LMCLEAR KEY [KEY ...]` | `integer` 删除列表数 | `LMCLEAR a b` → `2` |
| `LEXPIRE` | 写 | `LEXPIRE KEY 秒` | `integer` 1/0 | `LEXPIRE q 600` → `1` |
| `LEXPIREAT` | 写 | `LEXPIREAT KEY 时间戳` | `integer` 1/0 | `LEXPIREAT q 1767225600` → `1` |
| `LTTL` | 读 | `LTTL KEY` | `integer` 剩余秒，无过期为 `-1` | `LTTL q` → `599` |

**说明。** `LPUSH a b` 得到 `[b, a]`（逐个压入头部，与 Redis 一致）。`LREM` 的 `count > 0` 从头部删、
`count < 0` 从尾部删、`count = 0` 删全部。`LINSERT` / `LREM` 存储层没有原语，会**重建整个列表**：复杂度
**O(n)**，会保留列表的 TTL，但重建过程中后端失败会留下被截断的列表（见 `COMMAND_AUDIT.md`）。阻塞命令
（`BLPOP`）**不提供**。

---

## Sets · 集合

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `SADD` | W | `SADD KEY MEMBER [MEMBER ...]` | `integer` members added | `SADD tags go db` → `2` |
| `SREM` | W | `SREM KEY MEMBER [MEMBER ...]` | `integer` members removed | `SREM tags db` → `1` |
| `SCARD` | R | `SCARD KEY` | `integer` cardinality | `SCARD tags` → `1` |
| `SISMEMBER` | R | `SISMEMBER KEY MEMBER` | `integer` 0/1 | `SISMEMBER tags go` → `1` |
| `SMEMBERS` | R | `SMEMBERS KEY` | `array` of members | `SMEMBERS tags` → `[go]` |
| `SDIFF` | R | `SDIFF KEY [KEY ...]` | `array` members in the first but no later set | `SDIFF a b` → `[x]` |
| `SDIFFSTORE` | W | `SDIFFSTORE dest KEY [KEY ...]` | `integer` size of the result | `SDIFFSTORE out a b` → `1` |
| `SINTER` | R | `SINTER KEY [KEY ...]` | `array` members in every set | `SINTER a b` → `[y]` |
| `SINTERSTORE` | W | `SINTERSTORE dest KEY [KEY ...]` | `integer` size of the result | `SINTERSTORE out a b` → `1` |
| `SUNION` | R | `SUNION KEY [KEY ...]` | `array` members in any set | `SUNION a b` → `[x,y]` |
| `SUNIONSTORE` | W | `SUNIONSTORE dest KEY [KEY ...]` | `integer` size of the result | `SUNIONSTORE out a b` → `2` |
| `SMOVE` | W | `SMOVE source destination MEMBER` | `integer` 1 moved, 0 not a member | `SMOVE a b x` → `1` |
| `SPOP` | W | `SPOP KEY [count]` | bare: `bulk` member; with `count`: `array` | `SPOP queue:ready` → `job:7` |
| `SRANDMEMBER` | R | `SRANDMEMBER KEY [count]` | bare: `bulk`; positive count: distinct `array`; negative: `array` with repeats | `SRANDMEMBER tags 1` → `[go]` |
| `SKEYEXISTS` | R | `SKEYEXISTS KEY` | `integer` 0/1 — does the set exist | `SKEYEXISTS tags` → `1` |
| `SCLEAR` | W | `SCLEAR KEY` | `integer` members removed | `SCLEAR tags` → `1` |
| `SMCLEAR` | W | `SMCLEAR KEY [KEY ...]` | `integer` sets removed | `SMCLEAR a b` → `2` |
| `SEXPIRE` | W | `SEXPIRE KEY seconds` | `integer` 1/0 | `SEXPIRE tags 60` → `1` |
| `SEXPIREAT` | W | `SEXPIREAT KEY unixTime` | `integer` 1/0 | `SEXPIREAT tags 1767225600` → `1` |
| `STTL` | R | `STTL KEY` | `integer` seconds left, `-1` none | `STTL tags` → `59` |
| `SPERSIST` | W | `SPERSIST KEY` | `integer` 1 removed, 0 none | `SPERSIST tags` → `1` |

**Notes.** `SPOP` picks the member **deterministically across replicas** (the result is identical on
every node). With `count` it clamps to the set size, `count = 0` answers an empty array and a negative
`count` is an error. `SRANDMEMBER` accepts `|count| <= 1048576`; a negative count may repeat members.
`SMOVE` onto the same key is a no-op returning `1` and keeps the TTL. `SPERSIST` is known to leave the
expiration in place on the `ipfs` backend.

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `SADD` | 写 | `SADD KEY MEMBER [MEMBER ...]` | `integer` 新增成员数 | `SADD tags go db` → `2` |
| `SREM` | 写 | `SREM KEY MEMBER [MEMBER ...]` | `integer` 删除成员数 | `SREM tags db` → `1` |
| `SCARD` | 读 | `SCARD KEY` | `integer` 基数 | `SCARD tags` → `1` |
| `SISMEMBER` | 读 | `SISMEMBER KEY MEMBER` | `integer` 0/1 | `SISMEMBER tags go` → `1` |
| `SMEMBERS` | 读 | `SMEMBERS KEY` | `array` 成员列表 | `SMEMBERS tags` → `[go]` |
| `SDIFF` | 读 | `SDIFF KEY [KEY ...]` | `array` 在首个集合、不在后续集合 | `SDIFF a b` → `[x]` |
| `SDIFFSTORE` | 写 | `SDIFFSTORE 目标 KEY [KEY ...]` | `integer` 结果大小 | `SDIFFSTORE out a b` → `1` |
| `SINTER` | 读 | `SINTER KEY [KEY ...]` | `array` 交集 | `SINTER a b` → `[y]` |
| `SINTERSTORE` | 写 | `SINTERSTORE 目标 KEY [KEY ...]` | `integer` 结果大小 | `SINTERSTORE out a b` → `1` |
| `SUNION` | 读 | `SUNION KEY [KEY ...]` | `array` 并集 | `SUNION a b` → `[x,y]` |
| `SUNIONSTORE` | 写 | `SUNIONSTORE 目标 KEY [KEY ...]` | `integer` 结果大小 | `SUNIONSTORE out a b` → `2` |
| `SMOVE` | 写 | `SMOVE 源 目标 MEMBER` | `integer` 1 移动成功，0 不是成员 | `SMOVE a b x` → `1` |
| `SPOP` | 写 | `SPOP KEY [count]` | 无 count：`bulk` 成员；有 count：`array` | `SPOP queue:ready` → `job:7` |
| `SRANDMEMBER` | 读 | `SRANDMEMBER KEY [count]` | 无 count：`bulk`；正数去重 `array`；负数可重复 | `SRANDMEMBER tags 1` → `[go]` |
| `SKEYEXISTS` | 读 | `SKEYEXISTS KEY` | `integer` 0/1，集合是否存在 | `SKEYEXISTS tags` → `1` |
| `SCLEAR` | 写 | `SCLEAR KEY` | `integer` 删除成员数 | `SCLEAR tags` → `1` |
| `SMCLEAR` | 写 | `SMCLEAR KEY [KEY ...]` | `integer` 删除集合数 | `SMCLEAR a b` → `2` |
| `SEXPIRE` | 写 | `SEXPIRE KEY 秒` | `integer` 1/0 | `SEXPIRE tags 60` → `1` |
| `SEXPIREAT` | 写 | `SEXPIREAT KEY 时间戳` | `integer` 1/0 | `SEXPIREAT tags 1767225600` → `1` |
| `STTL` | 读 | `STTL KEY` | `integer` 剩余秒，无过期为 `-1` | `STTL tags` → `59` |
| `SPERSIST` | 写 | `SPERSIST KEY` | `integer` 1 移除，0 本来没有 | `SPERSIST tags` → `1` |

**说明。** `SPOP` 抽取是**跨副本确定性**的（每个节点结果一致）。带 `count` 时会夹到集合大小，`count = 0`
返回空数组，负数报错。`SRANDMEMBER` 限制 `|count| <= 1048576`，负数允许重复。`SMOVE` 移到同一个键是
no-op 且返回 `1`，并保留 TTL。`SPERSIST` 在 `ipfs` 后端已知会留下过期时间。

---

## Sorted sets · 有序集合

Scores are **64-bit integers**, not doubles.

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `ZADD` | W | `ZADD KEY score MEMBER [score MEMBER ...]` | `integer` members added | `ZADD board 100 ada 90 bob` → `2` |
| `ZINCRBY` | R ⚠ | `ZINCRBY KEY increment MEMBER` | `bulk` new score | `ZINCRBY board 5 ada` → `105` |
| `ZREM` | W | `ZREM KEY MEMBER [MEMBER ...]` | `integer` members removed | `ZREM board bob` → `1` |
| `ZCARD` | R | `ZCARD KEY` | `integer` cardinality | `ZCARD board` → `1` |
| `ZCOUNT` | R | `ZCOUNT KEY min max` | `integer` members within the score range | `ZCOUNT board 0 200` → `1` |
| `ZSCORE` | R | `ZSCORE KEY MEMBER` | `bulk` score, `null` if absent | `ZSCORE board ada` → `105` |
| `ZRANK` | R | `ZRANK KEY MEMBER` | `integer` rank (0-based), `null` if absent | `ZRANK board ada` → `0` |
| `ZREVRANK` | R | `ZREVRANK KEY MEMBER` | `integer` reverse rank, `null` if absent | `ZREVRANK board ada` → `0` |
| `ZRANGE` | R | `ZRANGE KEY start stop [WITHSCORES]` | `array` members (and scores) | `ZRANGE board 0 -1 WITHSCORES` → `[ada, 105]` |
| `ZREVRANGE` | R | `ZREVRANGE KEY start stop [WITHSCORES]` | `array` members (and scores) | `ZREVRANGE board 0 0` → `[ada]` |
| `ZRANGEBYSCORE` | R | `ZRANGEBYSCORE KEY min max [WITHSCORES]` | `array` members (and scores) | `ZRANGEBYSCORE board 100 200` → `[ada]` |
| `ZREVRANGEBYSCORE` | R | `ZREVRANGEBYSCORE KEY max min [WITHSCORES]` | `array` members (and scores) | `ZREVRANGEBYSCORE board 200 100` → `[ada]` |
| `ZREMRANGEBYRANK` | R ⚠ | `ZREMRANGEBYRANK KEY start stop` | `integer` members removed | `ZREMRANGEBYRANK board 0 0` → `1` |
| `ZREMRANGEBYSCORE` | R ⚠ | `ZREMRANGEBYSCORE KEY min max` | `integer` members removed | `ZREMRANGEBYSCORE board 0 50` → `0` |
| `ZCLEAR` | W | `ZCLEAR KEY` | `integer` members removed | `ZCLEAR board` → `1` |

**Notes.** There is **no TTL command for sorted sets** — clear and recreate instead. ⚠ `ZINCRBY`,
`ZREMRANGEBYRANK` and `ZREMRANGEBYSCORE` are currently registered as **read** commands, so they mutate
the local state without going through the Raft log: on a cluster they neither replicate nor survive a
leader change. Use `ZADD` with a computed score, or delete through a write command, until that is fixed.

**中文**

分值类型是 **64 位整数**，不是浮点。

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `ZADD` | 写 | `ZADD KEY score MEMBER [score MEMBER ...]` | `integer` 新增成员数 | `ZADD board 100 ada 90 bob` → `2` |
| `ZINCRBY` | 读 ⚠ | `ZINCRBY KEY 增量 MEMBER` | `bulk` 新分值 | `ZINCRBY board 5 ada` → `105` |
| `ZREM` | 写 | `ZREM KEY MEMBER [MEMBER ...]` | `integer` 删除成员数 | `ZREM board bob` → `1` |
| `ZCARD` | 读 | `ZCARD KEY` | `integer` 基数 | `ZCARD board` → `1` |
| `ZCOUNT` | 读 | `ZCOUNT KEY min max` | `integer` 分值区间内成员数 | `ZCOUNT board 0 200` → `1` |
| `ZSCORE` | 读 | `ZSCORE KEY MEMBER` | `bulk` 分值，缺失为 `null` | `ZSCORE board ada` → `105` |
| `ZRANK` | 读 | `ZRANK KEY MEMBER` | `integer` 升序排名（0 起），缺失为 `null` | `ZRANK board ada` → `0` |
| `ZREVRANK` | 读 | `ZREVRANK KEY MEMBER` | `integer` 降序排名，缺失为 `null` | `ZREVRANK board ada` → `0` |
| `ZRANGE` | 读 | `ZRANGE KEY start stop [WITHSCORES]` | `array` 成员（及分值） | `ZRANGE board 0 -1 WITHSCORES` → `[ada, 105]` |
| `ZREVRANGE` | 读 | `ZREVRANGE KEY start stop [WITHSCORES]` | `array` 成员（及分值） | `ZREVRANGE board 0 0` → `[ada]` |
| `ZRANGEBYSCORE` | 读 | `ZRANGEBYSCORE KEY min max [WITHSCORES]` | `array` 成员（及分值） | `ZRANGEBYSCORE board 100 200` → `[ada]` |
| `ZREVRANGEBYSCORE` | 读 | `ZREVRANGEBYSCORE KEY max min [WITHSCORES]` | `array` 成员（及分值） | `ZREVRANGEBYSCORE board 200 100` → `[ada]` |
| `ZREMRANGEBYRANK` | 读 ⚠ | `ZREMRANGEBYRANK KEY start stop` | `integer` 删除成员数 | `ZREMRANGEBYRANK board 0 0` → `1` |
| `ZREMRANGEBYSCORE` | 读 ⚠ | `ZREMRANGEBYSCORE KEY min max` | `integer` 删除成员数 | `ZREMRANGEBYSCORE board 0 50` → `0` |
| `ZCLEAR` | 写 | `ZCLEAR KEY` | `integer` 删除成员数 | `ZCLEAR board` → `1` |

**说明。** 有序集合**没有独立的 TTL 命令**，需要清理后重建。⚠ `ZINCRBY`、`ZREMRANGEBYRANK`、
`ZREMRANGEBYSCORE` 目前被注册为**读命令**，修改的是本地状态、不经过 Raft 日志：在集群里既不复制，也不
能在 leader 切换后存活。修复前请用 `ZADD` 写入算好的分值，或改用写命令删除。

---

## Scan · 扫描

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `SCAN` | R | `SCAN cursor [MATCH glob] [COUNT n] [TYPE type]` | `array` `[cursor, keys]`; cursor `0` ends the walk | `SCAN 0 MATCH user:* COUNT 100` → `[cursor, [user:1, user:2]]` |
| `HSCAN` | R | `HSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [field, value, ...]]` | `HSCAN user:1 0 COUNT 10` → `[0, [name, ada]]` |
| `SSCAN` | R | `SSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [member, ...]]` | `SSCAN tags 0` → `[0, [go]]` |
| `ZSCAN` | R | `ZSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [member, score, ...]]` | `ZSCAN board 0` → `[0, [ada, 105]]` |
| `XSCAN` | R | `XSCAN type cursor [MATCH regex] [COUNT n] [ASC\|DESC]` | `array` `[cursor, keys]`; empty cursor ends the walk | `XSCAN KV "" COUNT 5` → `[key:5, [..]]` |
| `XHSCAN` | R | `XHSCAN KEY cursor [MATCH regex] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [field, value, ...]]` | `XHSCAN user:1 "" COUNT 5` → `[name, [name, ada]]` |
| `XSSCAN` | R | `XSSCAN KEY cursor [MATCH regex] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [member, ...]]` | `XSSCAN tags ""` → `[go, [go]]` |
| `XZSCAN` | R | `XZSCAN KEY cursor [MATCH regex] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [member, score, ...]]` | `XZSCAN board ""` → `[ada, [ada, 105]]` |

**Notes.** Two families, two contracts:

- The **Redis-family** (`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`) starts at cursor `0` and ends when it answers
  `0`. The cursor is **opaque, not a number**: `HSCAN`/`SSCAN`/`ZSCAN` answer the last field or member
  name they returned, so a field literally named `0` collides with the end marker, and client helpers
  that parse the cursor as an integer (for example go-redis' typed `HScan`) are not supported — use the
  generic command API. `MATCH` takes a **Redis glob**. `SCAN` without `TYPE` walks all five keyspaces
  through one composite cursor.
- The **X-family** (`XSCAN`/`XHSCAN`/`XSSCAN`/`XZSCAN`) starts at an **empty cursor** and ends when it
  echoes an empty cursor. `MATCH` takes a **regular expression**, and `ASC`/`DESC` choose the direction.
- `COUNT` is a hint in both families and is capped at 10000. A page may come back **empty while the
  cursor still advances** — never treat a short or empty page as the end of the walk; loop until the
  cursor is `0` (Redis family) or empty (X family).
- `TYPE` accepts `string`, `hash`, `list`, `set`, `zset`; unknown types answer an empty result.

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `SCAN` | 读 | `SCAN cursor [MATCH glob] [COUNT n] [TYPE 类型]` | `array` `[cursor, keys]`，游标为 `0` 表示结束 | `SCAN 0 MATCH user:* COUNT 100` → `[cursor, [user:1, user:2]]` |
| `HSCAN` | 读 | `HSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [field, value, ...]]` | `HSCAN user:1 0 COUNT 10` → `[0, [name, ada]]` |
| `SSCAN` | 读 | `SSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [member, ...]]` | `SSCAN tags 0` → `[0, [go]]` |
| `ZSCAN` | 读 | `ZSCAN KEY cursor [MATCH glob] [COUNT n]` | `array` `[cursor, [member, score, ...]]` | `ZSCAN board 0` → `[0, [ada, 105]]` |
| `XSCAN` | 读 | `XSCAN 类型 cursor [MATCH 正则] [COUNT n] [ASC\|DESC]` | `array` `[cursor, keys]`，空游标表示结束 | `XSCAN KV "" COUNT 5` → `[key:5, [..]]` |
| `XHSCAN` | 读 | `XHSCAN KEY cursor [MATCH 正则] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [field, value, ...]]` | `XHSCAN user:1 "" COUNT 5` → `[name, [name, ada]]` |
| `XSSCAN` | 读 | `XSSCAN KEY cursor [MATCH 正则] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [member, ...]]` | `XSSCAN tags ""` → `[go, [go]]` |
| `XZSCAN` | 读 | `XZSCAN KEY cursor [MATCH 正则] [COUNT n] [ASC\|DESC]` | `array` `[cursor, [member, score, ...]]` | `XZSCAN board ""` → `[ada, [ada, 105]]` |

**说明。** 两族两套契约：

- **Redis 族**（`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`）从游标 `0` 开始，返回 `0` 结束。游标是**不透明的、不是
  数字**：`HSCAN`/`SSCAN`/`ZSCAN` 返回的是最后一个字段名/成员名，因此名字恰好是 `0` 的字段会与结束标记
  冲突；把游标当整数解析的客户端助手（例如 go-redis 的类型化 `HScan`）**不受支持**，请用通用命令接口。
  `MATCH` 是 **Redis glob**。不带 `TYPE` 的 `SCAN` 会用一个复合游标走遍五种键空间。
- **X 族**（`XSCAN`/`XHSCAN`/`XSSCAN`/`XZSCAN`）从**空游标**开始，回显空游标结束；`MATCH` 是**正则**，
  `ASC`/`DESC` 控制方向。
- 两族的 `COUNT` 都只是提示，上限 10000。**一页可能返回空结果但游标仍然前进** —— 不要把"短页/空页"当成
  结束，要循环到游标为 `0`（Redis 族）或为空（X 族）。
- `TYPE` 支持 `string`、`hash`、`list`、`set`、`zset`；未知类型返回空结果。

---

## Server · 服务器

**English**

| Cmd | RW | Syntax | Returns | Example → reply |
| --- | --- | --- | --- | --- |
| `INFO` | R | `INFO` | `bulk` multi-line report (server, store, memory, gc, per-backend metrics) | `INFO` → `# Server\r\nversion:1.0.0-rc.1\r\n...` |
| `FLUSHALL` | W | `FLUSHALL` | `integer` keys removed | `FLUSHALL` → `OK` |
| `FLUSHDB` | W | `FLUSHDB` | `integer` keys removed — alias of `FLUSHALL`, this engine has one logical database | `FLUSHDB` → `OK` |

**Notes.** `INFO` is a read command and is served by the node that receives it. `FLUSHALL` goes through
the Raft log. There is a single logical database; `SELECT` is not implemented.

**中文**

| 命令 | 读写 | 语法 | 返回 | 示例 → 回复 |
| --- | --- | --- | --- | --- |
| `INFO` | 读 | `INFO` | `bulk` 多行报告（server、store、内存、GC、后端指标） | `INFO` → `# Server\r\nversion:1.0.0-rc.1\r\n...` |
| `FLUSHALL` | 写 | `FLUSHALL` | `integer` 删除键数 | `FLUSHALL` → `OK` |
| `FLUSHDB` | 写 | `FLUSHDB` | `integer` 删除键数 —— `FLUSHALL` 的别名，本引擎只有一个逻辑库 | `FLUSHDB` → `OK` |

**说明。** `INFO` 是读命令，由接收请求的节点直接返回。`FLUSHALL` 走 Raft 日志。引擎只有一个逻辑数据库，
未实现 `SELECT`。

---

## Recipes · 典型用法

**English**

```text
# 1. Cache with expiry — write, read, inspect, extend
SET cache:user:1 '{"n":"ada"}' EX 300      -> OK
GET cache:user:1                            -> {"n":"ada"}
TTL cache:user:1                            -> 299
EXPIRE cache:user:1 600                     -> 1      (extend)

# 2. Conditional write / deduplication — exactly one writer wins
SETNX job:42:done 1                         -> 1      (first caller)
SETNX job:42:done 1                         -> 0      (everyone else, no error)
SET job:42:claim me NX EX 30                -> OK     (or null when already claimed)

# 3. Distributed lock — the token is the caller's identity
SET lock:report <token> NX PX 5000           -> OK / null
# release only if you still own it: there is no compare-and-delete, so read the
# token and delete only when it matches your own (a small race remains) — or
# give the lock a short TTL and tolerate late release.
GET lock:report                             -> <token>
DEL lock:report                             -> 1

# 4. Paginate a keyspace (Redis family: cursor "0" starts and ends)
SCAN 0 MATCH user:* COUNT 100               -> [ "<opaque>", [user:1, ...] ]
SCAN "<opaque>" MATCH user:* COUNT 100      -> [ "0", [user:99] ]   # done

# 5. Native scan (X family: empty cursor starts and ends, MATCH is a regex)
XSCAN HASH "" MATCH ^user: COUNT 500        -> [ user:1042, [ ... ] ]
XSCAN HASH user:1042 MATCH ^user: COUNT 500 -> [ "", [ ... ] ]      # done

# 6. Bounded work queue (avoid LINSERT/LREM on long lists — they are O(n))
RPUSH queue:pending job:1                   -> 1
LPOP queue:pending                          -> job:1
LTRIM queue:log 0 999                       -> OK     (keep the last 1000)

# 7. Hit counter and top-N leaderboard (in a single node)
HINCRBY stats:daily 2026-11-07 1            -> 1
ZADD board 100 ada                          -> 1
ZREVRANGE board 0 9 WITHSCORES              -> [ada, 100]
# clusters: ZINCRBY is not replicated yet — write scores with ZADD instead.
```

**中文**

```text
# 1. 带过期的缓存：写入、读取、查看、续期
SET cache:user:1 '{"n":"ada"}' EX 300      -> OK
GET cache:user:1                            -> {"n":"ada"}
TTL cache:user:1                            -> 299
EXPIRE cache:user:1 600                     -> 1      （续期）

# 2. 条件写 / 去重：只有一个写入者成功
SETNX job:42:done 1                         -> 1      （第一个调用者）
SETNX job:42:done 1                         -> 0      （其他人，不报错）
SET job:42:claim me NX EX 30                -> OK     （已被占用时返回 null）

# 3. 分布式锁：token 是调用方的身份
SET lock:report <token> NX PX 5000           -> OK / null
# 本引擎没有 compare-and-delete：只能先读 token 再判断后删除（仍有小竞态），
# 或者给锁一个较短的 TTL，容忍"晚释放"。

# 4. 遍历键空间（Redis 族：游标 "0" 既是开始也是结束）
SCAN 0 MATCH user:* COUNT 100               -> [ "<不透明>", [user:1, ...] ]
SCAN "<不透明>" MATCH user:* COUNT 100      -> [ "0", [user:99] ]   # 结束

# 5. 原生扫描（X 族：空游标开始与结束，MATCH 是正则）
XSCAN HASH "" MATCH ^user: COUNT 500        -> [ user:1042, [ ... ] ]
XSCAN HASH user:1042 MATCH ^user: COUNT 500 -> [ "", [ ... ] ]      # 结束

# 6. 有界工作队列（长列表上避免 LINSERT/LREM —— 它们是 O(n)）
RPUSH queue:pending job:1                   -> 1
LPOP queue:pending                          -> job:1
LTRIM queue:log 0 999                       -> OK     （只保留最近 1000 条）

# 7. 计数器与 Top-N 榜单（单节点）
HINCRBY stats:daily 2026-11-07 1            -> 1
ZADD board 100 ada                          -> 1
ZREVRANGE board 0 9 WITHSCORES              -> [ada, 100]
# 集群下 ZINCRBY 尚未复制 —— 请改用 ZADD 写入算好的分值。
```

---

## Limits · 限额

| Limit | Value | 限制项 | 取值 |
| --- | --- | --- | --- |
| Key size | 1 MiB | 键大小 | 1 MiB |
| Value size | 64 MiB on `goleveldb`/`hybriddb`/`badger`; backend-dependent elsewhere | 值大小 | `goleveldb`/`hybriddb`/`badger` 为 64 MiB，其它后端视驱动而定 |
| `COUNT` on any scan | 10000 (clamped, `COUNT` is a hint) | 任意 scan 的 `COUNT` | 10000（超出即夹到上限，`COUNT` 只是提示） |
| `SRANDMEMBER` / `SPOP` | absolute count <= 1048576; `SPOP` clamps to the set size | `SRANDMEMBER` / `SPOP` | 绝对计数 <= 1048576；`SPOP` 夹到集合大小 |
| Expiry | whole seconds; `PX` rounds up; unrepresentable `EX` is refused | 过期时间 | 整秒；`PX` 向上取整；无法表示的 `EX` 直接报错 |
| Databases | 1 (`SELECT` unimplemented) | 数据库数量 | 1（未实现 `SELECT`） |
| Blocking commands | not available (`BLPOP`/`BRPOP` unregistered) | 阻塞命令 | 不可用（`BLPOP`/`BRPOP` 未注册） |

## Divergences from Redis · 与 Redis 的偏差

Full list and rationale in [`COMPATIBILITY.md`](COMPATIBILITY.md). The ones a client will hit first:

1. No `WRONGTYPE` — one keyspace per type, so a wrong-type command reads that type's own view.
   （没有 `WRONGTYPE`：每种类型一个键空间）
2. Expiration is per type (`HEXPIRE`, `LEXPIRE`, `SEXPIRE`, `*TTL`, `*PERSIST`); sorted sets have none.
   （过期时间按类型分开；有序集合没有）
3. Scan cursors are opaque and, for the `HSCAN` family, names rather than integers.
   （扫描游标不透明，`HSCAN` 族是名字而非整数）
4. `SET ... PX` rounds up to whole seconds; `SET` reports a null bulk when `NX`/`XX` blocks the write.
   （`PX` 向上取整到秒；`NX`/`XX` 不满足时返回 null）
5. On the decentralized backends an existing empty value reads back as `null`.
   （去中心化后端上"存在但为空"的值读回为 `null`）
6. `ZINCRBY`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE` do not replicate in a cluster (registered as reads).
   （这三条在集群中不复制）

