# Replication semantics

This document describes exactly what IceFireDB-SQLProxy 1.0.0 replicates, how a peer applies
it, and how to tell when a peer has fallen behind. It is the reference behind the "what it
guarantees — and what it does not" section of the README.

## Message flow

```
client ──COM_QUERY/COM_STMT_EXECUTE──▶ proxy A ──execute──▶ MySQL A
                                          │
                                          └──publish(libp2p gossipsub, admin topic)──▶ proxy B ──apply──▶ MySQL B
```

1. The proxy classifies the statement lexically (`internal/mysql/statement.go`): comments and
   whitespace are stripped, multi-statement batches are split at top-level semicolons, and a
   leading `WITH` is resolved through its CTE list. The classification decides three things:
   whether a read-only tenant may run it, whether the statement is replicated, and whether an
   inbound message may be executed.
2. The statement runs on the local MySQL first. Nothing is published unless it succeeded.
3. What is published depends on the session state:
   - autocommit write → one `sql` message;
   - write inside `BEGIN` … `COMMIT` → buffered, published as one `txn` message at `COMMIT`;
   - write inside a transaction that ends in `ROLLBACK` → discarded.
4. A peer validates the message (size, sender, signature, role, statement class, duplicate ID)
   and applies it:
   - `sql` → executed on the connection bound to that sender, which keeps a transaction open if
     the statement opened one (legacy behaviour, for peers that talk raw SQL);
   - `txn` → applied on one connection inside an explicit `BEGIN`/`COMMIT`; any failure rolls the
     whole batch back and increments `sqlproxy_replication_apply_failed_total`.

## The message envelope

```json
{
  "v": 1,
  "id": "8f2c…",            // random 128-bit id, used for de-duplication
  "kind": "sql" | "txn",
  "role": "admin" | "readonly",
  "stmts": ["INSERT INTO t VALUES (1)"],
  "ts": 1735689600000000000,
  "mac": "…"                 // HMAC-SHA256 over v|id|ts|kind|role|stmts, hex
}
```

`mac` is present only when `p2p.authToken` is set. The HMAC covers the statement text, so a peer
cannot be tricked into executing a modified statement; the role is covered too, so a message
captured from one topic cannot be replayed into the other.

A payload that is not JSON is treated as a legacy (pre-1.0.0) bare SQL string. It is applied only
when no `authToken` is configured, and only if it classifies as a data write.

## Bounds

| Limit | Value | Why |
| --- | --- | --- |
| Message size | 1 MiB | A peer must not be able to make this node allocate unbounded memory |
| Statements per message | 1000 | Bounds the work one message can cause |
| De-duplication window | 4096 message IDs | Gossipsub can deliver a message twice; applying a write twice is worse |
| Publish queue | 256 messages | A slow topic must not stall a client's write |
| Idle peer transaction | 5 minutes | A peer that opens a transaction and never commits must not hold the connection and its locks forever |

Records are dropped, counted and logged rather than queued for later: this proxy has no durable
outbox, and pretending otherwise would hide the loss.

## Failure modes and what you see

| Failure | Effect | Signal |
| --- | --- | --- |
| Peer offline while a write is published | Write never reaches that peer | `applied` on the peer does not grow while `published` on the sender does |
| Publish queue full | Write stays local only | `sqlproxy_replication_publish_dropped_total` grows, error log names the message id |
| Peer cannot apply a statement (constraint, missing schema, …) | Message dropped, transaction rolled back | `sqlproxy_replication_apply_failed_total`, error log with the statement |
| Message unsigned or wrongly signed while `authToken` is set | Message refused | `sqlproxy_replication_rejected_total{reason="signature"}` |
| Sender not in `allowedPeers` | Message refused | `…{reason="peer"}` |
| Message carries a non-write statement | Message refused | `…{reason="illegal_statement"}` |
| Duplicate delivery | Second copy ignored | `…{reason="duplicate"}` |
| Local statement failed | Never published | Normal MySQL error to the client, no replication counter moves |

## Detecting divergence

There is no anti-entropy, so the only reliable check is comparing the two sides:

1. On the sender, read `sqlproxy_replication_published_total` over an interval.
2. On each peer, read `sqlproxy_replication_applied_total` over the same interval.
3. A sustained gap is lost writes. The error log names the message id and the statement for
   rejected and failed messages.

Recovery is out of band: take a consistent dump from a node you trust and restore it on the node
that fell behind, or replay the missing writes from your application's own log.

## Known semantic gaps

- Session state is per node. `SET` statements, temporary tables, and server variables do not
  replicate, so a statement whose result depends on them may differ on a peer.
- Argument inlining assumes the default SQL mode (backslash escapes enabled). Under
  `NO_BACKSLASH_ESCAPES` a replicated string literal can be misparsed.
- DDL causes an implicit commit in MySQL, so `BEGIN; CREATE TABLE …; INSERT …; COMMIT` is not one
  transaction on any node — and is replicated as a `txn` batch that behaves the same way.
- Conflict resolution does not exist. Two nodes updating the same row converge on whichever write
  is applied last, independently, per node.
- Mixed-version clusters (pre-1.0.0 with 1.0.0) are not supported once `p2p.authToken` is set.
