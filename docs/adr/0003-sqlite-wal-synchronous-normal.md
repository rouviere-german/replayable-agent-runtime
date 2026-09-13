# 3. Single-node SQLite in WAL mode with synchronous=NORMAL by default

- Status: accepted
- Date: 2026-09-13

## Context

The journal needs atomic, serializable appends conditional on a lease (owner and fencing token), a
place for inbox and outbox rows in the same transaction, and durability across killed processes. The
specification targets a single node with an append p95 below 10 ms and offline operation.

Options considered:

- **PostgreSQL**: multi-node workers and point-in-time recovery, but an external service for every
  test, demo and benchmark, and the portfolio must run offline on a small laptop.
- **SQLite, rollback journal**: one writer and readers blocked during writes.
- **SQLite, WAL**: one writer at a time, readers never blocked, file-local. With `synchronous=FULL`
  each commit fsyncs the WAL; with `NORMAL` the WAL is synced at checkpoints, so a commit survives a
  killed process (the data is in the OS page cache) but the latest commits may be lost if the machine
  loses power (SQLite documentation, "Write-Ahead Logging" and "PRAGMA synchronous").

## Decision

SQLite in WAL mode through SQLAlchemy Core, schema managed by Alembic. Writers open transactions
with `BEGIN IMMEDIATE`, so read-check-write sequences (claims, fenced appends, approval decisions)
are serialized; readers use a separate engine with deferred transactions. `synchronous=NORMAL` is
the default and `FULL` is one setting away (`APP_JOURNAL_SYNCHRONOUS`). Both are benchmarked.

## Consequences

- Kill -9 safety, which is what the chaos suites test, holds in both modes. Power-loss safety for
  the latest commits needs `FULL`; with `NORMAL` a lost commit moves already-executed effects into the
  at-least-once window described in docs/SEMANTICS.md.
- `FULL` syncs the WAL on every commit. On the development machine it roughly doubled append latency
  (p50 1.65 ms and p95 3.05 ms versus 0.81 ms and 1.86 ms with `NORMAL`, BENCHMARKS.md) and still met
  the 10 ms p95 target. `NORMAL` stays the default because the target machines in this portfolio
  are laptops and CI runners whose fsync cost varies widely, and because kill -9 safety is the
  guarantee under test; deployments that must survive power loss should set `FULL`.
- One writer at a time bounds throughput; workers on one host share the file. Workers on several
  hosts, network filesystems (WAL needs shared memory) and multi-region replication are non-goals.
  Moving to PostgreSQL would keep the same transaction shapes (`SELECT ... FOR UPDATE` instead of
  `BEGIN IMMEDIATE`) but is not implemented.
