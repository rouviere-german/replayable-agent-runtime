# 6. Synchronous journal I/O on the event loop, heartbeat on its own thread

- Status: accepted
- Date: 2026-09-14

## Context

Journal appends are synchronous SQLite transactions (`BEGIN IMMEDIATE`, head check, insert,
commit) called directly from the activation's coroutine, so while one runs nothing else on the
worker's event loop progresses. A worker activates one run at a time, so no other run waits on that
loop, but the lease heartbeat used to be an asyncio task on the same loop: a burst of slow appends,
or one append stalled by a WAL checkpoint or a busy disk, could delay renewal past the lease and a
healthy worker would lose its run to a reclaim.

Measured on the benchmark machine (BENCHMARKS.md, `synchronous=NORMAL`): append p50 0.8 ms, p95
1.3 ms and a maximum of 19.9 ms over 2,000 appends; p95 2.8 ms with `FULL`. The default heartbeat
is 10 s against a 30 s lease, so the margin is 20 s.

Replay is O(history): every activation re-executes the workflow from the first event, with recorded
results served below the head. There is no sticky cache of live workflows and no continue-as-new.

## Decision

- Keep appends synchronous on the loop. Offloading them with `asyncio.to_thread` would let other
  coroutines run during an append, but the activation itself must wait for the commit before
  workflow code may continue, so it would gain nothing and add a thread hop of the same order as
  the append.
- Run lease renewal on a dedicated thread per activation (`Worker._heartbeat`), stopped when the
  activation ends. The journal is thread-safe (one writer engine with a connection pool and
  `BEGIN IMMEDIATE` per transaction), so renewals and appends serialize on SQLite's write lock,
  which the busy timeout covers.
- Keep replay linear with the history cap (ADR 2) and no sticky cache or continue-as-new.

## Consequences

- A stalled loop no longer loses the lease:
  `tests/integration/test_worker.py::test_lease_renewal_keeps_going_while_journal_io_blocks_the_event_loop`
  blocks the loop for 0.3 s with a 20 ms heartbeat and checks that the lease expiry keeps moving.
- A renewal can still wait for the write lock while an append commits: milliseconds at the measured
  p95, against a 20 s margin.
- Offloading appends becomes necessary when a single append approaches that margin (three orders
  of magnitude above the measured maximum), or when one process must interleave several
  activations on one loop, which this runtime does not do.
- A sticky cache or continue-as-new becomes necessary when resume latency matters at long
  histories. Replay-to-head costs about 18-20 µs per event on the benchmark machine (18.3 ms at
  1,001 events, 81.0 ms at 4,001; full resume 52.5 and 216.4 ms, see BENCHMARKS.md), so a resume
  at the 10,000-event cap is expected to take roughly half a second. Workflows that need more than that must be split into child runs by their author; a
  sticky cache would only help a worker that re-activates the same run repeatedly between waits.
