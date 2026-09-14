# 4. Fencing tokens at the journal and at sinks, plus idempotency keys

- Status: accepted
- Date: 2026-09-13, amended 2026-09-14 (token announcement at activation start)

## Context

Leases expire. A worker that is paused (stop-the-world pause, `docker pause`, a VM migration) can
wake up after another worker has reclaimed its run and carry on as if it still owned it. Leases
alone cannot prevent this: the paused worker does not know time has passed (Kleppmann, "How to do
distributed locking", 2016). Separately, a worker can crash after an external call and before the
journal commit, so the next owner must repeat the call without knowing whether it happened.

## Decision

Three mechanisms, each for a different failure:

1. **Journal fencing.** Every claim increments `runs.fencing`; every append, suspend, block and
   finish is conditional on `owner` and `fencing` inside a `BEGIN IMMEDIATE` transaction. A zombie
   cannot add events or change run state.
2. **Sink fencing.** Tool handlers receive the lease's fencing token and the bundled sinks refuse
   tokens lower than the highest they have seen per run. Every activation announces its token to
   the sinks (`SinkStore.announce`) before it replays or re-executes anything, so the fence is
   raised when the new lease starts rather than when the new owner first writes. This covers the
   zombie that is already inside a sink call when it is paused, which journal fencing cannot
   reach, including one that wakes up before the new owner has written.
3. **Idempotency keys.** Every step's key is `run_id:seq` of its `StepScheduled` event, stable across
   re-executions, and a transactional outbox row with that key is written before the call. Sinks
   that support keys deduplicate the at-least-once window.

## Consequences

- The zombie scenario is tested in-process in three wake-up orderings: after the new owner
  finished, and before the owner's first write, stale-token writes are always rejected, and a
  control with sink fencing disabled shows the duplicate on the notifier (no idempotency keys) in
  both orderings while the ticket store still deduplicates by key. This shows each mechanism is
  doing work.
- The announcement leaves one window: a stale write that completes between the claim commit and
  the announcement lands before any sink knows the new token. It is treated like a crash inside the
  effect window (idempotency keys deduplicate it; the notifier delivers twice) and the third
  ordering of the zombie evaluation measures it. Closing it would need the claim and the sink
  fences in one transaction, which the separate sink database rules out on purpose: the sinks stand
  in for external systems.
- Fencing only helps at sinks that check tokens. Real third-party APIs rarely do; for them the
  runtime offers idempotency keys and documents at-least-once delivery in the crash window.
- Lease duration trades failover time against spurious reclaims: the default is a 30 s lease with a
  10 s heartbeat. Clock skew between hosts is out of scope because deployment is single-node.
- The outbox is written in the same transaction as `StepScheduled` and closed with the completion;
  there is no separate relay process. Pending outbox rows show exactly which effects are inside the
  at-least-once window after a crash (`Journal.list_outbox`).
