# Execution semantics

This document states what the runtime guarantees, and what it does not, when workers crash, pause,
race or are redeployed. The tests and benchmarks that check each statement are listed in
[CLAIMS.md](../CLAIMS.md).

## Model

A **workflow** is an async Python function `(ctx, input) -> result`. All interaction with the
outside world goes through the `WorkflowContext`:

| Command | Journaled as | Runs live once, then |
|---|---|---|
| `ctx.llm(request)` | `StepScheduled`, then `StepCompleted` (response, usage, cost) or `StepFailed` | returns the recorded response or re-raises the recorded error type |
| `ctx.tool(name, args)` | `CommandRejected`, or [`ApprovalRequested`, `ApprovalGranted`/`Denied`,] `StepScheduled`, `StepCompleted`/`StepFailed` | returns the recorded output |
| `ctx.now()`, `ctx.uuid()` | `StepCompleted` with the value | returns the recorded value |
| `ctx.sleep(s)` | `TimerScheduled`, `TimerFired` | returns immediately once fired |
| `ctx.wait_signal(name)` | `SignalReceived` with the payload | returns the recorded payload |
| `ctx.await_approval(name, details)` | `ApprovalRequested`, `ApprovalGranted`/`Denied` | returns the approver or raises the denial |
| `ctx.patched(id)` | `PatchMarker` | `True` where the marker was recorded, `False` in older histories |
| `ctx.compensate(tool, args)` | nothing at registration; each compensation is a journaled command when it runs | |

Commands are numbered 0, 1, 2... in the order the workflow issues them. Every event records its
command number and the command's fingerprint: SHA-256 over the canonical JSON of kind, name and
arguments (for model calls, the canonical hash of the whole request).

## Replay

An **activation** loads the run's history, verifies its hash chain against the head stored in
`runs`, and executes the workflow from the start:

- **Below the head**, each command's fingerprint is compared with the recorded one. A mismatch, a
  command issued after the history ended in strict replay, or a workflow that finishes while
  recorded commands remain, raises `NonDeterminismError`. The run is then **blocked** (not failed):
  an operator deploys code that matches the history, typically by guarding the change with
  `ctx.patched`, and runs `rar runs retry`.
- **Past the head**, commands run live. Their events are committed before control returns to
  workflow code.

Workflow code must be deterministic given its input and command results. It must not read the clock,
random numbers, environment or network directly, iterate over unordered collections whose order
affects commands, or catch `BaseException`. Suspension, cancellation, nondeterminism, lost leases
and history overflow are `BaseException` subclasses so that `except Exception` cannot swallow them.

Failures are recorded first and then re-raised from the record, so workflow code sees the same
exception type live and on replay. Unknown error types are re-raised as `ActivityFailedError`.

## Crash windows and effect guarantees

A worker can die at any instant. What happens depends on where:

| Crash point | On recovery | Effect guarantee |
|---|---|---|
| Before a command's first event commits | the command runs live again as if new | no effect happened yet |
| After `StepScheduled` commits, before the external call | the call runs with the same idempotency key `run_id:seq` | exactly one call |
| **After the external call returns, before `StepCompleted` commits** | the call runs **again** with the same key and fencing token of the new lease | **at least once** |
| After `StepCompleted` commits | the recorded result is replayed; nothing is called | exactly once |
| Between `ApprovalRequested` and its decision | the run waits for the same approval | the approval is not requested twice |

The bold row is the only window in which work is repeated. What a repetition means depends on the
receiver:

- **Idempotent sinks** (the bundled `TicketStore`) store the result per idempotency key and return
  it for a repeated key. Effects there are *effectively once*.
- **Non-idempotent sinks** (the bundled `Notifier`, like e-mail or webhooks) deliver again. Effects
  there are *at least once*, and only inside the window.
- **Model calls** in the window are paid again. After recovery, no call whose completion was
  committed is ever repeated; the evaluation measures this separately for crashes at commit
  boundaries (0 repeated paid calls) and inside the window (at most the interrupted step).

Tool outputs larger than the runtime's `max_output_bytes` are refused. For a read tool nothing
happened, so the step is recorded as failed. A side-effecting tool has already been applied when its
output is measured, so the step is recorded as **completed** with the output's size and SHA-256 in
place of the output, and the command then raises `ToolPolicyError`, live and on replay. The journal
never hides an applied effect behind `StepFailed`; workflow code can only roll such an effect back
with a compensation it can register without the lost output.

## Workers, leases and fencing

- A worker claims a run atomically under `BEGIN IMMEDIATE`:
  `UPDATE runs SET owner=?, fencing=fencing+1, lease_expires_at_us=? WHERE run_id=? AND
  lease_expires_at_us < now RETURNING fencing`. Fencing tokens increase monotonically per run.
- While it holds the lease, a heartbeat renews it. The heartbeat runs on a thread of its own, so a
  slow journal append on the worker's event loop cannot starve it
  (docs/adr/0006-synchronous-journal-io-and-an-out-of-loop-heartbeat.md). A worker that stops
  heartbeating (crash, stop-the-world pause, `docker pause`, network partition from its disk) loses
  the lease when it expires.
- **Journal fencing:** every append checks `owner` and `fencing` inside the same transaction. A
  worker whose lease was reclaimed cannot append (`FencedOutError`), so it cannot fork the history.
- **Sink fencing:** tool handlers receive the lease's fencing token; the bundled sinks refuse tokens
  lower than the highest they have seen for the run. Every activation *announces* its token to every
  fencing-aware sink before it replays or re-executes anything, so a paused worker that wakes up
  inside a sink call is refused from the moment the new owner's activation starts, whether or not
  the owner has written yet, even at a sink without idempotency keys.
- **The claim window:** between the claim commit and the announcement (one sink transaction later)
  no sink can know the new token, so a stale write that completes exactly there is applied. It is
  then treated like a crash inside the effect window: the new owner re-executes the step with the
  same idempotency key, so the idempotent ticket store deduplicates it and the notifier delivers
  twice. The journal still refuses the zombie's next append, so it cannot fork the history. Sinks
  that ignore fencing tokens (most third-party APIs) rely on idempotency keys alone.
- Lease expiry compares wall-clock timestamps written by different processes. Clock skew between
  workers larger than the lease margin can make a live lease look expired; journal and sink fencing
  still prevent double application, but the work is interrupted. Single-node deployment keeps all
  workers on one clock.

## Signals, timers, approvals and cancellation

- External inputs are stored in inbox tables and bump `runs.inbox_version` in the same transaction.
  A worker parks a run only if the version is unchanged since it claimed the run, so an input that
  arrives during an activation wakes the run instead of being lost.
- Signals are consumed in arrival order at the `wait_signal` command that asks for them; consumption
  and the `SignalReceived` event commit together.
- Timers fire when the runtime's clock passes `fire_at`; the scheduler loop makes due runs runnable.
  A timer never fires early; it can fire late by up to the poll interval plus queueing.
- An approval is bound to the SHA-256 of the exact call (tool name plus canonical, validated
  arguments) or gate (name plus details). The approver must present that hash, must be a different
  principal from the run starter, and the approvals row must still match its arguments and the
  hash-chained `ApprovalRequested` event. A decision is recorded in the journal as `ApprovalGranted`
  or `ApprovalDenied` at the command it releases.
- Cancellation is delivered at the next command that is not already recorded (it preempts a pending
  wait). Registered compensations then run newest first as journaled commands, and the run ends with
  `RunCancelled`. A failure (any exception out of the workflow) runs compensations the same way
  before `RunFailed`. Compensation tools are not charged against the step budget and need no new
  approval; they cannot be called directly.

## Budgets

Limits are part of the hash-chained `RunStarted` event; spend is recomputed from the run's own
events on every activation. A step is charged when its `StepScheduled` commits, so re-executing an
in-flight step does not charge it twice. Tokens and cost are known only after a call, so one call can
overshoot; all later steps are then refused with a journaled `CommandRejected`. The runtime's
configured budget can only tighten recorded limits.

## History cap

Replay cost grows with history length. When an append would take a run past
`APP_MAX_HISTORY_EVENTS` (default 10,000), the run fails with `HistoryLimitExceededError` instead of
slowing down every activation:

- A command's first event is only appended if its follow-up fits too (`StepScheduled` needs room
  for its completion, `TimerScheduled` for `TimerFired`, `ApprovalRequested` for the decision), so
  the cap never leaves an external call executed but unrecorded.
- Registered compensations still run, newest first. Compensation commands may use a reserve of
  `compensation_reserve_events` (default 256) events above the cap; compensations that do not fit
  even there count in `compensations_failed`. The terminal `RunFailed` event always fits and records
  the command that overflowed, so strict replay reproduces the failure.

A history therefore holds at most `APP_MAX_HISTORY_EVENTS` + 256 + 1 events. Long-lived processes
should be split into child runs; continue-as-new is not implemented.

## Durability

The journal is SQLite in WAL mode. With `APP_JOURNAL_SYNCHRONOUS=NORMAL` (default) a committed
transaction survives a killed process but the most recent commits can be lost on power failure or an
OS crash; recovery then behaves as if the process had crashed before those commits, which can move
effects that already happened into the at-least-once window. `FULL` syncs every commit at a higher
append latency (see BENCHMARKS.md). See docs/adr/0003-sqlite-wal-synchronous-normal.md.

## Integrity

Every event commits to its payload digest and to its predecessor's hash; `runs.head_seq` and
`runs.head_hash` anchor the tail. An activation verifies the chain before replaying and blocks a run
whose chain does not verify.

Some facts are repeated in mutable columns for querying: the `runs` row repeats `RunStarted`'s
workflow, input and starter, and each `approvals` row repeats its `ApprovalRequested` event (and,
once journaled, its decision). The engine executes the workflow with the input and starter from the
verified `RunStarted` event and blocks a run whose `runs` row disagrees. An approval decision
verifies the run's whole chain, checks both rows against their events and applies separation of
duties to the chained starter. `rar audit verify` reports any mismatch between these columns and the
chain. Someone with write access to the database file can rebuild a
consistent chain *and* head; that is detected only against a head hash stored elsewhere
(`rar audit verify --expect-head`, or an exported copy).
