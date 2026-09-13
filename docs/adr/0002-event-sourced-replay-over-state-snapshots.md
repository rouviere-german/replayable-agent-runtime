# 2. Event-sourced replay instead of snapshotting coroutine state

- Status: accepted
- Date: 2026-09-13

## Context

A run can wait hours for an approval, and the process that started it can be redeployed or killed
at any time. Its progress has to survive outside process memory. Two broad designs exist:

1. **Snapshot state**: serialize the workflow's local variables or coroutine frame at every wait and
   restore it later. Python coroutines cannot be serialized safely; approximating it needs pickle
   (banned in this portfolio because loading it executes code) or a hand-written state machine per
   workflow, which gives up plain async code.
2. **Event-sourced replay**, as in Temporal, Azure Durable Functions and Restate: journal the
   *results of nondeterministic operations* and rebuild in-memory state by re-running the workflow
   code against that journal (Temporal Technologies, "Temporal: Workflow Execution"; Microsoft,
   "Durable Functions: orchestrator code constraints").

## Decision

Use replay. Workflows are ordinary async functions; every nondeterministic operation goes through
`WorkflowContext` and is journaled as typed events with a command fingerprint. On each activation
the engine re-executes the workflow from the start, serving recorded results below the history head
and running commands live past it. Fingerprint mismatches raise `NonDeterminismError` and block the
run rather than failing it, because the fix is a code change, not a new attempt. Versioning is done
with `ctx.patched`.

Each activation replays from the start; there is no in-memory cache of running coroutines between
activations.

## Consequences

- Workflow code stays readable, and history is an audit trail by construction.
- Workflow code must be deterministic; the runtime detects violations at replay time but cannot
  prevent them. Documented rules are in docs/SEMANTICS.md.
- Replay cost grows linearly with history length (and model requests that carry the whole
  conversation make each fingerprint proportional to it). BENCHMARKS.md measures replay time against
  history length. Histories are capped by `APP_MAX_HISTORY_EVENTS` and the run fails loudly above the
  cap instead of degrading every activation. Continue-as-new and a sticky activation cache, the usual
  mitigations, are non-goals for now.
- Changing workflow code for in-flight runs requires `ctx.patched` guards; unguarded changes block
  runs until the code matches again. The mutation suite checks both directions.
