# 5. Hash-chain events over payload digests, anchored by a stored head

- Status: accepted
- Date: 2026-09-13

## Context

The journal is both the replay source and the audit record of what agents did and who approved it.
It must detect modification, deletion, insertion and reordering of events (tampering and
repudiation are the focus of the threat model). Two further needs pull in different directions:
exports shared outside the operator team must not leak personal data from prompts and tool output,
yet a redacted export should still prove it is an unaltered slice of the journal.

## Decision

Each event stores `payload_sha256 = sha256(payload)` and
`hash = sha256(canonical_json({v, run_id, seq, command, type, fingerprint, payload_sha256,
recorded_at_us, prev_hash}))`, with `prev_hash` of seq 0 set to 64 zeros. `runs.head_seq` and
`runs.head_hash` are updated in the same transaction as every append.

- Full exports carry payloads, so every payload digest and every hash is checked.
- Redacted exports replace payloads with redacted versions and keep the digests: the chain still
  verifies, and the verifier reports `payloads_verified: false`.
- Activations verify the full chain against the stored head before replaying and block runs that
  fail.

## Consequences

- The tamper suite shows detection of every single-column modification, deletion, adjacent
  reordering, tail truncation and forged append in the database, and of every single-byte flip in a
  full export.
- Verifying is linear in history length; it is part of the resume time measured in BENCHMARKS.md.
- A hash chain is tamper-evident, not tamper-proof: whoever can write the database can recompute the
  whole chain and the head. Detecting that requires a head hash kept somewhere the attacker cannot
  write (`--expect-head`); signing heads or anchoring them externally is a non-goal.
- Redacted exports cannot prove payload contents, only order and linkage.
