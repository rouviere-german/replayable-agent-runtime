"""Replayable agent runtime: durable execution for LLM agents.

Sub-packages:
- ``runtime``: the append-only, hash-chained SQLite journal, deterministic replay engine,
  WorkflowContext, fenced worker leases, transactional outbox, saga compensations, workflow
  patching, the FastAPI control plane and the crash-injection harness.
- ``agentic``: provider protocol (Fake, Replay, Anthropic), untrusted-data envelopes, tool
  registry with effect classes, approval rules, budgets, hash-chain verification.
- ``core``: settings, redacting logs, tracing, metrics, hardened FastAPI app factory, JWT auth.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
