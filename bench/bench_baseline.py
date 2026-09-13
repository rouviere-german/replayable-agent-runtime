"""Performance benchmarks with CI regression thresholds.

Usage:
  uv run python bench/bench_baseline.py            # run, write bench/results/latest.json
  uv run python bench/bench_baseline.py --check    # also enforce bench/thresholds.json
  uv run python bench/bench_baseline.py --markdown BENCHMARKS.md

Measured on a temporary on-disk SQLite journal in WAL mode:

- ``journal_append_normal`` / ``journal_append_full``: one fenced append transaction (BEGIN
  IMMEDIATE, head check, insert, COMMIT) with a 200-byte payload, ``synchronous=NORMAL`` (default)
  and ``FULL``;
- ``resume_1k_events``: an external input wakes a parked run with 1,001 journaled events; a worker
  claims it, loads and hash-verifies the history, replays the workflow to the head and parks it
  again (the input is not the one the workflow waits for, so no events are appended);
- ``replay_<n>_events``: the same resume at several history lengths;
- ``verify_chain_1k_events``: hash-chain verification alone.

Thresholds for append p95 (10 ms) and 1k-event resume p95 (150 ms) come from the project
specification; the others are several times the locally measured values.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from pydantic import JsonValue

from replayable_agent_runtime.agentic.audit import verify_chain
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.core.ratelimit import RateLimiter
from replayable_agent_runtime.core.redaction import redact_text
from replayable_agent_runtime.runtime.builtin_tools import build_tool_registry
from replayable_agent_runtime.runtime.chaos import echo_policy
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.db import SynchronousMode
from replayable_agent_runtime.runtime.engine import Runtime
from replayable_agent_runtime.runtime.events import (
    CommandKind,
    EventType,
    NewEvent,
    command_fingerprint,
)
from replayable_agent_runtime.runtime.journal import Journal
from replayable_agent_runtime.runtime.sinks import SinkStore
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results" / "latest.json"
THRESHOLDS = ROOT / "bench" / "thresholds.json"
HISTORY_LENGTHS = (101, 251, 501, 1001, 2001, 4001)
Stats = dict[str, float]


def _stats(samples_seconds: list[float]) -> Stats:
    millis = sorted(sample * 1_000 for sample in samples_seconds)
    cuts = statistics.quantiles(millis, n=100, method="inclusive")
    return {
        "runs": float(len(millis)),
        "p50_ms": round(statistics.median(millis), 3),
        "p95_ms": round(cuts[94], 3),
        "max_ms": round(millis[-1], 3),
    }


def _timed(function: Callable[[], object], runs: int, warmup: int = 5) -> Stats:
    for _ in range(warmup):
        function()
    samples = []
    for _ in range(runs):
        start = time.perf_counter()
        function()
        samples.append(time.perf_counter() - start)
    return _stats(samples)


def bench_append(workdir: Path, synchronous: SynchronousMode, runs: int) -> Stats:
    journal = Journal(workdir / f"append-{synchronous}.db", synchronous=synchronous)
    try:
        journal.create_run(
            run_id="append",
            workflow="bench",
            input_value=None,
            started_by="bench",
            budget=BudgetLimits.of(steps=1, tokens=1, usd=0.0),
        )
        claim = journal.claim("bench", 3600)
        assert claim is not None  # noqa: S101 - benchmark precondition
        fingerprint = command_fingerprint(CommandKind.NOW, "clock", {})
        counter = iter(range(10**9))

        def append() -> None:
            command = next(counter)
            journal.append(
                claim.lease,
                [
                    NewEvent(
                        EventType.STEP_COMPLETED,
                        {"kind": "now", "name": "clock", "output": "x" * 160 + str(command)},
                        command,
                        fingerprint,
                    )
                ],
            )

        return _timed(append, runs=runs, warmup=20)
    finally:
        journal.close()


async def _history_workflow(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    assert isinstance(value, dict)  # noqa: S101 - benchmark input
    for index in range(int(str(value["steps"]))):
        await ctx.tool("kb_lookup", {"key": f"k{index % 1000}"})
    await ctx.wait_signal("go")
    return "done"


def bench_resume(workdir: Path, events: int, runs: int) -> tuple[Stats, Stats]:
    """Time full resumes (claim, load, verify, replay, park) of a run with ``events`` events."""
    steps = (events - 1) // 2
    journal = Journal(workdir / f"resume-{events}.db")
    sinks = SinkStore(workdir / f"resume-{events}-sinks.db")
    try:
        workflows = WorkflowRegistry(
            [WorkflowDefinition("history", _history_workflow, frozenset({"kb_lookup"}))]
        )
        tools = build_tool_registry(workspace=workdir / "workspace", sinks=sinks)
        runtime = Runtime(
            journal=journal, workflows=workflows, tools=tools, provider=FakeProvider(echo_policy)
        )
        ControlPlane(
            journal, workflows, default_budget=BudgetLimits.of(steps=10**6, tokens=1, usd=0.0)
        ).start_run(workflow="history", input_value={"steps": steps}, principal="bench", run_id="r")
        worker = Worker(runtime, worker_id="bench", heartbeat_seconds=None)
        first = asyncio.run(worker.run_once(run_id="r"))
        if first is None or first.outcome != "waiting":
            raise RuntimeError("the benchmark run did not park on its signal")
        assert journal.get_run("r").head_seq + 1 == events  # noqa: S101
        wall: list[float] = []
        engine: list[float] = []
        for index in range(runs):
            # An unrelated external input wakes the parked run (as any signal or approval would);
            # the workflow still waits for "go", so the resume appends no events.
            journal.send_signal("r", "nudge", {"n": index}, sent_by="bench")
            start = time.perf_counter()
            outcome = asyncio.run(worker.run_once(run_id="r"))
            wall.append(time.perf_counter() - start)
            if outcome is None or outcome.replay_seconds is None:
                raise RuntimeError("the resume did not reach the history head")
            engine.append(outcome.replay_seconds)
        return _stats(wall), _stats(engine)
    finally:
        journal.close()
        sinks.close()


def bench_verify(workdir: Path) -> Stats:
    journal = Journal(workdir / "resume-1001.db", migrate=False)
    try:
        history = journal.load_history("r")
        run = journal.get_run("r")
        return _timed(
            lambda: verify_chain(history, run_id="r", expect_head=(run.head_seq, run.head_hash)),
            runs=50,
        )
    finally:
        journal.close()


def machine() -> dict[str, object]:
    return {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
        "processor": platform.processor() or "unknown",
    }


def run(quick: bool = False) -> dict[str, Stats]:
    results: dict[str, Stats] = {}
    append_runs = 300 if quick else 2_000
    resume_runs = 10 if quick else 40
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        results["journal_append_normal"] = bench_append(workdir, "NORMAL", append_runs)
        results["journal_append_full"] = bench_append(workdir, "FULL", append_runs // 4)
        for events in HISTORY_LENGTHS:
            wall, engine = bench_resume(workdir, events, resume_runs if events <= 1001 else 10)
            label = "resume_1k_events" if events == 1001 else f"resume_{events}_events"
            results[label] = wall
            results[f"replay_to_head_{events}_events"] = engine
        results["verify_chain_1k_events"] = bench_verify(workdir)
    messages = tuple(ChatMessage.user(f"message {i} " + "x" * 400) for i in range(10))
    request = LLMRequest(system="bench", messages=messages)
    results["llm_request_sha256_10_messages"] = _timed(request.sha256, runs=2_000)
    line = "user jane.doe@example.com paid; " + "lorem ipsum dolor sit amet " * 35
    results["redact_text_1kb"] = _timed(lambda: redact_text(line), runs=2_000)
    limiter = RateLimiter(rate=1e9, burst=1_000_000_000)
    results["rate_limiter_check"] = _timed(lambda: limiter.check("client"), runs=20_000)
    return results


def check_thresholds(results: dict[str, Stats]) -> list[str]:
    thresholds: dict[str, dict[str, float]] = json.loads(THRESHOLDS.read_text(encoding="utf-8"))
    problems = []
    for name, limits in thresholds.items():
        if name not in results:
            problems.append(f"{name}: benchmark missing")
            continue
        for metric, limit in limits.items():
            value = results[name][metric]
            if value > limit:
                problems.append(f"{name}.{metric} = {value} exceeds threshold {limit}")
    return problems


def table(results: dict[str, Stats]) -> list[str]:
    lines = ["| benchmark | runs | p50 (ms) | p95 (ms) | max (ms) |", "|---|---:|---:|---:|---:|"]
    for name, stats in results.items():
        lines.append(
            f"| {name} | {stats['runs']:.0f} | {stats['p50_ms']} | {stats['p95_ms']} | "
            f"{stats['max_ms']} |"
        )
    return lines


def markdown(results: dict[str, Stats], spec: dict[str, object]) -> str:
    def resume_label(events: int) -> str:
        return "resume_1k_events" if events == 1001 else f"resume_{events}_events"

    replay_rows = [
        f"| {events} | {results[f'replay_to_head_{events}_events']['p50_ms']} | "
        f"{results[resume_label(events)]['p50_ms']} |"
        for events in HISTORY_LENGTHS
    ]
    return "\n".join(
        [
            "# Benchmarks",
            "",
            "Generated by "
            "`uv run python bench/bench_baseline.py --check --markdown BENCHMARKS.md`.",
            "Single-machine numbers for spotting regressions, not performance claims.",
            "",
            f"Machine: {spec['platform']}, {spec['processor']}, {spec['cpu_count']} logical CPUs, "
            f"Python {spec['python']}. Journal on the local disk in a temporary directory.",
            "",
            "## All benchmarks",
            "",
            *table(results),
            "",
            "## Replay time versus history length",
            "",
            "`replay to head` is the engine's own measurement from activation start to the first "
            "command that is not fully recorded; `full resume` adds the lease claim, loading and "
            "hash-verifying the history, and parking the run again.",
            "",
            "| events in history | replay to head p50 (ms) | full resume p50 (ms) |",
            "|---:|---:|---:|",
            *replay_rows,
            "",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="journal and replay benchmarks")
    parser.add_argument("--check", action="store_true", help="enforce bench/thresholds.json")
    parser.add_argument("--quick", action="store_true", help="fewer iterations (smoke run)")
    parser.add_argument("--markdown", type=Path, default=None, help="write a markdown report")
    args = parser.parse_args(argv)

    results = run(quick=args.quick)
    spec = machine()
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(
        json.dumps({"machine": spec, "results": results}, indent=2) + "\n", encoding="utf-8"
    )
    print("\n".join(table(results)))
    if args.markdown is not None:
        args.markdown.write_text(markdown(results, spec), encoding="utf-8", newline="\n")
    if args.check:
        problems = check_thresholds(results)
        for problem in problems:
            print(f"REGRESSION: {problem}")
        if problems:
            return 1
        print("all benchmarks within thresholds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
