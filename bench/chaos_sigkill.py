"""SIGKILL chaos harness: kill real worker processes at random journal offsets.

Usage:
  uv run python bench/chaos_sigkill.py --seeds 200 --check
  uv run python bench/chaos_sigkill.py --seeds 20 --start 1000

Each seed runs a random program in a subprocess, kills it with SIGKILL (TerminateProcess on
Windows) right after a chosen journal commit or right after a chosen external call, resumes the run
in a fresh process-local environment and compares it with an uncrashed run. See
src/replayable_agent_runtime/runtime/sigkill.py. Writes bench/results/sigkill.json.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from dataclasses import asdict
from pathlib import Path

from replayable_agent_runtime.runtime.sigkill import SigkillResult, run_seeds

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results" / "sigkill.json"


def summarize(results: list[SigkillResult]) -> dict[str, float]:
    window = [r for r in results if r.in_effect_window]
    boundary = [r for r in results if not r.in_effect_window]
    return {
        "seeds": float(len(results)),
        "killed": float(sum(r.killed for r in results)),
        "killed_after_commit": float(
            sum(r.killed and r.kill_kind == "after_commit" for r in results)
        ),
        "killed_inside_effect_window": float(len(window)),
        "recovered_equivalent": float(sum(r.equivalent for r in results)),
        "journals_verified": float(sum(r.journal_ok for r in results)),
        "duplicate_paid_calls_after_commit_kills": float(
            sum(r.duplicate_provider_calls for r in boundary)
        ),
        "duplicate_paid_calls_inside_window_kills": float(
            sum(r.duplicate_provider_calls for r in window)
        ),
        "harness_errors": float(sum(r.error is not None for r in results)),
        "passed": float(sum(r.passed for r in results)),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SIGKILL chaos harness")
    parser.add_argument("--seeds", type=int, default=200)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    started = time.perf_counter()

    def report(result: SigkillResult) -> None:
        if not result.passed:
            print(f"FAILED seed {result.seed}: {result}", flush=True)

    # A seed that raises is recorded as a failure (run_seeds), and leftover files a killed worker
    # still holds on Windows must not turn cleanup into a crash that hides the summary.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        seeds = range(args.start, args.start + args.seeds)
        results = run_seeds(Path(tmp), seeds, report=report)
    summary = summarize(results)
    summary["seconds"] = round(time.perf_counter() - started, 1)
    for key, value in summary.items():
        print(f"- {key}: {int(value) if float(value).is_integer() else value}")
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(
        json.dumps({"summary": summary, "seeds": [asdict(r) for r in results]}, indent=2) + "\n",
        encoding="utf-8",
    )
    if args.check:
        ok = summary["passed"] == summary["seeds"] and summary["killed"] == summary["seeds"]
        print("all seeds recovered" if ok else "CHAOS CHECK FAILED")
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
