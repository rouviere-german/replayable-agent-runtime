"""Evaluation suites from the project specification, with pass/fail gates.

Usage:
  uv run python bench/eval_suites.py --check                  # full run
  uv run python bench/eval_suites.py --examples 50 --zombie-trials 2 --check

- crash equivalence: random programs (LLM, read tool, tickets, notifications, clock, uuid, timers,
  signals, human gates, patches, compensations, failures) with 1-3 random crash points of every
  kind; the recovered run must match an uncrashed run of the same program;
- provider accounting: paid provider calls repeated for the same step after recovery;
- zombie worker: stale-token writes rejected with fencing, and duplicated in the control;
- mutation suite: 22 nondeterministic edits (including edits that raise or return early) raise
  NonDeterminismError on strict replay and block an in-flight run through the live engine; patched
  edits replay cleanly and finish the in-flight run;
- tamper suite: modifications, deletions, reorderings, truncations and forged appends in the
  database, and every single-byte flip of a full export.

Writes bench/results/eval.json.
"""

from __future__ import annotations

import argparse
import json
import random
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from replayable_agent_runtime.runtime.chaos import (
    ChaosEnvironment,
    random_crash_points,
    random_program,
)
from replayable_agent_runtime.runtime.mutations import evaluate_mutations
from replayable_agent_runtime.runtime.tamper import run_db_tamper_suite, run_export_flip_suite
from replayable_agent_runtime.runtime.zombie import run_zombie_scenario

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results" / "eval.json"


def crash_equivalence(examples: int, seed: int) -> dict[str, float]:
    rng = random.Random(seed)
    totals = {
        "examples": 0,
        "crashes_fired": 0,
        "examples_with_window_crash": 0,
        "equivalent": 0,
        "strictly_equivalent_without_window_crash": 0,
        "examples_without_window_crash": 0,
        "duplicate_paid_calls_without_window_crash": 0,
        "duplicate_paid_calls_in_window_steps": 0,
        "journals_verified": 0,
        "unstable_recorded_values": 0,
    }
    with tempfile.TemporaryDirectory() as tmp:
        for index in range(examples):
            program = random_program(rng, max_ops=8)
            env = ChaosEnvironment(Path(tmp) / f"example-{index}")
            try:
                reference = env.run(program, "reference")
                points = random_crash_points(
                    rng,
                    horizon=reference.history_length,
                    kinds=("before_commit", "after_commit", "after_effect"),
                )
                crashed = env.run(program, "crashed", points)
            finally:
                env.close()
            duplicates = sum(count - 1 for count in crashed.duplicate_provider_seqs.values())
            totals["examples"] += 1
            totals["crashes_fired"] += crashed.crashes
            totals["journals_verified"] += int(crashed.journal_ok and reference.journal_ok)
            totals["unstable_recorded_values"] += int(not crashed.observations_stable)
            if crashed.window_step_seqs:
                totals["examples_with_window_crash"] += 1
                totals["duplicate_paid_calls_in_window_steps"] += duplicates
                ok = crashed.at_least_once_equivalent_to(reference) and set(
                    crashed.duplicate_provider_seqs
                ) <= set(crashed.window_step_seqs)
            else:
                totals["examples_without_window_crash"] += 1
                totals["duplicate_paid_calls_without_window_crash"] += duplicates
                strict = crashed.equivalent_to(reference)
                totals["strictly_equivalent_without_window_crash"] += int(strict)
                ok = strict and duplicates == 0
            totals["equivalent"] += int(ok)
    result = {key: float(value) for key, value in totals.items()}
    result["equivalence_rate"] = totals["equivalent"] / max(1, totals["examples"])
    return result


def zombie(trials: int) -> dict[str, float]:
    counts = {
        "trials_per_mode": trials,
        "fenced_stale_writes": 0,
        "fenced_stale_writes_rejected": 0,
        "fenced_duplicate_applications": 0,
        "control_notifier_duplicate_applications": 0,
        "control_ticket_duplicate_applications": 0,
    }
    with tempfile.TemporaryDirectory() as tmp:
        for trial in range(trials):
            for sink in ("notifier", "tickets"):
                fenced = run_zombie_scenario(
                    Path(tmp) / f"fenced-{sink}-{trial}", sink=sink, enforce_fencing=True
                )
                counts["fenced_stale_writes"] += 1
                counts["fenced_stale_writes_rejected"] += fenced.stale_rejections
                counts["fenced_duplicate_applications"] += fenced.duplicate_applications
            control_notifier = run_zombie_scenario(
                Path(tmp) / f"control-notifier-{trial}", sink="notifier", enforce_fencing=False
            )
            control_tickets = run_zombie_scenario(
                Path(tmp) / f"control-tickets-{trial}", sink="tickets", enforce_fencing=False
            )
            counts["control_notifier_duplicate_applications"] += (
                control_notifier.duplicate_applications
            )
            counts["control_ticket_duplicate_applications"] += (
                control_tickets.duplicate_applications
            )
    result = {key: float(value) for key, value in counts.items()}
    result["stale_write_rejection_rate"] = counts["fenced_stale_writes_rejected"] / max(
        1, counts["fenced_stale_writes"]
    )
    return result


def mutations() -> dict[str, float]:
    with tempfile.TemporaryDirectory() as tmp:
        results = evaluate_mutations(Path(tmp))
    mutants = [r for r in results if r.expected_nondeterminism]
    patched = [r for r in results if not r.expected_nondeterminism]
    return {
        "nondeterministic_edits": float(len(mutants)),
        "nondeterministic_edits_detected": float(sum(r.raised for r in mutants)),
        "nondeterministic_edits_blocked_in_flight": float(
            sum(r.in_flight_outcome == "blocked" and r.in_flight_appended == 0 for r in mutants)
        ),
        "patched_edits": float(len(patched)),
        "patched_edits_replayed_cleanly": float(sum(not r.raised for r in patched)),
        "patched_edits_completed_in_flight": float(
            sum(r.in_flight_outcome == "completed" for r in patched)
        ),
    }


def tamper() -> dict[str, float]:
    with tempfile.TemporaryDirectory() as tmp:
        env = ChaosEnvironment(Path(tmp) / "tamper")
        try:
            program = random_program(random.Random(4242), max_ops=8)
            env.run(program, "tamper-run")
            env.journal.close()
            db_report = run_db_tamper_suite(env.journal.path, "tamper-run")
            reopened = ChaosEnvironment(Path(tmp) / "tamper")
            raw = "".join(reopened.journal.export_run("tamper-run", redact=False)).encode()
            reopened.close()
        finally:
            env.sinks.close()
        export_report = run_export_flip_suite(raw)
    result: dict[str, float] = {}
    for kind, attempts in db_report.attempts.items():
        result[f"db_{kind}_attempts"] = float(attempts)
        result[f"db_{kind}_detected"] = float(db_report.detected.get(kind, 0))
    result["export_byte_flips"] = float(export_report.total)
    result["export_byte_flips_detected"] = float(export_report.total_detected)
    result["detection_rate"] = (db_report.total_detected + export_report.total_detected) / (
        db_report.total + export_report.total
    )
    return result


def gates(report: dict[str, dict[str, float]]) -> list[str]:
    crash, zombies, mutation, tampering = (
        report["crash_equivalence"],
        report["zombie"],
        report["mutations"],
        report["tamper"],
    )
    checks = {
        "crash equivalence rate is 100%": crash["equivalence_rate"] == 1.0,
        "no duplicate paid call outside a crash window": (
            crash["duplicate_paid_calls_without_window_crash"] == 0
        ),
        "every journal verifies after recovery": crash["journals_verified"] == crash["examples"],
        "recorded clock and uuid values are stable": crash["unstable_recorded_values"] == 0,
        "every stale-token write is rejected": zombies["stale_write_rejection_rate"] == 1.0,
        "fencing leaves no duplicate application": zombies["fenced_duplicate_applications"] == 0,
        "the control (fencing off) shows duplicates": (
            zombies["control_notifier_duplicate_applications"] >= zombies["trials_per_mode"]
        ),
        "at least 20 nondeterministic edits, all detected by strict replay": (
            mutation["nondeterministic_edits"] >= 20
            and mutation["nondeterministic_edits_detected"] == mutation["nondeterministic_edits"]
        ),
        "every nondeterministic edit blocks an in-flight run without appending": (
            mutation["nondeterministic_edits_blocked_in_flight"]
            == mutation["nondeterministic_edits"]
        ),
        "patched edits replay cleanly and finish in-flight runs": (
            mutation["patched_edits_replayed_cleanly"] == mutation["patched_edits"]
            and mutation["patched_edits_completed_in_flight"] == mutation["patched_edits"]
        ),
        "every tampering is detected": tampering["detection_rate"] == 1.0,
    }
    return [name for name, ok in checks.items() if not ok]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="evaluation suites")
    parser.add_argument("--examples", type=int, default=500)
    parser.add_argument("--zombie-trials", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260913)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    report: dict[str, dict[str, float]] = {}
    timings: dict[str, float] = {}
    examples: int = args.examples
    seed: int = args.seed
    trials: int = args.zombie_trials

    def run_crash_equivalence() -> dict[str, float]:
        return crash_equivalence(examples, seed)

    def run_zombie() -> dict[str, float]:
        return zombie(trials)

    suites: list[tuple[str, Callable[[], dict[str, float]]]] = [
        ("crash_equivalence", run_crash_equivalence),
        ("zombie", run_zombie),
        ("mutations", mutations),
        ("tamper", tamper),
    ]
    for name, suite in suites:
        start = time.perf_counter()
        report[name] = suite()
        timings[name] = round(time.perf_counter() - start, 1)
        print(f"## {name} ({timings[name]} s)")
        for key, value in report[name].items():
            shown = int(value) if float(value).is_integer() else round(value, 4)
            print(f"- {key}: {shown}")
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(
        json.dumps({"results": report, "seconds": timings}, indent=2) + "\n", encoding="utf-8"
    )
    if args.check:
        failures = gates(report)
        for failure in failures:
            print(f"GATE FAILED: {failure}")
        if failures:
            return 1
        print("all evaluation gates passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
