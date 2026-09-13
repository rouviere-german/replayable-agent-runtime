"""SIGKILL chaos: kill a real worker process at a journal offset, recover it, compare outcomes.

For each seed:

1. generate a random program and record the outcome of an uncrashed in-process run;
2. start the same program in a subprocess (``python -m replayable_agent_runtime.runtime.chaos``)
   that pauses right after committing a chosen journal event, or right after a chosen external
   call returns and before its completion is journaled;
3. kill the subprocess with ``Popen.kill()``: SIGKILL on POSIX, ``TerminateProcess`` on Windows,
   so no cleanup, ``finally`` block or lease release runs;
4. resume the run in a fresh environment whose clock is past the dead worker's lease;
5. compare status, result, external-effect ledger and paid provider calls with the reference, and
   verify the journal hash chain.
"""

from __future__ import annotations

import os
import random
import subprocess  # nosec B404 - only starts this package's own chaos driver with a fixed argv
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from replayable_agent_runtime.runtime.chaos import (
    ChaosEnvironment,
    CrashInjector,
    random_program,
)
from replayable_agent_runtime.runtime.events import EventType

__all__ = ["SigkillResult", "run_sigkill_seed"]

_CLOCK_START = 1_700_000_000.0
_RECOVERY_CLOCK_OFFSET = 1_000_000.0
_RUN_ID = "sigkill-run"


@dataclass(frozen=True, slots=True)
class SigkillResult:
    seed: int
    kill_kind: str
    kill_at: int
    killed: bool
    status: str
    equivalent: bool
    in_effect_window: bool
    duplicate_provider_calls: int
    journal_ok: bool
    seconds: float

    @property
    def passed(self) -> bool:
        if not self.journal_ok or not self.equivalent:
            return False
        return self.in_effect_window or self.duplicate_provider_calls == 0


def _child_environment() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("APP_")}
    env.pop("ANTHROPIC_API_KEY", None)
    return env


def run_sigkill_seed(workdir: Path, seed: int, *, timeout_seconds: float = 180.0) -> SigkillResult:
    started = time.perf_counter()
    rng = random.Random(seed)  # noqa: S311  # nosec B311 - reproducible test data, not crypto
    program = random_program(rng, max_ops=8)

    reference_env = ChaosEnvironment(workdir / "reference", clock_start=_CLOCK_START)
    try:
        reference = reference_env.run(program, _RUN_ID)
        reference_history = reference_env.journal.load_history(_RUN_ID)
    finally:
        reference_env.close()
    scheduled_seqs = [r.seq for r in reference_history if r.type == EventType.STEP_SCHEDULED]
    if scheduled_seqs and rng.random() < 0.5:
        kind = "after_effect"
        at = rng.randrange(len(scheduled_seqs))
    else:
        kind = "after_commit"
        at = rng.randrange(1, len(reference_history))

    crash_dir = workdir / "crashed"
    setup = ChaosEnvironment(crash_dir, clock_start=_CLOCK_START)
    setup.start(program, _RUN_ID)
    setup.close()
    marker = crash_dir / "paused.marker"
    command = [
        sys.executable,
        "-m",
        "replayable_agent_runtime.runtime.chaos",
        "--workdir", str(crash_dir),
        "--run-id", _RUN_ID,
        "--clock-start", str(_CLOCK_START),
        "--pause-kind", kind,
        "--pause-at", str(at),
        "--marker", str(marker),
    ]  # fmt: skip
    process = subprocess.Popen(  # noqa: S603  # nosec B603 - fixed argv, our own module
        command,
        env=_child_environment(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + timeout_seconds
    while not marker.exists() and process.poll() is None:
        if time.monotonic() > deadline:
            process.kill()
            raise TimeoutError(f"seed {seed}: the worker neither paused nor finished")
        time.sleep(0.01)
    killed = marker.exists()
    if killed:
        process.kill()
    process.wait(timeout=60)

    recovery = ChaosEnvironment(crash_dir, clock_start=_CLOCK_START + _RECOVERY_CLOCK_OFFSET)
    try:
        injector = CrashInjector(())
        if killed and kind == "after_effect":
            injector.window_seqs.append(scheduled_seqs[at])
        activations, _ = recovery.drive(_RUN_ID)
        crashed = recovery.outcome(_RUN_ID, injector, activations, 1 if killed else 0)
    finally:
        recovery.close()
    in_window = bool(crashed.window_step_seqs)
    equivalent = (
        crashed.at_least_once_equivalent_to(reference)
        if in_window
        else crashed.equivalent_to(reference)
    )
    duplicates = sum(count - 1 for count in crashed.duplicate_provider_seqs.values())
    return SigkillResult(
        seed=seed,
        kill_kind=kind,
        kill_at=at,
        killed=killed,
        status=crashed.status,
        equivalent=equivalent,
        in_effect_window=in_window,
        duplicate_provider_calls=duplicates,
        journal_ok=crashed.journal_ok,
        seconds=round(time.perf_counter() - started, 3),
    )
