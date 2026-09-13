"""The SIGKILL harness must survive Windows releasing a killed worker's files late."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from replayable_agent_runtime.runtime.sigkill import SigkillResult, open_with_retry, run_seeds

pytestmark = pytest.mark.unit


def _disk_io_error() -> OperationalError:
    return OperationalError("PRAGMA journal_mode=WAL", {}, sqlite3.OperationalError("disk I/O"))


def test_reopening_databases_retries_while_the_killed_worker_still_holds_them() -> None:
    """Regression: 'disk I/O error' right after TerminateProcess aborted the 200-seed run."""
    attempts: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise _disk_io_error()
        return "reopened"

    assert open_with_retry(flaky, sleep=sleeps.append) == "reopened"
    assert len(attempts) == 3
    assert sleeps == [0.05, 0.1]

    def always_locked() -> str:
        raise _disk_io_error()

    with pytest.raises(OperationalError):
        open_with_retry(always_locked, attempts=4, sleep=sleeps.append)
    assert sleeps[2:] == [0.05, 0.1, 0.2]


def _passing(seed: int) -> SigkillResult:
    return SigkillResult(
        seed=seed,
        kill_kind="after_commit",
        kill_at=1,
        killed=True,
        status="completed",
        equivalent=True,
        in_effect_window=False,
        duplicate_provider_calls=0,
        journal_ok=True,
        seconds=0.0,
    )


def test_a_seed_that_raises_is_counted_as_a_failure_and_later_seeds_still_run(
    tmp_path: Path,
) -> None:
    """Regression: one exception aborted the chaos run before it printed any summary."""

    def fake_seed(workdir: Path, seed: int) -> SigkillResult:
        workdir.mkdir(parents=True)
        if seed == 11:
            raise PermissionError("[WinError 32] journal.db is used by another process")
        return _passing(seed)

    reported: list[SigkillResult] = []
    results = run_seeds(tmp_path, range(10, 13), run_seed=fake_seed, report=reported.append)
    assert [result.seed for result in results] == [10, 11, 12]
    assert [result.passed for result in results] == [True, False, True]
    assert results[1].error is not None
    assert "PermissionError" in results[1].error
    assert results[1].status == "error"
    assert reported == results
    # Seed directories are removed as the run goes, so 200 seeds do not fill the disk.
    assert not any(tmp_path.iterdir())
