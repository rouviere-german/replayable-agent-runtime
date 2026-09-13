"""A few seeds of the SIGKILL chaos harness; CI runs 200 seeds via bench/chaos_sigkill.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from replayable_agent_runtime.runtime.sigkill import run_sigkill_seed

pytestmark = [pytest.mark.integration, pytest.mark.chaos]


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_a_sigkilled_worker_recovers_to_the_uncrashed_outcome(tmp_path: Path, seed: int) -> None:
    result = run_sigkill_seed(tmp_path, seed)
    assert result.killed
    assert result.journal_ok
    assert result.equivalent
    assert result.passed
    if not result.in_effect_window:
        assert result.duplicate_provider_calls == 0
