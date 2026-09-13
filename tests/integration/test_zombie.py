from __future__ import annotations

from pathlib import Path

import pytest

from replayable_agent_runtime.runtime.zombie import run_zombie_scenario

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("sink", ["notifier", "tickets"])
def test_a_paused_zombie_worker_cannot_apply_a_stale_write(tmp_path: Path, sink: str) -> None:
    outcome = run_zombie_scenario(tmp_path, sink=sink, enforce_fencing=True)  # type: ignore[arg-type]
    assert outcome.run_status == "completed"
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.stale_rejections == 1
    assert outcome.duplicate_applications == 0
    assert sorted(outcome.applications_per_step.values()) == [1, 1]


def test_the_control_without_fencing_shows_the_zombie_duplicating_a_notification(
    tmp_path: Path,
) -> None:
    outcome = run_zombie_scenario(tmp_path, sink="notifier", enforce_fencing=False)
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == 1
    assert sorted(outcome.applications_per_step.values()) == [1, 2]
    # The journal still refuses the zombie's next append.
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.run_status == "completed"


def test_without_fencing_idempotency_keys_still_deduplicate_ticket_writes(tmp_path: Path) -> None:
    outcome = run_zombie_scenario(tmp_path, sink="tickets", enforce_fencing=False)
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == 0
    assert outcome.zombie_outcome == "lease_lost"
