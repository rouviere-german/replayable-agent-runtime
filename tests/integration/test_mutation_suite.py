from __future__ import annotations

from pathlib import Path

import pytest

from replayable_agent_runtime.runtime.chaos import ChaosEnvironment
from replayable_agent_runtime.runtime.journal import RunStatus
from replayable_agent_runtime.runtime.mutations import (
    MUTANTS,
    PATCHED,
    TOOLS,
    WORKFLOW,
    evaluate_mutations,
    scripted,
)
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition

pytestmark = pytest.mark.integration


def test_all_twenty_unguarded_edits_raise_and_every_patched_edit_replays(tmp_path: Path) -> None:
    results = evaluate_mutations(tmp_path)
    mutants = [result for result in results if result.expected_nondeterminism]
    patched = [result for result in results if not result.expected_nondeterminism]
    assert len(mutants) == 20
    assert len({result.name for result in mutants}) == 20
    assert len(patched) == len(PATCHED) == 4
    assert [result.name for result in results if not result.as_expected] == []
    assert all(result.detail == "completed" for result in patched)
    assert all("command" in result.detail for result in mutants)


def test_a_patched_edit_takes_the_new_branch_in_new_runs(tmp_path: Path) -> None:
    name, steps = PATCHED[1]
    assert name == "patched_ticket_title"
    env = ChaosEnvironment(tmp_path)
    try:
        env.workflows.register(WorkflowDefinition(WORKFLOW, scripted(steps), TOOLS))
        env.control.start_run(
            workflow=WORKFLOW, input_value=None, principal="chaos-starter", run_id="new-run"
        )
        env.drive("new-run")
        assert env.journal.get_run("new-run").status is RunStatus.COMPLETED
        titles = [entry.args for entry in env.sinks.ledger("new-run") if entry.op == "create"]
        assert titles == ['{"body":"see summary","title":"incident v2"}']
    finally:
        env.close()


def test_mutant_names_are_unique_descriptions(tmp_path: Path) -> None:
    del tmp_path
    names = [name for name, _ in (*MUTANTS, *PATCHED)]
    assert len(names) == len(set(names)) == 24
