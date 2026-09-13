# Contributing

## Setup

Requirements: [uv](https://docs.astral.sh/uv/) and Python 3.12 (uv can install it).

```bash
uv sync --frozen
uv run python tasks.py check      # lint, typecheck, test, audit, baseline
```

If the repository sits on a small or slow drive, keep the virtual environment elsewhere:

```powershell
$env:UV_PROJECT_ENVIRONMENT = "C:/Users/<you>/.venvs/<repo>"
uv sync --frozen
```

## Tasks

`tasks.py` is the cross-platform task runner; the Makefile only delegates to it.

| Target | What it runs |
|---|---|
| `lint` | `ruff check` and `ruff format --check` |
| `format` | `ruff format` and `ruff check --fix` |
| `typecheck` | `mypy --strict` over src, tests, scripts, bench and tasks.py |
| `test` | pytest with branch coverage; fails below 85% |
| `bench` | `bench/bench_baseline.py --check` and `bench/eval_suites.py --check` |
| `chaos` | `bench/chaos_sigkill.py --seeds 200 --check` (subprocesses, several minutes) |
| `demo` | the offline kill-and-resume demo (no API key) |
| `audit` | bandit and pip-audit |
| `baseline` | `scripts/check_baseline.py` |

## Rules for changes

- **Determinism first.** Workflow code touches the outside world only through `WorkflowContext`.
  A change to what an existing workflow issues must be guarded with `ctx.patched`; the mutation
  suite (`src/replayable_agent_runtime/runtime/mutations.py`) shows what counts as a change.
- **Crash windows are part of the contract.** A new command kind or sink needs a row in
  docs/SEMANTICS.md and coverage in the crash-equivalence property tests.
- **Tests first for behavior.** Mark tests `unit` or `integration` (`chaos` for subprocess kills).
  Use hypothesis for invariants rather than hand-picked examples alone.
- **Offline by default.** Tests, demo and bench must not touch the network or need
  `ANTHROPIC_API_KEY`; `tests/conftest.py` blocks non-loopback sockets.
- **Evidence for claims.** A new README claim needs a row in `CLAIMS.md` pointing to a test,
  bench script or CI job. Numbers must be copied from real `bench/` output with the command.
- **Threat model.** New entry points, tools or stored state need a STRIDE row in
  `docs/THREAT_MODEL.md` with a control and a test.
- **Decisions.** Record significant trade-offs as ADRs in `docs/adr/`.
- **No pickle; no secrets in git.** Document new settings in `.env.example` (a test enforces it).
- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/).
