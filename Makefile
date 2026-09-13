# Linux/CI convenience targets. Every target delegates to tasks.py, which is the single source
# of truth and also runs on Windows: `uv run python tasks.py <target>`.

UV ?= uv
TASK := $(UV) run python tasks.py
IMAGE ?= replayable-agent-runtime:local

.PHONY: install lint format typecheck test bench chaos demo bandit pip-audit audit baseline check docker-build compose-up compose-zombie

install:
	$(UV) sync --frozen

lint:
	$(TASK) lint

format:
	$(TASK) format

typecheck:
	$(TASK) typecheck

test:
	$(TASK) test

bench:
	$(TASK) bench

chaos:
	$(TASK) chaos

demo:
	$(TASK) demo

bandit:
	$(TASK) bandit

pip-audit:
	$(TASK) pip-audit

audit:
	$(TASK) audit

baseline:
	$(TASK) baseline

check:
	$(TASK) check

docker-build:
	docker build -t $(IMAGE) .

compose-up:
	docker compose up --build

compose-zombie:
	bash scripts/compose_zombie_test.sh
