#!/usr/bin/env bash
# Compose zombie-worker test (CI only; needs Docker). Not run as part of the local build.
#
# 1. Start the API and worker-a, whose fake knowledge-base service answers slowly.
# 2. Start a research run and wait until worker-a is inside the second knowledge-base read.
# 3. `docker compose pause worker-a` (a frozen process that still believes it holds the lease).
# 4. Wait past the lease, start worker-b: it reclaims the run with a higher fencing token,
#    re-executes the interrupted read and parks the run on its approvals; approve them via the CLI.
# 5. `docker compose unpause worker-a`: its late append must be fenced out.
# 6. Check: run completed, journal verifies, exactly one ticket write and one notification were
#    journaled, and worker-a logged lease_lost.
#
# This exercises journal fencing: worker-a is paused inside a read, so its late append (not a sink
# write) is what gets refused. Sink-level fencing against a paused *writer*, in every wake-up
# ordering, is covered in-process by tests/integration/test_zombie.py and bench/eval_suites.py.
set -euo pipefail

cd "$(dirname "$0")/.."
export APP_JWT_SECRET="${APP_JWT_SECRET:-$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')}"
export RAR_WORKER_A_LATENCY=4
RUN_ID=compose-zombie
rar() { docker compose exec -T api python -m replayable_agent_runtime "$@"; }
json() { python3 -c "import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

cleanup() {
  docker compose logs --no-color > compose-zombie.log 2>&1 || true
  docker compose down -v || true
}
trap cleanup EXIT

docker compose build
docker compose up -d api worker-a
sleep 5
rar runs start --workflow research_ticket --input '{"topic": "cache"}' --as alice --run-id "$RUN_ID" >/dev/null

for _ in $(seq 1 120); do
  in_flight=$(rar runs history "$RUN_ID" | json "sum(1 for e in data if e['type']=='StepScheduled' and e['payload'].get('name')=='http_get') - sum(1 for e in data if e['type']=='StepCompleted' and e['payload'].get('name')=='http_get')")
  scheduled=$(rar runs history "$RUN_ID" | json "sum(1 for e in data if e['type']=='StepScheduled' and e['payload'].get('name')=='http_get')")
  if [ "$scheduled" -ge 2 ] && [ "$in_flight" -ge 1 ]; then break; fi
  sleep 0.5
done
docker compose pause worker-a
sleep 15
docker compose up -d worker-b

for _ in $(seq 1 120); do
  status=$(rar runs show "$RUN_ID" | json "data['status']")
  if [ "$status" = "completed" ]; then break; fi
  rar approvals list --run-id "$RUN_ID" --status pending > pending.json
  count=$(json "len(data)" < pending.json)
  for index in $(seq 0 $((count - 1))); do
    approval_id=$(json "data[$index]['approval_id']" < pending.json)
    call_hash=$(json "data[$index]['call_hash']" < pending.json)
    rar approvals decide "$approval_id" --approve --call-hash "$call_hash" --as bob >/dev/null
  done
  sleep 1
done

docker compose unpause worker-a
sleep 10

test "$(rar runs show "$RUN_ID" | json "data['status']")" = "completed"
rar audit verify --run-id "$RUN_ID" >/dev/null
writes=$(rar runs history "$RUN_ID" | json "sum(1 for e in data if e['type']=='StepCompleted' and e['payload'].get('name') in ('create_ticket','notify'))")
test "$writes" = "2"
docker compose logs worker-a | grep -q lease_lost
echo "compose zombie test passed"
