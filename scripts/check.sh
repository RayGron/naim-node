#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"
db_path="${PWD}/var/controller.sqlite"
artifacts_root="${PWD}/var/artifacts"
runtime_root="${PWD}/var/runtime"
state_root="${PWD}/var/hostd-state"
bad_state_root="${PWD}/var/hostd-state-blocker"
infer_model_root="${PWD}/var/infer-model-state"
infer_model_config="${infer_model_root}/infer-runtime.json"
runtime_infer_config="${runtime_root}/runtime-infer-local.json"
parallel_db_path="${PWD}/var/controller-parallel.sqlite"
parallel_artifacts_root="${PWD}/var/artifacts-parallel"
parallel_runtime_root="${PWD}/var/runtime-parallel"
parallel_state_root="${PWD}/var/hostd-state-parallel"
preemption_artifacts_root="${PWD}/var/artifacts-preemption"
preemption_runtime_root="${PWD}/var/runtime-preemption"
preemption_state_root="${PWD}/var/hostd-state-preemption"
rebalance_db_path="${PWD}/var/controller-rebalance.sqlite"
rebalance_artifacts_root="${PWD}/var/artifacts-rebalance"
rebalance_runtime_root="${PWD}/var/runtime-rebalance"
rebalance_state_root="${PWD}/var/hostd-state-rebalance"
threshold_db_path="${PWD}/var/controller-threshold.sqlite"
threshold_artifacts_root="${PWD}/var/artifacts-threshold"
threshold_runtime_root="${PWD}/var/runtime-threshold"
threshold_state_root="${PWD}/var/hostd-state-threshold"
budget_db_path="${PWD}/var/controller-budget.sqlite"
budget_artifacts_root="${PWD}/var/artifacts-budget"
budget_runtime_root="${PWD}/var/runtime-budget"
budget_state_root="${PWD}/var/hostd-state-budget"
drain_db_path="${PWD}/var/controller-drain.sqlite"
drain_artifacts_root="${PWD}/var/artifacts-drain"
drain_runtime_root="${PWD}/var/runtime-drain"
drain_state_root="${PWD}/var/hostd-state-drain"
compute_db_path="${PWD}/var/controller-compute.sqlite"
compute_artifacts_root="${PWD}/var/artifacts-compute"
compute_runtime_root="${PWD}/var/runtime-compute"
compute_state_root="${PWD}/var/hostd-state-compute"
api_db_path="${PWD}/var/controller-api.sqlite"
api_artifacts_root="${PWD}/var/artifacts-api"
http_server_pid=""

cleanup() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

cmake -E rm -f "${db_path}"
cmake -E rm -f "${parallel_db_path}"
cmake -E rm -f "${rebalance_db_path}"
cmake -E rm -f "${threshold_db_path}"
cmake -E rm -f "${budget_db_path}"
cmake -E rm -f "${drain_db_path}"
cmake -E rm -f "${compute_db_path}"
cmake -E rm -f "${api_db_path}"
cmake -E remove_directory "${artifacts_root}"
cmake -E remove_directory "${parallel_artifacts_root}"
cmake -E remove_directory "${rebalance_artifacts_root}"
cmake -E remove_directory "${threshold_artifacts_root}"
cmake -E remove_directory "${budget_artifacts_root}"
cmake -E remove_directory "${drain_artifacts_root}"
cmake -E remove_directory "${compute_artifacts_root}"
cmake -E remove_directory "${api_artifacts_root}"
cmake -E remove_directory "${runtime_root}"
cmake -E remove_directory "${parallel_runtime_root}"
cmake -E remove_directory "${rebalance_runtime_root}"
cmake -E remove_directory "${threshold_runtime_root}"
cmake -E remove_directory "${budget_runtime_root}"
cmake -E remove_directory "${drain_runtime_root}"
cmake -E remove_directory "${compute_runtime_root}"
cmake -E remove_directory "${preemption_artifacts_root}"
cmake -E remove_directory "${preemption_runtime_root}"
cmake -E remove_directory "${state_root}"
cmake -E remove_directory "${parallel_state_root}"
cmake -E remove_directory "${rebalance_state_root}"
cmake -E remove_directory "${threshold_state_root}"
cmake -E remove_directory "${budget_state_root}"
cmake -E remove_directory "${drain_state_root}"
cmake -E remove_directory "${compute_state_root}"
cmake -E remove_directory "${preemption_state_root}"
cmake -E remove_directory "${infer_model_root}"
cmake -E rm -f "${bad_state_root}"

"${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug

"${build_dir}/comet-controller" show-demo-plan >/dev/null
"${build_dir}/comet-controller" render-demo-compose --node node-a >/dev/null
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null
http_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
"${build_dir}/comet-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${http_port}" >/tmp/comet-controller-serve.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${http_port}/health" >/tmp/comet-controller-health.json 2>/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${http_port}/health" | grep -F '"service":"comet-controller"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/health" | grep -F '"status":"ok"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/state" | grep -F '"desired_generation":null' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/state" | grep -F '"desired_state":null' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-assignments" | grep -F '"assignments":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-observations" | grep -F '"observations":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-health" | grep -F '"items":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/disk-state" | grep -F '"items":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/rollout-actions" | grep -F '"actions":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/rebalance-plan" | grep -F '"rebalance_plan":[]' >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""
"${build_dir}/comet-controller" init-db --db "${api_db_path}" >/dev/null
http_api_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
"${build_dir}/comet-controller" serve --db "${api_db_path}" --listen-host 127.0.0.1 --listen-port "${http_api_port}" >/tmp/comet-controller-api-mutations.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${http_api_port}/health" >/tmp/comet-controller-api-mutations-health.json 2>/dev/null; then
    break
  fi
  sleep 0.1
done
api_validate_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/bundles/validate?bundle=${PWD}/config/demo-plane")"
printf '%s' "${api_validate_output}" | grep -F '"action":"validate-bundle"' >/dev/null
printf '%s' "${api_validate_output}" | grep -F 'bundle validation: OK' >/dev/null
api_preview_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/bundles/preview?bundle=${PWD}/config/demo-plane&node=node-a")"
printf '%s' "${api_preview_output}" | grep -F '"action":"preview-bundle"' >/dev/null
printf '%s' "${api_preview_output}" | grep -F 'preview:' >/dev/null
api_import_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/bundles/import?bundle=${PWD}/config/demo-plane")"
printf '%s' "${api_import_output}" | grep -F '"action":"import-bundle"' >/dev/null
printf '%s' "${api_import_output}" | grep -F "imported bundle '${PWD}/config/demo-plane'" >/dev/null
api_apply_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/bundles/apply?bundle=${PWD}/config/demo-plane&artifacts_root=${api_artifacts_root}")"
printf '%s' "${api_apply_output}" | grep -F '"action":"apply-bundle"' >/dev/null
printf '%s' "${api_apply_output}" | grep -F "applied bundle '${PWD}/config/demo-plane'" >/dev/null
api_availability_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/node-availability?node=node-b&availability=unavailable&message=api-http")"
printf '%s' "${api_availability_output}" | grep -F '"action":"set-node-availability"' >/dev/null
printf '%s' "${api_availability_output}" | grep -F 'updated node availability for node-b' >/dev/null
api_failed_assignment_id="$(python3 - <<'PY' "${api_db_path}"
import sqlite3, sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
row = conn.execute(
    "SELECT id FROM host_assignments WHERE status='pending' ORDER BY id LIMIT 1"
).fetchone()
if row is None:
    raise SystemExit("no pending assignment found for retry fixture")
conn.execute(
    "UPDATE host_assignments SET status='failed', status_message='api-fixture failed assignment' WHERE id=?",
    (row[0],),
)
conn.commit()
conn.close()
print(row[0])
PY
)"
test -n "${api_failed_assignment_id}"
api_retry_output="$(curl -fsS -X POST "http://127.0.0.1:${http_api_port}/api/v1/retry-host-assignment?id=${api_failed_assignment_id}")"
printf '%s' "${api_retry_output}" | grep -F '"action":"retry-host-assignment"' >/dev/null
printf '%s' "${api_retry_output}" | grep -F "requeued host assignment id=${api_failed_assignment_id}" >/dev/null
"${build_dir}/comet-controller" show-state --controller "http://127.0.0.1:${http_api_port}" | grep -F '"desired_generation": 2' >/dev/null
"${build_dir}/comet-controller" show-host-assignments --controller "http://127.0.0.1:${http_api_port}" --node node-a | grep -F '"assignments"' >/dev/null
"${build_dir}/comet-controller" show-node-availability --controller "http://127.0.0.1:${http_api_port}" --node node-b | grep -F '"availability": "unavailable"' >/dev/null
"${build_dir}/comet-controller" validate-bundle --controller "http://127.0.0.1:${http_api_port}" --bundle "${PWD}/config/demo-plane" | grep -F 'bundle validation: OK' >/dev/null
"${build_dir}/comet-controller" set-node-availability --controller "http://127.0.0.1:${http_api_port}" --node node-a --availability draining --message cli-http | grep -F 'updated node availability for node-a' >/dev/null
COMET_CONTROLLER="http://127.0.0.1:${http_api_port}" "${build_dir}/comet-controller" show-node-availability --node node-a | grep -F '"availability": "draining"' >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""
"${build_dir}/comet-controller" validate-bundle --bundle "${PWD}/config/demo-plane" >/dev/null
invalid_bundle_dir="$(mktemp -d "${PWD}/var/invalid-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${invalid_bundle_dir}"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"priority": 100/"priority": 250/; s/"memory_cap_mb": 8192/"memory_cap_mb": 12288/' "${invalid_bundle_dir}/workers/worker-b.json"
if "${build_dir}/comet-controller" validate-bundle --bundle "${invalid_bundle_dir}" >/tmp/comet-invalid-bundle.txt 2>&1; then
  echo "check: expected invalid scheduler bundle to be rejected" >&2
  exit 1
fi
grep -F "mixes share_mode=exclusive with other workers" /tmp/comet-invalid-bundle.txt >/dev/null
grep -F "gpu memory oversubscription" /tmp/comet-invalid-bundle.txt >/dev/null
auto_bundle_dir="$(mktemp -d "${PWD}/var/auto-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${auto_bundle_dir}"
perl -0pi -e 's/^\s*"node": "node-b",\n//m; s/^\s*"gpu_device": "0",\n//m;' "${auto_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" validate-bundle --bundle "${auto_bundle_dir}" | grep -F "node=node-a gpu=1 workers=worker-b exclusive=worker-b" >/dev/null
preemption_bundle_dir="$(mktemp -d "${PWD}/var/preemption-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${preemption_bundle_dir}"
perl -0pi -e 's/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.75/; s/"memory_cap_mb": 16384/"memory_cap_mb": 12288/' "${preemption_bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"priority": 100/"priority": 50/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/; s/"preemptible": false/"preemptible": true/' "${preemption_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_bundle_dir}" | grep -F "preemption-hints:" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_bundle_dir}" | grep -F "node=node-a gpu=0 victims=worker-b reason=protect guaranteed workers before best-effort workers on the same GPU" >/dev/null
preemption_candidate_bundle_dir="$(mktemp -d "${PWD}/var/preemption-candidate-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${preemption_candidate_bundle_dir}"
perl -0pi -e 's/"gpus": \["0", "1"\]/"gpus": ["0"]/; s/"0": 24576,\n\s*"1": 24576/"0": 24576/' "${preemption_candidate_bundle_dir}/plane.json"
perl -0pi -e 's/"node": "node-a"/"node": "node-b"/; s/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.75/; s/"memory_cap_mb": 16384/"memory_cap_mb": 12288/; s/"name": "worker-a",/"name": "worker-a",\n  "placement_mode": "movable",/' "${preemption_candidate_bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"priority": 100/"priority": 50/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/' "${preemption_candidate_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "action=preempt-best-effort-to-exclusive" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "victims=worker-b" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "worker=worker-a decision=deferred next_action=preempt-best-effort-to-exclusive next_target=node-a:0 victims=worker-b defer_reason=requires-controlled-preemption" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "rollout-actions:" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "step=1 worker=worker-a action=evict-best-effort target=node-a:0 victims=worker-b" >/dev/null
"${build_dir}/comet-controller" validate-bundle --bundle "${preemption_candidate_bundle_dir}" | grep -F "step=2 worker=worker-a action=retry-placement target=node-a:0" >/dev/null
preemption_db_path="${PWD}/var/controller-preemption.sqlite"
cmake -E rm -f "${preemption_db_path}"
"${build_dir}/comet-controller" init-db --db "${preemption_db_path}" >/dev/null
"${build_dir}/comet-controller" import-bundle --bundle "${preemption_candidate_bundle_dir}" --db "${preemption_db_path}" | grep -F "rollout-gates:" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "message=scheduler rollout actions pending on target node node-a for workers worker-a" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${preemption_db_path}" | grep -F "worker=worker-a placement_mode=movable current=node-b:0 class=rollout-class decision=hold state=active-rollout target=node-a:0" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${preemption_db_path}" | grep -F "rebalance-loop-status:" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${preemption_db_path}" | grep -F "state=waiting-for-convergence reason=unconverged-nodes=2" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${preemption_db_path}" | grep -F "actionable=0 safe_direct=0 rollout_class=1 gated=0 blocked_active_rollouts=1 assignment_busy=0 observation_gated=0 stable_holds=0 below_threshold=0 deferred=0 no_candidate=0" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | grep -F "id=" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | grep -F "step=1 worker=worker-a action=evict-best-effort target=node-a:0 status=pending victims=worker-b" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | grep -F "step=2 worker=worker-a action=retry-placement target=node-a:0 status=pending" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | grep -F "phase=planned" >/dev/null
first_rollout_action_id="$("${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | sed -n 's/^  - id=\([0-9][0-9]*\).*/\1/p' | head -n 1)"
second_rollout_action_id="$("${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" --node node-a | sed -n 's/^  - id=\([0-9][0-9]*\).*/\1/p' | sed -n '2p')"
test -n "${first_rollout_action_id}"
test -n "${second_rollout_action_id}"
http_preemption_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
"${build_dir}/comet-controller" serve --db "${preemption_db_path}" --listen-host 127.0.0.1 --listen-port "${http_preemption_port}" >/tmp/comet-controller-preemption-serve.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${http_preemption_port}/api/v1/rollout-actions?node=node-a" >/tmp/comet-controller-preemption-api.json 2>/dev/null; then
    break
  fi
  sleep 0.1
done
preemption_set_output="$(curl -fsS -X POST "http://127.0.0.1:${http_preemption_port}/api/v1/set-rollout-action-status?id=${first_rollout_action_id}&status=acknowledged&message=api-http")"
printf '%s' "${preemption_set_output}" | grep -F '"action":"set-rollout-action-status"' >/dev/null
printf '%s' "${preemption_set_output}" | grep -F 'updated rollout action id='"${first_rollout_action_id}" >/dev/null
preemption_enqueue_output="$(curl -fsS -X POST "http://127.0.0.1:${http_preemption_port}/api/v1/enqueue-rollout-eviction?id=${first_rollout_action_id}")"
printf '%s' "${preemption_enqueue_output}" | grep -F '"action":"enqueue-rollout-eviction"' >/dev/null
printf '%s' "${preemption_enqueue_output}" | grep -F 'enqueued rollout eviction action id='"${first_rollout_action_id}" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "type=evict-workers status=pending" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${preemption_db_path}" --node node-a --runtime-root "${preemption_runtime_root}" --state-root "${preemption_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "type=evict-workers status=applied" >/dev/null
preemption_reconcile_output="$(curl -fsS -X POST "http://127.0.0.1:${http_preemption_port}/api/v1/reconcile-rollout-actions?artifacts_root=${preemption_artifacts_root}")"
printf '%s' "${preemption_reconcile_output}" | grep -F '"action":"reconcile-rollout-actions"' >/dev/null
printf '%s' "${preemption_reconcile_output}" | grep -F "applied ready rollout action id=${second_rollout_action_id}" >/dev/null
"${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "desired generation: 2" >/dev/null
"${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "worker-a role=worker node=node-a gpu=0 fraction=1 placement_mode=movable share_mode=exclusive priority=200 preemptible=false memory_cap_mb=12288 placement=auto placement_action=materialized-retry-placement placement_score=22 placement_decision=applied" >/dev/null
"${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "decision=applied" >/dev/null
"${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "phase=retry-materialized" >/dev/null
if "${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "worker-b role=worker" >/dev/null; then
  echo "check: expected worker-b to be evicted from desired state after materialized retry placement" >&2
  exit 1
fi
"${build_dir}/comet-controller" show-rollout-actions --db "${preemption_db_path}" | grep -F "(empty)" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "generation=2 attempts=0/3 type=apply-node-state status=pending" >/dev/null
if "${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "scheduler rollout actions pending on target node" >/dev/null; then
  echo "check: expected rollout gate message to disappear after materialized retry placement" >&2
  exit 1
fi
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""
"${build_dir}/comet-hostd" apply-next-assignment --db "${preemption_db_path}" --node node-a --runtime-root "${preemption_runtime_root}" --state-root "${preemption_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${preemption_db_path}" --node node-a | grep -F "generation=2 attempts=1/3 type=apply-node-state status=applied" >/dev/null
"${build_dir}/comet-controller" show-state --db "${preemption_db_path}" | grep -F "phase=rollout-applied" >/dev/null
movable_bundle_dir="$(mktemp -d "${PWD}/var/movable-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${movable_bundle_dir}"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' "${movable_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" validate-bundle --bundle "${movable_bundle_dir}" | grep -F "worker=worker-b decision=proposed next_action=upgrade-to-exclusive next_target=node-a:1" >/dev/null
"${build_dir}/comet-controller" init-db --db "${rebalance_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${movable_bundle_dir}" --db "${rebalance_db_path}" --artifacts-root "${rebalance_artifacts_root}" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${rebalance_db_path}" --artifacts-root "${rebalance_artifacts_root}" | grep -F "cluster_ready=no active_rollouts=0 blocking_assignment_nodes=2 unconverged_nodes=2" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${rebalance_db_path}" --artifacts-root "${rebalance_artifacts_root}" | grep -F "state=waiting-for-convergence reason=unconverged-nodes=2" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${rebalance_db_path}" --artifacts-root "${rebalance_artifacts_root}" | grep -F "rebalance proposals: blocked by controller gate" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=gated decision=hold state=assignment-in-flight target=node-a:1 action=upgrade-to-exclusive" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${rebalance_db_path}" --node node-a --runtime-root "${rebalance_runtime_root}" --state-root "${rebalance_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${rebalance_db_path}" --node node-b --runtime-root "${rebalance_runtime_root}" --state-root "${rebalance_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=safe-direct decision=propose state=ready-move target=node-a:1 action=upgrade-to-exclusive" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "cluster_ready=yes active_rollouts=0 blocking_assignment_nodes=0 unconverged_nodes=0" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "state=actionable reason=safe-direct-workers=1" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "actionable=1 safe_direct=1 rollout_class=0 gated=0 blocked_active_rollouts=0 assignment_busy=0 observation_gated=0 stable_holds=0 below_threshold=0 deferred=0 no_candidate=0" >/dev/null
http_rebalance_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
"${build_dir}/comet-controller" serve --db "${rebalance_db_path}" --listen-host 127.0.0.1 --listen-port "${http_rebalance_port}" >/tmp/comet-controller-rebalance-serve.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${http_rebalance_port}/api/v1/rebalance-plan" >/tmp/comet-controller-rebalance-api.json 2>/dev/null; then
    break
  fi
  sleep 0.1
done
rebalance_apply_output="$(curl -fsS -X POST "http://127.0.0.1:${http_rebalance_port}/api/v1/apply-rebalance-proposal?worker=worker-b&artifacts_root=${rebalance_artifacts_root}")"
printf '%s' "${rebalance_apply_output}" | grep -F '"action":"apply-rebalance-proposal"' >/dev/null
printf '%s' "${rebalance_apply_output}" | grep -F "applied rebalance proposal for worker 'worker-b'" >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""
"${build_dir}/comet-controller" show-state --db "${rebalance_db_path}" | grep -F "desired generation: 2" >/dev/null
"${build_dir}/comet-controller" show-state --db "${rebalance_db_path}" | grep -F "worker-b role=worker node=node-a gpu=1 fraction=1 placement_mode=movable share_mode=exclusive priority=100 preemptible=true memory_cap_mb=8192 placement=auto placement_action=materialized-rebalance-upgrade-to-exclusive placement_score=145 placement_decision=applied" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${rebalance_db_path}" | grep -F "plane_action=rebalance phase=verifying-move worker=worker-b generation=2 stable_samples=0/3 rollback_attempts=0" >/dev/null
budget_bundle_dir="$(mktemp -d "${PWD}/var/budget-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${budget_bundle_dir}"
cat > "${budget_bundle_dir}/plane.json" <<'EOF'
{
  "name": "alpha",
  "control_root": "/comet/shared/control/alpha",
  "runtime": {
    "primary_infer_node": "node-a",
    "net_if": "eth0",
    "models_root": "/comet/shared/models",
    "gguf_cache_dir": "/comet/shared/models/gguf",
    "infer_log_dir": "/comet/shared/logs/infer",
    "llama_port": 8000,
    "llama_ctx_size": 8192,
    "llama_threads": 8,
    "llama_gpu_layers": 99,
    "inference_healthcheck_retries": 300,
    "inference_healthcheck_interval_sec": 5
  },
  "gateway": {
    "listen_host": "0.0.0.0",
    "listen_port": 8080,
    "server_name": "alpha.local"
  },
  "shared_disk_gb": 200,
  "nodes": [
    {
      "name": "node-a",
      "platform": "linux",
      "gpus": ["0"],
      "gpu_memory_mb": {
        "0": 24576
      }
    },
    {
      "name": "node-b",
      "platform": "linux",
      "gpus": ["0", "1"],
      "gpu_memory_mb": {
        "0": 24576,
        "1": 24576
      }
    },
    {
      "name": "node-c",
      "platform": "linux",
      "gpus": ["0", "1"],
      "gpu_memory_mb": {
        "0": 24576,
        "1": 24576
      }
    }
  ]
}
EOF
cat > "${budget_bundle_dir}/workers/worker-a.json" <<'EOF'
{
  "name": "worker-a",
  "node": "node-a",
  "gpu_device": "0",
  "share_mode": "exclusive",
  "gpu_fraction": 1.0,
  "priority": 200,
  "preemptible": false,
  "memory_cap_mb": 16384,
  "private_disk_gb": 40,
  "image": "comet/worker-runtime:dev"
}
EOF
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/' "${budget_bundle_dir}/workers/worker-b.json"
cat > "${budget_bundle_dir}/workers/worker-c.json" <<'EOF'
{
  "name": "worker-c",
  "node": "node-c",
  "gpu_device": "0",
  "placement_mode": "movable",
  "share_mode": "shared",
  "gpu_fraction": 0.25,
  "priority": 90,
  "preemptible": true,
  "memory_cap_mb": 4096,
  "private_disk_gb": 40,
  "image": "comet/worker-runtime:dev"
}
EOF
"${build_dir}/comet-controller" init-db --db "${budget_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${budget_bundle_dir}" --db "${budget_db_path}" --artifacts-root "${budget_artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-a --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-b --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-c --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "iteration=0/1 exhausted=no" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "state=actionable reason=safe-direct-workers=2" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=safe-direct decision=propose state=ready-in-place-upgrade target=node-b:1 action=upgrade-to-exclusive score=185 preemption_required=no" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "worker=worker-c placement_mode=movable current=node-c:0 class=safe-direct decision=propose state=ready-in-place-upgrade target=node-c:1 action=upgrade-to-exclusive score=185 preemption_required=no" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${budget_db_path}" --artifacts-root "${budget_artifacts_root}" | grep -F "applied rebalance proposal for worker 'worker-b'" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-a --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-b --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${budget_db_path}" --node node-c --runtime-root "${budget_runtime_root}" --state-root "${budget_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "iteration=1/1 exhausted=yes" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "state=waiting-for-convergence reason=active-rollouts=1" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "worker=worker-c placement_mode=movable current=node-c:0 class=gated decision=hold state=active-scheduler-action" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${budget_db_path}" | grep -F "plane_action=rebalance phase=verifying-move worker=worker-b generation=2 stable_samples=0/3 rollback_attempts=0" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${budget_db_path}" --artifacts-root "${budget_artifacts_root}" | grep -F "rebalance proposals: blocked by controller gate" >/dev/null
threshold_bundle_dir="$(mktemp -d "${PWD}/var/threshold-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${threshold_bundle_dir}"
perl -0pi -e 's/"gpus": \["0", "1"\]/"gpus": ["0"]/; s/"0": 24576,\n\s*"1": 24576/"0": 24576/' "${threshold_bundle_dir}/plane.json"
perl -0pi -e 's/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.25/; s/"memory_cap_mb": 16384/"memory_cap_mb": 4096/' "${threshold_bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' "${threshold_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" init-db --db "${threshold_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${threshold_bundle_dir}" --db "${threshold_db_path}" --artifacts-root "${threshold_artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${threshold_db_path}" --node node-a --runtime-root "${threshold_runtime_root}" --state-root "${threshold_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${threshold_db_path}" --node node-b --runtime-root "${threshold_runtime_root}" --state-root "${threshold_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${threshold_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=stable decision=hold state=below-threshold target=node-a:0 action=move-with-current-fraction score=32 preemption_required=no gate_reason=score-below-threshold(32<100)" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${threshold_db_path}" | grep -F "state=complete reason=remaining-moves-below-threshold=1" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${threshold_db_path}" | grep -F "actionable=0 safe_direct=0 rollout_class=0 gated=0 blocked_active_rollouts=0 assignment_busy=0 observation_gated=0 stable_holds=1 below_threshold=1 deferred=0 no_candidate=0" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${threshold_db_path}" --artifacts-root "${threshold_artifacts_root}" | grep -F "rebalance proposals: none actionable" >/dev/null
drain_bundle_dir="$(mktemp -d "${PWD}/var/drain-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${drain_bundle_dir}"
perl -0pi -e 's/"gpus": \["0"\]/"gpus": ["0", "1"]/; s/"0": 24576/"0": 24576,\n        "1": 24576/' "${drain_bundle_dir}/plane.json"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' "${drain_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" init-db --db "${drain_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${drain_bundle_dir}" --db "${drain_db_path}" --artifacts-root "${drain_artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${drain_db_path}" --node node-a --runtime-root "${drain_runtime_root}" --state-root "${drain_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${drain_db_path}" --node node-b --runtime-root "${drain_runtime_root}" --state-root "${drain_state_root}" --compose-mode skip >/dev/null
python3 - <<'PY' "${drain_db_path}"
import sqlite3, sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.execute(
    "INSERT INTO node_availability_overrides(node_name, availability, status_message, updated_at) "
    "VALUES(?, ?, ?, datetime('now')) "
    "ON CONFLICT(node_name) DO UPDATE SET availability=excluded.availability, status_message=excluded.status_message, updated_at=excluded.updated_at",
    ("node-b", "draining", "test fixture"),
)
conn.commit()
conn.close()
PY
"${build_dir}/comet-controller" show-rebalance-plan --db "${drain_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=safe-direct decision=propose state=ready-drain-move target=node-a:1 action=upgrade-to-exclusive" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${drain_db_path}" | grep -F "actionable=1 safe_direct=1 rollout_class=0 gated=0 blocked_active_rollouts=0 assignment_busy=0 observation_gated=0 stable_holds=0 below_threshold=0 deferred=0 no_candidate=0" >/dev/null
compute_bundle_dir="$(mktemp -d "${PWD}/var/compute-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${compute_bundle_dir}"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' "${compute_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" init-db --db "${compute_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${compute_bundle_dir}" --db "${compute_db_path}" --artifacts-root "${compute_artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${compute_db_path}" --node node-a --runtime-root "${compute_runtime_root}" --state-root "${compute_state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${compute_db_path}" --node node-b --runtime-root "${compute_runtime_root}" --state-root "${compute_state_root}" --compose-mode skip >/dev/null
python3 - <<'PY' "${compute_db_path}"
import json, sqlite3, sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
telemetry = {
    "degraded": False,
    "source": "fixture",
    "devices": [
        {
            "gpu_device": "1",
            "total_vram_mb": 24576,
            "used_vram_mb": 2048,
            "free_vram_mb": 22000,
            "gpu_utilization_pct": 97,
            "processes": [
                {"pid": 4242, "used_vram_mb": 1024, "instance_name": "foreign-load"}
            ],
        }
    ],
}
conn.execute(
    "UPDATE host_observations SET status=?, applied_generation=?, gpu_telemetry_json=?, heartbeat_at=datetime('now') WHERE node_name=?",
    ("idle", 1, json.dumps(telemetry), "node-a"),
)
conn.commit()
conn.close()
PY
"${build_dir}/comet-controller" show-rebalance-plan --db "${compute_db_path}" | grep -F "worker=worker-b placement_mode=movable current=node-b:0 class=gated decision=hold state=gated-target target=node-a:1 action=upgrade-to-exclusive" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${compute_db_path}" | grep -F "gate_reason=compute-pressure" >/dev/null
python3 - <<'PY' "${compute_db_path}"
import json, sqlite3, sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
telemetry = {
    "degraded": False,
    "source": "fixture",
    "devices": [
        {
            "gpu_device": "1",
            "total_vram_mb": 24576,
            "used_vram_mb": 22000,
            "free_vram_mb": 2048,
            "gpu_utilization_pct": 10,
            "processes": [],
        }
    ],
}
conn.execute(
    "UPDATE host_observations SET status=?, applied_generation=?, gpu_telemetry_json=?, heartbeat_at=datetime('now') WHERE node_name=?",
    ("idle", 1, json.dumps(telemetry), "node-a"),
)
conn.commit()
conn.close()
PY
"${build_dir}/comet-controller" show-rebalance-plan --db "${compute_db_path}" | grep -F "gate_reason=observed-insufficient-vram" >/dev/null
"${build_dir}/comet-controller" show-rebalance-plan --db "${compute_db_path}" | grep -F "observation_gated=1" >/dev/null
"${build_dir}/comet-controller" preview-bundle --bundle "${PWD}/config/demo-plane" --node node-a >/dev/null
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" plan-bundle --bundle "${PWD}/config/demo-plane" --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" plan-host-ops --bundle "${PWD}/config/demo-plane" --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${PWD}/config/demo-plane" --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
"${build_dir}/comet-controller" show-node-availability --db "${db_path}" | grep -F "(empty)" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" show-host-health --db "${db_path}" | grep -F "health=unknown" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${http_port}" >/tmp/comet-controller-serve.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-assignments?node=node-a" >/tmp/comet-controller-api.json 2>/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-assignments?node=node-a" | grep -F '"node_name":"node-a"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-observations?node=node-a" | grep -F '"node_name":"node-a"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-observations?node=node-a" | grep -F '"observations":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-health?node=node-a" | grep -F '"node_name":"node-a"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/host-health?node=node-a" | grep -F '"health":"unknown"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/disk-state?node=node-a" | grep -F '"disk_name":"plane-alpha-shared"' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/rollout-actions?node=node-a" | grep -F '"actions":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/rebalance-plan?node=node-a" | grep -F '"rebalance_plan":[]' >/dev/null
curl -fsS "http://127.0.0.1:${http_port}/api/v1/rebalance-plan?node=node-a" | grep -F '"controller_gate"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/scheduler-tick?artifacts_root=${artifacts_root}" | grep -F '"action":"scheduler-tick"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/scheduler-tick?artifacts_root=${artifacts_root}" | grep -F 'step=rebalance-reconcile' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/reconcile-rebalance-proposals?artifacts_root=${artifacts_root}" | grep -F '"action":"reconcile-rebalance-proposals"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/reconcile-rebalance-proposals?artifacts_root=${artifacts_root}" | grep -F 'blocked by controller gate' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/reconcile-rollout-actions?artifacts_root=${artifacts_root}" | grep -F '"action":"reconcile-rollout-actions"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${http_port}/api/v1/reconcile-rollout-actions?artifacts_root=${artifacts_root}" | grep -F 'no rollout actions for current generation' >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""
"${build_dir}/comet-controller" render-infer-runtime --db "${db_path}" | grep -F '"gpu_nodes"' >/dev/null
"${build_dir}/comet-controller" render-compose --db "${db_path}" --node node-a >/dev/null
test -f "${artifacts_root}/alpha/node-a/docker-compose.yml"
test -f "${artifacts_root}/alpha/infer-runtime.json"
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh validate-config --config "${artifacts_root}/alpha/infer-runtime.json" | grep -F "infer runtime config: OK" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh list-profiles | grep -F "generic" >/dev/null
test ! -e /mnt/e/dev/Repos/comet-node/runtime/infer/http_probe.py
test ! -e /mnt/e/dev/Repos/comet-node/runtime/infer/runtime_supervisor.py
test ! -e /mnt/e/dev/Repos/comet-node/runtime/infer/runtime_launcher.py
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh bootstrap-runtime --config "${artifacts_root}/alpha/infer-runtime.json" --profile generic | grep -F "runtime_mode=llama-library" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh plan-launch --config "${artifacts_root}/alpha/infer-runtime.json" | grep -F "primary-infer-local-worker=node:node-a worker:worker-a" >/dev/null
mkdir -p "${infer_model_root}"
perl -MJSON::PP -e '
  use strict;
  use warnings;
  use utf8;
  use Cwd qw(abs_path);
  use File::Spec;

  my ($src, $dst, $root) = @ARGV;
  open my $in, "<:raw", $src or die "open $src: $!";
  local $/;
  my $json_text = <$in>;
  close $in;

  my $data = JSON::PP->new->utf8->decode($json_text);
  my $control_root = File::Spec->catdir($root, "control");
  $data->{plane}->{control_root} = $control_root;
  $data->{control}->{root} = $control_root;

  my %path_map = (
    models_root => File::Spec->catdir($root, "models"),
    gguf_cache_dir => File::Spec->catdir($root, "models", "gguf"),
    infer_log_dir => File::Spec->catdir($root, "logs", "infer"),
  );

  for my $key (keys %path_map) {
    $data->{inference}->{$key} = $path_map{$key};
  }

  open my $out, ">:raw", $dst or die "open $dst: $!";
  print {$out} JSON::PP->new->utf8->canonical->pretty->encode($data);
  close $out;
' "${artifacts_root}/alpha/infer-runtime.json" "${infer_model_config}" "${infer_model_root}"
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh preload-model --config "${infer_model_config}" --alias qwen35 --source-model-id Qwen/Qwen3.5-7B-Instruct --local-model-path "${infer_model_root}/models/qwen35" --apply | grep -F "preload-model-plan:" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh cache-status --config "${infer_model_config}" --alias qwen35 --local-model-path "${infer_model_root}/models/qwen35" | grep -F "registry=present" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh switch-model --config "${infer_model_config}" --model-id Qwen/Qwen3.5-7B-Instruct --tp 1 --pp 1 --gpu-memory-utilization 0.85 --runtime-profile qwen3_5 --apply | grep -F 'runtime_profile="qwen3_5"' >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh gateway-plan --config "${infer_model_config}" --apply | grep -F "upstream_models_url=http://127.0.0.1:8000/v1/models" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh gateway-status --config "${infer_model_config}" | grep -F "active_model=Qwen/Qwen3.5-7B-Instruct served=Qwen3.5-7B-Instruct" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh status --config "${infer_model_config}" | grep -F "runtime_phase=planned" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh status --config "${infer_model_config}" | grep -F "launch_ready=no" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh show-active-model --config "${infer_model_config}" | grep -F '"model_id": "Qwen/Qwen3.5-7B-Instruct"' >/dev/null
"${build_dir}/comet-hostd" show-demo-ops --node node-b >/dev/null
"${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-b | grep -F "status=idle" >/dev/null
"${build_dir}/comet-controller" show-host-health --db "${db_path}" --node node-b | grep -F "health=online status=idle" >/dev/null
"${build_dir}/comet-hostd" show-state-ops --db "${db_path}" --node node-a --artifacts-root "${artifacts_root}" --runtime-root "${runtime_root}" --state-root "${state_root}" | grep -F "write-infer-runtime" >/dev/null
"${build_dir}/comet-hostd" apply-state-ops --db "${db_path}" --node node-a --artifacts-root "${artifacts_root}" --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" show-runtime-status --node node-a --state-root "${state_root}" | grep -F "runtime_status: empty" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "disk-runtime-state:" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "disk=plane-alpha-shared node=node-a state=directory-backed" >/dev/null
perl -MJSON::PP -e '
  use strict;
  use warnings;
  use utf8;
  use File::Spec;

  my ($src, $dst, $runtime_root) = @ARGV;
  open my $in, "<:raw", $src or die "open $src: $!";
  local $/;
  my $json_text = <$in>;
  close $in;

  my $data = JSON::PP->new->utf8->decode($json_text);
  my $shared_root = File::Spec->catdir($runtime_root, "var", "lib", "comet", "disks", "planes", "alpha", "shared");
  my $control_root = File::Spec->catdir($shared_root, "control", "alpha");
  $data->{plane}->{control_root} = $control_root;
  $data->{control}->{root} = $control_root;

  my %path_map = (
    models_root => File::Spec->catdir($shared_root, "models"),
    gguf_cache_dir => File::Spec->catdir($shared_root, "models", "gguf"),
    infer_log_dir => File::Spec->catdir($shared_root, "logs", "infer"),
  );

  for my $key (keys %path_map) {
    $data->{inference}->{$key} = $path_map{$key};
  }

  open my $out, ">:raw", $dst or die "open $dst: $!";
  print {$out} JSON::PP->new->utf8->canonical->pretty->encode($data);
  close $out;
' "${artifacts_root}/alpha/infer-runtime.json" "${runtime_infer_config}" "${runtime_root}"
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh preload-model --config "${runtime_infer_config}" --alias qwen35 --source-model-id Qwen/Qwen3.5-7B-Instruct --local-model-path "${runtime_root}/var/lib/comet/disks/planes/alpha/shared/models/qwen35" --apply | grep -F "preload-model-plan:" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh switch-model --config "${runtime_infer_config}" --model-id Qwen/Qwen3.5-7B-Instruct --tp 1 --pp 1 --gpu-memory-utilization 0.85 --runtime-profile qwen3_5 --apply | grep -F 'runtime_profile="qwen3_5"' >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh gateway-plan --config "${runtime_infer_config}" --apply | grep -F "upstream_models_url=http://127.0.0.1:8000/v1/models" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh status --config "${runtime_infer_config}" --apply | grep -F "launch_ready=no" >/dev/null
"${build_dir}/comet-hostd" show-runtime-status --node node-a --state-root "${state_root}" | grep -F "launch_ready=no" >/dev/null
"${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "runtime_launch_ready=no runtime_model=Qwen/Qwen3.5-7B-Instruct" >/dev/null
/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh stop --config "${runtime_infer_config}" --apply | grep -F "launch_ready=no" >/dev/null
"${build_dir}/comet-hostd" show-runtime-status --node node-a --state-root "${state_root}" | grep -F "launch_ready=no" >/dev/null
"${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "runtime_launch_ready=no runtime_model=(empty)" >/dev/null
"${build_dir}/comet-hostd" show-state-ops --db "${db_path}" --node node-b --artifacts-root "${artifacts_root}" --runtime-root "${runtime_root}" --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=0/3 type=apply-node-state status=pending" >/dev/null
: > "${bad_state_root}"
if "${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${bad_state_root}" --compose-mode skip >/dev/null 2>&1; then
  echo "check: expected first blocked assignment attempt to fail" >&2
  exit 1
fi
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=1/3 type=apply-node-state status=pending" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-b | grep -F "status=failed" >/dev/null
"${build_dir}/comet-controller" plan-bundle --bundle "${PWD}/config/demo-plane" --db "${db_path}" | grep -F "skipped_nodes=node-b(failed)" >/dev/null
if "${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${bad_state_root}" --compose-mode skip >/dev/null 2>&1; then
  echo "check: expected second blocked assignment attempt to fail" >&2
  exit 1
fi
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=2/3 type=apply-node-state status=pending" >/dev/null
if "${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${bad_state_root}" --compose-mode skip >/dev/null 2>&1; then
  echo "check: expected third blocked assignment attempt to fail" >&2
  exit 1
fi
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=3/3 type=apply-node-state status=failed" >/dev/null
"${build_dir}/comet-controller" retry-host-assignment --db "${db_path}" --id 2 >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=3/4 type=apply-node-state status=pending" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" show-local-state --node node-b --state-root "${state_root}" >/dev/null
"${build_dir}/comet-hostd" show-state-ops --db "${db_path}" --node node-b --artifacts-root "${artifacts_root}" --runtime-root "${runtime_root}" --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "disk=worker-b-private node=node-b state=directory-backed" >/dev/null
python3 - <<'PY' "${db_path}"
import sqlite3
import sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.execute(
    "DELETE FROM disk_runtime_state WHERE disk_name = ? AND node_name = ?",
    ("worker-b-private", "node-b"),
)
conn.commit()
conn.close()
PY
rm -rf "${runtime_root}/nodes/node-b/var/lib/comet/disks/instances/worker-b/private"
"${build_dir}/comet-hostd" apply-state-ops --db "${db_path}" --node node-b --artifacts-root "${artifacts_root}" --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
test -d "${runtime_root}/nodes/node-b/var/lib/comet/disks/instances/worker-b/private"
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "disk=worker-b-private node=node-b state=directory-backed-fallback" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "disk=worker-b-private kind=worker-private node=node-b" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "realized_state=directory-backed-fallback" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "attempts=4/4 type=apply-node-state status=applied" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-b | grep -F "status=applied applied_generation=1" >/dev/null
"${build_dir}/comet-controller" show-host-health --db "${db_path}" --node node-b | grep -F "health=online status=applied applied_generation=1" >/dev/null
"${build_dir}/comet-controller" set-node-availability --db "${db_path}" --node node-b --availability unavailable --message "check maintenance" >/dev/null
"${build_dir}/comet-controller" show-node-availability --db "${db_path}" --node node-b | grep -F "availability=unavailable" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "type=drain-node-state status=pending" >/dev/null
"${build_dir}/comet-controller" plan-bundle --bundle "${PWD}/config/demo-plane" --db "${db_path}" | grep -F "skipped_nodes=node-b(unavailable)" >/dev/null
"${build_dir}/comet-controller" set-node-availability --db "${db_path}" --node node-b --availability active --message "back online" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F "generation=1 attempts=0/3 type=apply-node-state status=pending" >/dev/null

"${build_dir}/comet-controller" init-db --db "${parallel_db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${PWD}/config/demo-plane" --db "${parallel_db_path}" --artifacts-root "${parallel_artifacts_root}" >/dev/null
(
  "${build_dir}/comet-hostd" apply-next-assignment --db "${parallel_db_path}" --node node-a --runtime-root "${parallel_runtime_root}" --state-root "${parallel_state_root}" --compose-mode skip >/dev/null
) &
parallel_pid_a=$!
(
  "${build_dir}/comet-hostd" apply-next-assignment --db "${parallel_db_path}" --node node-b --runtime-root "${parallel_runtime_root}" --state-root "${parallel_state_root}" --compose-mode skip >/dev/null
) &
parallel_pid_b=$!
wait "${parallel_pid_a}"
wait "${parallel_pid_b}"
"${build_dir}/comet-controller" show-host-assignments --db "${parallel_db_path}" --node node-a | grep -F "status=applied" >/dev/null
"${build_dir}/comet-controller" show-host-assignments --db "${parallel_db_path}" --node node-b | grep -F "status=applied" >/dev/null

test -d "${runtime_root}/var/lib/comet/disks/planes/alpha/shared"
test -f "${runtime_root}/var/lib/comet/disks/planes/alpha/shared/control/alpha/infer-runtime.json"
test -f "${infer_model_root}/control/model-cache-registry.json"
test -f "${infer_model_root}/control/active-model.json"
test -f "${infer_model_root}/control/gateway-plan.json"
test -f "${runtime_root}/var/lib/comet/disks/planes/alpha/shared/control/alpha/runtime-status.json"
test -d "${runtime_root}/nodes/node-b/var/lib/comet/disks/instances/worker-b/private"
test -f "${artifacts_root}/alpha/node-b/docker-compose.yml"
test -f "${state_root}/node-b/applied-state.json"
test -f "${state_root}/node-b/applied-generation.txt"

echo "check: OK"
