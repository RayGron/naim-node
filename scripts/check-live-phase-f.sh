#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"

skip_build=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) skip_build=1 ;;
    *)
      echo "usage: $0 [--skip-build]" >&2
      exit 1
      ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug
fi

basic_db="${PWD}/var/controller-phase-f-live.sqlite"
basic_artifacts="${PWD}/var/artifacts-phase-f-live"
safe_db="${PWD}/var/controller-phase-f-safe.sqlite"
safe_artifacts="${PWD}/var/artifacts-phase-f-safe"
safe_runtime="${PWD}/var/runtime-phase-f-safe"
safe_state="${PWD}/var/hostd-state-phase-f-safe"
preemption_db="${PWD}/var/controller-phase-f-preemption.sqlite"
preemption_artifacts="${PWD}/var/artifacts-phase-f-preemption"
preemption_runtime="${PWD}/var/runtime-phase-f-preemption"
preemption_state="${PWD}/var/hostd-state-phase-f-preemption"
http_server_pid=""

cleanup() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cmake -E rm -f "${basic_db}" "${safe_db}" "${preemption_db}"
cmake -E remove_directory "${basic_artifacts}"
cmake -E remove_directory "${safe_artifacts}"
cmake -E remove_directory "${safe_runtime}"
cmake -E remove_directory "${safe_state}"
cmake -E remove_directory "${preemption_artifacts}"
cmake -E remove_directory "${preemption_runtime}"
cmake -E remove_directory "${preemption_state}"

next_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

start_server() {
  local db_path="$1"
  local port="$2"
  "${build_dir}/comet-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${port}" >/tmp/comet-phase-f-serve.log 2>&1 &
  http_server_pid="$!"
  for _ in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "phase-f-live: controller did not become ready on port ${port}" >&2
  exit 1
}

stop_server() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
    http_server_pid=""
  fi
}

echo "phase-f-live: basic API and CLI-over-HTTP"
"${build_dir}/comet-controller" init-db --db "${basic_db}" >/dev/null
basic_port="$(next_port)"
start_server "${basic_db}" "${basic_port}"

curl -fsS "http://127.0.0.1:${basic_port}/health" | grep -F '"service":"comet-controller"' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/health" | grep -F '"api_version":"v1"' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/state" | grep -F '"desired_generation":null' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/host-assignments" | grep -F '"assignments":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/host-observations" | grep -F '"observations":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/host-health" | grep -F '"items":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/node-availability" | grep -F '"items":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/disk-state" | grep -F '"items":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/rollout-actions" | grep -F '"actions":[]' >/dev/null
curl -fsS "http://127.0.0.1:${basic_port}/api/v1/rebalance-plan" | grep -F '"rebalance_plan":[]' >/dev/null

curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/bundles/validate?bundle=${PWD}/config/demo-plane" | grep -F '"action":"validate-bundle"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/bundles/preview?bundle=${PWD}/config/demo-plane&node=node-a" | grep -F '"action":"preview-bundle"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/bundles/import?bundle=${PWD}/config/demo-plane" | grep -F '"action":"import-bundle"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/bundles/apply?bundle=${PWD}/config/demo-plane&artifacts_root=${basic_artifacts}" | grep -F '"action":"apply-bundle"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/node-availability?node=node-b&availability=unavailable&message=phase-f-live" | grep -F '"action":"set-node-availability"' >/dev/null
failed_assignment_id="$(python3 - <<'PY' "${basic_db}"
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT id FROM host_assignments WHERE status='pending' ORDER BY id LIMIT 1").fetchone()
if row is None:
    raise SystemExit("no pending assignment found")
conn.execute("UPDATE host_assignments SET status='failed', status_message='phase-f-live fixture' WHERE id=?", (row[0],))
conn.commit()
conn.close()
print(row[0])
PY
)"
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/retry-host-assignment?id=${failed_assignment_id}" | grep -F '"action":"retry-host-assignment"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/scheduler-tick?artifacts_root=${basic_artifacts}" | grep -F '"action":"scheduler-tick"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/reconcile-rebalance-proposals?artifacts_root=${basic_artifacts}" | grep -F '"action":"reconcile-rebalance-proposals"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${basic_port}/api/v1/reconcile-rollout-actions?artifacts_root=${basic_artifacts}" | grep -F '"action":"reconcile-rollout-actions"' >/dev/null

"${build_dir}/comet-controller" show-state --controller "http://127.0.0.1:${basic_port}" | grep -F '"desired_generation": 2' >/dev/null
"${build_dir}/comet-controller" show-host-assignments --controller "http://127.0.0.1:${basic_port}" --node node-a | grep -F '"assignments"' >/dev/null
"${build_dir}/comet-controller" show-node-availability --controller "http://127.0.0.1:${basic_port}" --node node-b | grep -F '"availability": "unavailable"' >/dev/null
"${build_dir}/comet-controller" validate-bundle --controller "http://127.0.0.1:${basic_port}" --bundle "${PWD}/config/demo-plane" | grep -F 'bundle validation: OK' >/dev/null
"${build_dir}/comet-controller" set-node-availability --controller "http://127.0.0.1:${basic_port}" --node node-a --availability draining --message cli-phase-f | grep -F 'updated node availability for node-a' >/dev/null
COMET_CONTROLLER="http://127.0.0.1:${basic_port}" "${build_dir}/comet-controller" show-node-availability --node node-a | grep -F '"availability": "draining"' >/dev/null
stop_server

echo "phase-f-live: safe-direct rebalance mutation"
safe_bundle_dir="$(mktemp -d "${PWD}/var/phase-f-safe-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${safe_bundle_dir}"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' "${safe_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" init-db --db "${safe_db}" >/dev/null
safe_port="$(next_port)"
curl_safe_output="$("${build_dir}/comet-controller" apply-bundle --bundle "${safe_bundle_dir}" --db "${safe_db}" --artifacts-root "${safe_artifacts}")"
printf '%s' "${curl_safe_output}" | grep -F "applied bundle '${safe_bundle_dir}'" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${safe_db}" --node node-a --runtime-root "${safe_runtime}" --state-root "${safe_state}" --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${safe_db}" --node node-b --runtime-root "${safe_runtime}" --state-root "${safe_state}" --compose-mode skip >/dev/null
start_server "${safe_db}" "${safe_port}"
curl -fsS "http://127.0.0.1:${safe_port}/api/v1/rebalance-plan" | grep -F '"worker_name":"worker-b"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${safe_port}/api/v1/apply-rebalance-proposal?worker=worker-b&artifacts_root=${safe_artifacts}" | grep -F '"action":"apply-rebalance-proposal"' >/dev/null
"${build_dir}/comet-controller" show-state --controller "http://127.0.0.1:${safe_port}" | grep -F '"desired_generation": 2' >/dev/null
stop_server

echo "phase-f-live: rollout/preemption mutations"
preemption_bundle_dir="$(mktemp -d "${PWD}/var/phase-f-preemption-bundle.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${preemption_bundle_dir}"
perl -0pi -e 's/"gpus": \["0", "1"\]/"gpus": ["0"]/; s/"0": 24576,\n\s*"1": 24576/"0": 24576/' "${preemption_bundle_dir}/plane.json"
perl -0pi -e 's/"node": "node-a"/"node": "node-b"/; s/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.75/; s/"memory_cap_mb": 16384/"memory_cap_mb": 12288/; s/"name": "worker-a",/"name": "worker-a",\n  "placement_mode": "movable",/' "${preemption_bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"priority": 100/"priority": 50/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/' "${preemption_bundle_dir}/workers/worker-b.json"
"${build_dir}/comet-controller" init-db --db "${preemption_db}" >/dev/null
preemption_port="$(next_port)"
start_server "${preemption_db}" "${preemption_port}"
curl -fsS -X POST "http://127.0.0.1:${preemption_port}/api/v1/bundles/import?bundle=${preemption_bundle_dir}" | grep -F '"action":"import-bundle"' >/dev/null
curl -fsS "http://127.0.0.1:${preemption_port}/api/v1/rollout-actions?node=node-a" | grep -F '"actions":' >/dev/null
first_action_id="$(python3 - <<'PY' "${preemption_db}"
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT id FROM rollout_actions WHERE step=1 ORDER BY id LIMIT 1").fetchone()
if row is None:
    raise SystemExit("no first rollout action")
conn.close()
print(row[0])
PY
)"
second_action_id="$(python3 - <<'PY' "${preemption_db}"
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT id FROM rollout_actions WHERE step=2 ORDER BY id LIMIT 1").fetchone()
if row is None:
    raise SystemExit("no second rollout action")
conn.close()
print(row[0])
PY
)"
curl -fsS -X POST "http://127.0.0.1:${preemption_port}/api/v1/set-rollout-action-status?id=${first_action_id}&status=acknowledged&message=phase-f-live" | grep -F '"action":"set-rollout-action-status"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${preemption_port}/api/v1/enqueue-rollout-eviction?id=${first_action_id}" | grep -F '"action":"enqueue-rollout-eviction"' >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${preemption_db}" --node node-a --runtime-root "${preemption_runtime}" --state-root "${preemption_state}" --compose-mode skip >/dev/null
curl -fsS -X POST "http://127.0.0.1:${preemption_port}/api/v1/reconcile-rollout-actions?artifacts_root=${preemption_artifacts}" | grep -F '"action":"reconcile-rollout-actions"' >/dev/null

manual_bundle_dir="$(mktemp -d "${PWD}/var/phase-f-manual-rollout.XXXXXX")"
cp -R "${PWD}/config/demo-plane/." "${manual_bundle_dir}"
perl -0pi -e 's/"gpus": \["0", "1"\]/"gpus": ["0"]/; s/"0": 24576,\n\s*"1": 24576/"0": 24576/' "${manual_bundle_dir}/plane.json"
perl -0pi -e 's/"node": "node-a"/"node": "node-b"/; s/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.75/; s/"memory_cap_mb": 16384/"memory_cap_mb": 12288/; s/"name": "worker-a",/"name": "worker-a",\n  "placement_mode": "movable",/' "${manual_bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"priority": 100/"priority": 50/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/' "${manual_bundle_dir}/workers/worker-b.json"
manual_db="${PWD}/var/controller-phase-f-manual-rollout.sqlite"
manual_artifacts="${PWD}/var/artifacts-phase-f-manual-rollout"
cmake -E rm -f "${manual_db}"
cmake -E remove_directory "${manual_artifacts}"
"${build_dir}/comet-controller" init-db --db "${manual_db}" >/dev/null
manual_port="$(next_port)"
stop_server
start_server "${manual_db}" "${manual_port}"
curl -fsS -X POST "http://127.0.0.1:${manual_port}/api/v1/bundles/import?bundle=${manual_bundle_dir}" | grep -F '"action":"import-bundle"' >/dev/null
manual_second_action_id="$(python3 - <<'PY' "${manual_db}"
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT id FROM rollout_actions WHERE step=2 ORDER BY id LIMIT 1").fetchone()
if row is None:
    raise SystemExit("no retry rollout action")
conn.close()
print(row[0])
PY
)"
manual_first_action_id="$(python3 - <<'PY' "${manual_db}"
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT id FROM rollout_actions WHERE step=1 ORDER BY id LIMIT 1").fetchone()
if row is None:
    raise SystemExit("no eviction rollout action")
conn.close()
print(row[0])
PY
)"
curl -fsS -X POST "http://127.0.0.1:${manual_port}/api/v1/set-rollout-action-status?id=${manual_first_action_id}&status=ready-to-retry&message=phase-f-live" | grep -F '"action":"set-rollout-action-status"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${manual_port}/api/v1/set-rollout-action-status?id=${manual_second_action_id}&status=ready-to-retry&message=phase-f-live" | grep -F '"action":"set-rollout-action-status"' >/dev/null
curl -fsS -X POST "http://127.0.0.1:${manual_port}/api/v1/apply-ready-rollout-action?id=${manual_second_action_id}&artifacts_root=${manual_artifacts}" | grep -F '"action":"apply-ready-rollout-action"' >/dev/null
"${build_dir}/comet-controller" show-state --controller "http://127.0.0.1:${manual_port}" | grep -F '"desired_generation": 2' >/dev/null
stop_server

echo "phase-f-live: OK"
