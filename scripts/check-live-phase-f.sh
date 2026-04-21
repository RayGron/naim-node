#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"
build_dir="$("${script_dir}/print-build-dir.sh")"

skip_build=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) skip_build=1 ;;
    *) echo "usage: $0 [--skip-build]" >&2; exit 1 ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" Debug >/dev/null
fi

work_root="${PWD}/var/phase-f-v2-live"
basic_db="${work_root}/controller.sqlite"
basic_artifacts="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/hostd-state"
state_path="${work_root}/phase-f.desired-state.v2.json"
request_body="${work_root}/phase-f-upsert.json"
bad_state_root="${work_root}/bad-state"
http_server_pid=""

cleanup() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cmake -E remove_directory "${work_root}"
mkdir -p "${basic_artifacts}" "${runtime_root}" "${state_root}"

next_port() { "${script_dir}/naim-devtool.sh" free-port; }

start_server() {
  local db_path="$1"
  local port="$2"
  "${build_dir}/naim-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${port}" >/tmp/naim-phase-f-v2-serve.log 2>&1 &
  http_server_pid="$!"
  for _ in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "phase-f-live: controller did not become ready on port ${port}" >&2
  tail -n 40 /tmp/naim-phase-f-v2-serve.log >&2 || true
  exit 1
}

stop_server() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
    http_server_pid=""
  fi
}

echo "phase-f-live: v2 API and CLI-over-HTTP"
"${build_dir}/naim-controller" init-db --db "${basic_db}" >/dev/null
auth_token="phase-f-v2-session"
naim_live_seed_admin_session "${basic_db}" "${auth_token}"
auth_header=(-H "X-Naim-Session-Token: ${auth_token}")
naim_live_seed_connected_hostd "${basic_db}" node-a 2
naim_live_seed_connected_hostd "${basic_db}" node-b 2
naim_live_write_compute_state "${state_path}" phase-f-v2 2
python3 - "${state_path}" "${request_body}" "${basic_artifacts}" <<'PY'
import json
import sys
state_path, request_body, artifacts_root = sys.argv[1:4]
with open(state_path, encoding="utf-8") as source:
    state = json.load(source)
with open(request_body, "w", encoding="utf-8") as output:
    json.dump({"desired_state_v2": state, "artifacts_root": artifacts_root}, output)
PY

basic_port="$(next_port)"
start_server "${basic_db}" "${basic_port}"

curl -fsS "http://127.0.0.1:${basic_port}/health" | grep -F '"service":"naim-controller"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/health" | grep -F '"api_version":"v1"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/state" | grep -F '"desired_generation":null' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/host-assignments" | grep -F '"assignments":[]' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/host-observations" | grep -F '"observations":[]' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/host-health" | grep -F '"items":[' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/node-availability" | grep -F '"items":[' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/disk-state" | grep -F '"items":[]' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/rollout-actions" | grep -F '"actions":[]' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/rebalance-plan" | grep -F '"rebalance_plan":[]' >/dev/null

curl -fsS "${auth_header[@]}" -X POST -H 'Content-Type: application/json' --data-binary "@${request_body}" \
  "http://127.0.0.1:${basic_port}/api/v1/planes" | grep -F '"action":"upsert-plane-state"' >/dev/null
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${basic_port}/api/v1/planes/phase-f-v2/start" | grep -F '"action":"start-plane"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/planes" | grep -F '"name":"phase-f-v2"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/planes/phase-f-v2" | grep -F '"state":"running"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/planes/phase-f-v2/dashboard" | grep -F '"plane_name":"phase-f-v2"' >/dev/null

"${build_dir}/naim-hostd" apply-next-assignment --db "${basic_db}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${basic_db}" --node node-a --state-root "${state_root}" >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/host-observations?node=node-a" | grep -F '"applied_generation":1' >/dev/null
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${basic_port}/api/v1/node-availability?node=node-b&availability=unavailable&message=phase-f-v2" | grep -F '"action":"set-node-availability"' >/dev/null
"${build_dir}/naim-controller" show-state --db "${basic_db}" | grep -F 'plane: phase-f-v2' >/dev/null
"${build_dir}/naim-controller" show-host-assignments --db "${basic_db}" --node node-a | grep -F 'status=applied' >/dev/null
"${build_dir}/naim-controller" show-node-availability --db "${basic_db}" --node node-b | grep -F 'availability=unavailable' >/dev/null
"${build_dir}/naim-controller" set-node-availability --db "${basic_db}" --node node-a --availability draining --message cli-phase-f | grep -F 'updated node availability for node-a' >/dev/null
"${build_dir}/naim-controller" show-node-availability --db "${basic_db}" --node node-a | grep -F 'availability=draining' >/dev/null

failed_assignment_id="$(sqlite3 "${basic_db}" "SELECT id FROM host_assignments WHERE node_name='node-b' AND status='pending' ORDER BY id LIMIT 1;")"
test -n "${failed_assignment_id}"
sqlite3 "${basic_db}" "UPDATE host_assignments SET status='failed', status_message='phase-f-v2 fixture' WHERE id=${failed_assignment_id};"
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${basic_port}/api/v1/retry-host-assignment?id=${failed_assignment_id}" | grep -F '"action":"retry-host-assignment"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${basic_port}/api/v1/events?plane=phase-f-v2" | grep -F '"event_type":"started"' >/dev/null

: > "${bad_state_root}"
if "${build_dir}/naim-hostd" apply-next-assignment --db "${basic_db}" --node node-b --runtime-root "${runtime_root}" --state-root "${bad_state_root}" --compose-mode skip >/dev/null 2>&1; then
  echo "phase-f-live: expected invalid state root to fail" >&2
  exit 1
fi
"${build_dir}/naim-controller" show-host-assignments --db "${basic_db}" --node node-b | grep -F 'status=pending' >/dev/null
stop_server

echo "phase-f-live: OK"
