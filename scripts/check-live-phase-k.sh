#!/usr/bin/env bash
set -euo pipefail

wait_for_http() {
  local url="$1"
  local attempts="${2:-100}"
  for _ in $(seq 1 "${attempts}"); do
    curl -fsS "${url}" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

wait_for_command_match() {
  local attempts="$1"
  local needle="$2"
  shift 2
  for _ in $(seq 1 "${attempts}"); do
    if "$@" | grep -F "${needle}" >/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"
build_dir="$("${script_dir}/print-build-dir.sh")"
skip_build=0
if [[ "${1:-}" == "--skip-build" ]]; then
  skip_build=1
fi

remote_root="${PWD}/var/phase-k-live-remote"
remote_db_path="${remote_root}/controller.sqlite"
remote_artifacts_root="${remote_root}/artifacts"
remote_runtime_root="${remote_root}/runtime"
remote_state_root="${remote_root}/hostd-state"
remote_install_root="${remote_root}/install"
remote_config_path="${remote_install_root}/etc/naim-node/config.toml"
remote_layout_state_root="${remote_install_root}/var/lib/naim-node"
remote_layout_log_root="${remote_install_root}/var/log/naim-node"
remote_layout_systemd_dir="${remote_install_root}/etc/systemd/system"
remote_rotated_install_root="${remote_root}/install-rotated"
remote_rotated_config_path="${remote_rotated_install_root}/etc/naim-node/config.toml"
remote_rotated_layout_state_root="${remote_rotated_install_root}/var/lib/naim-node"
remote_rotated_layout_log_root="${remote_rotated_install_root}/var/log/naim-node"
remote_rotated_layout_systemd_dir="${remote_rotated_install_root}/etc/systemd/system"
remote_state_path="${remote_root}/phase-k-remote.desired-state.v2.json"

local_root="${PWD}/var/phase-k-live-local"
local_config_path="${local_root}/etc/naim-node/config.toml"
local_layout_state_root="${local_root}/var/lib/naim-node"
local_layout_log_root="${local_root}/var/log/naim-node"
local_layout_systemd_dir="${local_root}/etc/systemd/system"
local_db_path="${local_layout_state_root}/controller.sqlite"
local_artifacts_root="${local_layout_state_root}/artifacts"
local_runtime_root="${local_layout_state_root}/runtime"
local_hostd_state_root="${local_layout_state_root}/hostd-state"
local_state_path="${local_root}/phase-k-local.desired-state.v2.json"

remote_controller_pid=""
local_controller_pid=""

cleanup() {
  if [[ -n "${remote_controller_pid}" ]]; then kill "${remote_controller_pid}" >/dev/null 2>&1 || true; wait "${remote_controller_pid}" >/dev/null 2>&1 || true; fi
  if [[ -n "${local_controller_pid}" ]]; then kill "${local_controller_pid}" >/dev/null 2>&1 || true; wait "${local_controller_pid}" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

cmake -E remove_directory "${remote_root}"
cmake -E remove_directory "${local_root}"
mkdir -p "${remote_artifacts_root}" "${remote_runtime_root}" "${remote_state_root}"

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" Debug >/dev/null
fi

remote_http_port="$("${script_dir}/naim-devtool.sh" free-port)"

"${build_dir}/naim-controller" init-db --db "${remote_db_path}" >/dev/null
"${build_dir}/naim-node" install hostd --controller "http://127.0.0.1:${remote_http_port}" --node node-a --config "${remote_config_path}" --state-root "${remote_layout_state_root}" --log-root "${remote_layout_log_root}" --systemd-dir "${remote_layout_systemd_dir}" --skip-systemctl >/dev/null
"${build_dir}/naim-node" service verify hostd --systemd-dir "${remote_layout_systemd_dir}" --skip-systemctl >/dev/null
"${build_dir}/naim-node" connect-hostd --db "${remote_db_path}" --node node-a --address "http://127.0.0.1:29999" --public-key "${remote_layout_state_root}/keys/hostd.pub.b64" >/dev/null
naim_live_write_compute_state "${remote_state_path}" phase-k-remote 1
python3 - "${remote_state_path}" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding='utf-8') as source:
    state = json.load(source)
state['topology']['nodes'] = [{'name': 'node-a', 'execution_mode': 'mixed', 'gpu_memory_mb': {'0': 24576}}]
state['worker']['assignments'] = [{'node': 'node-a', 'gpu_device': '0'}]
with open(path, 'w', encoding='utf-8') as output:
    json.dump(state, output, indent=2)
    output.write('\n')
PY
"${build_dir}/naim-controller" apply-state-file --state "${remote_state_path}" --db "${remote_db_path}" --artifacts-root "${remote_artifacts_root}" >/dev/null
"${build_dir}/naim-controller" serve --db "${remote_db_path}" --listen-host 127.0.0.1 --listen-port "${remote_http_port}" >/tmp/naim-phase-k-remote.log 2>&1 &
remote_controller_pid="$!"
wait_for_http "http://127.0.0.1:${remote_http_port}/health"
"${build_dir}/naim-hostd" report-observed-state --controller "http://127.0.0.1:${remote_http_port}" --node node-a --state-root "${remote_state_root}" --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" >/dev/null
"${build_dir}/naim-controller" start-plane --db "${remote_db_path}" --plane phase-k-remote >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --controller "http://127.0.0.1:${remote_http_port}" --node node-a --runtime-root "${remote_runtime_root}" --state-root "${remote_state_root}" --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --controller "http://127.0.0.1:${remote_http_port}" --node node-a --state-root "${remote_state_root}" --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" >/dev/null
"${build_dir}/naim-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${remote_db_path}" --node node-a | grep -F 'applied_generation=1' >/dev/null
curl -fsS "http://127.0.0.1:${remote_http_port}/api/v1/hostd/hosts?node=node-a" | grep -F '"session_state":"connected"' >/dev/null
"${build_dir}/naim-node" install hostd --controller "http://127.0.0.1:${remote_http_port}" --node node-a --config "${remote_rotated_config_path}" --state-root "${remote_rotated_layout_state_root}" --log-root "${remote_rotated_layout_log_root}" --systemd-dir "${remote_rotated_layout_systemd_dir}" --skip-systemctl >/dev/null
"${build_dir}/naim-controller" rotate-hostd-key --db "${remote_db_path}" --node node-a --public-key "${remote_rotated_layout_state_root}/keys/hostd.pub.b64" >/dev/null
"${build_dir}/naim-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "rotation-pending"' >/dev/null
if "${build_dir}/naim-hostd" report-observed-state --controller "http://127.0.0.1:${remote_http_port}" --node node-a --state-root "${remote_state_root}" --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" >/dev/null 2>&1; then
  echo "phase-k-live: old host key unexpectedly authenticated after rotation" >&2
  exit 1
fi
"${build_dir}/naim-hostd" report-observed-state --controller "http://127.0.0.1:${remote_http_port}" --node node-a --state-root "${remote_state_root}" --host-private-key "${remote_rotated_layout_state_root}/keys/hostd.key.b64" >/dev/null
"${build_dir}/naim-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
"${build_dir}/naim-controller" revoke-hostd --db "${remote_db_path}" --node node-a >/dev/null
"${build_dir}/naim-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"registration_state": "revoked"' >/dev/null
if "${build_dir}/naim-hostd" report-observed-state --controller "http://127.0.0.1:${remote_http_port}" --node node-a --state-root "${remote_state_root}" --host-private-key "${remote_rotated_layout_state_root}/keys/hostd.key.b64" >/dev/null 2>&1; then
  echo "phase-k-live: revoked host unexpectedly authenticated" >&2
  exit 1
fi
kill "${remote_controller_pid}" >/dev/null 2>&1 || true
wait "${remote_controller_pid}" >/dev/null 2>&1 || true
remote_controller_pid=""

local_http_port="$("${script_dir}/naim-devtool.sh" free-port)"
local_skills_port="$("${script_dir}/naim-devtool.sh" free-port)"

"${build_dir}/naim-node" install controller --with-hostd --config "${local_config_path}" --state-root "${local_layout_state_root}" --log-root "${local_layout_log_root}" --systemd-dir "${local_layout_systemd_dir}" --listen-port "${local_http_port}" --node node-a --skip-systemctl >/dev/null
"${build_dir}/naim-node" service verify controller-hostd --systemd-dir "${local_layout_systemd_dir}" --skip-systemctl >/dev/null
mkdir -p "${local_artifacts_root}"
naim_live_write_compute_state "${local_state_path}" phase-k-local 1
python3 - "${local_state_path}" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding='utf-8') as source:
    state = json.load(source)
state['topology']['nodes'] = [{'name': 'node-a', 'execution_mode': 'mixed', 'gpu_memory_mb': {'0': 24576}}]
state['worker']['assignments'] = [{'node': 'node-a', 'gpu_device': '0'}]
with open(path, 'w', encoding='utf-8') as output:
    json.dump(state, output, indent=2)
    output.write('\n')
PY
"${build_dir}/naim-controller" apply-state-file --state "${local_state_path}" --db "${local_db_path}" --artifacts-root "${local_artifacts_root}" >/dev/null
local_auth_token="phase-k-local-session"
naim_live_seed_admin_session "${local_db_path}" "${local_auth_token}"
local_auth_header=(-H "X-Naim-Session-Token: ${local_auth_token}")
"${build_dir}/naim-node" run controller --db "${local_db_path}" --artifacts-root "${local_artifacts_root}" --listen-host 127.0.0.1 --listen-port "${local_http_port}" --with-hostd --node node-a --runtime-root "${local_runtime_root}" --state-root "${local_hostd_state_root}" --compose-mode skip --poll-interval-sec 1 --skills-factory-listen-port "${local_skills_port}" --skip-systemctl >/tmp/naim-phase-k-local.log 2>&1 &
local_controller_pid="$!"
wait_for_http "http://127.0.0.1:${local_http_port}/health"
sqlite3 "${local_db_path}" "UPDATE registered_hosts SET derived_role='worker', role_reason='phase-k live fixture', capabilities_json='{"capacity_summary":{"gpu_count":1,"storage_root":"/tmp/naim","storage_total_bytes":200000000000,"storage_free_bytes":150000000000,"total_memory_bytes":137438953472}}' WHERE node_name='node-a';"
curl -fsS "${local_auth_header[@]}" -X POST "http://127.0.0.1:${local_http_port}/api/v1/planes/phase-k-local/start" | grep -F '"action":"start-plane"' >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${local_db_path}" --node node-a --runtime-root "${local_runtime_root}" --state-root "${local_hostd_state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${local_db_path}" --node node-a --state-root "${local_hostd_state_root}" >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${local_db_path}" --node node-a | grep -F 'applied_generation=1' >/dev/null
"${build_dir}/naim-controller" show-hostd-hosts --db "${local_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
curl -fsS "http://127.0.0.1:${local_http_port}/api/v1/hostd/hosts?node=node-a" | grep -F '"session_state":"connected"' >/dev/null
kill "${local_controller_pid}" >/dev/null 2>&1 || true
wait "${local_controller_pid}" >/dev/null 2>&1 || true
local_controller_pid=""

echo "phase-k-live: OK"
