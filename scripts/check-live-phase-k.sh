#!/usr/bin/env bash
set -euo pipefail

wait_for_http() {
  local url="$1"
  local attempts="${2:-100}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
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
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"
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
remote_config_path="${remote_install_root}/etc/comet-node/config.toml"
remote_layout_state_root="${remote_install_root}/var/lib/comet-node"
remote_layout_log_root="${remote_install_root}/var/log/comet-node"
remote_layout_systemd_dir="${remote_install_root}/etc/systemd/system"
remote_rotated_install_root="${remote_root}/install-rotated"
remote_rotated_config_path="${remote_rotated_install_root}/etc/comet-node/config.toml"
remote_rotated_layout_state_root="${remote_rotated_install_root}/var/lib/comet-node"
remote_rotated_layout_log_root="${remote_rotated_install_root}/var/log/comet-node"
remote_rotated_layout_systemd_dir="${remote_rotated_install_root}/etc/systemd/system"

local_root="${PWD}/var/phase-k-live-local"
local_config_path="${local_root}/etc/comet-node/config.toml"
local_layout_state_root="${local_root}/var/lib/comet-node"
local_layout_log_root="${local_root}/var/log/comet-node"
local_layout_systemd_dir="${local_root}/etc/systemd/system"
local_db_path="${local_layout_state_root}/controller.sqlite"
local_artifacts_root="${local_layout_state_root}/artifacts"
local_runtime_root="${local_layout_state_root}/runtime"
local_hostd_state_root="${local_layout_state_root}/hostd-state"

remote_controller_pid=""
local_controller_pid=""

cleanup() {
  if [[ -n "${remote_controller_pid}" ]]; then
    kill "${remote_controller_pid}" >/dev/null 2>&1 || true
    wait "${remote_controller_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${local_controller_pid}" ]]; then
    kill "${local_controller_pid}" >/dev/null 2>&1 || true
    wait "${local_controller_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cmake -E remove_directory "${remote_root}"
cmake -E remove_directory "${local_root}"

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug
fi

remote_http_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"

"${build_dir}/comet-controller" init-db --db "${remote_db_path}" >/dev/null
"${build_dir}/comet-node" install hostd \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --config "${remote_config_path}" \
  --state-root "${remote_layout_state_root}" \
  --log-root "${remote_layout_log_root}" \
  --systemd-dir "${remote_layout_systemd_dir}" \
  --skip-systemctl >/dev/null
"${build_dir}/comet-node" service verify hostd \
  --systemd-dir "${remote_layout_systemd_dir}" \
  --skip-systemctl >/dev/null
"${build_dir}/comet-node" connect-hostd \
  --db "${remote_db_path}" \
  --node node-a \
  --address "http://127.0.0.1:29999" \
  --public-key "${remote_layout_state_root}/keys/hostd.pub.b64" >/dev/null
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${PWD}/config/demo-plane" \
  --db "${remote_db_path}" \
  --artifacts-root "${remote_artifacts_root}" >/dev/null
"${build_dir}/comet-controller" serve \
  --db "${remote_db_path}" \
  --listen-host 127.0.0.1 \
  --listen-port "${remote_http_port}" >/tmp/comet-phase-k-remote.log 2>&1 &
remote_controller_pid="$!"
wait_for_http "http://127.0.0.1:${remote_http_port}/health"
"${build_dir}/comet-hostd" apply-next-assignment \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --runtime-root "${remote_runtime_root}" \
  --state-root "${remote_state_root}" \
  --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" \
  --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" report-observed-state \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --state-root "${remote_state_root}" \
  --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" >/dev/null
"${build_dir}/comet-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${remote_db_path}" --node node-a | grep -F 'status=idle applied_generation=1' >/dev/null
curl -fsS "http://127.0.0.1:${remote_http_port}/api/v1/hostd/hosts?node=node-a" | grep -F '"session_state":"connected"' >/dev/null
"${build_dir}/comet-node" install hostd \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --config "${remote_rotated_config_path}" \
  --state-root "${remote_rotated_layout_state_root}" \
  --log-root "${remote_rotated_layout_log_root}" \
  --systemd-dir "${remote_rotated_layout_systemd_dir}" \
  --skip-systemctl >/dev/null
"${build_dir}/comet-controller" rotate-hostd-key \
  --db "${remote_db_path}" \
  --node node-a \
  --public-key "${remote_rotated_layout_state_root}/keys/hostd.pub.b64" >/dev/null
"${build_dir}/comet-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "rotation-pending"' >/dev/null
if "${build_dir}/comet-hostd" report-observed-state \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --state-root "${remote_state_root}" \
  --host-private-key "${remote_layout_state_root}/keys/hostd.key.b64" >/dev/null 2>&1; then
  echo "phase-k-live: old host key unexpectedly authenticated after rotation" >&2
  exit 1
fi
"${build_dir}/comet-hostd" report-observed-state \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --state-root "${remote_state_root}" \
  --host-private-key "${remote_rotated_layout_state_root}/keys/hostd.key.b64" >/dev/null
"${build_dir}/comet-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
"${build_dir}/comet-controller" revoke-hostd --db "${remote_db_path}" --node node-a >/dev/null
"${build_dir}/comet-controller" show-hostd-hosts --db "${remote_db_path}" --node node-a | grep -F '"registration_state": "revoked"' >/dev/null
if "${build_dir}/comet-hostd" report-observed-state \
  --controller "http://127.0.0.1:${remote_http_port}" \
  --node node-a \
  --state-root "${remote_state_root}" \
  --host-private-key "${remote_rotated_layout_state_root}/keys/hostd.key.b64" >/dev/null 2>&1; then
  echo "phase-k-live: revoked host unexpectedly authenticated" >&2
  exit 1
fi
kill "${remote_controller_pid}" >/dev/null 2>&1 || true
wait "${remote_controller_pid}" >/dev/null 2>&1 || true
remote_controller_pid=""

local_http_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"

"${build_dir}/comet-node" install controller \
  --with-hostd \
  --config "${local_config_path}" \
  --state-root "${local_layout_state_root}" \
  --log-root "${local_layout_log_root}" \
  --systemd-dir "${local_layout_systemd_dir}" \
  --listen-port "${local_http_port}" \
  --node node-a \
  --skip-systemctl >/dev/null
"${build_dir}/comet-node" service verify controller-hostd \
  --systemd-dir "${local_layout_systemd_dir}" \
  --skip-systemctl >/dev/null
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${PWD}/config/demo-plane" \
  --db "${local_db_path}" \
  --artifacts-root "${local_artifacts_root}" >/dev/null
"${build_dir}/comet-node" run controller \
  --db "${local_db_path}" \
  --artifacts-root "${local_artifacts_root}" \
  --listen-host 127.0.0.1 \
  --listen-port "${local_http_port}" \
  --with-hostd \
  --node node-a \
  --runtime-root "${local_runtime_root}" \
  --state-root "${local_hostd_state_root}" \
  --compose-mode skip \
  --poll-interval-sec 1 >/tmp/comet-phase-k-local.log 2>&1 &
local_controller_pid="$!"
wait_for_http "http://127.0.0.1:${local_http_port}/health"
wait_for_command_match 80 "status=idle applied_generation=1" \
  "${build_dir}/comet-controller" show-host-observations --db "${local_db_path}" --node node-a
"${build_dir}/comet-controller" show-hostd-hosts --db "${local_db_path}" --node node-a | grep -F '"session_state": "connected"' >/dev/null
curl -fsS "http://127.0.0.1:${local_http_port}/api/v1/hostd/hosts?node=node-a" | grep -F '"session_state":"connected"' >/dev/null
kill "${local_controller_pid}" >/dev/null 2>&1 || true
wait "${local_controller_pid}" >/dev/null 2>&1 || true
local_controller_pid=""

echo "phase-k-live: OK"
