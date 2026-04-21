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

work_root="${PWD}/var/phase-g-v2-live"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/hostd-state"
bad_state_root="${work_root}/bad-state"
state_path="${work_root}/desired-state.v2.json"
http_server_pid=""
cleanup() { if [[ -n "${http_server_pid}" ]]; then kill "${http_server_pid}" >/dev/null 2>&1 || true; wait "${http_server_pid}" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

cmake -E remove_directory "${work_root}"
mkdir -p "${artifacts_root}" "${runtime_root}" "${state_root}"

"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
auth_token="phase-g-v2-session"
naim_live_seed_admin_session "${db_path}" "${auth_token}"
auth_header=(-H "X-Naim-Session-Token: ${auth_token}")
naim_live_seed_connected_hostd "${db_path}" node-a 2
naim_live_seed_connected_hostd "${db_path}" node-b 2
naim_live_write_compute_state "${state_path}" phase-g-v2 2
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${state_path}"

port="$("${script_dir}/naim-devtool.sh" free-port)"
"${build_dir}/naim-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${port}" >/tmp/naim-phase-g-v2-serve.log 2>&1 &
http_server_pid="$!"
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1 && break; sleep 0.1; done
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${port}/api/v1/events" | grep -F '"event_type":"created"' >/dev/null

"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node node-a | grep -F 'applied_generation=1' >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node node-a | grep -F 'disk_telemetry_source=statvfs' >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node node-a | grep -F 'network_telemetry_source=sysfs' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"disk_telemetry"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${port}/api/v1/disk-state?node=node-a" | grep -F '"items"' >/dev/null

NAIM_DISABLE_NVML=1 "${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node node-a | grep -F 'telemetry_degraded=yes' >/dev/null

curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${port}/api/v1/node-availability?node=node-a&availability=draining&message=phase-g-v2" | grep -F '"action":"set-node-availability"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${port}/api/v1/events?node=node-a&category=node-availability" | grep -F '"event_type":"updated"' >/dev/null

: > "${bad_state_root}"
if "${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${bad_state_root}" --compose-mode skip >/dev/null 2>&1; then
  echo "phase-g-live: expected node-b assignment to fail" >&2
  exit 1
fi
"${build_dir}/naim-controller" show-host-assignments --db "${db_path}" --node node-b | grep -F 'status=pending' >/dev/null

echo "phase-g-live: OK"
