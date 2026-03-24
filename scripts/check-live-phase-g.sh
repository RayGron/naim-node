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
  "${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug >/dev/null
fi

db_path="${PWD}/var/controller-phase-g-live.sqlite"
artifacts_root="${PWD}/var/artifacts-phase-g-live"
runtime_root="${PWD}/var/runtime-phase-g-live"
state_root="${PWD}/var/hostd-state-phase-g-live"
bad_state_root="${PWD}/var/hostd-state-phase-g-live-blocker"
http_server_pid=""

mkdir -p "${PWD}/var"

cleanup() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

next_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

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

cmake -E rm -f "${db_path}"
cmake -E remove_directory "${artifacts_root}"
cmake -E remove_directory "${runtime_root}"
cmake -E remove_directory "${state_root}"
cmake -E rm -f "${bad_state_root}"

echo "phase-g-live: init db and apply bundle"
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${PWD}/config/demo-plane" \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" >/dev/null

port="$(next_port)"
"${build_dir}/comet-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${port}" >/tmp/comet-phase-g-serve.log 2>&1 &
http_server_pid="$!"
wait_for_http "http://127.0.0.1:${port}/health"

echo "phase-g-live: baseline API"
curl -fsS "http://127.0.0.1:${port}/api/v1/events" | grep -F '"category":"bundle"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/events" | grep -F '"event_type":"applied"' >/dev/null

echo "phase-g-live: node apply and telemetry"
"${build_dir}/comet-hostd" apply-state-ops \
  --db "${db_path}" \
  --node node-a \
  --artifacts-root "${artifacts_root}" \
  --runtime-root "${runtime_root}" \
  --state-root "${state_root}" \
  --compose-mode skip >/dev/null
"${build_dir}/comet-hostd" report-observed-state \
  --db "${db_path}" \
  --node node-a \
  --state-root "${state_root}" >/dev/null

"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "disk_telemetry_source=statvfs" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "disk_read_bytes=" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "disk_faults=" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "read_bytes=" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "fault_count=" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "network_telemetry_source=sysfs" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "telemetry_source=" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-a | grep -F "read_bytes=" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-a | grep -F "perf_counters=" >/dev/null
"${build_dir}/comet-controller" show-events --db "${db_path}" --node node-a | grep -F "category=host-observation type=reported" >/dev/null

curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"gpu_telemetry"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"disk_telemetry"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"read_bytes"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"fault_count"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"network_telemetry"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/disk-state?node=node-a" | grep -F '"io_bytes"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/disk-state?node=node-a" | grep -F '"perf_counters_available"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/events?node=node-a" | grep -F '"category":"host-observation"' >/dev/null

echo "phase-g-live: degraded gpu telemetry fallback"
COMET_DISABLE_NVML=1 "${build_dir}/comet-hostd" report-observed-state \
  --db "${db_path}" \
  --node node-a \
  --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "telemetry_degraded=yes" >/dev/null
if command -v nvidia-smi >/dev/null 2>&1; then
  "${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "telemetry_source=nvidia-smi" >/dev/null
  curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"source":"nvidia-smi"' >/dev/null
else
  "${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a | grep -F "telemetry_source=unavailable" >/dev/null
  curl -fsS "http://127.0.0.1:${port}/api/v1/host-observations?node=node-a" | grep -F '"source":"unavailable"' >/dev/null
fi

echo "phase-g-live: controller event emission"
curl -fsS -X POST "http://127.0.0.1:${port}/api/v1/node-availability?node=node-a&availability=draining&message=phase-g-live" | grep -F '"action":"set-node-availability"' >/dev/null
"${build_dir}/comet-controller" show-events --db "${db_path}" --node node-a | grep -F "category=node-availability type=updated" >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/events?node=node-a&category=node-availability" | grep -F '"event_type":"updated"' >/dev/null

echo "phase-g-live: failed assignment event"
: > "${bad_state_root}"
if "${build_dir}/comet-hostd" apply-next-assignment \
  --db "${db_path}" \
  --node node-b \
  --runtime-root "${runtime_root}" \
  --state-root "${bad_state_root}" \
  --compose-mode skip >/dev/null 2>&1; then
  echo "phase-g-live: expected node-b assignment to fail" >&2
  exit 1
fi

"${build_dir}/comet-controller" show-events --db "${db_path}" --node node-b | grep -F "category=host-assignment type=failed" >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/events?node=node-b" | grep -F '"category":"host-assignment"' >/dev/null
curl -fsS "http://127.0.0.1:${port}/api/v1/events?node=node-b" | grep -F '"event_type":"failed"' >/dev/null

echo "phase-g-live: OK"
