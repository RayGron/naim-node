#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"
build_dir="$("${script_dir}/print-build-dir.sh")"

skip_build=0
skip_image_build=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) skip_build=1 ;;
    --skip-image-build) skip_image_build=1 ;;
    *) echo "usage: $0 [--skip-build] [--skip-image-build]" >&2; exit 1 ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" Debug >/dev/null
fi

if [[ "${skip_image_build}" -eq 0 ]]; then
  (cd "${PWD}/ui/operator-react" && npm run build >/dev/null)
  docker build -f "${PWD}/runtime/web-ui/Dockerfile" -t naim/web-ui:dev "${PWD}" >/dev/null
fi

base="${PWD}/var/live-phase-h"
db_path="${base}/controller.sqlite"
artifacts_root="${base}/artifacts"
runtime_root="${base}/runtime"
state_root="${base}/state"
web_ui_root="${base}/web-ui"
stream_log="${base}/events-stream.log"
state_path="${base}/alpha.desired-state.v2.json"
controller_pid=""

cleanup() {
  if [[ -n "${controller_pid}" ]]; then
    kill "${controller_pid}" >/dev/null 2>&1 || true
    wait "${controller_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -f "${db_path}" ]]; then
    "${build_dir}/naim-controller" stop-web-ui --db "${db_path}" --web-ui-root "${web_ui_root}" --compose-mode exec >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

next_port() { "${script_dir}/naim-devtool.sh" free-port; }
wait_for_http() {
  local url="$1"
  local attempts="${2:-100}"
  for _ in $(seq 1 "${attempts}"); do
    curl -fsS "${url}" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

detect_controller_upstream() {
  local controller_port="$1"
  if docker run --rm curlimages/curl:8.12.1 -fsS --max-time 5 "http://host.docker.internal:${controller_port}/api/v1/health" >/dev/null 2>&1; then
    printf 'http://host.docker.internal:%s' "${controller_port}"
    return 0
  fi
  local host_ip
  host_ip="$(hostname -I | awk '{print $1}')"
  if [[ -n "${host_ip}" ]] && docker run --rm curlimages/curl:8.12.1 -fsS --max-time 5 "http://${host_ip}:${controller_port}/api/v1/health" >/dev/null 2>&1; then
    printf 'http://%s:%s' "${host_ip}" "${controller_port}"
    return 0
  fi
  return 1
}

cmake -E remove_directory "${base}"
mkdir -p "${artifacts_root}" "${runtime_root}" "${state_root}" "${web_ui_root}"

echo "phase-h-live: init v2 plane"
"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
auth_token="phase-h-v2-session"
naim_live_seed_admin_session "${db_path}" "${auth_token}"
auth_header=(-H "X-Naim-Session-Token: ${auth_token}")
naim_live_seed_connected_hostd "${db_path}" node-a 2
naim_live_seed_connected_hostd "${db_path}" node-b 2
naim_live_write_compute_state "${state_path}" alpha 2
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${state_path}"
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null

controller_port="$(next_port)"
ui_port="$(next_port)"

echo "phase-h-live: start controller"
"${build_dir}/naim-controller" serve --db "${db_path}" --listen-host 0.0.0.0 --listen-port "${controller_port}" >/tmp/naim-phase-h-serve.log 2>&1 &
controller_pid="$!"
wait_for_http "http://127.0.0.1:${controller_port}/api/v1/health"

controller_upstream="$(detect_controller_upstream "${controller_port}")"
echo "phase-h-live: controller_upstream=${controller_upstream}"

echo "phase-h-live: ensure sidecar"
"${build_dir}/naim-controller" ensure-web-ui --db "${db_path}" --web-ui-root "${web_ui_root}" --listen-port "${ui_port}" --controller-upstream "${controller_upstream}" --compose-mode exec >/dev/null
wait_for_http "http://127.0.0.1:${ui_port}/health"

echo "phase-h-live: proxied rest"
curl -fsS "http://127.0.0.1:${ui_port}/" | grep -F 'Naim Operator' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/planes" | grep -F '"name":"alpha"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/planes/alpha/dashboard" | grep -F '"plane_name":"alpha"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/planes/alpha/dashboard" | grep -F '"alerts"' >/dev/null

echo "phase-h-live: proxied sse"
last_event_id="$(sqlite3 "${db_path}" "SELECT COALESCE(MAX(id), 0) FROM event_log;")"
rm -f "${stream_log}"
curl -NsS --max-time 10 "${auth_header[@]}" -H "Last-Event-ID: ${last_event_id}" "http://127.0.0.1:${ui_port}/api/v1/events/stream?plane=alpha&limit=20" >"${stream_log}" 2>/dev/null &
stream_pid="$!"
sleep 1
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${ui_port}/api/v1/planes/alpha/stop" | grep -F '"action":"stop-plane"' >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
for _ in $(seq 1 30); do
  grep -F 'event: plane.stopped' "${stream_log}" >/dev/null 2>&1 && break
  sleep 0.2
done
kill "${stream_pid}" >/dev/null 2>&1 || true
wait "${stream_pid}" >/dev/null 2>&1 || true
grep -F 'event: plane.stopped' "${stream_log}" >/dev/null
grep -F '"event_type":"stopped"' "${stream_log}" >/dev/null

echo "phase-h-live: proxied lifecycle"
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/planes/alpha" | grep -F '"state":"stopped"' >/dev/null
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:${ui_port}/api/v1/planes/alpha/start" | grep -F '"action":"start-plane"' >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode skip >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/planes/alpha" | grep -F '"state":"running"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/events?plane=alpha&category=plane&limit=10" | grep -F '"event_type":"started"' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${ui_port}/api/v1/events?plane=alpha&category=web-ui&limit=10" | grep -F '"category":"web-ui"' >/dev/null

echo "phase-h-live: sidecar status"
"${build_dir}/naim-controller" show-web-ui-status --db "${db_path}" --web-ui-root "${web_ui_root}" | grep -F 'running=yes' >/dev/null

echo "phase-h-live: OK"
