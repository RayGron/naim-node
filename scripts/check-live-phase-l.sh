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

require_image() {
  local image="$1"
  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "phase-l-live: required image is missing: ${image}" >&2
    echo "phase-l-live: rerun without --skip-image-build or prebuild runtime images" >&2
    exit 1
  fi
}

if [[ "${skip_image_build}" -eq 0 ]]; then
  (cd "${PWD}/ui/operator-react" && npm run build >/dev/null)
  docker build -f "${PWD}/runtime/web-ui/Dockerfile" -t naim/web-ui:dev "${PWD}" >/dev/null
else
  require_image "naim/web-ui:dev"
fi

wait_for_http() {
  local url="$1"
  local attempts="${2:-150}"
  for _ in $(seq 1 "${attempts}"); do
    curl -fsS "${url}" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  return 1
}

wait_for_match() {
  local attempts="$1"
  local needle="$2"
  shift 2
  for _ in $(seq 1 "${attempts}"); do
    if "$@" | grep -F "${needle}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

next_port() { "${script_dir}/naim-devtool.sh" free-port; }

base="${PWD}/var/live-phase-l"
install_root="${base}/install"
state_root="${install_root}/var/lib/naim-node"
db_path="${state_root}/controller.sqlite"
web_ui_root="${state_root}/web-ui"
desired_state_path="${base}/alpha.desired-state.v2.json"
request_body_path="${base}/alpha-upsert.json"
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

cmake -E remove_directory "${base}"
mkdir -p "${base}"

controller_port="$(next_port)"
skills_port="$(next_port)"
run_log="${base}/naim-phase-l-run.log"

echo "phase-l-live: install controller"
install_output="$(NAIM_INSTALL_ROOT="${install_root}" "${build_dir}/naim-node" install controller --with-hostd --with-web-ui --node node-a --listen-port "${controller_port}" --skip-systemctl)"
printf '%s' "${install_output}" | grep -F 'installed controller' >/dev/null
printf '%s' "${install_output}" | grep -F "controller_api_url=http://127.0.0.1:${controller_port}" >/dev/null

echo "phase-l-live: start platform"
NAIM_INSTALL_ROOT="${install_root}" "${build_dir}/naim-node" run controller --hostd-compose-mode skip --poll-interval-sec 1 --skills-factory-listen-port "${skills_port}" --skip-systemctl >"${run_log}" 2>&1 &
controller_pid="$!"
wait_for_http "http://127.0.0.1:${controller_port}/health"
auth_token="phase-l-v2-session"
naim_live_seed_admin_session "${db_path}" "${auth_token}"
auth_header=(-H "X-Naim-Session-Token: ${auth_token}")
wait_for_http "http://127.0.0.1:18081/health"

echo "phase-l-live: web ui reachable"
curl -fsS "http://127.0.0.1:18081/" | grep -F 'Naim Operator' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:18081/api/v1/planes" | grep -F '"items":[]' >/dev/null

echo "phase-l-live: load first v2 plane through web ui path"
naim_live_write_compute_state "${desired_state_path}" alpha 1
python3 - "${desired_state_path}" "${request_body_path}" "${state_root}/artifacts" <<'PY'
import json
import sys
state_path, request_path, artifacts_root = sys.argv[1:4]
with open(state_path, encoding='utf-8') as source:
    state = json.load(source)
state['topology']['nodes'] = [{'name': 'node-a', 'execution_mode': 'mixed', 'gpu_memory_mb': {'0': 24576}}]
state['worker']['assignments'] = [{'node': 'node-a', 'gpu_device': '0'}]
with open(state_path, 'w', encoding='utf-8') as output:
    json.dump(state, output, indent=2)
    output.write('\n')
with open(request_path, 'w', encoding='utf-8') as output:
    json.dump({'desired_state_v2': state, 'artifacts_root': artifacts_root}, output)
PY
curl -fsS "${auth_header[@]}" -X POST -H 'Content-Type: application/json' --data-binary "@${request_body_path}" "http://127.0.0.1:18081/api/v1/planes" | grep -F '"action":"upsert-plane-state"' >/dev/null
sqlite3 "${db_path}" "UPDATE registered_hosts SET derived_role='worker', role_reason='phase-l live fixture' WHERE node_name='node-a';"
curl -fsS "${auth_header[@]}" -X POST "http://127.0.0.1:18081/api/v1/planes/alpha/start" | grep -F '"action":"start-plane"' >/dev/null

wait_for_match 80 '"name":"alpha"' curl -fsS "${auth_header[@]}" "http://127.0.0.1:18081/api/v1/planes"
wait_for_match 80 '"session_state": "connected"' "${build_dir}/naim-controller" show-hostd-hosts --db "${db_path}" --node node-a
wait_for_match 80 'applied_generation=1' "${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node node-a
wait_for_match 80 'instances=1' "${build_dir}/naim-hostd" show-local-state --node node-a --state-root "${state_root}/hostd-state"

echo "phase-l-live: OK"
