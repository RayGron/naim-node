#!/usr/bin/env bash
set -euo pipefail

wait_for_http() {
  local url="$1"
  local attempts="${2:-100}"
  local log_path="${3:-}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  if [[ -n "${log_path}" && -f "${log_path}" ]]; then
    echo "check: HTTP endpoint did not become ready: ${url}" >&2
    tail -n 40 "${log_path}" >&2 || true
  fi
  return 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"
build_dir="$("${script_dir}/print-build-dir.sh")"
work_root="${repo_root}/var/check-v2"
launcher_root="${work_root}/launcher-defaults"
ui_smoke_root="${work_root}/ui-smoke"
web_ui_runtime_root="${work_root}/web-ui-runtime"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/hostd-state"
llm_model_path="${work_root}/models/check-model.gguf"
http_server_pid=""

cleanup() {
  if [[ -n "${http_server_pid}" ]]; then
    kill "${http_server_pid}" >/dev/null 2>&1 || true
    wait "${http_server_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cmake -E remove_directory "${work_root}"
mkdir -p "${work_root}" "${artifacts_root}" "${runtime_root}" "${state_root}" "$(dirname "${llm_model_path}")"
: > "${llm_model_path}"

"${script_dir}/build-target.sh" Debug

"${build_dir}/naim-node" version | grep -F 'naim-node 0.1.0' >/dev/null
"${build_dir}/naim-node" doctor controller | grep -F 'controller_binary=yes' >/dev/null

controller_port="$("${script_dir}/naim-devtool.sh" free-port)"
skills_port="$("${script_dir}/naim-devtool.sh" free-port)"
launcher_install_output="$(NAIM_INSTALL_ROOT="${launcher_root}" \
"${build_dir}/naim-node" install controller \
  --with-hostd \
  --with-web-ui \
  --listen-port "${controller_port}" \
  --skip-systemctl)"
printf '%s' "${launcher_install_output}" | grep -F "installed controller" >/dev/null
NAIM_INSTALL_ROOT="${launcher_root}" \
"${build_dir}/naim-node" service verify controller-hostd --skip-systemctl >/dev/null
NAIM_INSTALL_ROOT="${launcher_root}" \
"${build_dir}/naim-node" run controller \
  --listen-host 127.0.0.1 \
  --compose-mode skip \
  --skills-factory-listen-port "${skills_port}" \
  --skip-systemctl >/tmp/naim-node-default-run.log 2>&1 &
http_server_pid="$!"
wait_for_http "http://127.0.0.1:${controller_port}/health" 100 /tmp/naim-node-default-run.log
for _ in $(seq 1 80); do
  if "${build_dir}/naim-controller" show-hostd-hosts \
      --db "${launcher_root}/var/lib/naim-node/controller.sqlite" \
      --node local-hostd | grep -F '"session_state": "connected"' >/dev/null; then
    break
  fi
  sleep 0.2
done
"${build_dir}/naim-controller" show-hostd-hosts \
  --db "${launcher_root}/var/lib/naim-node/controller.sqlite" \
  --node local-hostd | grep -F '"session_state": "connected"' >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""

"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
auth_token="check-v2-session"
naim_live_seed_admin_session "${db_path}" "${auth_token}"
auth_header=(-H "X-Naim-Session-Token: ${auth_token}")
naim_live_seed_connected_hostd "${db_path}" local-hostd 4
compute_state="${work_root}/gpu-worker.desired-state.v2.json"
naim_live_write_compute_state "${compute_state}" check-gpu-worker 2
python3 - "${compute_state}" <<'PYSTATE'
import json, sys
path = sys.argv[1]
with open(path, encoding='utf-8') as source:
    state = json.load(source)
state['topology']['nodes'] = [{'name': 'local-hostd', 'execution_mode': 'mixed', 'gpu_memory_mb': {'0': 24576, '1': 24576, '2': 24576, '3': 24576}}]
state['worker']['assignments'] = [{'node': 'local-hostd', 'gpu_device': '0'}, {'node': 'local-hostd', 'gpu_device': '1'}]
with open(path, 'w', encoding='utf-8') as output:
    json.dump(state, output, indent=2)
    output.write('\n')
PYSTATE
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${compute_state}"
"${build_dir}/naim-hostd" apply-next-assignment \
  --db "${db_path}" \
  --node local-hostd \
  --runtime-root "${runtime_root}" \
  --state-root "${state_root}" \
  --compose-mode skip >/dev/null
"${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node local-hostd --state-root "${state_root}" >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node local-hostd | grep -F 'applied_generation=1' >/dev/null
"${build_dir}/naim-hostd" show-local-state --node local-hostd --state-root "${state_root}" | grep -F 'instances=2' >/dev/null

llm_state="${work_root}/llm.desired-state.v2.json"
naim_live_write_llm_state "${llm_state}" check-llm "${llm_model_path}" "$("${script_dir}/naim-devtool.sh" free-port)" "$("${script_dir}/naim-devtool.sh" free-port)"
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${llm_state}"
"${build_dir}/naim-hostd" apply-next-assignment \
  --db "${db_path}" \
  --node local-hostd \
  --runtime-root "${runtime_root}" \
  --state-root "${state_root}" \
  --compose-mode skip >/dev/null
"${build_dir}/naim-controller" show-plane --db "${db_path}" --plane check-llm | grep -F 'state=running' >/dev/null

mkdir -p "${ui_smoke_root}/assets"
cp -R "${repo_root}/ui/operator/." "${ui_smoke_root}"
http_port="$("${script_dir}/naim-devtool.sh" free-port)"
"${build_dir}/naim-controller" serve --db "${db_path}" --listen-host 127.0.0.1 --listen-port "${http_port}" --ui-root "${ui_smoke_root}" >/tmp/naim-controller-check-v2.log 2>&1 &
http_server_pid="$!"
wait_for_http "http://127.0.0.1:${http_port}/health" 100 /tmp/naim-controller-check-v2.log
curl -fsS "http://127.0.0.1:${http_port}/" | grep -F 'Naim Command Deck' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${http_port}/api/v1/planes" | grep -F 'check-llm' >/dev/null
curl -fsS "${auth_header[@]}" "http://127.0.0.1:${http_port}/api/v1/events" | grep -F '"event_type":"started"' >/dev/null
kill "${http_server_pid}" >/dev/null 2>&1 || true
wait "${http_server_pid}" >/dev/null 2>&1 || true
http_server_pid=""

"${build_dir}/naim-controller" ensure-web-ui --db "${db_path}" --web-ui-root "${web_ui_runtime_root}" --listen-port 19081 --controller-upstream "http://host.docker.internal:18080" --compose-mode skip >/dev/null
test -f "${web_ui_runtime_root}/docker-compose.yml"
grep -F 'image: naim/web-ui:dev' "${web_ui_runtime_root}/docker-compose.yml" >/dev/null
"${build_dir}/naim-controller" stop-web-ui --db "${db_path}" --web-ui-root "${web_ui_runtime_root}" --compose-mode skip >/dev/null

echo "check: OK"
