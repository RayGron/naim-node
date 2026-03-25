#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

plane_name="${1:-qwen25-0.5b-vllm-split}"
controller_url="${COMET_CONTROLLER_URL:-http://127.0.0.1:18080}"
controller_db="${COMET_NODE_CONTROLLER_DB:-/var/lib/comet-node/controller.sqlite}"
runtime_root="${COMET_SPLIT_HOSTD_RUNTIME_ROOT:-/var/lib/comet-node/runtime}"
hostd_poll_interval="${COMET_SPLIT_HOSTD_POLL_INTERVAL_SEC:-2}"
controller_public_key_path="${COMET_NODE_CONTROLLER_PUBLIC_KEY:-/var/lib/comet-node/keys/controller.pub.b64}"
host_private_key_path="${COMET_NODE_HOST_PRIVATE_KEY:-/var/lib/comet-node/keys/hostd.key.b64}"

docker_cmd=(docker)
if ! docker info >/dev/null 2>&1; then
  docker_cmd=(sudo -n docker)
fi
if ! "${docker_cmd[@]}" info >/dev/null 2>&1; then
  echo "error: docker is not available for split-host live validation" >&2
  exit 1
fi

sudo_prefix=""
if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  sudo_prefix="sudo -n"
fi
if [[ -z "${sudo_prefix}" ]]; then
  echo "error: split-host live validation requires passwordless sudo" >&2
  exit 1
fi

build_dir="${repo_root}/build/linux/x64"
launcher_bin="${build_dir}/comet-node"
controller_bin="${build_dir}/comet-controller"
if [[ ! -x "${launcher_bin}" || ! -x "${controller_bin}" ]]; then
  echo "error: required binaries are missing under ${build_dir}" >&2
  exit 1
fi
if [[ ! -f "${controller_public_key_path}" ]]; then
  echo "error: controller public key is missing: ${controller_public_key_path}" >&2
  exit 1
fi
if [[ ! -f "${host_private_key_path}" ]]; then
  echo "error: host private key is missing: ${host_private_key_path}" >&2
  exit 1
fi

controller_fingerprint="$(
  python3 - <<'PY' "${controller_public_key_path}"
import base64
import hashlib
import pathlib
import sys

public_key_b64 = pathlib.Path(sys.argv[1]).read_text().strip()
print(hashlib.sha256(base64.b64decode(public_key_b64)).hexdigest())
PY
)"

tmp_dir="$(mktemp -d "${repo_root}/var/check-live-vllm-split-host.XXXXXX")"
hostd_pid_dir="${tmp_dir}/pids"
mkdir -p "${hostd_pid_dir}"

cleanup() {
  set +e
  curl -sS -X DELETE "${controller_url}/api/v1/planes/${plane_name}" >/dev/null 2>&1 || true
  local node_name
  for node_name in infer-hostd worker-hostd-a worker-hostd-b; do
    terminate_existing_hostd_loops "${node_name}" >/dev/null 2>&1 || true
  done
  local pid_file
  for pid_file in "${hostd_pid_dir}"/*.pid; do
    [[ -f "${pid_file}" ]] || continue
    local pid
    pid="$(cat "${pid_file}")"
    ${sudo_prefix} kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  done
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

matching_hostd_loop_pids() {
  local node_name="$1"
  ${sudo_prefix} python3 - "${node_name}" <<'PY'
import subprocess
import sys

node_name = sys.argv[1]
target = f"comet-node run hostd --foreground --skip-systemctl --node {node_name}"
result = subprocess.run(
    ["ps", "-eo", "pid=,args="],
    check=True,
    capture_output=True,
    text=True,
)
pids = []
for raw_line in result.stdout.splitlines():
    line = raw_line.strip()
    if not line:
        continue
    pid_text, _, args = line.partition(" ")
    try:
        pid = int(pid_text)
    except ValueError:
        continue
    if target not in args:
        continue
    if "check-live-vllm-split-host.sh" in args or "python3 -" in args:
        continue
    pids.append(str(pid))

print(" ".join(pids))
PY
}

terminate_existing_hostd_loops() {
  local node_name="$1"
  local pids
  pids="$(matching_hostd_loop_pids "${node_name}")"
  if [[ -n "${pids}" ]]; then
    ${sudo_prefix} kill ${pids} >/dev/null 2>&1 || true
  fi
}

stop_existing_hostd_loop() {
  local node_name="$1"
  terminate_existing_hostd_loops "${node_name}"
  for _ in $(seq 1 30); do
    if [[ -z "$(matching_hostd_loop_pids "${node_name}")" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "error: stale hostd loop for ${node_name} is still running" >&2
  exit 1
}

wait_for_json_field() {
  local url="$1"
  local expr="$2"
  local attempts="${3:-90}"
  local payload=""
  for _ in $(seq 1 "${attempts}"); do
    if payload="$(curl -fsS "${url}" 2>/dev/null)"; then
      local matched
      matched="$(python3 - <<'PY' "${payload}" "${expr}"
import json
import sys

payload = json.loads(sys.argv[1])
expr = sys.argv[2]
namespace = {"payload": payload}
print("yes" if eval(expr, {"__builtins__": {}}, namespace) else "no")
PY
)"
      if [[ "${matched}" == "yes" ]]; then
        printf '%s\n' "${payload}"
        return 0
      fi
    fi
    sleep 2
  done
  return 1
}

resolve_container_name() {
  local service_name="$1"
  local container_name
  container_name="$("${docker_cmd[@]}" ps --format '{{.Names}}' | grep -F "${service_name}" | head -n1 || true)"
  if [[ -z "${container_name}" ]]; then
    echo "error: unable to find running container for ${service_name}" >&2
    exit 1
  fi
  printf '%s\n' "${container_name}"
}

register_host() {
  local node_name="$1"
  local execution_mode="$2"
  ${sudo_prefix} "${launcher_bin}" connect-hostd \
    --db "${controller_db}" \
    --node "${node_name}" \
    --public-key /var/lib/comet-node/keys/hostd.pub.b64 \
    --controller-fingerprint "${controller_fingerprint}" \
    --execution-mode "${execution_mode}" >/dev/null
}

start_hostd_loop() {
  local node_name="$1"
  local execution_mode="$2"
  local state_root="/var/lib/comet-node/${node_name}-state"
  local log_path="/var/log/${node_name}.log"
  stop_existing_hostd_loop "${node_name}"
  register_host "${node_name}" "${execution_mode}"
  ${sudo_prefix} mkdir -p "${state_root}"
  local pid
  pid="$(${sudo_prefix} bash -lc "
    set -euo pipefail
    nohup '${launcher_bin}' run hostd \
      --foreground \
      --skip-systemctl \
      --node '${node_name}' \
      --controller '${controller_url}' \
      --controller-fingerprint '${controller_fingerprint}' \
      --host-private-key '${host_private_key_path}' \
      --runtime-root '${runtime_root}' \
      --state-root '${state_root}' \
      --compose-mode exec \
      --poll-interval-sec '${hostd_poll_interval}' \
      >'${log_path}' 2>&1 &
    echo \$!
  ")"
  printf '%s\n' "${pid}" >"${hostd_pid_dir}/${node_name}.pid"
}

delete_competing_planes() {
  local plane_json
  plane_json="$(curl -fsS "${controller_url}/api/v1/planes")"
  python3 - <<'PY' "${plane_json}" "${plane_name}" >"${tmp_dir}/competing-planes.txt"
import json
import sys

payload = json.loads(sys.argv[1])
target_plane = sys.argv[2]
for item in payload.get("items", []):
    name = item.get("name")
    if name and name != target_plane:
        print(name)
PY
  while IFS= read -r other_plane; do
    [[ -n "${other_plane}" ]] || continue
    curl -sS -X DELETE "${controller_url}/api/v1/planes/${other_plane}" >/dev/null || true
  done <"${tmp_dir}/competing-planes.txt"

  for _ in $(seq 1 120); do
    if ! ${sudo_prefix} nvidia-smi --query-compute-apps=process_name --format=csv,noheader 2>/dev/null \
      | grep -F 'VLLM::EngineCore' >/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "error: competing GPU workloads are still running after plane cleanup" >&2
  exit 1
}

wait_for_plane_absent() {
  local target_plane="$1"
  for _ in $(seq 1 120); do
    local planes_payload
    planes_payload="$(curl -fsS "${controller_url}/api/v1/planes")"
    local absent
    absent="$(python3 - <<'PY' "${planes_payload}" "${target_plane}"
import json
import sys

payload = json.loads(sys.argv[1])
target_plane = sys.argv[2]
print(
    "yes"
    if all(item.get("name") != target_plane for item in payload.get("items", []))
    else "no"
)
PY
)"
    if [[ "${absent}" == "yes" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "error: plane ${target_plane} did not disappear from controller state" >&2
  exit 1
}

echo "[check-live-vllm-split] registering host roles"
start_hostd_loop "infer-hostd" "infer-only"
start_hostd_loop "worker-hostd-a" "worker-only"
start_hostd_loop "worker-hostd-b" "worker-only"

echo "[check-live-vllm-split] verifying registered hosts"
${sudo_prefix} "${controller_bin}" show-hostd-hosts --db "${controller_db}" --node infer-hostd | grep -F '"execution_mode": "infer-only"' >/dev/null
${sudo_prefix} "${controller_bin}" show-hostd-hosts --db "${controller_db}" --node worker-hostd-a | grep -F '"execution_mode": "worker-only"' >/dev/null
${sudo_prefix} "${controller_bin}" show-hostd-hosts --db "${controller_db}" --node worker-hostd-b | grep -F '"execution_mode": "worker-only"' >/dev/null
echo "registered_hosts=ok"

echo "[check-live-vllm-split] ensuring plane ${plane_name} is running"
delete_competing_planes
curl -sS -X DELETE "${controller_url}/api/v1/planes/${plane_name}" >/dev/null || true
wait_for_plane_absent "${plane_name}"
"${repo_root}/scripts/run-plane.sh" "${plane_name}" --no-wait >/dev/null

status_payload="$(
  wait_for_json_field \
    "${controller_url}/api/v1/planes/${plane_name}/interaction/status" \
    "payload.get('ready') is True and payload.get('worker_group_ready') == 2 and payload.get('primary_infer_node') == 'infer-hostd'" \
    180
)"
python3 - <<'PY' "${status_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
assert payload["ready"] is True, payload
assert payload["worker_group_expected"] == 2, payload
assert payload["worker_group_ready"] == 2, payload
assert payload["primary_infer_node"] == "infer-hostd", payload
print("split_status=ok")
PY

infer_container="$(resolve_container_name "infer-${plane_name}")"
worker_a_container="$(resolve_container_name "worker-${plane_name}-a")"
worker_b_container="$(resolve_container_name "worker-${plane_name}-b")"

echo "[check-live-vllm-split] verifying worker-group contracts"
worker_group_dir="$(
  python3 - <<'PY' "${status_payload}"
import json
import pathlib
import sys

payload = json.loads(sys.argv[1])
control_root = (payload.get("runtime_status") or {}).get("control_root", "")
assert control_root, payload
print(control_root.rstrip("/") + "/worker-group")
PY
)"
member_a_json="${tmp_dir}/worker-a.json"
member_b_json="${tmp_dir}/worker-b.json"
"${docker_cmd[@]}" exec "${infer_container}" cat "${worker_group_dir}/worker-qwen25-0.5b-vllm-split-a.json" >"${member_a_json}"
"${docker_cmd[@]}" exec "${infer_container}" cat "${worker_group_dir}/worker-qwen25-0.5b-vllm-split-b.json" >"${member_b_json}"
python3 - <<'PY' "${member_a_json}" "${member_b_json}"
import json
import pathlib
import sys

member_a = json.loads(pathlib.Path(sys.argv[1]).read_text())
member_b = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert member_a["rank"] == 0 and member_a["leader"] is True, member_a
assert member_b["rank"] == 1 and member_b["leader"] is False, member_b
assert member_a["base_url"] == "http://worker-qwen25-0.5b-vllm-split-a:18090", member_a
assert member_b["base_url"] == "", member_b
assert member_a["leader_api_base_url"] == "http://worker-qwen25-0.5b-vllm-split-a:18090", member_a
assert member_b["leader_api_base_url"] == "http://worker-qwen25-0.5b-vllm-split-a:18090", member_b
print("worker_group_contract=ok")
PY

echo "[check-live-vllm-split] checking split-host chat completion"
chat_request_json="${tmp_dir}/chat-request.json"
chat_response_json="${tmp_dir}/chat-response.json"
python3 - <<'PY' "${chat_request_json}"
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "model": "qwen2.5-0.5b-instruct",
    "messages": [{"role": "user", "content": "Reply with PONG only."}],
    "max_tokens": 16,
}))
PY
curl -fsS \
  -H 'Content-Type: application/json' \
  --data-binary "@${chat_request_json}" \
  "${controller_url}/api/v1/planes/${plane_name}/interaction/chat/completions" \
  >"${chat_response_json}"
python3 - <<'PY' "${chat_response_json}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
content = payload["choices"][0]["message"]["content"].strip()
assert content == "PONG", content
print("split_chat=PONG")
PY

echo "[check-live-vllm-split] checking degraded worker-group handling"
"${docker_cmd[@]}" stop "${worker_b_container}" >/dev/null
degraded_payload="$(
  wait_for_json_field \
    "${controller_url}/api/v1/planes/${plane_name}/interaction/status" \
    "payload.get('ready') is False and payload.get('degraded') is True and payload.get('worker_group_ready') == 1" \
    90
)"
python3 - <<'PY' "${degraded_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
assert payload["ready"] is False, payload
assert payload["degraded"] is True, payload
assert payload["worker_group_ready"] == 1, payload
assert payload["reason"] == "worker_group_partial", payload
print("split_degraded=ok")
PY

echo "[check-live-vllm-split] checking worker recovery"
"${docker_cmd[@]}" start "${worker_b_container}" >/dev/null
recovered_payload="$(
  wait_for_json_field \
    "${controller_url}/api/v1/planes/${plane_name}/interaction/status" \
    "payload.get('ready') is True and payload.get('worker_group_ready') == 2" \
    180
)"
python3 - <<'PY' "${recovered_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
assert payload["ready"] is True, payload
assert payload["worker_group_ready"] == 2, payload
print("split_recovery=ok")
PY

echo "[check-live-vllm-split] checking cleanup"
curl -fsS -X DELETE "${controller_url}/api/v1/planes/${plane_name}" >/dev/null
delete_payload="$(
  wait_for_json_field \
    "${controller_url}/api/v1/planes/${plane_name}/interaction/status" \
    "payload.get('plane_state') != 'running'" \
    90 || true
)"
find_output="$(${sudo_prefix} find /mnt/shared-storage/comet/disk-images -maxdepth 5 -type f | grep -F "${plane_name}" || true)"
if [[ -n "${find_output}" ]]; then
  echo "error: split-host cleanup left disk images behind" >&2
  printf '%s\n' "${find_output}" >&2
  exit 1
fi
echo "split_cleanup=ok"

echo "[check-live-vllm-split] success"
