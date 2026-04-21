#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-production-env.sh"

inventory_path=""
declare -a selected_workers=()
failures=0

usage() {
  cat <<'EOF'
Usage:
  naim-doctor.sh [--inventory <path>] [--worker <name> ...]

Verifies a deployed NAIM control plane and worker hostd containers.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      if [[ -z "${inventory_path}" ]]; then
        echo "error: --inventory requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    --worker)
      next_worker="${2:-}"
      if [[ -z "${next_worker}" ]]; then
        echo "error: --worker requires a name" >&2
        exit 1
      fi
      selected_workers+=("${next_worker}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

pass() {
  printf '[ok] %s\n' "$1"
}

fail() {
  printf '[fail] %s\n' "$1" >&2
  failures=$((failures + 1))
}

check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    pass "${label}"
  else
    fail "${label}"
  fi
}

load_inventory_for_worker() {
  local worker_name="${1:-}"
  if [[ -n "${inventory_path}" ]]; then
    eval "$(naim_load_inventory_env "${inventory_path}" "${worker_name}")"
  fi
}

load_inventory_for_worker "${selected_workers[0]:-}"

check control.compose_running ssh_main "cd '${NAIM_MAIN_ROOT}' && docker compose ps"
check control.controller_health ssh_main \
  "curl -fsS 'http://127.0.0.1:${NAIM_MAIN_CONTROLLER_LOCAL_PORT}/health'"
check control.web_ui_health ssh_main \
  "curl -fsS 'http://127.0.0.1:${NAIM_MAIN_WEB_UI_LOCAL_PORT}/health'"
check control.skills_factory_health ssh_main \
  "docker exec naim-skills-factory bash -lc 'exec 3<>/dev/tcp/127.0.0.1/18082; printf \"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n\" >&3; response=\"\$(cat <&3)\"; grep -F \"\\\"service\\\":\\\"naim-skills-factory\\\"\" <<<\"\${response}\" >/dev/null'"
check control.state_permissions ssh_main \
  "test \"\$(stat -c %a '${NAIM_MAIN_ROOT}/state' 2>/dev/null)\" = 750 && test \"\$(stat -c %a '${NAIM_MAIN_ROOT}/state/controller.sqlite' 2>/dev/null)\" = 640"

if [[ -n "${inventory_path}" && ${#selected_workers[@]} -eq 0 ]]; then
  while IFS= read -r worker; do
    [[ -n "${worker}" ]] && selected_workers+=("${worker}")
  done < <("${script_dir}/naim-inventory.py" workers --inventory "${inventory_path}")
fi

if [[ ${#selected_workers[@]} -eq 0 ]]; then
  selected_workers=("${NAIM_HOSTD_NODE}")
fi

for worker in "${selected_workers[@]}"; do
  load_inventory_for_worker "${worker}"
  node_name="${NAIM_HOSTD_NODE}"
  check "controller.host.${node_name}.connected" remote_main_bash \
    "http://127.0.0.1:${NAIM_MAIN_CONTROLLER_LOCAL_PORT}" \
    "${node_name}" <<'REMOTE'
set -euo pipefail
controller_url="$1"
node_name="$2"
python3 - "${controller_url}" "${node_name}" <<'PY'
import json
import sys
import urllib.parse
import urllib.request

controller_url, node_name = sys.argv[1:3]
url = controller_url + "/api/v1/hostd/hosts?node=" + urllib.parse.quote(node_name)
with urllib.request.urlopen(url) as response:
    payload = json.load(response)
items = payload.get("items", [])
if not items:
    raise SystemExit("host not found")
host = items[0]
checks = {
    "registration_state": host.get("registration_state") == "registered",
    "onboarding_state": host.get("onboarding_state") == "completed",
    "session_state": host.get("session_state") == "connected",
    "role_eligible": host.get("role_eligible") is True,
}
missing = [name for name, ok in checks.items() if not ok]
if missing:
    raise SystemExit("failed checks: " + ",".join(missing) + " host=" + json.dumps(host))
print(json.dumps({
    "node_name": host.get("node_name"),
    "session_state": host.get("session_state"),
    "derived_role": host.get("derived_role"),
    "role_eligible": host.get("role_eligible"),
    "capacity_summary": host.get("capacity_summary"),
}, sort_keys=True))
PY
REMOTE

  check "worker.${node_name}.compose_running" ssh_hpc1 \
    "cd '${NAIM_HOSTD_ROOT}' && docker compose ps"
  check "worker.${node_name}.hostd_supervisor_cmd" ssh_hpc1 \
    "docker inspect naim-hostd --format '{{json .Config.Entrypoint}} {{json .Config.Cmd}}' | grep -F '/runtime/bin/naim-node' | grep -F 'run'"
  check "worker.${node_name}.secret_cleanup" ssh_hpc1 \
    "test ! -f '${NAIM_HOSTD_ROOT}/.env' && ! grep -R 'onboarding_key = \".\\+\"' '${NAIM_HOSTD_ROOT}/naim-node.toml' >/dev/null"
  check "worker.${node_name}.key_permissions" ssh_hpc1 \
    "test \"\$(stat -c %a '${NAIM_HOSTD_ROOT}' 2>/dev/null)\" = 700 && test \"\$(stat -c %a '${NAIM_HOSTD_ROOT}/install-state/keys/hostd.key.b64' 2>/dev/null)\" = 600"
  check "worker.${node_name}.no_persistent_registry_auth" ssh_hpc1 \
    "python3 - <<'PY'
import json
from pathlib import Path

path = Path.home() / '.docker' / 'config.json'
if not path.exists() or path.stat().st_size == 0:
    raise SystemExit(0)
data = json.loads(path.read_text())
if data.get('auths') or data.get('credHelpers') or data.get('credsStore'):
    raise SystemExit(1)
PY"
  if [[ "${NAIM_HOSTD_ENABLE_NVIDIA}" == "yes" ]]; then
    check "worker.${node_name}.gpu_visible_in_hostd" ssh_hpc1 \
      "docker exec naim-hostd nvidia-smi --query-gpu=index,memory.total --format=csv,noheader | grep -q ."
  fi
done

if [[ "${failures}" -ne 0 ]]; then
  echo "doctor failed: ${failures} checks failed" >&2
  exit 1
fi

echo "doctor passed"
