#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/deploy/naim-production-env.sh"

release_tag="${NAIM_RELEASE_TAG:-${NAIM_IMAGE_TAG:-}}"
timeout_sec="${NAIM_MIGRATION_TIMEOUT_SEC:-600}"

usage() {
  cat <<'EOF'
Usage:
  main-bootstrap-controller-update.sh --tag <tag>

Updates the main control-plane bootstrap first, waits for the controller to
become healthy, refreshes local control-plane services on main, then asks the
updated controller to roll out managed dependents such as connected hostd nodes
and the configured Knowledge Vault service.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      release_tag="${2:-}"
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

if [[ -z "${release_tag}" ]]; then
  echo "error: --tag or NAIM_RELEASE_TAG is required" >&2
  usage >&2
  exit 1
fi

registry_password_file_on_main=""
cleanup() {
  if [[ -n "${registry_password_file_on_main}" ]]; then
    ssh_main "rm -f '${registry_password_file_on_main}'" || true
  fi
}
trap cleanup EXIT

registry_username="$(naim_registry_username)"
registry_password="$(naim_registry_password)"
if [[ -n "${registry_username}" && -n "${registry_password}" ]]; then
  registry_password_file_on_main="/tmp/naim-registry-password-bootstrap-release-$$"
  printf '%s' "${registry_password}" | ssh_main \
    "umask 077; cat > '${registry_password_file_on_main}'"
fi
unset registry_password

remote_main_bash \
  "${NAIM_MAIN_ROOT}" \
  "${release_tag}" \
  "${timeout_sec}" \
  "${NAIM_MAIN_CONTROLLER_LOCAL_PORT}" \
  "${NAIM_MAIN_WEB_UI_LOCAL_PORT}" \
  "${NAIM_REGISTRY}" \
  "${registry_username}" \
  "${registry_password_file_on_main}" <<'REMOTE'
set -euo pipefail

main_root="$1"
release_tag="$2"
timeout_sec="$3"
controller_port="$4"
web_ui_port="$5"
registry="$6"
registry_username="$7"
registry_password_file="$8"
manifest_path="${main_root}/releases/${release_tag}.json"
db_path="${main_root}/state/controller.sqlite"
compose_path="${main_root}/docker-compose.yml"

if [[ ! -f "${manifest_path}" ]]; then
  echo "release manifest is missing on main: ${manifest_path}" >&2
  exit 1
fi
if [[ ! -f "${db_path}" ]]; then
  echo "controller db is missing on main: ${db_path}" >&2
  exit 1
fi
if [[ ! -f "${compose_path}" ]]; then
  echo "control-plane compose file is missing on main: ${compose_path}" >&2
  exit 1
fi

eval "$(
  python3 - "${manifest_path}" <<'PY'
import json
import shlex
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
images = payload.get("images") or {}
controller = images.get("controller")
if not controller:
    raise SystemExit("release manifest is missing images.controller")
web_ui = images.get("web-ui", "")
knowledge = images.get("knowledge-runtime", "")
print("controller_image=" + shlex.quote(controller))
print("web_ui_image=" + shlex.quote(web_ui))
print("knowledge_image=" + shlex.quote(knowledge))
PY
)"

docker_config=""
cleanup_remote() {
  if [[ -n "${docker_config}" ]]; then
    rm -rf "${docker_config}"
  fi
}
trap cleanup_remote EXIT

if [[ -n "${registry_username}" && -n "${registry_password_file}" && -f "${registry_password_file}" ]]; then
  docker_config="$(mktemp -d)"
  export DOCKER_CONFIG="${docker_config}"
  docker login "${registry}" -u "${registry_username}" --password-stdin \
    < "${registry_password_file}" >/dev/null
fi

python3 - "${compose_path}" "${controller_image}" "${web_ui_image}" <<'PY'
from pathlib import Path
import sys

compose_path = Path(sys.argv[1])
controller_image = sys.argv[2]
web_ui_image = sys.argv[3]
targets = {
    "naim-controller": controller_image,
    "naim-skills-factory": controller_image,
}
if web_ui_image:
    targets["naim-web-ui"] = web_ui_image

lines = compose_path.read_text(encoding="utf-8").splitlines()
current_service = None
for index, line in enumerate(lines):
    stripped = line.strip()
    if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
        service_name = stripped[:-1]
        current_service = service_name if service_name in targets else None
        continue
    if current_service and line.startswith("    image: "):
        indent = line[: line.index("i")]
        lines[index] = f"{indent}image: {targets[current_service]}"
        current_service = None
compose_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

wait_for_http() {
  local url="$1"
  local attempts="${2:-45}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${url}" >&2
  return 1
}

docker compose -f "${compose_path}" pull naim-controller >/dev/null
docker compose -f "${compose_path}" up -d --remove-orphans naim-controller >/dev/null
wait_for_http "http://127.0.0.1:${controller_port}/health" 90

docker compose -f "${compose_path}" pull naim-skills-factory naim-web-ui >/dev/null
docker compose -f "${compose_path}" up -d --remove-orphans naim-skills-factory naim-web-ui >/dev/null
wait_for_http "http://127.0.0.1:${web_ui_port}/health" 90
docker exec naim-skills-factory bash -lc \
  'exec 3<>/dev/tcp/127.0.0.1/18082
   printf "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" >&3
   response="$(cat <&3)"
   grep -F "\"service\":\"naim-skills-factory\"" <<<"${response}" >/dev/null'

rollout_json="$(
  docker run --rm \
    -v "${main_root}/state:/naim/state" \
    -v "${main_root}/releases:/naim/releases:ro" \
    "${controller_image}" \
    rollout-release \
      --db /naim/state/controller.sqlite \
      --manifest "/naim/releases/${release_tag}.json"
)"

printf '%s\n' "${rollout_json}"

python3 - "${db_path}" "${timeout_sec}" "${rollout_json}" <<'PY'
import json
import sqlite3
import sys
import time

db_path, timeout_text, rollout_text = sys.argv[1:4]
payload = json.loads(rollout_text)
targeted_nodes = payload.get("targeted_nodes") or []
knowledge_services = payload.get("knowledge_vault_services") or []
notify_at = payload.get("notify_at") or ""

deadline = time.time() + int(timeout_text)
last = {}
while time.time() < deadline:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    pending_hostd = 0
    pending_knowledge = 0
    nodes = {}
    services = {}
    for node_name in targeted_nodes:
        pending_hostd += con.execute(
            "select count(*) as count from host_assignments "
            "where plane_name = ? and node_name = ? and status in ('pending','claimed')",
            ("system:hostd-release", node_name),
        ).fetchone()["count"]
        host = con.execute(
            "select session_state, last_heartbeat_at, status_message "
            "from registered_hosts where node_name = ?",
            (node_name,),
        ).fetchone()
        nodes[node_name] = {
            "session_state": None if host is None else host["session_state"],
            "last_heartbeat_at": None if host is None else host["last_heartbeat_at"],
            "status_message": None if host is None else host["status_message"],
        }
    for item in knowledge_services:
        service_id = item.get("service_id") or ""
        plane_name = "knowledge-vault:" + service_id
        pending_knowledge += con.execute(
            "select count(*) as count from host_assignments "
            "where plane_name = ? and status in ('pending','claimed')",
            (plane_name,),
        ).fetchone()["count"]
        service = con.execute(
            "select status, status_message, image, updated_at "
            "from knowledge_vault_services where service_id = ?",
            (service_id,),
        ).fetchone()
        services[service_id] = {
            "status": None if service is None else service["status"],
            "status_message": None if service is None else service["status_message"],
            "image": None if service is None else service["image"],
            "updated_at": None if service is None else service["updated_at"],
        }
    con.close()

    all_nodes_reconnected = True
    for state in nodes.values():
        if state["session_state"] != "connected":
            all_nodes_reconnected = False
            break
        if notify_at and (state["last_heartbeat_at"] or "") < notify_at:
            all_nodes_reconnected = False
            break

    last = {
        "pending_hostd_assignments": pending_hostd,
        "pending_knowledge_assignments": pending_knowledge,
        "nodes": nodes,
        "knowledge_vault_services": services,
    }
    print("bootstrap release rollout poll:", json.dumps(last, sort_keys=True), flush=True)
    if pending_hostd == 0 and pending_knowledge == 0 and all_nodes_reconnected:
        raise SystemExit(0)
    time.sleep(5)

raise SystemExit(
    "bootstrap release rollout did not settle before timeout; last="
    + json.dumps(last, sort_keys=True)
)
PY
REMOTE
