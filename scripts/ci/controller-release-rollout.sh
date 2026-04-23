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
  controller-release-rollout.sh --tag <tag>

Signals the main controller that a new release is available, lets it enqueue
hostd self-update assignments for connected nodes, and waits until those nodes
reconnect after applying the rollout.
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
  registry_password_file_on_main="/tmp/naim-registry-password-release-rollout-$$"
  printf '%s' "${registry_password}" | ssh_main \
    "umask 077; cat > '${registry_password_file_on_main}'"
fi
unset registry_password

remote_main_bash \
  "${NAIM_MAIN_ROOT}" \
  "${release_tag}" \
  "${timeout_sec}" \
  "${NAIM_REGISTRY}" \
  "${registry_username}" \
  "${registry_password_file_on_main}" <<'REMOTE'
set -euo pipefail

main_root="$1"
release_tag="$2"
timeout_sec="$3"
registry="$4"
registry_username="$5"
registry_password_file="$6"
manifest_path="${main_root}/releases/${release_tag}.json"
db_path="${main_root}/state/controller.sqlite"

if [[ ! -f "${manifest_path}" ]]; then
  echo "release manifest is missing on main: ${manifest_path}" >&2
  exit 1
fi
if [[ ! -f "${db_path}" ]]; then
  echo "controller db is missing on main: ${db_path}" >&2
  exit 1
fi

controller_image="$(
  python3 - "${manifest_path}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
image = (payload.get("images") or {}).get("controller")
if not image:
    raise SystemExit("release manifest is missing images.controller")
print(image)
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

docker pull "${controller_image}" >/dev/null

notify_json="$(
  docker run --rm \
    -v "${main_root}/state:/naim/state" \
    -v "${main_root}/releases:/naim/releases:ro" \
    "${controller_image}" \
    notify-release \
      --db /naim/state/controller.sqlite \
      --manifest "/naim/releases/${release_tag}.json"
)"

printf '%s\n' "${notify_json}"

python3 - "${db_path}" "${timeout_sec}" "${notify_json}" <<'PY'
import json
import sqlite3
import sys
import time

db_path, timeout_text, notify_text = sys.argv[1:4]
payload = json.loads(notify_text)
targeted_nodes = payload.get("targeted_nodes") or []
notify_at = payload.get("notify_at") or ""
if not targeted_nodes:
    raise SystemExit(0)

deadline = time.time() + int(timeout_text)
last = {}
while time.time() < deadline:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    pending = 0
    nodes = {}
    for node_name in targeted_nodes:
        pending += con.execute(
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
    con.close()
    last = {"pending_assignments": pending, "nodes": nodes}
    all_reconnected = True
    for state in nodes.values():
        if state["session_state"] != "connected":
            all_reconnected = False
            break
        if notify_at and (state["last_heartbeat_at"] or "") < notify_at:
            all_reconnected = False
            break
    print("release rollout poll:", json.dumps(last, sort_keys=True), flush=True)
    if pending == 0 and all_reconnected:
      raise SystemExit(0)
    time.sleep(5)

raise SystemExit("controller-driven hostd rollout did not settle before timeout; last=" + json.dumps(last, sort_keys=True))
PY
REMOTE
