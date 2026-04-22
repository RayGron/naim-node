#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/deploy/naim-production-env.sh"

inventory_path=""
release_tag="${NAIM_RELEASE_TAG:-${NAIM_IMAGE_TAG:-}}"
timeout_sec="${NAIM_MIGRATION_TIMEOUT_SEC:-600}"

usage() {
  cat <<'EOF'
Usage:
  redeploy-migrate-production.sh --tag <tag> [--inventory <path>]

Deploys the registered NAIM image tag to main and worker hostd containers,
triggers controller reconciliation, and waits until pending assignments settle.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      shift 2
      ;;
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

export NAIM_IMAGE_TAG="${release_tag}"

deploy_args=()
if [[ -n "${inventory_path}" ]]; then
  deploy_args+=(--inventory "${inventory_path}")
fi
"${repo_root}/scripts/deploy/naim-deploy.sh" "${deploy_args[@]}"

if [[ -n "${inventory_path}" ]]; then
  eval "$(NAIM_IMAGE_TAG="${release_tag}" naim_load_inventory_env "${inventory_path}")"
fi

remote_main_bash "${NAIM_MAIN_ROOT}" "${NAIM_MAIN_CONTROLLER_LOCAL_PORT}" "${timeout_sec}" <<'REMOTE'
set -euo pipefail
main_root="$1"
controller_port="$2"
timeout_sec="$3"
controller_url="http://127.0.0.1:${controller_port}"
db_path="${main_root}/state/controller.sqlite"

curl -fsS -X POST "${controller_url}/api/v1/scheduler-tick" >/dev/null 2>&1 || true
curl -fsS -X POST "${controller_url}/api/v1/reconcile-rollout-actions" >/dev/null 2>&1 || true
curl -fsS -X POST "${controller_url}/api/v1/reconcile-rebalance-proposals" >/dev/null 2>&1 || true

sudo python3 - "${db_path}" "${timeout_sec}" <<'PY'
import sqlite3
import sys
import time

db_path, timeout_text = sys.argv[1:3]
deadline = time.time() + int(timeout_text)
last = {}

while time.time() < deadline:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    pending = con.execute(
        "select count(*) as count from host_assignments where status in ('pending','claimed')"
    ).fetchone()["count"]
    rollout = con.execute(
        "select count(*) as count from rollout_actions where status in ('pending','acknowledged','ready-to-retry')"
    ).fetchone()["count"]
    planes = con.execute(
        "select count(*) as count from planes where state not in ('stopped','deleted')"
    ).fetchone()["count"]
    con.close()
    last = {"pending_assignments": pending, "active_rollout_actions": rollout, "active_planes": planes}
    print("migration poll:", last, flush=True)
    if pending == 0:
        raise SystemExit(0)
    time.sleep(5)

raise SystemExit("migration did not settle before timeout; last=" + repr(last))
PY
REMOTE

doctor_args=()
if [[ -n "${inventory_path}" ]]; then
  doctor_args+=(--inventory "${inventory_path}")
fi
"${repo_root}/scripts/deploy/naim-doctor.sh" "${doctor_args[@]}"
