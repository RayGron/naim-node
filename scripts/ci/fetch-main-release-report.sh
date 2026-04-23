#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/deploy/naim-production-env.sh"

release_tag="${NAIM_RELEASE_TAG:-${NAIM_IMAGE_TAG:-}}"
output_path=""
manifest_path=""

usage() {
  cat <<'EOF'
Usage:
  fetch-main-release-report.sh --tag <tag> [--output <path>] [--manifest <path>]

Fetches a short controller-centric release report from the main node.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      release_tag="${2:-}"
      shift 2
      ;;
    --output)
      output_path="${2:-}"
      shift 2
      ;;
    --manifest)
      manifest_path="${2:-}"
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

report_text="$(
  remote_main_bash \
    "${NAIM_MAIN_ROOT}" \
    "${release_tag}" \
    "${manifest_path}" <<'REMOTE'
set -euo pipefail
main_root="$1"
release_tag="$2"
manifest_path="$3"

db_path="${main_root}/state/controller.sqlite"
if [[ -z "${manifest_path}" ]]; then
  manifest_path="${main_root}/releases/${release_tag}.json"
fi

python3 - "${db_path}" "${release_tag}" "${manifest_path}" <<'PY'
import json
import sqlite3
import sys

db_path, release_tag, manifest_path = sys.argv[1:4]
manifest_summary = "not provided"
manifest_hostd = None
try:
    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest_summary = (
        f"{manifest.get('registry')}/{manifest.get('project')} "
        f"tag={manifest.get('tag')} images={len(manifest.get('images') or {})}"
    )
    manifest_hostd = (manifest.get("images") or {}).get("hostd")
except FileNotFoundError:
    pass

con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
hosts = con.execute(
    "select node_name, registration_state, session_state, status_message, last_heartbeat_at "
    "from registered_hosts order by node_name"
).fetchall()
assignments = con.execute(
    "select node_name, status, status_message, updated_at "
    "from host_assignments where plane_name = ? order by id desc",
    ("system:hostd-release",),
).fetchall()
latest_assignment_by_node = {}
for row in assignments:
    latest_assignment_by_node.setdefault(row["node_name"], row)
con.close()

lines = []
lines.append("## NAIM production release")
lines.append("")
lines.append(f"- tag: `{release_tag}`")
lines.append(f"- manifest: {manifest_summary}")
if manifest_hostd:
    lines.append(f"- hostd image: `{manifest_hostd}`")
lines.append("")
lines.append("### Main")
lines.append("- controller rollout: controller queued hostd self-update assignments")
lines.append("")
lines.append("### Nodes")
for host in hosts:
    assignment = latest_assignment_by_node.get(host["node_name"])
    summary = (
        f"- {host['node_name']}: registration={host['registration_state']} "
        f"session={host['session_state']} "
        f"heartbeat={host['last_heartbeat_at'] or 'n/a'}"
    )
    lines.append(summary)
    if host["status_message"]:
        lines.append(f"  - host status: {host['status_message']}")
    if assignment is not None:
        lines.append(
            f"  - hostd rollout: status={assignment['status']} updated_at={assignment['updated_at']}"
        )
        if assignment["status_message"]:
            lines.append(f"  - rollout detail: {assignment['status_message']}")
    else:
        lines.append("  - hostd rollout: no assignment recorded")

print("\n".join(lines))
PY
REMOTE
)"

printf '%s\n' "${report_text}"
if [[ -n "${output_path}" ]]; then
  printf '%s\n' "${report_text}" > "${output_path}"
fi
