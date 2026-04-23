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

Fetches a short bootstrap-and-controller release report from the main node.
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
import subprocess
import sys

db_path, release_tag, manifest_path = sys.argv[1:4]
manifest_summary = "not provided"
manifest_hostd = None
manifest_controller = None
manifest_web_ui = None
manifest_knowledge = None
try:
    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest_summary = (
        f"{manifest.get('registry')}/{manifest.get('project')} "
        f"tag={manifest.get('tag')} images={len(manifest.get('images') or {})}"
    )
    manifest_controller = (manifest.get("images") or {}).get("controller")
    manifest_hostd = (manifest.get("images") or {}).get("hostd")
    manifest_web_ui = (manifest.get("images") or {}).get("web-ui")
    manifest_knowledge = (manifest.get("images") or {}).get("knowledge-runtime")
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
knowledge_services = con.execute(
    "select service_id, node_name, image, status, status_message, updated_at "
    "from knowledge_vault_services order by service_id"
).fetchall()
knowledge_assignments = con.execute(
    "select plane_name, node_name, status, status_message, updated_at "
    "from host_assignments where plane_name like 'knowledge-vault:%' order by id desc"
).fetchall()
latest_assignment_by_node = {}
for row in assignments:
    latest_assignment_by_node.setdefault(row["node_name"], row)
latest_knowledge_assignment_by_plane = {}
for row in knowledge_assignments:
    latest_knowledge_assignment_by_plane.setdefault(row["plane_name"], row)
con.close()

docker_lines = []
try:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "--format",
            "{{.Names}}|{{.Image}}|{{.Status}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    docker_lines = [
        line for line in result.stdout.splitlines()
        if line.split("|", 1)[0] in {"naim-controller", "naim-skills-factory", "naim-web-ui"}
    ]
except Exception:
    docker_lines = []

lines = []
lines.append("## NAIM production release")
lines.append("")
lines.append(f"- tag: `{release_tag}`")
lines.append(f"- manifest: {manifest_summary}")
if manifest_controller:
    lines.append(f"- controller image: `{manifest_controller}`")
if manifest_hostd:
    lines.append(f"- hostd image: `{manifest_hostd}`")
if manifest_web_ui:
    lines.append(f"- web ui image: `{manifest_web_ui}`")
if manifest_knowledge:
    lines.append(f"- knowledge runtime image: `{manifest_knowledge}`")
lines.append("")
lines.append("### Main")
if docker_lines:
    for item in docker_lines:
        name, image, status = item.split("|", 2)
        lines.append(f"- {name}: image=`{image}` status={status}")
else:
    lines.append("- control-plane containers: unavailable")
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

if knowledge_services:
    lines.append("")
    lines.append("### Knowledge Vault")
    for service in knowledge_services:
        plane_name = "knowledge-vault:" + service["service_id"]
        assignment = latest_knowledge_assignment_by_plane.get(plane_name)
        lines.append(
            f"- {service['service_id']}: node={service['node_name']} status={service['status']} "
            f"image=`{service['image']}` updated_at={service['updated_at']}"
        )
        if service["status_message"]:
            lines.append(f"  - service status: {service['status_message']}")
        if assignment is not None:
            lines.append(
                f"  - rollout assignment: status={assignment['status']} "
                f"updated_at={assignment['updated_at']}"
            )
            if assignment["status_message"]:
                lines.append(f"  - rollout detail: {assignment['status_message']}")
        else:
            lines.append("  - rollout assignment: no assignment recorded")

print("\n".join(lines))
PY
REMOTE
)"

printf '%s\n' "${report_text}"
if [[ -n "${output_path}" ]]; then
  printf '%s\n' "${report_text}" > "${output_path}"
fi
