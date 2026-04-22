#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/deploy/naim-production-env.sh"

inventory_path=""
manifest_path=""
release_tag="${NAIM_RELEASE_TAG:-${NAIM_IMAGE_TAG:-}}"
output_path=""

usage() {
  cat <<'EOF'
Usage:
  production-release-report.sh --tag <tag> [--manifest <path>] [--inventory <path>] [--output <path>]

Writes a short Markdown report for a production release migration.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      shift 2
      ;;
    --manifest)
      manifest_path="${2:-}"
      shift 2
      ;;
    --tag)
      release_tag="${2:-}"
      shift 2
      ;;
    --output)
      output_path="${2:-}"
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
if [[ -n "${inventory_path}" ]]; then
  eval "$(NAIM_IMAGE_TAG="${release_tag}" naim_load_inventory_env "${inventory_path}")"
fi

doctor_log="$(mktemp)"
doctor_status="passed"
doctor_args=()
if [[ -n "${inventory_path}" ]]; then
  doctor_args+=(--inventory "${inventory_path}")
fi
if ! "${repo_root}/scripts/deploy/naim-doctor.sh" "${doctor_args[@]}" >"${doctor_log}" 2>&1; then
  doctor_status="failed"
fi

manifest_summary="not provided"
if [[ -n "${manifest_path}" && -f "${manifest_path}" ]]; then
  manifest_summary="$(
    python3 - "${manifest_path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
images = payload.get("images") or {}
print(f"{payload.get('registry')}/{payload.get('project')} tag={payload.get('tag')} images={len(images)}")
PY
  )"
fi

worker_names=()
if [[ -n "${inventory_path}" ]]; then
  while IFS= read -r worker; do
    [[ -n "${worker}" ]] && worker_names+=("${worker}")
  done < <("${repo_root}/scripts/deploy/naim-inventory.py" workers --inventory "${inventory_path}")
fi
if [[ ${#worker_names[@]} -eq 0 ]]; then
  worker_names=("${NAIM_HOSTD_NODE}")
fi

report_tmp="$(mktemp)"
{
  echo "## NAIM production release"
  echo
  echo "- tag: \`${release_tag}\`"
  echo "- manifest: ${manifest_summary}"
  echo "- doctor: ${doctor_status}"
  echo
  echo "### Main"
  ssh_main "docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E '^(naim-controller|naim-web-ui|naim-skills-factory)' || true" \
    | sed 's/^/- /'
  echo
  echo "### Workers"
  for worker in "${worker_names[@]}"; do
    if [[ -n "${inventory_path}" ]]; then
      eval "$(NAIM_IMAGE_TAG="${release_tag}" naim_load_inventory_env "${inventory_path}" "${worker}")"
    fi
    echo "- ${NAIM_HOSTD_NODE}:"
    ssh_hpc1 "docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E '(^naim-hostd|^naim-|lt-cypher-ai)' | head -30 || true" \
      | sed 's/^/  - /'
  done
  echo
  echo "### Doctor"
  sed 's/^/- /' "${doctor_log}" | tail -40
} > "${report_tmp}"

cat "${report_tmp}"
if [[ -n "${output_path}" ]]; then
  cp "${report_tmp}" "${output_path}"
fi

if [[ "${doctor_status}" != "passed" ]]; then
  exit 1
fi
