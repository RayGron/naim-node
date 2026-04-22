#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/deploy/naim-production-env.sh"

inventory_path=""
manifest_path=""
release_tag=""

usage() {
  cat <<'EOF'
Usage:
  register-release-on-main.sh --manifest <path> --tag <tag> [--inventory <path>]

Registers a pushed NAIM release manifest on the main server without starting
or stopping any containers.
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

if [[ -z "${manifest_path}" || -z "${release_tag}" ]]; then
  echo "error: --manifest and --tag are required" >&2
  usage >&2
  exit 1
fi
if [[ ! -f "${manifest_path}" ]]; then
  echo "error: manifest not found: ${manifest_path}" >&2
  exit 1
fi

if [[ -n "${inventory_path}" ]]; then
  eval "$(NAIM_IMAGE_TAG="${release_tag}" naim_load_inventory_env "${inventory_path}")"
fi

python3 - "${manifest_path}" "${release_tag}" <<'PY'
import json
import sys

path, expected_tag = sys.argv[1:3]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
if payload.get("tag") != expected_tag:
    raise SystemExit(f"manifest tag {payload.get('tag')!r} does not match {expected_tag!r}")
if not isinstance(payload.get("images"), dict) or not payload["images"]:
    raise SystemExit("manifest does not contain pushed images")
PY

manifest_b64="$(base64 -w0 "${manifest_path}")"
remote_main_bash "${NAIM_MAIN_ROOT}" "${release_tag}" "${manifest_b64}" <<'REMOTE'
set -euo pipefail
main_root="$1"
release_tag="$2"
manifest_b64="$3"

release_dir="${main_root}/releases"
install -d -m 0750 "${release_dir}"
manifest_path="${release_dir}/${release_tag}.json"
printf '%s' "${manifest_b64}" | base64 -d > "${manifest_path}"
chmod 0640 "${manifest_path}"
ln -sfn "${release_tag}.json" "${release_dir}/current.json"
printf '%s\n' "${release_tag}" > "${release_dir}/current-tag"
chmod 0640 "${release_dir}/current-tag"
echo "registered release ${release_tag} at ${manifest_path}"
REMOTE
