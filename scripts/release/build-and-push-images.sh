#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"

registry=""
project="naim"
tag=""
manifest_path=""
skip_web_ui="no"

usage() {
  cat <<'EOF'
Usage:
  build-and-push-images.sh --registry <registry> --tag <tag> [--project naim]
                           [--manifest <path>] [--skip-web-ui]

Builds NAIM runtime images, pushes them to the registry, and writes a release
manifest with the pushed image references.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)
      registry="${2:-}"
      shift 2
      ;;
    --project)
      project="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --manifest)
      manifest_path="${2:-}"
      shift 2
      ;;
    --skip-web-ui)
      skip_web_ui="yes"
      shift
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

if [[ -z "${registry}" || -z "${tag}" ]]; then
  echo "error: --registry and --tag are required" >&2
  usage >&2
  exit 1
fi

if [[ "${tag}" == "dev" || "${tag}" == "latest" ]]; then
  echo "error: use an immutable production tag, not '${tag}'" >&2
  exit 1
fi

if [[ -z "${manifest_path}" ]]; then
  manifest_path="${repo_root}/var/release-manifest-${tag}.json"
fi

mkdir -p "$(dirname -- "${manifest_path}")"

image_ref() {
  local image="$1"
  printf '%s/%s/%s:%s' "${registry}" "${project}" "${image}" "${tag}"
}

latest_image_ref() {
  local image="$1"
  printf '%s/%s/%s:latest' "${registry}" "${project}" "${image}"
}

delete_remote_latest_tag() {
  local image="$1"
  local username="${NAIM_REGISTRY_USERNAME:-}"
  local password_file="${NAIM_REGISTRY_PASSWORD_FILE:-}"
  if [[ -z "${username}" || -z "${password_file}" || ! -f "${password_file}" ]]; then
    return 0
  fi

  python3 - "${registry}" "${project}" "${image}" "${username}" "${password_file}" <<'PY'
import base64
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

registry, project, image, username, password_file = sys.argv[1:6]
password = open(password_file, encoding="utf-8").read().strip()
auth = base64.b64encode(f"{username}:{password}".encode()).decode()
base = f"https://{registry}/api/v2.0"
repository = urllib.parse.quote(image, safe="")
tag = urllib.parse.quote("latest", safe="")
request = urllib.request.Request(
    f"{base}/projects/{project}/repositories/{repository}/artifacts/latest/tags/{tag}",
    headers={"Authorization": "Basic " + auth},
    method="DELETE",
)
try:
    with urllib.request.urlopen(request, context=ssl._create_unverified_context(), timeout=30):
        pass
except urllib.error.HTTPError as error:
    if error.code == 403:
        print(
            f"warning: registry user cannot delete old latest tag for {image}; "
            "moving latest by push",
            file=sys.stderr,
        )
    elif error.code not in (404,):
        detail = error.read().decode("utf-8", "replace")
        raise SystemExit(f"failed to delete remote latest tag for {image}: HTTP {error.code} {detail}")
PY
}

base_ref="$(image_ref base-runtime)"
infer_ref="$(image_ref infer-runtime)"
worker_ref="$(image_ref worker-runtime)"
web_ui_ref="$(image_ref web-ui)"
skills_ref="$(image_ref skills-runtime)"
knowledge_ref="$(image_ref knowledge-runtime)"
webgateway_ref="$(image_ref webgateway-runtime)"
controller_ref="$(image_ref controller)"
hostd_ref="$(image_ref hostd)"

build_args=(
  "${base_ref}"
  "${infer_ref}"
  "${worker_ref}"
  "${web_ui_ref}"
  "${skills_ref}"
  "${webgateway_ref}"
  "${controller_ref}"
  "${hostd_ref}"
  "${knowledge_ref}"
)

if [[ "${skip_web_ui}" == "yes" ]]; then
  "${repo_root}/scripts/build-runtime-images.sh" --skip-web-ui "${build_args[@]}"
else
  "${repo_root}/scripts/build-runtime-images.sh" "${build_args[@]}"
fi

declare -a image_names=(
  base-runtime
  controller
  hostd
  infer-runtime
  worker-runtime
  skills-runtime
  knowledge-runtime
  webgateway-runtime
)
if [[ "${skip_web_ui}" != "yes" ]]; then
  image_names+=(web-ui)
fi

json_entries=()
for image in "${image_names[@]}"; do
  ref="$(image_ref "${image}")"
  echo "pushing ${ref}"
  docker push "${ref}"
  repo_digest="$(docker image inspect \
    --format '{{range .RepoDigests}}{{println .}}{{end}}' \
    "${ref}" | grep -F "${registry}/${project}/${image}@" | head -n1 || true)"
  if [[ -z "${repo_digest}" ]]; then
    repo_digest="${ref}"
  fi
  json_entries+=("$(printf '    \"%s\": \"%s\"' "${image}" "${repo_digest}")")
  latest_ref="$(latest_image_ref "${image}")"
  echo "moving ${latest_ref} to ${ref}"
  delete_remote_latest_tag "${image}"
  docker tag "${ref}" "${latest_ref}"
  docker push "${latest_ref}"
done

{
  printf '{\n'
  printf '  "registry": "%s",\n' "${registry}"
  printf '  "project": "%s",\n' "${project}"
  printf '  "tag": "%s",\n' "${tag}"
  printf '  "images": {\n'
  for index in "${!json_entries[@]}"; do
    suffix=","
    if [[ "${index}" -eq $((${#json_entries[@]} - 1)) ]]; then
      suffix=""
    fi
    printf '%s%s\n' "${json_entries[index]}" "${suffix}"
  done
  printf '  }\n'
  printf '}\n'
} > "${manifest_path}"

echo "release manifest written: ${manifest_path}"
