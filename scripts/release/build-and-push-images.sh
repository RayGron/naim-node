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
