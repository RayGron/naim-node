#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

skip_web_ui="no"
if [[ "${1:-}" == "--skip-web-ui" ]]; then
  skip_web_ui="yes"
  shift
fi

resolve_docker() {
  if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    echo "docker"
    return 0
  fi

  local windows_docker="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [[ -x "${windows_docker}" ]] && "${windows_docker}" version >/dev/null 2>&1; then
    echo "${windows_docker}"
    return 0
  fi

  echo "error: no working Docker CLI found; checked 'docker' and '${windows_docker}'" >&2
  return 1
}

docker_cmd="$(resolve_docker)"

base_tag="${1:-comet/base-runtime:dev}"
infer_tag="${2:-comet/infer-runtime:dev}"
worker_tag="${3:-comet/worker-runtime:dev}"
web_ui_tag="${4:-comet/web-ui:dev}"
build_vllm_worker="${COMET_BUILD_VLLM_WORKER:-no}"
worker_vllm_tag="${COMET_WORKER_VLLM_TAG:-comet/worker-vllm-runtime:dev}"
worker_vllm_base_image="${COMET_WORKER_VLLM_BASE_IMAGE:-vllm/vllm-openai:latest}"

build_web_ui_image() {
  local temp_root
  mkdir -p "${repo_root}/var"
  temp_root="$(mktemp -d "${repo_root}/var/web-ui-image.XXXXXX")"
  trap 'rm -rf "'"${temp_root}"'"' RETURN
  mkdir -p "${temp_root}/dist"
  cp "${repo_root}/runtime/web-ui/nginx.conf.template" "${temp_root}/nginx.conf.template"

  local -a helper_args=(
    run
    --rm
    -v "${repo_root}/ui/operator-react:/src:ro"
    -v "${temp_root}/dist:/out"
    -w /tmp/work
    node:20-bookworm-slim
    bash
    -lc
    "cp /src/package.json /src/package-lock.json . && npm ci && cp -R /src/. . && npm run build && cp -R dist/. /out/"
  )
  if [[ "$(uname -s)" == "Linux" ]]; then
    helper_args=(run --rm --security-opt apparmor=unconfined "${helper_args[@]:2}")
  fi

  "${docker_cmd}" "${helper_args[@]}"

  cat > "${temp_root}/Dockerfile" <<'EOF'
FROM nginx:1.29-alpine

COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY dist/ /usr/share/nginx/html/

EXPOSE 8080
EOF

  "${docker_cmd}" build -f "${temp_root}/Dockerfile" -t "${web_ui_tag}" "${temp_root}"
}

echo "building ${base_tag}"
"${docker_cmd}" build \
  -f "${repo_root}/runtime/base/Dockerfile" \
  -t "${base_tag}" \
  "${repo_root}"

echo "building ${infer_tag}"
"${docker_cmd}" build \
  -f "${repo_root}/runtime/infer/Dockerfile" \
  -t "${infer_tag}" \
  "${repo_root}"

echo "building ${worker_tag}"
"${docker_cmd}" build \
  -f "${repo_root}/runtime/worker/Dockerfile" \
  -t "${worker_tag}" \
  "${repo_root}"

if [[ "${build_vllm_worker}" == "yes" ]]; then
  echo "building ${worker_vllm_tag}"
  "${docker_cmd}" build \
    --build-arg "VLLM_BASE_IMAGE=${worker_vllm_base_image}" \
    -f "${repo_root}/runtime/worker-vllm/Dockerfile" \
    -t "${worker_vllm_tag}" \
    "${repo_root}"
fi

if [[ "${skip_web_ui}" != "yes" ]]; then
  echo "building ${web_ui_tag}"
  build_web_ui_image
fi

echo "runtime images ready"
echo "  base=${base_tag}"
echo "  infer=${infer_tag}"
echo "  worker=${worker_tag}"
if [[ "${build_vllm_worker}" == "yes" ]]; then
  echo "  worker_vllm=${worker_vllm_tag}"
fi
if [[ "${skip_web_ui}" != "yes" ]]; then
  echo "  web_ui=${web_ui_tag}"
fi
