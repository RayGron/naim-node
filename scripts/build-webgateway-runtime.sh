#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

build_dir="${1:-${repo_root}/build/linux/x64}"
image_tag="${2:-comet/webgateway-runtime:dev}"
jobs="${COMET_BUILD_JOBS:-8}"

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

  echo "error: no working Docker CLI found" >&2
  return 1
}

docker_cmd="$(resolve_docker)"

cmake --build "${build_dir}" --target comet-webgatewayd -j "${jobs}"

"${docker_cmd}" build \
  -f "${repo_root}/runtime/browsing/Dockerfile" \
  -t "${image_tag}" \
  "${repo_root}"

echo "webgateway-runtime-ready"
echo "  build_dir=${build_dir}"
echo "  image=${image_tag}"
