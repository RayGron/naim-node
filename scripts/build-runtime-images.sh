#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

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

echo "runtime images ready"
echo "  base=${base_tag}"
echo "  infer=${infer_tag}"
echo "  worker=${worker_tag}"
