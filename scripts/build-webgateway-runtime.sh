#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

build_dir="${1:-${repo_root}/build/linux/x64}"
image_tag="${2:-comet/webgateway-runtime:dev}"
jobs="${COMET_BUILD_JOBS:-8}"

declare -a docker_cmd

resolve_docker() {
  if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    docker_cmd=(docker)
    return 0
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n docker version >/dev/null 2>&1; then
    docker_cmd=(sudo -n docker)
    return 0
  fi

  local windows_docker="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [[ -x "${windows_docker}" ]] && "${windows_docker}" version >/dev/null 2>&1; then
    docker_cmd=("${windows_docker}")
    return 0
  fi

  echo "error: no working Docker CLI found" >&2
  return 1
}

resolve_docker

if [[ ! -d "${build_dir}" ]]; then
  echo "error: build directory not found: ${build_dir}" >&2
  exit 1
fi

copy_build_artifact() {
  local source_path="$1"
  local destination_path="$2"
  if [[ ! -e "${source_path}" ]]; then
    echo "error: required WebGateway runtime artifact not found: ${source_path}" >&2
    exit 1
  fi

  if [[ -d "${source_path}" ]]; then
    mkdir -p "$(dirname -- "${destination_path}")"
    cp -R "${source_path}" "${destination_path}"
  else
    mkdir -p "$(dirname -- "${destination_path}")"
    cp -p "${source_path}" "${destination_path}"
  fi
}

cmake --build "${build_dir}" --target comet-webgatewayd -j "${jobs}"

mkdir -p "${repo_root}/var"
stage_root="$(mktemp -d "${repo_root}/var/webgateway-runtime-image.XXXXXX")"
trap 'rm -rf "${stage_root}"' EXIT

mkdir -p "${stage_root}/runtime" "${stage_root}/build/linux/x64"
cp -R "${repo_root}/runtime/browsing" "${stage_root}/runtime/"

for artifact in \
  comet-webgatewayd \
  libcef.so \
  chrome-sandbox \
  chrome_100_percent.pak \
  chrome_200_percent.pak \
  icudtl.dat \
  libEGL.so \
  libGLESv2.so \
  resources.pak \
  v8_context_snapshot.bin \
  vk_swiftshader_icd.json \
  locales; do
  copy_build_artifact \
    "${build_dir}/${artifact}" \
    "${stage_root}/build/linux/x64/${artifact}"
done

"${docker_cmd[@]}" build \
  -f "${stage_root}/runtime/browsing/Dockerfile" \
  -t "${image_tag}" \
  "${stage_root}"

echo "webgateway-runtime-ready"
echo "  build_dir=${build_dir}"
echo "  image=${image_tag}"
