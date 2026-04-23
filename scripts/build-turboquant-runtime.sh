#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
source "${script_dir}/build-context.sh"

naim_resolve_build_context "${script_dir}" "$@"

thetom_repo="${NAIM_TURBOQUANT_LLAMA_REPO:-https://github.com/TheTom/llama-cpp-turboquant.git}"
thetom_ref="${NAIM_TURBOQUANT_LLAMA_REF:-9e3fb40e8bc0f873ad4d3d8329b17dacff28e4ca}"
source_dir="${NAIM_TURBOQUANT_LLAMA_SOURCE_DIR:-${repo_root}/var/turboquant/llama-cpp-turboquant}"
build_root="${NAIM_TURBOQUANT_BUILD_ROOT:-${repo_root}/build-turboquant}"
jobs="${NAIM_TURBOQUANT_BUILD_JOBS:-6}"

if [[ ! -d "${source_dir}/.git" ]]; then
  mkdir -p "$(dirname -- "${source_dir}")"
  git clone "${thetom_repo}" "${source_dir}"
fi

git -C "${source_dir}" fetch --tags origin
git -C "${source_dir}" checkout --detach "${thetom_ref}"

export NAIM_BUILD_ROOT="${build_root}"
export NAIM_CMAKE_ARGS="${NAIM_CMAKE_ARGS:-} -DNAIM_LLAMA_CPP_SOURCE_DIR=${source_dir}"
turboquant_build_dir="${build_root}/${TARGET_OS}/${TARGET_ARCH}"

"${script_dir}/configure-build.sh" "${TARGET_OS}" "${TARGET_ARCH}" "${BUILD_TYPE}" >/dev/null
"$("${script_dir}/find-cmake.sh")" --build "${turboquant_build_dir}" --target llama-server rpc-server -j "${jobs}"

echo "turboquant runtime ready: ${turboquant_build_dir}"
