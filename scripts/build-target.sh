#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/build-context.sh"

naim_resolve_build_context "${script_dir}" "$@"

cmake_exe="$("${script_dir}/find-cmake.sh")"

"${script_dir}/configure-build.sh" "${TARGET_OS}" "${TARGET_ARCH}" "${BUILD_TYPE}" >/dev/null
"${cmake_exe}" --build "${BUILD_DIR}"
