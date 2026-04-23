#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/hpc1-build-common.sh"

: "${NAIM_BUILD_TYPE:=Release}"

current_sha="$(naim_ci_require_release_sha)"
naim_ci_prepare_repo

naim_ci_ensure_writable_dir build-turboquant
naim_ci_ensure_writable_dir var/turboquant
naim_ci_prepare_shared_vcpkg_cache "$(pwd)"

export NAIM_BUILD_TYPE
bash "$(pwd)/scripts/build-turboquant-runtime.sh" linux x64 "${NAIM_BUILD_TYPE}"

echo "hpc1 turboquant build completed for ${current_sha}"
