#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/build-context.sh"

naim_resolve_target_context "${script_dir}" "$@"
echo "${BUILD_DIR}"
