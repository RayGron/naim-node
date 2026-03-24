#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_type="${1:-Debug}"

read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
"${script_dir}/build-target.sh" "${host_os}" "${host_arch}" "${build_type}"
