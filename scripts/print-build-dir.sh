#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: print-build-dir.sh <os> <arch>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"
target_os="${1}"
target_arch="${2}"

echo "${repo_dir}/build/${target_os}/${target_arch}"
