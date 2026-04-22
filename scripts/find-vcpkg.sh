#!/usr/bin/env bash
set -euo pipefail

mode="${1:---exe}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"

declare -a candidates=()
if [[ -n "${VCPKG_ROOT:-}" ]]; then
  candidates+=("${VCPKG_ROOT}")
fi
candidates+=(
  "${repo_dir}/.tools/vcpkg"
  "/mnt/shared-storage/naim/vcpkg"
  "/opt/vcpkg"
  "/usr/local/vcpkg"
  "/mnt/e/dev/tools/vcpkg"
  "${HOME}/vcpkg"
)

for root in "${candidates[@]}"; do
  if [[ -x "${root}/vcpkg" ]]; then
    if [[ "${mode}" == "--root" ]]; then
      echo "${root}"
    else
      echo "${root}/vcpkg"
    fi
    exit 0
  fi
done

if command -v vcpkg >/dev/null 2>&1; then
  exe_path="$(command -v vcpkg)"
  root_path="$(cd -- "$(dirname -- "${exe_path}")/.." && pwd)"
  if [[ "${mode}" == "--root" ]]; then
    echo "${root_path}"
  else
    echo "${exe_path}"
  fi
  exit 0
fi

echo "error: unable to find vcpkg; set VCPKG_ROOT or install vcpkg" >&2
exit 1
