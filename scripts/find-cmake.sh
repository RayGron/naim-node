#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
vcpkg_root="$("${script_dir}/find-vcpkg.sh" --root)"
host_kernel="$(uname -s)"

if command -v cmake >/dev/null 2>&1; then
  command -v cmake
  exit 0
fi

while IFS= read -r candidate; do
  if [[ -x "${candidate}" ]]; then
    echo "${candidate}"
    exit 0
  fi
done < <(find "${vcpkg_root}/downloads/tools" -maxdepth 4 -type f -name cmake | sort -r)

if [[ "${host_kernel}" == MINGW* || "${host_kernel}" == MSYS* || "${host_kernel}" == CYGWIN* || "${host_kernel}" == "Windows_NT" ]]; then
  if command -v cmake.exe >/dev/null 2>&1; then
    command -v cmake.exe
    exit 0
  fi
fi

while IFS= read -r candidate; do
  if [[ -x "${candidate}" ]]; then
    echo "${candidate}"
    exit 0
  fi
done < <(find "${vcpkg_root}/downloads/tools" -maxdepth 4 -type f -name cmake.exe | sort -r)

echo "error: unable to find cmake; install cmake or let vcpkg download it first" >&2
exit 1
