#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
vcpkg_root="$("${script_dir}/find-vcpkg.sh" --root)"

if command -v ninja >/dev/null 2>&1; then
  command -v ninja
  exit 0
fi

for candidate in \
  "${vcpkg_root}/downloads/tools/ninja-1.13.2-linux/ninja" \
  "${vcpkg_root}/downloads/tools/ninja-1.13.2-windows/ninja.exe" \
  "${vcpkg_root}/downloads/tools/ninja/1.12.1-windows/ninja.exe"; do
  if [[ -x "${candidate}" ]]; then
    echo "${candidate}"
    exit 0
  fi
done

echo "error: unable to find ninja; install ninja or let vcpkg download it first" >&2
exit 1
