#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: package-target.sh <os> <arch>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"
target_os="${1}"
target_arch="${2}"
build_dir="$("${script_dir}/print-build-dir.sh" "${target_os}" "${target_arch}")"

"${script_dir}/build-target.sh" "${target_os}" "${target_arch}" Release

dist_dir="${repo_dir}/dist/${target_os}/${target_arch}"
package_stem="comet-node-${target_os}-${target_arch}"
mkdir -p "${dist_dir}"

declare -a artifacts=()

for candidate in \
  "${build_dir}/comet-node" \
  "${build_dir}/comet-node.exe" \
  "${build_dir}/comet-controller" \
  "${build_dir}/comet-controller.exe" \
  "${build_dir}/comet-hostd" \
  "${build_dir}/comet-hostd.exe" \
  "${build_dir}/libcomet-common.a" \
  "${build_dir}/comet-common.lib"; do
  if [[ -f "${candidate}" ]]; then
    artifacts+=("$(basename "${candidate}")")
  fi
done

if [[ ${#artifacts[@]} -eq 0 ]]; then
  echo "error: no packageable build artifacts were found in '${build_dir}'" >&2
  exit 1
fi

case "${target_os}" in
  windows)
    package_file="${dist_dir}/${package_stem}.zip"
    package_format="zip"
    ;;
  *)
    package_file="${dist_dir}/${package_stem}.tar.gz"
    package_format="gnutar"
    ;;
esac

rm -f "${package_file}"

(
  cd "${build_dir}"
  cmake -E tar "cf" "${package_file}" --format="${package_format}" -- "${artifacts[@]}"
)

echo "${package_file}"
