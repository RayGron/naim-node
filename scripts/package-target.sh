#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/build-context.sh"

naim_resolve_target_context "${script_dir}" "$@"

"${script_dir}/build-target.sh" "${TARGET_OS}" "${TARGET_ARCH}" Release

dist_dir="${REPO_DIR}/dist/${TARGET_OS}/${TARGET_ARCH}"
package_stem="naim-node-${TARGET_OS}-${TARGET_ARCH}"
mkdir -p "${dist_dir}"

declare -a artifacts=()

for candidate in \
  "${BUILD_DIR}/naim-node" \
  "${BUILD_DIR}/naim-node.exe" \
  "${BUILD_DIR}/naim-controller" \
  "${BUILD_DIR}/naim-controller.exe" \
  "${BUILD_DIR}/naim-hostd" \
  "${BUILD_DIR}/naim-hostd.exe" \
  "${BUILD_DIR}/libnaim-common.a" \
  "${BUILD_DIR}/naim-common.lib"; do
  if [[ -f "${candidate}" ]]; then
    artifacts+=("$(basename "${candidate}")")
  fi
done

if [[ ${#artifacts[@]} -eq 0 ]]; then
  echo "error: no packageable build artifacts were found in '${BUILD_DIR}'" >&2
  exit 1
fi

case "${TARGET_OS}" in
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
  cd "${BUILD_DIR}"
  cmake -E tar "cf" "${package_file}" --format="${package_format}" -- "${artifacts[@]}"
)

echo "${package_file}"
