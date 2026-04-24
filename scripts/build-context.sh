#!/usr/bin/env bash

naim_is_build_type() {
  case "${1:-}" in
    Debug|Release|RelWithDebInfo|MinSizeRel)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

naim_validate_build_type() {
  local build_type="${1:-}"
  if naim_is_build_type "${build_type}"; then
    return 0
  fi
  echo "error: unsupported build type '${build_type}'" >&2
  echo "supported values: Debug, Release, RelWithDebInfo, MinSizeRel" >&2
  exit 1
}

naim_detect_host_target() {
  local script_dir="${1}"
  "${script_dir}/detect-host-target.sh"
}

naim_is_wsl() {
  grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null
}

naim_is_windows_mount_path() {
  [[ "${1:-}" == /mnt/* ]]
}

naim_default_cache_root() {
  if [[ -n "${NAIM_CACHE_ROOT:-}" ]]; then
    printf '%s\n' "${NAIM_CACHE_ROOT}"
    return
  fi
  if [[ -n "${XDG_CACHE_HOME:-}" ]]; then
    printf '%s\n' "${XDG_CACHE_HOME}/naim"
    return
  fi
  printf '%s\n' "${HOME:-/tmp}/.cache/naim"
}

naim_repo_path_hash() {
  printf '%s' "${1}" | sha256sum | awk '{print substr($1, 1, 16)}'
}

naim_sync_source_mirror() {
  if [[ "${NAIM_SOURCE_MIRROR_ACTIVE:-0}" != "1" ]]; then
    return
  fi
  if ! command -v rsync >/dev/null 2>&1; then
    echo "error: rsync is required for WSL source mirror builds" >&2
    echo "install rsync or set NAIM_WSL_SOURCE_MIRROR=off to build directly from the checkout" >&2
    exit 1
  fi

  mkdir -p "${NAIM_SOURCE_MIRROR_DIR}"
  echo "[build] syncing WSL source mirror" >&2
  echo "[build]   source=${NAIM_ORIGINAL_REPO_DIR}" >&2
  echo "[build]   mirror=${NAIM_SOURCE_MIRROR_DIR}" >&2

  rsync -a --delete --delete-excluded \
    --exclude '/.git/' \
    --exclude '/.local/' \
    --exclude '/build/' \
    --exclude '/build-*/' \
    --exclude '/dist/' \
    --exclude '/var/' \
    --exclude '/ui/operator-react/dist/' \
    --exclude '/ui/operator-react/node_modules/' \
    "${NAIM_ORIGINAL_REPO_DIR}/" \
    "${NAIM_SOURCE_MIRROR_DIR}/"
}

naim_drop_windows_mount_path_entries() {
  local value="${1:-}"
  local result=""
  local entry

  IFS=':' read -r -a entries <<< "${value}"
  for entry in "${entries[@]}"; do
    if [[ -z "${entry}" || "${entry}" == /mnt/* ]]; then
      continue
    fi
    if [[ -z "${result}" ]]; then
      result="${entry}"
    else
      result="${result}:${entry}"
    fi
  done

  printf '%s\n' "${result}"
}

naim_prepare_source_mirror_environment() {
  if [[ "${NAIM_SOURCE_MIRROR_ACTIVE:-0}" != "1" ]]; then
    return
  fi

  PATH="$(naim_drop_windows_mount_path_entries "${PATH:-}")"
  export PATH
}

naim_resolve_target_context() {
  local script_dir="${1}"
  shift

  local target_os=""
  local target_arch=""
  case $# in
    0)
      read -r target_os target_arch < <(naim_detect_host_target "${script_dir}")
      ;;
    2)
      target_os="${1}"
      target_arch="${2}"
      ;;
    *)
      echo "usage: $0 [<os> <arch>]" >&2
      exit 1
      ;;
  esac

  local resolved_target=""
  if ! resolved_target="$("${script_dir}/resolve-build-target.sh" "${target_os}" "${target_arch}")"; then
    exit 1
  fi
  eval "${resolved_target}"

  local repo_dir
  repo_dir="$(cd -- "${script_dir}/.." && pwd)"
  local build_root="${NAIM_BUILD_ROOT:-${repo_dir}/build}"

  NAIM_ORIGINAL_REPO_DIR="${repo_dir}"
  NAIM_SOURCE_MIRROR_ACTIVE=0
  NAIM_SOURCE_MIRROR_DIR=""
  NAIM_SOURCE_MIRROR_WORKSPACE=""

  local mirror_mode="${NAIM_WSL_SOURCE_MIRROR:-auto}"
  if [[ "${target_os}" == "linux" ]] \
    && [[ "${mirror_mode}" != "off" ]] \
    && naim_is_windows_mount_path "${repo_dir}" \
    && naim_is_wsl; then
    local cache_root
    local repo_hash
    local repo_name

    cache_root="${NAIM_WSL_BUILD_MIRROR_ROOT:-$(naim_default_cache_root)/wsl-build-mirror}"
    repo_hash="$(naim_repo_path_hash "${repo_dir}")"
    repo_name="$(basename -- "${repo_dir}")"

    NAIM_SOURCE_MIRROR_ACTIVE=1
    NAIM_SOURCE_MIRROR_WORKSPACE="${cache_root}/${repo_name}-${repo_hash}"
    NAIM_SOURCE_MIRROR_DIR="${NAIM_SOURCE_MIRROR_WORKSPACE}/src"
    build_root="${NAIM_BUILD_ROOT:-${NAIM_SOURCE_MIRROR_WORKSPACE}/build}"
    repo_dir="${NAIM_SOURCE_MIRROR_DIR}"
  fi

  TARGET_OS="${target_os}"
  TARGET_ARCH="${target_arch}"
  BUILD_DIR="${build_root}/${TARGET_OS}/${TARGET_ARCH}"
  REPO_DIR="${repo_dir}"
}

naim_resolve_build_context() {
  local script_dir="${1}"
  shift

  local build_type="${NAIM_BUILD_TYPE:-Debug}"
  local -a target_args=()

  case $# in
    0)
      ;;
    1)
      if naim_is_build_type "${1}"; then
        build_type="${1}"
      else
        echo "usage: $0 [<build-type>] | [<os> <arch> [<build-type>]]" >&2
        exit 1
      fi
      ;;
    2)
      target_args=("${1}" "${2}")
      ;;
    3)
      target_args=("${1}" "${2}")
      build_type="${3}"
      ;;
    *)
      echo "usage: $0 [<build-type>] | [<os> <arch> [<build-type>]]" >&2
      exit 1
      ;;
  esac

  naim_validate_build_type "${build_type}"
  naim_resolve_target_context "${script_dir}" "${target_args[@]}"
  BUILD_TYPE="${build_type}"
}
