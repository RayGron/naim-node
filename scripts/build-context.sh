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
