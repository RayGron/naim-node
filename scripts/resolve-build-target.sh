#!/usr/bin/env bash
set -euo pipefail

target_os="${1:-}"
target_arch="${2:-}"

if [[ -z "${target_os}" || -z "${target_arch}" ]]; then
  echo "usage: resolve-build-target.sh <os> <arch>" >&2
  exit 1
fi

case "${target_os}" in
  linux)
    ;;
  windows)
    ;;
  macos)
    ;;
  *)
    echo "error: unsupported target OS '${target_os}'" >&2
    exit 1
    ;;
esac

case "${target_arch}" in
  x64)
    arch_triplet="x64"
    ;;
  arm64)
    arch_triplet="arm64"
    ;;
  *)
    echo "error: unsupported target architecture '${target_arch}'" >&2
    exit 1
    ;;
esac

case "${target_os}" in
  linux)
    vcpkg_triplet="${arch_triplet}-linux"
    ;;
  windows)
    vcpkg_triplet="${arch_triplet}-windows"
    ;;
  macos)
    vcpkg_triplet="${arch_triplet}-osx"
    ;;
esac

preset_name="${target_os}-${target_arch}"

printf 'TARGET_OS=%s\n' "${target_os}"
printf 'TARGET_ARCH=%s\n' "${target_arch}"
printf 'VCPKG_TRIPLET=%s\n' "${vcpkg_triplet}"
printf 'PRESET_NAME=%s\n' "${preset_name}"
