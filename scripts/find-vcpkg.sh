#!/usr/bin/env bash
set -euo pipefail

mode="${1:---exe}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"

is_wsl() {
  grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null
}

is_windows_mount_path() {
  [[ "${1:-}" == /mnt/* ]]
}

default_cache_root() {
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

emit_vcpkg_path() {
  local root="$1"
  if [[ "${mode}" == "--root" ]]; then
    echo "${root}"
  else
    echo "${root}/vcpkg"
  fi
}

ensure_wsl_vcpkg_mirror() {
  local source_root="$1"
  local mirror_root="${NAIM_WSL_VCPKG_ROOT:-$(default_cache_root)/wsl-vcpkg}"
  local source_head=""
  local mirror_head=""

  if [[ "${NAIM_WSL_VCPKG_MIRROR:-auto}" == "off" ]]; then
    emit_vcpkg_path "${source_root}"
    return
  fi

  if ! command -v git >/dev/null 2>&1; then
    emit_vcpkg_path "${source_root}"
    return
  fi

  source_head="$(git -C "${source_root}" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "${source_head}" ]]; then
    emit_vcpkg_path "${source_root}"
    return
  fi

  if [[ -d "${mirror_root}/.git" ]]; then
    mirror_head="$(git -C "${mirror_root}" rev-parse HEAD 2>/dev/null || true)"
  fi

  if [[ ! -x "${mirror_root}/vcpkg" || "${mirror_head}" != "${source_head}" ]]; then
    local tmp_root="${mirror_root}.tmp"
    echo "[vcpkg] preparing WSL vcpkg mirror" >&2
    echo "[vcpkg]   source=${source_root}" >&2
    echo "[vcpkg]   mirror=${mirror_root}" >&2
    rm -rf "${tmp_root}"
    mkdir -p "$(dirname -- "${mirror_root}")"
    git clone --quiet "${source_root}" "${tmp_root}"
    git -C "${tmp_root}" checkout --quiet "${source_head}"
    "${tmp_root}/bootstrap-vcpkg.sh" -disableMetrics >/dev/null
    rm -rf "${mirror_root}"
    mv "${tmp_root}" "${mirror_root}"
  fi

  emit_vcpkg_path "${mirror_root}"
}

declare -a candidates=()
if [[ -n "${VCPKG_ROOT:-}" ]]; then
  candidates+=("${VCPKG_ROOT}")
fi
if is_wsl; then
  candidates+=(
    "${NAIM_WSL_VCPKG_ROOT:-$(default_cache_root)/wsl-vcpkg}"
  )
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
    if is_wsl && is_windows_mount_path "${root}"; then
      ensure_wsl_vcpkg_mirror "${root}"
    else
      emit_vcpkg_path "${root}"
    fi
    exit 0
  fi
done

if command -v vcpkg >/dev/null 2>&1; then
  exe_path="$(command -v vcpkg)"
  root_path="$(cd -- "$(dirname -- "${exe_path}")/.." && pwd)"
  if is_wsl && is_windows_mount_path "${root_path}"; then
    ensure_wsl_vcpkg_mirror "${root_path}"
  else
    emit_vcpkg_path "${root_path}"
  fi
  exit 0
fi

echo "error: unable to find vcpkg; set VCPKG_ROOT or install vcpkg" >&2
exit 1
