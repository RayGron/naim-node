#!/usr/bin/env bash
set -euo pipefail

: "${NAIM_RELEASE_SHA:?NAIM_RELEASE_SHA is required}"
: "${NAIM_BUILD_TYPE:=Release}"

current_sha="$(git rev-parse HEAD)"
if [[ "${current_sha}" != "${NAIM_RELEASE_SHA}" ]]; then
  echo "error: hpc1 workspace is at ${current_sha}, expected ${NAIM_RELEASE_SHA}" >&2
  exit 1
fi

git config --global --add safe.directory "$(pwd)" >/dev/null 2>&1 || true

ensure_writable_dir() {
  local path="$1"
  local owner=""
  if [[ ! -d "${path}" ]]; then
    return 0
  fi

  owner="$(stat -c '%u' "${path}" 2>/dev/null || true)"
  if [[ "${owner}" != "$(id -u)" ]] || [[ ! -w "${path}" ]]; then
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
      sudo chown -R "$(id -u):$(id -g)" "${path}"
    else
      echo "error: directory is not writable by $(id -un): $(pwd)/${path}" >&2
      exit 1
    fi
  fi
  chmod -R u+rwX "${path}"
}

ensure_writable_dir build
ensure_writable_dir build-turboquant
ensure_writable_dir var/turboquant

export NAIM_BUILD_TYPE
"$(pwd)/scripts/build-target.sh" "${NAIM_BUILD_TYPE}"
"$(pwd)/scripts/build-turboquant-runtime.sh" linux x64 "${NAIM_BUILD_TYPE}"

echo "hpc1 build completed for ${current_sha}"
