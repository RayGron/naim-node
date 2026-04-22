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

if [[ -d build ]]; then
  owner="$(stat -c '%u' build 2>/dev/null || true)"
  if [[ "${owner}" != "$(id -u)" ]] || [[ ! -w build ]]; then
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
      sudo chown -R "$(id -u):$(id -g)" build
    else
      echo "error: build directory is not writable by $(id -un): $(pwd)/build" >&2
      exit 1
    fi
  fi
  chmod -R u+rwX build
fi

export NAIM_BUILD_TYPE
"$(pwd)/scripts/build-target.sh" "${NAIM_BUILD_TYPE}"

echo "hpc1 build completed for ${current_sha}"
