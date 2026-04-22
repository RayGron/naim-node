#!/usr/bin/env bash
set -euo pipefail

: "${NAIM_RELEASE_SHA:?NAIM_RELEASE_SHA is required}"
: "${NAIM_BUILD_TYPE:=Release}"

current_sha="$(git rev-parse HEAD)"
if [[ "${current_sha}" != "${NAIM_RELEASE_SHA}" ]]; then
  echo "error: hpc1 workspace is at ${current_sha}, expected ${NAIM_RELEASE_SHA}" >&2
  exit 1
fi

export NAIM_BUILD_TYPE
"$(pwd)/scripts/build-target.sh" "${NAIM_BUILD_TYPE}"

echo "hpc1 build completed for ${current_sha}"
