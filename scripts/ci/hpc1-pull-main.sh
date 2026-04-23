#!/usr/bin/env bash
set -euo pipefail

: "${NAIM_RELEASE_SHA:?NAIM_RELEASE_SHA is required}"
: "${NAIM_RELEASE_BRANCH:=main}"

git fetch --prune origin "${NAIM_RELEASE_BRANCH}"

# This checkout is dedicated to CI builds, so local tracked changes are disposable.
if git show-ref --verify --quiet "refs/heads/${NAIM_RELEASE_BRANCH}"; then
  git checkout -f "${NAIM_RELEASE_BRANCH}"
else
  git checkout -B "${NAIM_RELEASE_BRANCH}" "origin/${NAIM_RELEASE_BRANCH}"
fi

git reset --hard "origin/${NAIM_RELEASE_BRANCH}"

current_sha="$(git rev-parse HEAD)"
if [[ "${current_sha}" != "${NAIM_RELEASE_SHA}" ]]; then
  echo "error: ${NAIM_RELEASE_BRANCH} resolved to ${current_sha}, expected ${NAIM_RELEASE_SHA}" >&2
  exit 1
fi

echo "hpc1 repository is at ${current_sha}"
