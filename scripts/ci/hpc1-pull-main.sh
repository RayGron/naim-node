#!/usr/bin/env bash
set -euo pipefail

: "${NAIM_RELEASE_SHA:?NAIM_RELEASE_SHA is required}"
: "${NAIM_RELEASE_BRANCH:=main}"

naim_ci_retry_git_fetch() {
  local remote="$1"
  local branch="$2"
  local attempt=1
  local max_attempts=5
  local delay_sec=5

  while (( attempt <= max_attempts )); do
    if git fetch --prune "${remote}" "${branch}"; then
      return 0
    fi
    if (( attempt == max_attempts )); then
      echo "error: git fetch ${remote} ${branch} failed after ${max_attempts} attempts" >&2
      return 1
    fi
    echo "warning: git fetch ${remote} ${branch} failed on attempt ${attempt}/${max_attempts}; retrying in ${delay_sec}s" >&2
    sleep "${delay_sec}"
    delay_sec=$(( delay_sec * 2 ))
    attempt=$(( attempt + 1 ))
  done
}

git config http.version HTTP/1.1 >/dev/null 2>&1 || true
naim_ci_retry_git_fetch origin "${NAIM_RELEASE_BRANCH}"

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
