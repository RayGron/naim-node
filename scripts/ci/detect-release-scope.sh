#!/usr/bin/env bash
set -euo pipefail

: "${NAIM_RELEASE_SHA:?NAIM_RELEASE_SHA is required}"
: "${NAIM_HPC1_REPO_PATH:?NAIM_HPC1_REPO_PATH is required}"
: "${NAIM_HPC1_SSH:?NAIM_HPC1_SSH is required}"
: "${NAIM_HPC1_SSH_PORT:?NAIM_HPC1_SSH_PORT is required}"
: "${NAIM_CI_SSH_OPTS:?NAIM_CI_SSH_OPTS is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

event_name="${GITHUB_EVENT_NAME:-}"
before_sha="${NAIM_RELEASE_BEFORE:-}"
zero_sha='0000000000000000000000000000000000000000'
turboquant_required="false"
turboquant_reason="cache-reuse"
matched_file=""
changed_files=""

is_turboquant_sensitive_path() {
  local path="$1"
  case "${path}" in
    CMakeLists.txt|vcpkg.json|vcpkg-configuration.json)
      return 0
      ;;
    cmake/*|cmake/**)
      return 0
      ;;
    runtime/infer/*|runtime/infer/**|runtime/worker/*|runtime/worker/**)
      return 0
      ;;
    scripts/build-context.sh|scripts/build-runtime-images.sh|scripts/build-turboquant-runtime.sh|scripts/configure-build.sh|scripts/detect-host-target.sh|scripts/find-cmake.sh|scripts/find-ninja.sh|scripts/find-vcpkg.sh|scripts/resolve-build-target.sh)
      return 0
      ;;
    scripts/ci/hpc1-build.sh|scripts/ci/hpc1-build-common.sh|scripts/ci/hpc1-build-turboquant.sh)
      return 0
      ;;
  esac

  return 1
}

if [[ "${event_name}" == "workflow_dispatch" ]]; then
  turboquant_required="true"
  turboquant_reason="workflow_dispatch"
else
  if [[ -z "${before_sha}" || "${before_sha}" == "${zero_sha}" ]]; then
    turboquant_required="true"
    turboquant_reason="missing_base_sha"
  else
    changed_files="$(git diff --name-only "${before_sha}" "${NAIM_RELEASE_SHA}")"
    while IFS= read -r path; do
      [[ -n "${path}" ]] || continue
      if is_turboquant_sensitive_path "${path}"; then
        turboquant_required="true"
        turboquant_reason="path_change"
        matched_file="${path}"
        break
      fi
    done <<< "${changed_files}"
  fi
fi

if [[ "${turboquant_required}" != "true" ]]; then
  turboquant_bin_dir="${NAIM_HPC1_REPO_PATH}/build-turboquant/linux/x64/bin"
  if ! ssh ${NAIM_CI_SSH_OPTS} -p "${NAIM_HPC1_SSH_PORT}" "${NAIM_HPC1_SSH}" \
      "test -x $(printf '%q' "${turboquant_bin_dir}/llama-server") && test -x $(printf '%q' "${turboquant_bin_dir}/rpc-server")"; then
    turboquant_required="true"
    turboquant_reason="artifacts_missing"
  fi
fi

{
  printf 'turboquant_required=%s\n' "${turboquant_required}"
  printf 'turboquant_reason=%s\n' "${turboquant_reason}"
} >> "${GITHUB_OUTPUT}"

echo "TurboQuant build required: ${turboquant_required}"
echo "Reason: ${turboquant_reason}"
if [[ -n "${matched_file}" ]]; then
  echo "Matched path: ${matched_file}"
fi
if [[ -n "${changed_files}" ]]; then
  echo "Changed files:"
  printf '%s\n' "${changed_files}"
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "## Release Scope"
    echo
    echo "- TurboQuant build required: \`${turboquant_required}\`"
    echo "- Reason: \`${turboquant_reason}\`"
    if [[ -n "${matched_file}" ]]; then
      echo "- Matched path: \`${matched_file}\`"
    fi
  } >> "${GITHUB_STEP_SUMMARY}"
fi
