#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"
source "${repo_root}/scripts/build-context.sh"

build_dir="${NAIM_BUILD_DIR:-}"
artifact_dir="${NAIM_KNOWLEDGE_GATE_ARTIFACT_DIR:-${repo_root}/var/knowledge-vault-release-gate}"
work_root="${NAIM_KNOWLEDGE_GATE_WORK_ROOT:-${repo_root}/var/knowledge-vault-release-gate-work}"
benchmark_output="${NAIM_KNOWLEDGE_BENCHMARK_OUTPUT:-${artifact_dir}/knowledge-vault-benchmark-baseline.json}"
report_path="${NAIM_KNOWLEDGE_GATE_REPORT:-${artifact_dir}/knowledge-vault-release-gate.json}"
benchmark_items="${NAIM_KNOWLEDGE_BENCHMARK_ITEMS:-25}"
target_args=("$@")

if [[ -n "${NAIM_RELEASE_SHA:-}" ]]; then
  current_sha="$(git -C "${repo_root}" rev-parse HEAD)"
  if [[ "${current_sha}" != "${NAIM_RELEASE_SHA}" ]]; then
    echo "error: Knowledge Vault gate workspace is at ${current_sha}, expected ${NAIM_RELEASE_SHA}" >&2
    exit 1
  fi
fi

if [[ -z "${build_dir}" ]]; then
  if [[ ${#target_args[@]} -eq 1 ]] && naim_is_build_type "${target_args[0]}"; then
    target_args=()
  fi
  build_dir="$("${repo_root}/scripts/print-build-dir.sh" "${target_args[@]}")"
fi

mkdir -p "${artifact_dir}" "${work_root}"

python3 "${script_dir}/knowledge-vault-release-gate.py" \
  --build-dir "${build_dir}" \
  --work-root "${work_root}" \
  --artifact-dir "${artifact_dir}" \
  --benchmark-output "${benchmark_output}" \
  --benchmark-items "${benchmark_items}" \
  --report "${report_path}"

echo "knowledge vault release gate report=${report_path}"
