#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-live-scheduler.sh --gguf <path-to-model.gguf> [--skip-build-images]

Runs a live v2 LLM runtime smoke under Docker/NVIDIA. It builds binaries/images,
applies a v2 LLM plane, materializes naim/* containers through hostd exec mode, and
waits for the live runtime observation to become ready.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"

gguf_path=""
skip_build_images="no"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gguf) gguf_path="${2:-}"; shift 2 ;;
    --skip-build-images) skip_build_images="yes"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument '$1'" >&2; usage >&2; exit 1 ;;
  esac
done
[[ -n "${gguf_path}" ]] || { echo "error: --gguf is required" >&2; usage >&2; exit 1; }
[[ -f "${gguf_path}" ]] || { echo "error: gguf file not found: ${gguf_path}" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "error: docker command not found" >&2; exit 1; }
nvidia-smi >/dev/null 2>&1 || { echo "error: nvidia-smi is not available on this host" >&2; exit 1; }

build_dir="$("${script_dir}/print-build-dir.sh")"
mkdir -p "${repo_root}/var"
work_root="$(mktemp -d "${repo_root}/var/live-scheduler-v2.XXXXXX")"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/hostd-state"
state_path="${work_root}/scheduler-v2.desired-state.v2.json"
plane_name="scheduler-v2"

compose_down() {
  local compose_file="$1"
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "${compose_file}" down -v --remove-orphans >/dev/null 2>&1 || true
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "${compose_file}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}

cleanup() {
  if [[ -f "${artifacts_root}/${plane_name}/local-hostd/docker-compose.yml" ]]; then
    compose_down "${artifacts_root}/${plane_name}/local-hostd/docker-compose.yml"
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo rm -rf "${work_root}" 2>/dev/null || true
  fi
  rm -rf "${work_root}" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "${artifacts_root}" "${runtime_root}" "${state_root}"

echo "[live-scheduler] building host binaries"
"${script_dir}/build-target.sh" Debug >/dev/null
if [[ "${skip_build_images}" != "yes" ]]; then
  echo "[live-scheduler] building runtime images"
  "${script_dir}/build-runtime-images.sh" >/dev/null
fi

gpu_count="$(nvidia-smi --query-gpu=index --format=csv,noheader,nounits | sed '/^\s*$/d' | wc -l | tr -d ' ')"
if [[ "${gpu_count}" -lt 1 ]]; then
  echo "error: this live test requires at least one visible NVIDIA GPU" >&2
  exit 1
fi

echo "[live-scheduler] deploying v2 llm plane"
"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
naim_live_seed_connected_hostd "${db_path}" local-hostd 1
naim_live_write_llm_state "${state_path}" "${plane_name}" "${gguf_path}" "$(${script_dir}/naim-devtool.sh free-port)" "$(${script_dir}/naim-devtool.sh free-port)"
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${state_path}"
"${build_dir}/naim-hostd" apply-next-assignment --db "${db_path}" --node local-hostd --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode exec >/dev/null

echo "[live-scheduler] waiting for live runtime observation"
for _ in $(seq 1 180); do
  "${build_dir}/naim-hostd" report-observed-state --db "${db_path}" --node local-hostd --state-root "${state_root}" >/dev/null || true
  observations="$("${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node local-hostd || true)"
  if printf '%s' "${observations}" | grep -F 'applied_generation=1' >/dev/null && \
     printf '%s' "${observations}" | grep -F 'instance name=infer-scheduler-v2 role=infer phase=running' >/dev/null; then
    break
  fi
  sleep 2
done
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node local-hostd | grep -F 'applied_generation=1' >/dev/null
"${build_dir}/naim-controller" show-host-observations --db "${db_path}" --node local-hostd | grep -F 'instance name=infer-scheduler-v2 role=infer phase=running' >/dev/null
"${build_dir}/naim-hostd" show-local-state --node local-hostd --state-root "${state_root}" | grep -E 'instances=[1-9][0-9]*' >/dev/null
if [[ -f "${artifacts_root}/${plane_name}/local-hostd/docker-compose.yml" ]]; then
  grep -F 'naim/infer-runtime:dev' "${artifacts_root}/${plane_name}/local-hostd/docker-compose.yml" >/dev/null
fi

echo "[live-scheduler] OK"
