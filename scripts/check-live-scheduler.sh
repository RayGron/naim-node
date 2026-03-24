#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-live-scheduler.sh --gguf <path-to-model.gguf> [--skip-build-images]

Runs a live safe-direct rebalance check under Docker/NVIDIA:
  1. builds comet binaries and runtime images
  2. deploys a movable-worker test plane
  3. loads a real GGUF into the shared disk
  4. waits for worker runtimes to become ready
  5. materializes one safe-direct rebalance
  6. drives scheduler-tick until post-move verification succeeds
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

gguf_path=""
skip_build_images="no"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gguf)
      gguf_path="${2:-}"
      shift 2
      ;;
    --skip-build-images)
      skip_build_images="yes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${gguf_path}" ]]; then
  echo "error: --gguf is required" >&2
  usage >&2
  exit 1
fi

if [[ ! -f "${gguf_path}" ]]; then
  echo "error: gguf file not found: ${gguf_path}" >&2
  exit 1
fi

resolve_docker_cmd() {
  if command -v docker >/dev/null 2>&1; then
    DOCKER_CMD=(docker)
    return
  fi
  local windows_docker="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [[ -x "${windows_docker}" ]]; then
    DOCKER_CMD=("${windows_docker}")
    return
  fi
  echo "error: docker command not found" >&2
  exit 1
}

wait_for_workers_ready() {
  local build_dir="$1"
  local db_path="$2"
  local state_root="$3"
  local attempts="$4"

  for ((attempt = 1; attempt <= attempts; ++attempt)); do
    "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
    "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null
    local observations
    observations="$("${build_dir}/comet-controller" show-host-observations --db "${db_path}")"
    if grep -F "instance name=worker-a role=worker phase=running ready=yes" <<<"${observations}" >/dev/null &&
       grep -F "instance name=worker-b role=worker phase=running ready=yes" <<<"${observations}" >/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "error: worker runtimes did not become ready in time" >&2
  "${build_dir}/comet-controller" show-host-observations --db "${db_path}" >&2 || true
  return 1
}

wait_for_verified_move() {
  local build_dir="$1"
  local db_path="$2"
  local artifacts_root="$3"
  local state_root="$4"
  local attempts="$5"

  for ((attempt = 1; attempt <= attempts; ++attempt)); do
    "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
    "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null
    "${build_dir}/comet-controller" scheduler-tick --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
    local state_text
    state_text="$("${build_dir}/comet-controller" show-state --db "${db_path}")"
    if grep -E "worker=worker-b .* last_phase=verified .*manual_intervention_required=no" <<<"${state_text}" >/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "error: scheduler move did not reach verified state in time" >&2
  "${build_dir}/comet-controller" show-state --db "${db_path}" >&2 || true
  "${build_dir}/comet-controller" show-host-observations --db "${db_path}" >&2 || true
  return 1
}

read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"

mkdir -p "${repo_root}/var"
work_root="$(mktemp -d "${repo_root}/var/live-scheduler.XXXXXX")"
bundle_dir="${work_root}/bundle"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/hostd-state"
runtime_infer_config="${work_root}/infer-runtime-live.json"

resolve_docker_cmd

cleanup() {
  if [[ -f "${artifacts_root}/alpha/node-a/docker-compose.yml" ]]; then
    "${DOCKER_CMD[@]}" compose -f "${artifacts_root}/alpha/node-a/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ -f "${artifacts_root}/alpha/node-b/docker-compose.yml" ]]; then
    "${DOCKER_CMD[@]}" compose -f "${artifacts_root}/alpha/node-b/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "${work_root}"
}
trap cleanup EXIT

echo "[live-scheduler] building host binaries"
"${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug >/dev/null

if [[ "${skip_build_images}" != "yes" ]]; then
  echo "[live-scheduler] building runtime images"
  "${script_dir}/build-runtime-images.sh" >/dev/null
fi

if ! nvidia-smi >/dev/null 2>&1; then
  echo "error: nvidia-smi is not available on this host" >&2
  exit 1
fi

gpu_count="$(nvidia-smi --query-gpu=index --format=csv,noheader,nounits | sed '/^\s*$/d' | wc -l | tr -d ' ')"
if [[ "${gpu_count}" -lt 2 ]]; then
  echo "error: check-live-scheduler.sh currently exercises a safe-direct cross-gpu rebalance and requires at least 2 visible GPUs; detected ${gpu_count}" >&2
  exit 1
fi

echo "[live-scheduler] preparing movable-worker bundle"
cp -R "${repo_root}/config/demo-plane/." "${bundle_dir}"
perl -0pi -e 's/"name": "worker-b",/"name": "worker-b",\n  "placement_mode": "movable",/' \
  "${bundle_dir}/workers/worker-b.json"

echo "[live-scheduler] deploying initial generation"
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${bundle_dir}" --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode exec >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode exec >/dev/null

echo "[live-scheduler] staging gguf on shared disk"
shared_root="${runtime_root}/var/lib/comet/disks/planes/alpha/shared"
control_root="${shared_root}/control/alpha"
shared_model_dir="${shared_root}/models/live-safe-direct"
mkdir -p "${shared_model_dir}"
model_filename="$(basename -- "${gguf_path}")"
shared_model_path="${shared_model_dir}/${model_filename}"
cp -f "${gguf_path}" "${shared_model_path}"

perl -MJSON::PP -e '
  use strict;
  use warnings;
  use utf8;
  use File::Spec;

  my ($src, $dst, $shared_root) = @ARGV;
  open my $in, "<:raw", $src or die "open $src: $!";
  local $/;
  my $json_text = <$in>;
  close $in;

  my $data = JSON::PP->new->utf8->decode($json_text);
  my $control_root = File::Spec->catdir($shared_root, "control", "alpha");
  $data->{plane}->{control_root} = $control_root;
  $data->{control}->{root} = $control_root;
  $data->{inference}->{models_root} = File::Spec->catdir($shared_root, "models");
  $data->{inference}->{gguf_cache_dir} = File::Spec->catdir($shared_root, "models", "gguf");
  $data->{inference}->{infer_log_dir} = File::Spec->catdir($shared_root, "logs", "infer");

  open my $out, ">:raw", $dst or die "open $dst: $!";
  print {$out} JSON::PP->new->utf8->canonical->pretty->encode($data);
  close $out;
' "${artifacts_root}/alpha/infer-runtime.json" "${runtime_infer_config}" "${shared_root}"

runtime_model_path="/comet/shared/models/live-safe-direct/${model_filename}"
local_model_id="local/live-safe-direct"

echo "[live-scheduler] activating model for infer and workers"
"/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh" preload-model \
  --config "${runtime_infer_config}" \
  --alias live-safe-direct \
  --source-model-id "${local_model_id}" \
  --local-model-path "${shared_model_path}" \
  --runtime-model-path "${runtime_model_path}" \
  --apply >/dev/null
"/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh" switch-model \
  --config "${runtime_infer_config}" \
  --model-id "${local_model_id}" \
  --served-model-name live-safe-direct \
  --runtime-profile generic \
  --apply >/dev/null

echo "[live-scheduler] waiting for worker GPU runtimes to become ready"
wait_for_workers_ready "${build_dir}" "${db_path}" "${state_root}" 90

echo "[live-scheduler] materializing safe-direct rebalance"
"${build_dir}/comet-controller" show-rebalance-plan --db "${db_path}" | grep -F \
  "worker=worker-b placement_mode=movable current=node-b:0 class=safe-direct decision=propose" >/dev/null
"${build_dir}/comet-controller" reconcile-rebalance-proposals --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-a --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode exec >/dev/null
"${build_dir}/comet-hostd" apply-next-assignment --db "${db_path}" --node node-b --runtime-root "${runtime_root}" --state-root "${state_root}" --compose-mode exec >/dev/null

echo "[live-scheduler] driving scheduler-tick until move is verified"
wait_for_verified_move "${build_dir}" "${db_path}" "${artifacts_root}" "${state_root}" 90

echo "[live-scheduler] asserting final live ownership"
"${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
"${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" | grep -F \
  "instance name=worker-b role=worker phase=running ready=yes" >/dev/null
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" | grep -F \
  "gpu device=1" >/dev/null
if [[ -e "${state_root}/node-b/applied-state.json" ]]; then
  echo "error: expected node-b local state to be cleared after worker-b move" >&2
  exit 1
fi

echo "[live-scheduler] OK"
