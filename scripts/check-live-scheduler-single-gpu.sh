#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-live-scheduler-single-gpu.sh --gguf <path-to-model.gguf> [--skip-build-images]

Runs a live deferred-preemption scheduler check on a host with at least one visible NVIDIA GPU:
  1. builds comet binaries and runtime images
  2. deploys a single-GPU movable/preemption test plane
  3. stages a real GGUF into the shared disk
  4. waits for worker runtimes to become ready on the shared GPU
  5. enqueues and verifies a real evict-workers assignment
  6. materializes retry-placement and applies the new generation
  7. confirms the winning worker owns the GPU and the victim is gone
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

show_pending_assignments_for_node() {
  local build_dir="$1"
  local db_path="$2"
  local node_name="$3"
  "${build_dir}/comet-controller" show-host-assignments --db "${db_path}" --node "${node_name}"
}

apply_pending_assignment_if_any() {
  local build_dir="$1"
  local db_path="$2"
  local node_name="$3"
  local runtime_root="$4"
  local state_root="$5"
  if show_pending_assignments_for_node "${build_dir}" "${db_path}" "${node_name}" |
      grep -F "status=pending" >/dev/null; then
    "${build_dir}/comet-hostd" apply-next-assignment \
      --db "${db_path}" \
      --node "${node_name}" \
      --runtime-root "${runtime_root}" \
      --state-root "${state_root}" \
      --compose-mode exec >/dev/null
  fi
}

report_observed_state() {
  local build_dir="$1"
  local db_path="$2"
  local state_root="$3"
  "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-a --state-root "${state_root}" >/dev/null
  "${build_dir}/comet-hostd" report-observed-state --db "${db_path}" --node node-b --state-root "${state_root}" >/dev/null
}

wait_for_workers_ready() {
  local build_dir="$1"
  local db_path="$2"
  local state_root="$3"
  local attempts="$4"

  for ((attempt = 1; attempt <= attempts; ++attempt)); do
    report_observed_state "${build_dir}" "${db_path}" "${state_root}"
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

wait_for_rollout_applied() {
  local build_dir="$1"
  local db_path="$2"
  local attempts="$3"

  for ((attempt = 1; attempt <= attempts; ++attempt)); do
    local state_text
    local observations
    state_text="$("${build_dir}/comet-controller" show-state --db "${db_path}")"
    observations="$("${build_dir}/comet-controller" show-host-observations --db "${db_path}")"
    if grep -F "phase=rollout-applied" <<<"${state_text}" >/dev/null &&
       grep -F "instance name=worker-a role=worker phase=running ready=yes" <<<"${observations}" >/dev/null; then
      if ! grep -F "instance name=worker-b role=worker phase=running ready=yes" <<<"${observations}" >/dev/null; then
        return 0
      fi
    fi
    sleep 2
  done

  echo "error: rollout did not reach rollout-applied with live worker ownership" >&2
  "${build_dir}/comet-controller" show-state --db "${db_path}" >&2 || true
  "${build_dir}/comet-controller" show-host-observations --db "${db_path}" >&2 || true
  return 1
}

read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"

mkdir -p "${repo_root}/var"
work_root="$(mktemp -d "${repo_root}/var/live-single-gpu.XXXXXX")"
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

echo "[live-single-gpu] building host binaries"
"${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug >/dev/null

if [[ "${skip_build_images}" != "yes" ]]; then
  echo "[live-single-gpu] building runtime images"
  "${script_dir}/build-runtime-images.sh" >/dev/null
fi

if ! nvidia-smi >/dev/null 2>&1; then
  echo "error: nvidia-smi is not available on this host" >&2
  exit 1
fi

gpu_count="$(nvidia-smi --query-gpu=index --format=csv,noheader,nounits | sed '/^\s*$/d' | wc -l | tr -d ' ')"
if [[ "${gpu_count}" -lt 1 ]]; then
  echo "error: this live test requires at least one visible NVIDIA GPU" >&2
  exit 1
fi

echo "[live-single-gpu] preparing deferred-preemption bundle"
cp -R "${repo_root}/config/demo-plane/." "${bundle_dir}"
perl -0pi -e 's/"gpus": \["0", "1"\]/"gpus": ["0"]/; s/"0": 24576,\n\s*"1": 24576/"0": 24576/' \
  "${bundle_dir}/plane.json"
perl -0pi -e 's/"node": "node-a"/"node": "node-b"/; s/"share_mode": "exclusive"/"share_mode": "shared"/; s/"gpu_fraction": 1.0/"gpu_fraction": 0.75/; s/"memory_cap_mb": 16384/"memory_cap_mb": 12288/; s/"name": "worker-a",/"name": "worker-a",\n  "placement_mode": "movable",/' \
  "${bundle_dir}/workers/worker-a.json"
perl -0pi -e 's/"node": "node-b"/"node": "node-a"/; s/"share_mode": "shared"/"share_mode": "best-effort"/; s/"gpu_fraction": 0.5/"gpu_fraction": 0.25/; s/"priority": 100/"priority": 50/; s/"memory_cap_mb": 8192/"memory_cap_mb": 4096/' \
  "${bundle_dir}/workers/worker-b.json"

echo "[live-single-gpu] deploying initial generation"
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null
"${build_dir}/comet-controller" apply-bundle --bundle "${bundle_dir}" --db "${db_path}" --artifacts-root "${artifacts_root}" >/dev/null
apply_pending_assignment_if_any "${build_dir}" "${db_path}" node-a "${runtime_root}" "${state_root}"
apply_pending_assignment_if_any "${build_dir}" "${db_path}" node-b "${runtime_root}" "${state_root}"

echo "[live-single-gpu] staging gguf on shared disk"
shared_root="${runtime_root}/var/lib/comet/disks/planes/alpha/shared"
shared_model_dir="${shared_root}/models/live-single-gpu"
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

runtime_model_path="/comet/shared/models/live-single-gpu/${model_filename}"
local_model_id="local/live-single-gpu"

echo "[live-single-gpu] activating shared model"
"/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh" preload-model \
  --config "${runtime_infer_config}" \
  --alias live-single-gpu \
  --source-model-id "${local_model_id}" \
  --local-model-path "${shared_model_path}" \
  --runtime-model-path "${runtime_model_path}" \
  --apply >/dev/null
"/mnt/e/dev/Repos/comet-node/runtime/infer/inferctl.sh" switch-model \
  --config "${runtime_infer_config}" \
  --model-id "${local_model_id}" \
  --served-model-name live-single-gpu \
  --runtime-profile generic \
  --apply >/dev/null

echo "[live-single-gpu] waiting for both workers to consume the shared GPU"
wait_for_workers_ready "${build_dir}" "${db_path}" "${state_root}" 120

echo "[live-single-gpu] confirming deferred-preemption plan"
"${build_dir}/comet-controller" show-rebalance-plan --db "${db_path}" | grep -F \
  "worker=worker-a placement_mode=movable current=node-b:0 class=rollout-class decision=hold state=active-rollout target=node-a:0" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${db_path}" --node node-a | grep -F \
  "step=1 worker=worker-a action=evict-best-effort target=node-a:0 status=pending victims=worker-b" >/dev/null
"${build_dir}/comet-controller" show-rollout-actions --db "${db_path}" --node node-a | grep -F \
  "step=2 worker=worker-a action=retry-placement target=node-a:0 status=pending" >/dev/null

first_rollout_action_id="$("${build_dir}/comet-controller" show-rollout-actions --db "${db_path}" --node node-a | sed -n 's/^  - id=\([0-9][0-9]*\).*/\1/p' | head -n 1)"
second_rollout_action_id="$("${build_dir}/comet-controller" show-rollout-actions --db "${db_path}" --node node-a | sed -n 's/^  - id=\([0-9][0-9]*\).*/\1/p' | sed -n '2p')"
test -n "${first_rollout_action_id}"
test -n "${second_rollout_action_id}"

echo "[live-single-gpu] enqueueing live eviction"
"${build_dir}/comet-controller" enqueue-rollout-eviction --db "${db_path}" --id "${first_rollout_action_id}" | grep -F \
  "status=acknowledged" >/dev/null
show_pending_assignments_for_node "${build_dir}" "${db_path}" node-a | grep -F \
  "type=evict-workers status=pending" >/dev/null

echo "[live-single-gpu] applying evict-workers with real GPU verification"
"${build_dir}/comet-hostd" apply-next-assignment \
  --db "${db_path}" \
  --node node-a \
  --runtime-root "${runtime_root}" \
  --state-root "${state_root}" \
  --compose-mode exec >/dev/null
show_pending_assignments_for_node "${build_dir}" "${db_path}" node-a | grep -F \
  "type=evict-workers status=applied" >/dev/null

echo "[live-single-gpu] materializing retry-placement after verified eviction"
"${build_dir}/comet-controller" reconcile-rollout-actions --db "${db_path}" --artifacts-root "${artifacts_root}" | grep -F \
  "applied ready rollout action id=${second_rollout_action_id}" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "desired generation: 2" >/dev/null
"${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F \
  "worker-a role=worker node=node-a gpu=0 fraction=1 placement_mode=movable share_mode=exclusive" >/dev/null
if "${build_dir}/comet-controller" show-state --db "${db_path}" | grep -F "worker-b role=worker" >/dev/null; then
  echo "error: expected worker-b to be removed from desired state after retry-placement" >&2
  exit 1
fi

echo "[live-single-gpu] applying new generation"
apply_pending_assignment_if_any "${build_dir}" "${db_path}" node-a "${runtime_root}" "${state_root}"
apply_pending_assignment_if_any "${build_dir}" "${db_path}" node-b "${runtime_root}" "${state_root}"

echo "[live-single-gpu] waiting for rollout-applied and live ownership"
wait_for_rollout_applied "${build_dir}" "${db_path}" 120

echo "[live-single-gpu] asserting final observed state"
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" | grep -F \
  "instance name=worker-a role=worker phase=running ready=yes" >/dev/null
if "${build_dir}/comet-controller" show-host-observations --db "${db_path}" | grep -F \
    "instance name=worker-b role=worker phase=running ready=yes" >/dev/null; then
  echo "error: expected worker-b to stay evicted from live runtime observations" >&2
  exit 1
fi
"${build_dir}/comet-controller" show-host-observations --db "${db_path}" | grep -F "gpu device=0" >/dev/null
if ! grep -F '"instances": []' "${state_root}/node-b/applied-state.json" >/dev/null; then
  echo "error: expected node-b local state to converge to an empty instance set after worker-a move" >&2
  exit 1
fi

echo "[live-single-gpu] OK"
