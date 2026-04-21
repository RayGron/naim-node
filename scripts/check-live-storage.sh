#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
source "${script_dir}/naim-live-v2-lib.sh"
build_dir="$("${script_dir}/print-build-dir.sh")"

skip_build=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) skip_build=1; shift ;;
    *) echo "usage: $0 [--skip-build]" >&2; exit 1 ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" Debug >/dev/null
fi

run_as_root() {
  local command="$1"
  if [[ "$(id -u)" == "0" ]]; then
    bash -lc "cd '${repo_root}' && ${command}"
    return
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo bash -lc "cd '${repo_root}' && ${command}"
    return
  fi
  echo "live-storage: unable to acquire root privileges" >&2
  exit 1
}

run_hostd_apply_as_root() {
  local command="$1"
  run_as_root "${command}" || { sleep 1; run_as_root "${command}"; }
}

command -v docker >/dev/null 2>&1 || { echo "live-storage: docker is required" >&2; exit 1; }
command -v mountpoint >/dev/null 2>&1 || { echo "live-storage: mountpoint is required" >&2; exit 1; }

mkdir -p "${repo_root}/var"
work_root="$(mktemp -d "${repo_root}/var/live-storage.XXXXXX")"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/state"
state_path="${work_root}/storage.desired-state.v2.json"
reduced_state_path="${work_root}/storage-reduced.desired-state.v2.json"
renamed_state_path="${work_root}/storage-renamed.desired-state.v2.json"

cleanup() {
  if [[ "${NAIM_KEEP_WORK_ROOT:-0}" == "1" ]]; then
    echo "[live-storage] keeping work root ${work_root}" >&2
    return
  fi
  if [[ -d "${work_root}" ]]; then
    if command -v findmnt >/dev/null 2>&1; then
      findmnt -rn -o TARGET | grep -F "${work_root}/" | sort -r | while IFS= read -r mount_path; do
        if [[ -z "${mount_path}" ]]; then
          continue
        fi
        if [[ "$(id -u)" == "0" ]]; then
          umount -l "${mount_path}" 2>/dev/null || true
        elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
          sudo umount -l "${mount_path}" 2>/dev/null || true
        fi
      done
    fi
    if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
      sudo rm -rf "${work_root}" 2>/dev/null || true
    fi
    rm -rf "${work_root}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mkdir -p "${artifacts_root}" "${runtime_root}" "${state_root}"

echo "[live-storage] init v2 db"
"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
naim_live_seed_connected_hostd "${db_path}" node-a 2
naim_live_seed_connected_hostd "${db_path}" node-b 2
naim_live_write_compute_state "${state_path}" storage-v2 2
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${state_path}"

echo "[live-storage] hostd apply on node-a and node-b as root"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-b --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"

shared_a="${runtime_root}/mnt/shared-storage/naim/disks/planes/storage-v2/shared"
private_worker_a="${runtime_root}/nodes/node-a/mnt/shared-storage/naim/disks/instances/worker-storage-v2-a/private"
private_worker_b="${runtime_root}/nodes/node-b/mnt/shared-storage/naim/disks/instances/worker-storage-v2-b/private"

echo "[live-storage] verify mounted runtime state"
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" | grep -F "disk=plane-storage-v2-shared kind=plane-shared node=node-a" | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" | grep -F "disk=worker-storage-v2-a-private kind=worker-private node=node-a" | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" | grep -F "disk=worker-storage-v2-b-private kind=worker-private node=node-b" | grep -F "realized_state=mounted" >/dev/null
run_as_root "mountpoint -q '${shared_a}'"
run_as_root "mountpoint -q '${private_worker_a}'"
run_as_root "mountpoint -q '${private_worker_b}'"

echo "[live-storage] verify containers see mounted volumes"
docker run --rm -v "${shared_a}:/naim/shared" alpine:3.20 sh -lc 'echo shared-container-ok >/naim/shared/container-check.txt && test -f /naim/shared/container-check.txt'
docker run --rm -v "${private_worker_a}:/naim/private" alpine:3.20 sh -lc 'echo worker-private-ok >/naim/private/container-check.txt && test -f /naim/private/container-check.txt'
run_as_root "test -f '${shared_a}/container-check.txt'"
run_as_root "test -f '${private_worker_a}/container-check.txt'"

echo "[live-storage] verify restart reconciliation"
sqlite3 "${db_path}" "DELETE FROM disk_runtime_state; UPDATE host_assignments SET status='pending', attempt_count=0 WHERE plane_name='storage-v2';"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-b --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" --node node-a | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "realized_state=mounted" >/dev/null

echo "[live-storage] apply reduced v2 state for node-b teardown"
"${build_dir}/naim-controller" stop-plane --db "${db_path}" --plane storage-v2 >/dev/null
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-b --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
naim_live_write_compute_state "${reduced_state_path}" storage-v2 1
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${reduced_state_path}"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-b --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
if run_as_root "mountpoint -q '${private_worker_b}'"; then
  echo "live-storage: expected worker-b private mount to be removed" >&2
  exit 1
fi

echo "[live-storage] apply renamed plane for shared teardown"
"${build_dir}/naim-controller" delete-plane --db "${db_path}" --plane storage-v2 >/dev/null
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
naim_live_write_compute_state "${renamed_state_path}" storage-v2-renamed 1
naim_live_apply_v2_state "${build_dir}" "${db_path}" "${artifacts_root}" "${renamed_state_path}"
run_hostd_apply_as_root "'${build_dir}/naim-hostd' apply-next-assignment --db '${db_path}' --node node-a --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
"${build_dir}/naim-controller" show-disk-state --db "${db_path}" | grep -F "disk=plane-storage-v2-renamed-shared" | grep -F "realized_state=mounted" >/dev/null
shared_renamed="${runtime_root}/mnt/shared-storage/naim/disks/planes/storage-v2-renamed/shared"
if run_as_root "mountpoint -q '${shared_a}'"; then
  echo "live-storage: expected old shared mount to be removed" >&2
  exit 1
fi
run_as_root "mountpoint -q '${shared_renamed}'"

echo "[live-storage] OK"
