#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${repo_root}/build/linux/x64"

skip_build=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      skip_build=1
      shift
      ;;
    *)
      echo "usage: $0 [--skip-build]" >&2
      exit 1
      ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${repo_root}/scripts/build-target.sh" linux x64 Debug >/dev/null
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
  if [[ -n "${WSL_DISTRO_NAME:-}" ]] && [[ -x /mnt/c/Windows/System32/wsl.exe ]]; then
    /mnt/c/Windows/System32/wsl.exe -d "${WSL_DISTRO_NAME}" -u root -- \
      bash -lc "cd '${repo_root}' && ${command}"
    return
  fi
  echo "live-storage: unable to acquire root privileges" >&2
  exit 1
}

run_hostd_apply_as_root() {
  local command="$1"
  if run_as_root "${command}"; then
    return
  fi
  sleep 1
  run_as_root "${command}"
}

command -v docker >/dev/null 2>&1 || {
  echo "live-storage: docker is required" >&2
  exit 1
}

command -v mountpoint >/dev/null 2>&1 || {
  echo "live-storage: mountpoint is required" >&2
  exit 1
}

work_root="$(mktemp -d "${repo_root}/var/live-storage.XXXXXX")"
db_path="${work_root}/controller.sqlite"
artifacts_root="${work_root}/artifacts"
runtime_root="${work_root}/runtime"
state_root="${work_root}/state"
reduced_bundle="${work_root}/bundle-reduced"
infer_move_bundle="${work_root}/bundle-infer-move"
plane_rename_bundle="${work_root}/bundle-plane-rename"
live_bundle="${work_root}/bundle-live"
plane_db_path="${work_root}/plane-controller.sqlite"
plane_artifacts_root="${work_root}/artifacts-plane"
plane_runtime_root="${work_root}/runtime-plane"
plane_state_root="${work_root}/state-plane"
plane_live_bundle="${work_root}/bundle-plane-live"

cleanup() {
  if [[ -d "${work_root}" ]]; then
    if [[ "$(id -u)" == "0" ]]; then
      rm -rf "${work_root}"
    elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
      sudo rm -rf "${work_root}" 2>/dev/null || true
    elif [[ -n "${WSL_DISTRO_NAME:-}" ]] && [[ -x /mnt/c/Windows/System32/wsl.exe ]]; then
      /mnt/c/Windows/System32/wsl.exe -d "${WSL_DISTRO_NAME}" -u root -- \
        bash -lc "rm -rf '${work_root}'" >/dev/null 2>&1 || true
    fi
    rm -rf "${work_root}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[live-storage] init db"
"${build_dir}/comet-controller" init-db --db "${db_path}" >/dev/null

echo "[live-storage] prepare compact live bundle"
mkdir -p "${live_bundle}/workers"
python3 - <<'PY' "${repo_root}" "${live_bundle}"
import json
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1])
live_bundle = pathlib.Path(sys.argv[2])

plane = json.loads((repo_root / "config/demo-plane/plane.json").read_text())
plane["shared_disk_gb"] = 1
(live_bundle / "plane.json").write_text(json.dumps(plane, indent=2) + "\n")

infer = json.loads((repo_root / "config/demo-plane/infer.json").read_text())
infer["private_disk_gb"] = 1
(live_bundle / "infer.json").write_text(json.dumps(infer, indent=2) + "\n")

for worker_name in ("worker-a", "worker-b"):
    worker = json.loads((repo_root / f"config/demo-plane/workers/{worker_name}.json").read_text())
    worker["private_disk_gb"] = 1
    (live_bundle / "workers" / f"{worker_name}.json").write_text(json.dumps(worker, indent=2) + "\n")
PY

echo "[live-storage] apply full bundle"
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${live_bundle}" \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" >/dev/null

echo "[live-storage] hostd apply on node-a and node-b as root"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-a --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-b --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"

shared_a="${runtime_root}/var/lib/comet/disks/planes/alpha/shared"
private_infer="${runtime_root}/nodes/node-a/var/lib/comet/disks/instances/infer-main/private"
private_worker_b="${runtime_root}/nodes/node-b/var/lib/comet/disks/instances/worker-b/private"

echo "[live-storage] verify mounted runtime state"
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" | grep -F "disk=plane-alpha-shared kind=plane-shared node=node-a" | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" | grep -F "disk=infer-main-private kind=infer-private node=node-a" | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" | grep -F "disk=worker-b-private kind=worker-private node=node-b" | grep -F "realized_state=mounted" >/dev/null

run_as_root "mountpoint -q '${shared_a}'"
run_as_root "mountpoint -q '${private_infer}'"
run_as_root "mountpoint -q '${private_worker_b}'"

echo "[live-storage] verify containers see mounted volumes"
docker run --rm -v "${shared_a}:/comet/shared" alpine:3.20 sh -lc \
  'echo shared-container-ok >/comet/shared/container-check.txt && test -f /comet/shared/container-check.txt'
docker run --rm -v "${private_infer}:/comet/private" alpine:3.20 sh -lc \
  'echo infer-private-ok >/comet/private/container-check.txt && test -f /comet/private/container-check.txt'

run_as_root "test -f '${shared_a}/container-check.txt'"
run_as_root "test -f '${private_infer}/container-check.txt'"

echo "[live-storage] verify restart reconciliation"
python3 - <<'PY' "${db_path}"
import sqlite3
import sys
db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.execute("DELETE FROM disk_runtime_state")
conn.commit()
conn.close()
PY

run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-a --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-b --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"

"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-a | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "realized_state=mounted" >/dev/null

echo "[live-storage] prepare reduced bundle for teardown"
mkdir -p "${reduced_bundle}"
mkdir -p "${reduced_bundle}/workers"
python3 - <<'PY' "${repo_root}" "${reduced_bundle}"
import json
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1])
bundle_root = pathlib.Path(sys.argv[2])

plane = json.loads((repo_root / "config/demo-plane/plane.json").read_text())
plane["shared_disk_gb"] = 1
(bundle_root / "plane.json").write_text(json.dumps(plane, indent=2) + "\n")

infer = json.loads((repo_root / "config/demo-plane/infer.json").read_text())
infer["private_disk_gb"] = 1
(bundle_root / "infer.json").write_text(json.dumps(infer, indent=2) + "\n")

worker = json.loads((repo_root / "config/demo-plane/workers/worker-a.json").read_text())
worker["private_disk_gb"] = 1
(bundle_root / "workers" / "worker-a.json").write_text(json.dumps(worker, indent=2) + "\n")
PY

echo "[live-storage] apply reduced bundle"
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${reduced_bundle}" \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" >/dev/null

echo "[live-storage] hostd apply reduced node-b state as root"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-b --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"

"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "disk=worker-b-private node=node-b realized_state=removed" >/dev/null
if run_as_root "mountpoint -q '${private_worker_b}'"; then
  echo "live-storage: expected worker-b private mount to be removed" >&2
  exit 1
fi

echo "[live-storage] prepare infer-move bundle for infer private teardown"
mkdir -p "${infer_move_bundle}/workers"
python3 - <<'PY' "${repo_root}" "${infer_move_bundle}"
import json
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1])
bundle_root = pathlib.Path(sys.argv[2])

plane = json.loads((repo_root / "config/demo-plane/plane.json").read_text())
plane["shared_disk_gb"] = 1
(bundle_root / "plane.json").write_text(json.dumps(plane, indent=2) + "\n")

infer = json.loads((repo_root / "config/demo-plane/infer.json").read_text())
infer["node"] = "node-b"
infer["private_disk_gb"] = 1
(bundle_root / "infer.json").write_text(json.dumps(infer, indent=2) + "\n")

worker = json.loads((repo_root / "config/demo-plane/workers/worker-a.json").read_text())
worker["private_disk_gb"] = 1
(bundle_root / "workers" / "worker-a.json").write_text(json.dumps(worker, indent=2) + "\n")
PY

echo "[live-storage] apply infer-move bundle"
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${infer_move_bundle}" \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" >/dev/null

echo "[live-storage] hostd apply infer-move node-a and node-b as root"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-a --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${db_path}' --node node-b --artifacts-root '${artifacts_root}' --runtime-root '${runtime_root}' --state-root '${state_root}' --compose-mode skip >/dev/null"

"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-a | grep -F "disk=infer-main-private node=node-a realized_state=removed" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${db_path}" --node node-b | grep -F "disk=infer-main-private kind=infer-private node=node-b" | grep -F "realized_state=mounted" >/dev/null
if run_as_root "mountpoint -q '${private_infer}'"; then
  echo "live-storage: expected infer-main private mount to be removed" >&2
  exit 1
fi

private_infer_node_b="${runtime_root}/nodes/node-b/var/lib/comet/disks/instances/infer-main/private"
run_as_root "mountpoint -q '${private_infer_node_b}'"

echo "[live-storage] prepare plane-rename bundle for shared disk teardown"
mkdir -p "${plane_live_bundle}/workers" "${plane_rename_bundle}/workers"
python3 - <<'PY' "${repo_root}" "${plane_live_bundle}" "${plane_rename_bundle}"
import json
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1])
live_bundle_root = pathlib.Path(sys.argv[2])
rename_bundle_root = pathlib.Path(sys.argv[3])

plane_alpha = json.loads((repo_root / "config/demo-plane/plane.json").read_text())
plane_alpha["shared_disk_gb"] = 1
(live_bundle_root / "plane.json").write_text(json.dumps(plane_alpha, indent=2) + "\n")

infer = json.loads((repo_root / "config/demo-plane/infer.json").read_text())
infer["private_disk_gb"] = 1
(live_bundle_root / "infer.json").write_text(json.dumps(infer, indent=2) + "\n")

for worker_name in ("worker-a", "worker-b"):
    worker = json.loads((repo_root / f"config/demo-plane/workers/{worker_name}.json").read_text())
    worker["private_disk_gb"] = 1
    (live_bundle_root / "workers" / f"{worker_name}.json").write_text(json.dumps(worker, indent=2) + "\n")

plane_beta = dict(plane_alpha)
plane_beta["name"] = "beta"
plane_beta["control_root"] = "/comet/shared/control/beta"
(rename_bundle_root / "plane.json").write_text(json.dumps(plane_beta, indent=2) + "\n")
(rename_bundle_root / "infer.json").write_text(json.dumps(infer, indent=2) + "\n")
for worker_name in ("worker-a", "worker-b"):
    worker = json.loads((repo_root / f"config/demo-plane/workers/{worker_name}.json").read_text())
    worker["private_disk_gb"] = 1
    (rename_bundle_root / "workers" / f"{worker_name}.json").write_text(json.dumps(worker, indent=2) + "\n")
PY

echo "[live-storage] init separate db for shared teardown"
"${build_dir}/comet-controller" init-db --db "${plane_db_path}" >/dev/null

echo "[live-storage] apply fresh alpha bundle for shared teardown"
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${plane_live_bundle}" \
  --db "${plane_db_path}" \
  --artifacts-root "${plane_artifacts_root}" >/dev/null

echo "[live-storage] hostd apply fresh alpha node-a and node-b as root"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${plane_db_path}' --node node-a --artifacts-root '${plane_artifacts_root}' --runtime-root '${plane_runtime_root}' --state-root '${plane_state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${plane_db_path}' --node node-b --artifacts-root '${plane_artifacts_root}' --runtime-root '${plane_runtime_root}' --state-root '${plane_state_root}' --compose-mode skip >/dev/null"

echo "[live-storage] apply plane-rename bundle"
"${build_dir}/comet-controller" apply-bundle \
  --bundle "${plane_rename_bundle}" \
  --db "${plane_db_path}" \
  --artifacts-root "${plane_artifacts_root}" >/dev/null

echo "[live-storage] hostd apply plane-rename node-a and node-b as root"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${plane_db_path}' --node node-a --artifacts-root '${plane_artifacts_root}' --runtime-root '${plane_runtime_root}' --state-root '${plane_state_root}' --compose-mode skip >/dev/null"
run_hostd_apply_as_root "'${build_dir}/comet-hostd' apply-state-ops --db '${plane_db_path}' --node node-b --artifacts-root '${plane_artifacts_root}' --runtime-root '${plane_runtime_root}' --state-root '${plane_state_root}' --compose-mode skip >/dev/null"

"${build_dir}/comet-controller" show-disk-state --db "${plane_db_path}" | grep -F "disk=plane-alpha-shared" | grep -F "realized_state=removed" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${plane_db_path}" | grep -F "disk=plane-beta-shared kind=plane-shared node=node-a" | grep -F "realized_state=mounted" >/dev/null
"${build_dir}/comet-controller" show-disk-state --db "${plane_db_path}" | grep -F "disk=plane-beta-shared kind=plane-shared node=node-b" | grep -F "realized_state=mounted" >/dev/null

shared_alpha="${plane_runtime_root}/var/lib/comet/disks/planes/alpha/shared"
shared_beta="${plane_runtime_root}/var/lib/comet/disks/planes/beta/shared"
if run_as_root "mountpoint -q '${shared_alpha}'"; then
  echo "live-storage: expected alpha shared mount to be removed" >&2
  exit 1
fi
run_as_root "mountpoint -q '${shared_beta}'"

echo "[live-storage] OK"
