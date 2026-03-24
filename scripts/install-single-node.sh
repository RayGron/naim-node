#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  install-single-node.sh [--build-type Debug|Release] [--listen-port <port>] [--node <name>] [--with-web-ui] [--with-vllm-worker] [--skip-prereqs] [--skip-image-build]

Builds comet-node on the current Linux host, installs controller+local-hostd as systemd services,
and starts them.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

build_type="Debug"
listen_port="18080"
node_name="local-hostd"
with_web_ui="no"
with_vllm_worker="no"
skip_prereqs="no"
skip_image_build="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-type)
      build_type="${2:-}"
      shift 2
      ;;
    --listen-port)
      listen_port="${2:-}"
      shift 2
      ;;
    --node)
      node_name="${2:-}"
      shift 2
      ;;
    --with-web-ui)
      with_web_ui="yes"
      shift
      ;;
    --with-vllm-worker)
      with_vllm_worker="yes"
      shift
      ;;
    --skip-prereqs)
      skip_prereqs="yes"
      shift
      ;;
    --skip-image-build)
      skip_image_build="yes"
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

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: install-single-node.sh currently supports Linux hosts only" >&2
  exit 1
fi

run_as_root() {
  if [[ "$(id -u)" == "0" ]]; then
    "$@"
    return
  fi
  if ! command -v sudo >/dev/null 2>&1; then
    echo "error: sudo is required for installation steps" >&2
    exit 1
  fi
  sudo "$@"
}

run_as_invoking_user() {
  if [[ "$(id -u)" == "0" ]] && [[ -n "${SUDO_USER:-}" ]] && [[ "${SUDO_USER}" != "root" ]]; then
    local user_home
    user_home="$(getent passwd "${SUDO_USER}" | cut -d: -f6)"
    sudo -u "${SUDO_USER}" env \
      PATH="${PATH}" \
      HOME="${user_home}" \
      VCPKG_ROOT="${VCPKG_ROOT:-}" \
      "$@"
    return
  fi
  "$@"
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-30}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_existing_comet_processes() {
  echo "[install-single-node] stopping existing comet services and stray processes"
  run_as_root systemctl stop comet-node-controller.service comet-node-hostd.service >/dev/null 2>&1 || true

  local patterns=(
    "/comet-node run controller"
    "/comet-controller serve"
    "/comet-node run hostd"
  )
  local pattern
  for pattern in "${patterns[@]}"; do
    run_as_root pkill -f "${pattern}" >/dev/null 2>&1 || true
  done
}

install_prereqs_if_needed() {
  if [[ "${skip_prereqs}" == "yes" ]]; then
    return
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "[install-single-node] skipping apt prerequisites: apt-get not found"
    return
  fi

  local packages=(
    ca-certificates
    curl
    cmake
    git
    jq
    ninja-build
    pkg-config
    python3
    python3-venv
  )
  local docker_packages=(
    docker.io
    docker-compose-plugin
  )

  echo "[install-single-node] installing apt prerequisites"
  run_as_root apt-get update
  run_as_root apt-get install -y "${packages[@]}"
  run_as_root apt-get install -y "${docker_packages[@]}" || run_as_root apt-get install -y docker.io || true
  run_as_root apt-get clean || true
}

apt_package_has_candidate() {
  local package_name="$1"
  local policy_output
  policy_output="$(apt-cache policy "${package_name}" 2>/dev/null || true)"
  [[ -n "${policy_output}" ]] && ! grep -Fq 'Candidate: (none)' <<<"${policy_output}"
}

have_nvcc() {
  if command -v nvcc >/dev/null 2>&1; then
    return 0
  fi
  local candidate=""
  for candidate in \
    /usr/local/cuda/bin/nvcc \
    /usr/local/cuda-13.1/bin/nvcc \
    /usr/local/cuda-13.0/bin/nvcc \
    /usr/lib/nvidia-cuda-toolkit/bin/nvcc; do
    if [[ -x "${candidate}" ]]; then
      return 0
    fi
  done
  return 1
}

install_cuda_toolkit_if_needed() {
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    return
  fi
  if have_nvcc; then
    return
  fi
  if [[ "${skip_prereqs}" == "yes" ]]; then
    echo "[install-single-node] skipping CUDA toolkit install because --skip-prereqs was requested"
    return
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "[install-single-node] skipping CUDA toolkit install: apt-get not found"
    return
  fi

  local package_name=""
  local candidates=(
    cuda-toolkit-13-1
    cuda-toolkit-13-0
    nvidia-cuda-toolkit
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if apt_package_has_candidate "${candidate}"; then
      package_name="${candidate}"
      break
    fi
  done

  if [[ -z "${package_name}" ]]; then
    echo "[install-single-node] CUDA toolkit package was not found in apt sources"
    return
  fi

  echo "[install-single-node] installing CUDA toolkit package ${package_name}"
  run_as_root apt-get install -y "${package_name}"
  run_as_root apt-get clean || true
}

config_summary="$(
  python3 - <<'PY' "${repo_root}/config/comet-node-config.json"
import json
import pathlib
import sys

config_path = pathlib.Path(sys.argv[1])
payload = json.loads(config_path.read_text())
paths = payload.get("paths", {})
print(paths.get("storage_root", "/var/lib/comet"))
print(paths.get("model_cache_root", ""))
PY
)"
storage_root="$(printf '%s\n' "${config_summary}" | sed -n '1p')"
model_cache_root="$(printf '%s\n' "${config_summary}" | sed -n '2p')"

install_prereqs_if_needed
install_cuda_toolkit_if_needed

echo "[install-single-node] building host binaries (${build_type})"
run_as_invoking_user "${script_dir}/build-host.sh" "${build_type}"

if [[ "${skip_image_build}" != "yes" ]]; then
  echo "[install-single-node] building runtime images"
  image_build_args=()
  if [[ "${with_web_ui}" != "yes" ]]; then
    image_build_args+=(--skip-web-ui)
  fi
  image_build_env=(env PATH="${PATH}" HOME="${HOME}")
  if [[ "${with_vllm_worker}" == "yes" ]]; then
    image_build_env+=(COMET_BUILD_VLLM_WORKER=yes)
  fi
  run_as_root "${image_build_env[@]}" "${script_dir}/build-runtime-images.sh" "${image_build_args[@]}"
fi

build_dir="$("${script_dir}/print-host-build-dir.sh")"
launcher_binary="${build_dir}/comet-node"

if [[ ! -x "${launcher_binary}" ]]; then
  echo "error: launcher binary not found: ${launcher_binary}" >&2
  exit 1
fi

stop_existing_comet_processes

install_args=(
  install
  controller
  --with-hostd
  --listen-port "${listen_port}"
  --node "${node_name}"
)
if [[ "${with_web_ui}" == "yes" ]]; then
  install_args+=(--with-web-ui)
fi

echo "[install-single-node] installing controller and local hostd services"
run_as_root "${launcher_binary}" "${install_args[@]}"

echo "[install-single-node] verifying services"
run_as_root systemctl is-active --quiet comet-node-controller.service
run_as_root systemctl is-active --quiet comet-node-hostd.service
if ! wait_for_http "http://127.0.0.1:${listen_port}/health" 30; then
  echo "error: controller health endpoint did not become ready on port ${listen_port}" >&2
  exit 1
fi

echo "installed=ok"
echo "controller_api_url=http://127.0.0.1:${listen_port}"
echo "node=${node_name}"
echo "storage_root=${storage_root}"
if [[ -n "${model_cache_root}" ]]; then
  echo "model_cache_root=${model_cache_root}"
fi
echo "next_step=${repo_root}/scripts/run-plane.sh qwen35-9b-min"
