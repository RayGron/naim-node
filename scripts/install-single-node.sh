#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  install-single-node.sh [--build-type Debug|Release] [--listen-port <port>] [--node <name>] [--with-web-ui] [--skip-prereqs] [--skip-image-build]

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
    docker-compose-v2
  )

  echo "[install-single-node] installing apt prerequisites"
  run_as_root apt-get update
  run_as_root apt-get install -y "${packages[@]}"
  run_as_root apt-get install -y "${docker_packages[@]}" || true
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

echo "[install-single-node] building host binaries (${build_type})"
"${script_dir}/build-host.sh" "${build_type}"

if [[ "${skip_image_build}" != "yes" ]]; then
  echo "[install-single-node] building runtime images"
  image_build_args=()
  if [[ "${with_web_ui}" != "yes" ]]; then
    image_build_args+=(--skip-web-ui)
  fi
  run_as_root env PATH="${PATH}" HOME="${HOME}" "${script_dir}/build-runtime-images.sh" "${image_build_args[@]}"
fi

build_dir="$("${script_dir}/print-host-build-dir.sh")"
launcher_binary="${build_dir}/comet-node"

if [[ ! -x "${launcher_binary}" ]]; then
  echo "error: launcher binary not found: ${launcher_binary}" >&2
  exit 1
fi

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

echo "installed=ok"
echo "controller_api_url=http://127.0.0.1:${listen_port}"
echo "node=${node_name}"
echo "storage_root=${storage_root}"
if [[ -n "${model_cache_root}" ]]; then
  echo "model_cache_root=${model_cache_root}"
fi
echo "next_step=${repo_root}/scripts/run-plane.sh qwen35-9b-min"
