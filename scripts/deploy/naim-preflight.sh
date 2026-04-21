#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-production-env.sh"

inventory_path=""
declare -a selected_workers=()
failures=0
warnings=0

usage() {
  cat <<'EOF'
Usage:
  naim-preflight.sh [--inventory <path>] [--worker <name> ...]

Checks SSH, Docker, Compose, sudo, GPU runtime, and basic network assumptions
before a NAIM production deploy.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      if [[ -z "${inventory_path}" ]]; then
        echo "error: --inventory requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    --worker)
      next_worker="${2:-}"
      if [[ -z "${next_worker}" ]]; then
        echo "error: --worker requires a name" >&2
        exit 1
      fi
      selected_workers+=("${next_worker}")
      shift 2
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

pass() {
  printf '[ok] %s\n' "$1"
}

fail() {
  printf '[fail] %s\n' "$1" >&2
  failures=$((failures + 1))
}

warn() {
  printf '[warn] %s\n' "$1" >&2
  warnings=$((warnings + 1))
}

check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    pass "${label}"
  else
    fail "${label}"
  fi
}

check_warn() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    pass "${label}"
  else
    warn "${label}"
  fi
}

load_inventory_for_worker() {
  local worker_name="${1:-}"
  if [[ -n "${inventory_path}" ]]; then
    eval "$(naim_load_inventory_env "${inventory_path}" "${worker_name}")"
  fi
}

check command.local.ssh command -v ssh
check command.local.python3 command -v python3

load_inventory_for_worker "${selected_workers[0]:-}"

controller_image="$(naim_image controller)"
web_ui_image="$(naim_image web-ui)"
hostd_image="$(naim_image hostd)"

printf 'control_host=%s\n' "${NAIM_MAIN_SSH}"
printf 'controller_image=%s\n' "${controller_image}"
printf 'web_ui_image=%s\n' "${web_ui_image}"
printf 'hostd_image=%s\n' "${hostd_image}"

check control.ssh ssh_main true
check control.python3 ssh_main 'command -v python3'
check control.curl ssh_main 'command -v curl'
check control.docker ssh_main 'command -v docker && docker version'
check control.compose ssh_main 'docker compose version'
check control.sudo_noninteractive ssh_main 'sudo -n true'

if [[ "${NAIM_IMAGE_TAG}" == "dev" ]]; then
  warn "registry tag is 'dev'; production should use an immutable release tag or digest"
fi

if [[ -n "${NAIM_REGISTRY_USERNAME}" ||
      -n "${NAIM_REGISTRY_PASSWORD_FILE}" ||
      -n "${NAIM_REGISTRY_PASSWORD_COMMAND}" ||
      -n "${NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL}" ||
      -n "${NAIM_HARBOR_CONFIG_ON_CONTROL}" ]]; then
  pass "registry credential source configured"
else
  warn "no registry credential source configured; pulls must work anonymously or via existing Docker credentials"
fi

if [[ -n "${inventory_path}" && ${#selected_workers[@]} -eq 0 ]]; then
  while IFS= read -r worker; do
    [[ -n "${worker}" ]] && selected_workers+=("${worker}")
  done < <("${script_dir}/naim-inventory.py" workers --inventory "${inventory_path}")
fi

if [[ ${#selected_workers[@]} -eq 0 ]]; then
  selected_workers=("${NAIM_HOSTD_NODE}")
fi

for worker in "${selected_workers[@]}"; do
  load_inventory_for_worker "${worker}"
  printf 'worker=%s ssh=%s port=%s\n' \
    "${NAIM_HOSTD_NODE}" \
    "${NAIM_HPC1_SSH}" \
    "${NAIM_HPC1_SSH_PORT}"

  check "worker.${NAIM_HOSTD_NODE}.ssh" ssh_hpc1 true
  check "worker.${NAIM_HOSTD_NODE}.python3" ssh_hpc1 'command -v python3'
  check "worker.${NAIM_HOSTD_NODE}.curl" ssh_hpc1 'command -v curl'
  check "worker.${NAIM_HOSTD_NODE}.docker" ssh_hpc1 'command -v docker && docker version'
  check "worker.${NAIM_HOSTD_NODE}.compose" ssh_hpc1 'docker compose version'
  check "worker.${NAIM_HOSTD_NODE}.sudo_noninteractive" ssh_hpc1 'sudo -n true'
  check "worker.${NAIM_HOSTD_NODE}.docker_sock" ssh_hpc1 'test -S /var/run/docker.sock'

  if [[ "${NAIM_HOSTD_ENABLE_NVIDIA}" == "yes" ]]; then
    check "worker.${NAIM_HOSTD_NODE}.nvidia_smi" ssh_hpc1 'command -v nvidia-smi && nvidia-smi -L'
    check "worker.${NAIM_HOSTD_NODE}.nvidia_runtime" ssh_hpc1 \
      'docker info --format "{{json .Runtimes}}" | grep -F nvidia'
  fi

  check_warn "worker.${NAIM_HOSTD_NODE}.controller_endpoint_reachable" ssh_hpc1 \
    "curl -fsS '${NAIM_HOSTD_CONTROLLER_URL}/api/v1/health'"
done

if [[ "${failures}" -ne 0 ]]; then
  echo "preflight failed: ${failures} failed, ${warnings} warnings" >&2
  exit 1
fi

echo "preflight passed: ${warnings} warnings"
