#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "naim-production-env.sh must be sourced" >&2
  exit 1
fi

naim_deploy_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

: "${NAIM_CONTROL_SSH:=baal@51.68.181.52}"
: "${NAIM_WORKER_SSH:=baal@195.168.22.186}"
: "${NAIM_WORKER_SSH_PORT:=2222}"

: "${NAIM_MAIN_SSH:=${NAIM_CONTROL_SSH}}"
: "${NAIM_HPC1_SSH:=${NAIM_WORKER_SSH}}"
: "${NAIM_HPC1_SSH_PORT:=${NAIM_WORKER_SSH_PORT}}"

: "${NAIM_REGISTRY:=chainzano.com}"
: "${NAIM_REGISTRY_PROJECT:=naim}"
: "${NAIM_IMAGE_TAG:=dev}"
: "${NAIM_REGISTRY_USERNAME:=}"
: "${NAIM_REGISTRY_PASSWORD_FILE:=}"
: "${NAIM_REGISTRY_PASSWORD_COMMAND:=}"
: "${NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL:=}"
: "${NAIM_HARBOR_CONFIG_ON_CONTROL:=/opt/harbor/harbor.yml}"

: "${NAIM_CONTROL_ROOT:=/opt/naim/control-plane}"
: "${NAIM_CONTROL_CONTROLLER_LOCAL_PORT:=18084}"
: "${NAIM_CONTROL_WEB_UI_LOCAL_PORT:=18083}"
: "${NAIM_CONTROL_HOSTD_PUBLIC_PORT:=18080}"

: "${NAIM_MAIN_ROOT:=${NAIM_CONTROL_ROOT}}"
: "${NAIM_MAIN_CONTROLLER_LOCAL_PORT:=${NAIM_CONTROL_CONTROLLER_LOCAL_PORT}}"
: "${NAIM_MAIN_WEB_UI_LOCAL_PORT:=${NAIM_CONTROL_WEB_UI_LOCAL_PORT}}"
: "${NAIM_MAIN_HOSTD_PUBLIC_PORT:=${NAIM_CONTROL_HOSTD_PUBLIC_PORT}}"

: "${NAIM_WORKER_NAME:=hpc1}"
: "${NAIM_WORKER_ROOT:=/opt/naim/hostd}"
: "${NAIM_WORKER_SHARED_ROOT:=/mnt/shared-storage/naim}"
: "${NAIM_WORKER_CONTROLLER_URL:=http://51.68.181.52:18080}"
: "${NAIM_WORKER_POLL_INTERVAL_SEC:=2}"
: "${NAIM_WORKER_INVENTORY_SCAN_INTERVAL_SEC:=30}"
: "${NAIM_WORKER_ENABLE_NVIDIA:=yes}"

: "${NAIM_HOSTD_NODE:=${NAIM_WORKER_NAME}}"
: "${NAIM_HOSTD_ROOT:=${NAIM_WORKER_ROOT}}"
: "${NAIM_HOSTD_SHARED_ROOT:=${NAIM_WORKER_SHARED_ROOT}}"
: "${NAIM_HOSTD_CONTROLLER_URL:=${NAIM_WORKER_CONTROLLER_URL}}"
: "${NAIM_HOSTD_POLL_INTERVAL_SEC:=${NAIM_WORKER_POLL_INTERVAL_SEC}}"
: "${NAIM_HOSTD_INVENTORY_SCAN_INTERVAL_SEC:=${NAIM_WORKER_INVENTORY_SCAN_INTERVAL_SEC}}"
: "${NAIM_HOSTD_ENABLE_NVIDIA:=${NAIM_WORKER_ENABLE_NVIDIA}}"

naim_image() {
  local image_name="$1"
  printf "%s/%s/%s:%s" \
    "${NAIM_REGISTRY}" \
    "${NAIM_REGISTRY_PROJECT}" \
    "${image_name}" \
    "${NAIM_IMAGE_TAG}"
}

ssh_main() {
  ssh ${NAIM_SSH_OPTS:-} "${NAIM_MAIN_SSH}" "$@"
}

ssh_hpc1() {
  ssh ${NAIM_SSH_OPTS:-} -p "${NAIM_HPC1_SSH_PORT}" "${NAIM_HPC1_SSH}" "$@"
}

remote_main_bash() {
  ssh_main "bash -s" -- "$@"
}

remote_hpc1_bash() {
  ssh_hpc1 "bash -s" -- "$@"
}

ssh_control() {
  ssh_main "$@"
}

ssh_worker() {
  ssh_hpc1 "$@"
}

remote_control_bash() {
  remote_main_bash "$@"
}

remote_worker_bash() {
  remote_hpc1_bash "$@"
}

naim_load_inventory_env() {
  local inventory_path="$1"
  local worker_name="${2:-}"
  local args=(env --inventory "${inventory_path}")
  if [[ -n "${worker_name}" ]]; then
    args+=(--worker "${worker_name}")
  fi
  "${naim_deploy_dir}/naim-inventory.py" "${args[@]}"
}

naim_registry_username() {
  if [[ -n "${NAIM_REGISTRY_USERNAME}" ]]; then
    printf '%s' "${NAIM_REGISTRY_USERNAME}"
    return 0
  fi
  if [[ -n "${NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL}" ||
        -n "${NAIM_HARBOR_CONFIG_ON_CONTROL}" ]]; then
    printf 'admin'
    return 0
  fi
  printf ''
}

naim_registry_password() {
  if [[ -n "${NAIM_REGISTRY_PASSWORD_COMMAND}" ]]; then
    eval "${NAIM_REGISTRY_PASSWORD_COMMAND}"
    return 0
  fi
  if [[ -n "${NAIM_REGISTRY_PASSWORD_FILE}" ]]; then
    cat "${NAIM_REGISTRY_PASSWORD_FILE}"
    return 0
  fi
  if [[ -n "${NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL}" ]]; then
    ssh_main "cat '${NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL}'"
    return 0
  fi
  ssh_main "if [[ -f '${NAIM_HARBOR_CONFIG_ON_CONTROL}' ]]; then awk -F': *' '/^harbor_admin_password:/ {print \$2; exit}' '${NAIM_HARBOR_CONFIG_ON_CONTROL}'; fi"
}
