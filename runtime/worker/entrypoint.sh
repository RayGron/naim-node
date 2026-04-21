#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp
boot_mode="${NAIM_WORKER_BOOT_MODE:-llama-idle}"
echo "[naim-worker] booting plane=${NAIM_PLANE_NAME:-unknown} instance=${NAIM_INSTANCE_NAME:-unknown}"
echo "[naim-worker] control_root=${NAIM_CONTROL_ROOT:-unknown}"
echo "[naim-worker] boot_mode=${boot_mode}"

case "${boot_mode}" in
  llama-load|llama-idle|llama-rpc)
    exec /runtime/bin/naim-workerd
    ;;
  *)
    echo "[naim-worker] unsupported boot mode: ${boot_mode}" >&2
    exit 1
    ;;
esac
