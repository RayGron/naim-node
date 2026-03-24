#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp
boot_mode="${COMET_WORKER_BOOT_MODE:-llama-load}"
echo "[comet-worker] booting plane=${COMET_PLANE_NAME:-unknown} instance=${COMET_INSTANCE_NAME:-unknown}"
echo "[comet-worker] control_root=${COMET_CONTROL_ROOT:-unknown}"
echo "[comet-worker] boot_mode=${boot_mode}"

case "${boot_mode}" in
  llama-load)
    exec /runtime/bin/comet-workerd
    ;;
  vllm-openai|vllm-serve)
    if ! command -v python3 >/dev/null 2>&1; then
      echo "[comet-worker] vLLM boot mode requires the vLLM worker image with Python runtime" >&2
      exit 1
    fi
    exec python3 /runtime/worker/vllm_launcher.py
    ;;
  *)
    echo "[comet-worker] unsupported boot mode: ${boot_mode}" >&2
    exit 1
    ;;
esac
