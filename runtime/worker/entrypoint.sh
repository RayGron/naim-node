#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp
echo "[comet-worker] booting plane=${COMET_PLANE_NAME:-unknown} instance=${COMET_INSTANCE_NAME:-unknown}"
echo "[comet-worker] control_root=${COMET_CONTROL_ROOT:-unknown}"
exec /runtime/bin/comet-workerd
