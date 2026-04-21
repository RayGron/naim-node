#!/usr/bin/env bash
set -euo pipefail

status_path="${NAIM_WEBGATEWAY_RUNTIME_STATUS_PATH:-${NAIM_BROWSING_RUNTIME_STATUS_PATH:-/naim/private/webgateway-runtime-status.json}}"
state_root="${NAIM_WEBGATEWAY_STATE_ROOT:-${NAIM_BROWSING_STATE_ROOT:-/naim/private/sessions}}"

mkdir -p "$(dirname "${status_path}")"
mkdir -p "${state_root}"

exec /runtime/bin/naim-webgatewayd
