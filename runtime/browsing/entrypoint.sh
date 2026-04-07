#!/usr/bin/env bash
set -euo pipefail

status_path="${COMET_WEBGATEWAY_RUNTIME_STATUS_PATH:-${COMET_BROWSING_RUNTIME_STATUS_PATH:-/comet/private/webgateway-runtime-status.json}}"
state_root="${COMET_WEBGATEWAY_STATE_ROOT:-${COMET_BROWSING_STATE_ROOT:-/comet/private/sessions}}"

mkdir -p "$(dirname "${status_path}")"
mkdir -p "${state_root}"

exec /runtime/bin/comet-webgatewayd
