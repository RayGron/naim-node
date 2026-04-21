#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run-plane.sh [plane-name] [--desired-state <path>] [--controller-url <url>] [--artifacts-root <path>] [--wait-seconds <n>] [--cpu] [--no-wait]

Loads a plane desired-state into the local controller, starts the plane,
and waits until interaction/status reports ready.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

plane_name="v2-llama-rpc-backend"
desired_state_path=""
controller_url="http://127.0.0.1:18080"
artifacts_root="/var/lib/naim-node/artifacts"
wait_seconds="900"
force_cpu="no"
wait_for_ready="yes"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --controller-url)
      controller_url="${2:-}"
      shift 2
      ;;
    --desired-state)
      desired_state_path="${2:-}"
      shift 2
      ;;
    --artifacts-root)
      artifacts_root="${2:-}"
      shift 2
      ;;
    --wait-seconds)
      wait_seconds="${2:-}"
      shift 2
      ;;
    --cpu)
      force_cpu="yes"
      shift
      ;;
    --no-wait)
      wait_for_ready="no"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
    *)
      plane_name="$1"
      shift
      ;;
  esac
done

if [[ -z "${desired_state_path}" ]]; then
  desired_state_path="${repo_root}/config/${plane_name}/desired-state.v2.json"
fi
if [[ ! -f "${desired_state_path}" ]]; then
  sibling_app_desired_state="${repo_root}/../${plane_name}/deploy/naim-node/desired-state.v2.json"
  if [[ -f "${sibling_app_desired_state}" ]]; then
    desired_state_path="${sibling_app_desired_state}"
  fi
fi
if [[ ! -f "${desired_state_path}" ]]; then
  echo "error: desired state not found: ${desired_state_path}" >&2
  exit 1
fi

config_summary="$("${repo_root}/scripts/naim-devtool.sh" config-summary --config "${repo_root}/config/naim-node-config.json")"
storage_root="$(printf '%s\n' "${config_summary}" | sed -n '1p')"
model_cache_root="$(printf '%s\n' "${config_summary}" | sed -n '2p')"

wait_for_http() {
  local url="$1"
  local attempts="$2"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

resolve_docker() {
  if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    echo "docker"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n docker version >/dev/null 2>&1; then
    echo "sudo -n docker"
    return 0
  fi
  echo ""
}

resolve_sudo_prefix() {
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    echo "sudo -n"
    return 0
  fi
  echo ""
}

hostd_report_observation() {
  local sudo_prefix="$1"
  local hostd_bin="${NAIM_HOSTD_BIN:-$("${repo_root}/scripts/print-build-dir.sh")/naim-hostd}"
  local db_path="${NAIM_NODE_CONTROLLER_DB:-/var/lib/naim-node/controller.sqlite}"
  local node_name="${NAIM_NODE_NAME:-local-hostd}"
  local state_root="${NAIM_NODE_HOSTD_STATE_ROOT:-/var/lib/naim-node/hostd-state}"
  if [[ ! -x "${hostd_bin}" || ! -f "${db_path}" ]]; then
    return 0
  fi
  if [[ -n "${sudo_prefix}" ]]; then
    ${sudo_prefix} "${hostd_bin}" report-observed-state \
      --db "${db_path}" \
      --node "${node_name}" \
      --state-root "${state_root}" >/dev/null 2>&1 || true
    return 0
  fi
  "${hostd_bin}" report-observed-state \
    --db "${db_path}" \
    --node "${node_name}" \
    --state-root "${state_root}" >/dev/null 2>&1 || true
}

download_to_cache_if_needed() {
  local source_url="$1"
  local target_path="$2"

  mkdir -p "$(dirname -- "${target_path}")"
  if [[ -s "${target_path}" ]]; then
    echo "[run-plane] reusing cached model ${target_path}"
    return
  fi

  echo "[run-plane] downloading model to shared cache ${target_path}"
  curl -fL --progress-bar --output "${target_path}.part" "${source_url}"
  mv "${target_path}.part" "${target_path}"
}

tmp_dir="$(mktemp -d "${repo_root}/var/run-plane.XXXXXX")"
sudo_prefix="$(resolve_sudo_prefix)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

echo "[run-plane] waiting for controller at ${controller_url}"
if ! wait_for_http "${controller_url}/health" 60; then
  echo "error: controller is not reachable at ${controller_url}" >&2
  exit 1
fi

"${repo_root}/scripts/naim-devtool.sh" prepare-run-plane-state \
  --input "${desired_state_path}" \
  --output "${tmp_dir}/desired-state.runtime.json" \
  --model-cache-root "${model_cache_root}" \
  --plane-name "${plane_name}" \
  --force-cpu "${force_cpu}"

bootstrap_local_path="$("${repo_root}/scripts/naim-devtool.sh" run-plane-fields --state "${tmp_dir}/desired-state.runtime.json")"
bootstrap_cached_path="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '1p')"
bootstrap_is_multipart="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '2p')"
gateway_port="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '3p')"
runtime_engine="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '4p')"
bootstrap_model_id="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '5p')"
bootstrap_source_url="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '6p')"

if [[ "${bootstrap_is_multipart}" != "yes" ]] && [[ -n "${bootstrap_cached_path}" ]]; then
  if [[ -n "${bootstrap_source_url}" ]]; then
    download_to_cache_if_needed "${bootstrap_source_url}" "${bootstrap_cached_path}"
  fi
fi

payload_path="${tmp_dir}/apply-plane.json"
"${repo_root}/scripts/naim-devtool.sh" write-apply-payload \
  --desired-state "${tmp_dir}/desired-state.runtime.json" \
  --output "${payload_path}" \
  --artifacts-root "${artifacts_root}"

echo "[run-plane] applying ${plane_name}"
curl -fsS \
  -H 'Content-Type: application/json' \
  -X POST \
  --data-binary "@${payload_path}" \
  "${controller_url}/api/v1/planes" >/dev/null

echo "[run-plane] starting ${plane_name}"
curl -fsS \
  -X POST \
  "${controller_url}/api/v1/planes/${plane_name}/start" >/dev/null

if [[ "${wait_for_ready}" != "yes" ]]; then
  echo "plane=${plane_name}"
  echo "started=yes"
  echo "controller_url=${controller_url}"
  if [[ -n "${gateway_port}" ]]; then
    echo "gateway_url=http://127.0.0.1:${gateway_port}"
  fi
  echo "storage_root=${storage_root}"
  echo "model_cache_root=${model_cache_root}"
  if [[ "${force_cpu}" == "yes" ]]; then
    echo "runtime_mode=cpu"
  fi
  exit 0
fi

echo "[run-plane] waiting for ${plane_name} readiness"
deadline=$((SECONDS + wait_seconds))
status_payload=""
while (( SECONDS < deadline )); do
  hostd_report_observation "${sudo_prefix}"
  if status_payload="$(curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/status" 2>/dev/null)"; then
    ready="$(
      jq -r 'if .ready then "yes" else "no" end' <<<"${status_payload}"
)"
    if [[ "${ready}" == "yes" ]]; then
      break
    fi
  fi
  sleep 2
done

if [[ -z "${status_payload}" ]]; then
  echo "error: failed to read interaction status for ${plane_name}" >&2
  exit 1
fi

ready="$(
  jq -r 'if .ready then "yes" else "no" end' <<<"${status_payload}"
)"
if [[ "${ready}" != "yes" ]]; then
  echo "error: plane ${plane_name} did not become ready within ${wait_seconds}s" >&2
  printf '%s\n' "${status_payload}" >&2
  exit 1
fi

hostd_report_observation "${sudo_prefix}"
models_payload="$(curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/models")"

echo "plane=${plane_name}"
echo "ready=yes"
echo "controller_url=${controller_url}"
if [[ -n "${gateway_port}" ]]; then
  echo "gateway_url=http://127.0.0.1:${gateway_port}"
fi
echo "storage_root=${storage_root}"
echo "model_cache_root=${model_cache_root}"
if [[ "${force_cpu}" == "yes" ]]; then
  echo "runtime_mode=cpu"
fi
echo "models=${models_payload}"
