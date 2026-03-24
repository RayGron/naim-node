#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"

skip_build=0
skip_image_build=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) skip_build=1 ;;
    --skip-image-build) skip_image_build=1 ;;
    *)
      echo "usage: $0 [--skip-build] [--skip-image-build]" >&2
      exit 1
      ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug >/dev/null
fi

require_image() {
  local image="$1"
  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "phase-l-live: required image is missing: ${image}" >&2
    echo "phase-l-live: rerun without --skip-image-build or prebuild runtime images" >&2
    exit 1
  fi
}

if [[ "${skip_image_build}" -eq 0 ]]; then
  docker build -f "${PWD}/runtime/web-ui/Dockerfile" -t comet/web-ui:dev "${PWD}" >/dev/null
else
  require_image "comet/web-ui:dev"
fi

wait_for_http() {
  local url="$1"
  local attempts="${2:-150}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_for_match() {
  local attempts="$1"
  local needle="$2"
  shift 2
  for _ in $(seq 1 "${attempts}"); do
    if "$@" | grep -F "${needle}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

next_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

base="${PWD}/var/live-phase-l"
install_root="${base}/install"
state_root="${install_root}/var/lib/comet-node"
db_path="${state_root}/controller.sqlite"
web_ui_root="${state_root}/web-ui"
controller_pid=""

cleanup() {
  if [[ -n "${controller_pid}" ]]; then
    kill "${controller_pid}" >/dev/null 2>&1 || true
    wait "${controller_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -f "${db_path}" ]]; then
    "${build_dir}/comet-controller" stop-web-ui \
      --db "${db_path}" \
      --web-ui-root "${web_ui_root}" \
      --compose-mode exec >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cmake -E remove_directory "${base}"

controller_port="$(next_port)"
run_log="/tmp/comet-phase-l-run.log"

echo "phase-l-live: install controller"
install_output="$(
  COMET_INSTALL_ROOT="${install_root}" \
  "${build_dir}/comet-node" install controller \
    --with-hostd \
    --with-web-ui \
    --node node-a \
    --listen-port "${controller_port}" \
    --skip-systemctl
)"
printf '%s' "${install_output}" | grep -F 'installed controller' >/dev/null
printf '%s' "${install_output}" | grep -F "controller_api_url=http://127.0.0.1:${controller_port}" >/dev/null

echo "phase-l-live: start platform"
COMET_INSTALL_ROOT="${install_root}" \
  "${build_dir}/comet-node" run controller \
  --hostd-compose-mode skip \
  --poll-interval-sec 1 >"${run_log}" 2>&1 &
controller_pid="$!"

wait_for_http "http://127.0.0.1:${controller_port}/health"
wait_for_http "http://127.0.0.1:18081/health"

echo "phase-l-live: web ui reachable"
curl -fsS "http://127.0.0.1:18081/" | grep -F 'Comet Operator' >/dev/null
curl -fsS "http://127.0.0.1:18081/api/v1/planes" | grep -F '"items":[]' >/dev/null

echo "phase-l-live: load first plane through web ui path"
curl -fsS -X POST \
  "http://127.0.0.1:18081/api/v1/bundles/preview?bundle=${PWD}/config/demo-plane" \
  | grep -F '"action":"preview-bundle"' >/dev/null
curl -fsS -X POST \
  "http://127.0.0.1:18081/api/v1/bundles/apply?bundle=${PWD}/config/demo-plane" \
  | grep -F '"action":"apply-bundle"' >/dev/null

wait_for_match 80 '"name":"alpha"' curl -fsS "http://127.0.0.1:18081/api/v1/planes"
wait_for_match 80 '"session_state": "connected"' \
  "${build_dir}/comet-controller" show-hostd-hosts --db "${db_path}" --node node-a
for _ in $(seq 1 80); do
  observations="$("${build_dir}/comet-controller" show-host-observations --db "${db_path}" --node node-a)"
  if printf '%s' "${observations}" | grep -F 'applied_generation=1' >/dev/null 2>&1; then
    break
  fi
  if printf '%s' "${observations}" | grep -F 'message=manual heartbeat' >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! printf '%s' "${observations}" | grep -F 'applied_generation=1' >/dev/null 2>&1; then
  printf '%s' "${observations}" | grep -F 'message=manual heartbeat' >/dev/null
fi
for _ in $(seq 1 80); do
  local_state="$("${build_dir}/comet-hostd" show-local-state --node node-a --state-root "${state_root}/hostd-state")"
  if printf '%s' "${local_state}" | grep -E 'instances=[1-9][0-9]*' >/dev/null 2>&1; then
    break
  fi
  if printf '%s' "${local_state}" | grep -F 'state: empty' >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! printf '%s' "${local_state}" | grep -E 'instances=[1-9][0-9]*' >/dev/null 2>&1; then
  printf '%s' "${local_state}" | grep -F 'state: empty' >/dev/null
fi

echo "phase-l-live: OK"
