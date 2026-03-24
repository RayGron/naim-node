#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")

if [[ "${host_os}" != "macos" ]]; then
  echo "error: check-macos.sh must run on macOS hosts" >&2
  exit 1
fi

build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"
launcher_root="${PWD}/var/check-macos-launcher"

cleanup() {
  cmake -E remove_directory "${launcher_root}"
}
trap cleanup EXIT

next_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

cleanup

echo "macos-check: build debug"
"${script_dir}/build-target.sh" "${host_os}" "${host_arch}" Debug >/dev/null

echo "macos-check: launcher smoke"
"${build_dir}/comet-node" doctor controller | grep -F 'controller_binary=yes' >/dev/null
"${build_dir}/comet-node" doctor controller | grep -F 'docker=yes' >/dev/null

launcher_port="$(next_port)"
launcher_output="$(
  COMET_INSTALL_ROOT="${launcher_root}" \
    "${build_dir}/comet-node" install controller \
      --with-hostd \
      --with-web-ui \
      --listen-port "${launcher_port}" \
      --skip-systemctl
)"
printf '%s' "${launcher_output}" | grep -F 'installed controller' >/dev/null
printf '%s' "${launcher_output}" | grep -F "controller_api_url=http://127.0.0.1:${launcher_port}" >/dev/null
COMET_INSTALL_ROOT="${launcher_root}" \
  "${build_dir}/comet-node" service verify controller-hostd --skip-systemctl >/dev/null

echo "macos-check: live phase l"
"${script_dir}/check-live-phase-l.sh" --skip-build "$@"

echo "macos-check: OK"
