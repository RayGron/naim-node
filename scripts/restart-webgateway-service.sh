#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <plane-name> <node-name> [artifacts-root] [service-name]" >&2
  exit 1
fi

plane_name="$1"
node_name="$2"
artifacts_root="${3:-}"
service_name="${4:-webgateway-${plane_name}}"

if [[ -z "${artifacts_root}" ]]; then
  if [[ -d "/var/lib/comet-node/hostd-state/artifacts" ]]; then
    artifacts_root="/var/lib/comet-node/hostd-state/artifacts"
  else
    artifacts_root="/var/lib/comet-node/artifacts"
  fi
fi

compose_file="${artifacts_root}/${plane_name}/${node_name}/docker-compose.yml"

declare -a docker_cmd

resolve_docker() {
  if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    docker_cmd=(docker)
    return 0
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n docker version >/dev/null 2>&1; then
    docker_cmd=(sudo -n docker)
    return 0
  fi

  local windows_docker="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [[ -x "${windows_docker}" ]] && "${windows_docker}" version >/dev/null 2>&1; then
    docker_cmd=("${windows_docker}")
    return 0
  fi

  echo "error: no working Docker CLI found" >&2
  return 1
}

if [[ ! -f "${compose_file}" ]]; then
  echo "error: compose file not found: ${compose_file}" >&2
  exit 1
fi

resolve_docker

"${docker_cmd[@]}" compose -f "${compose_file}" up -d --no-deps --force-recreate "${service_name}"
"${docker_cmd[@]}" compose -f "${compose_file}" ps "${service_name}"
