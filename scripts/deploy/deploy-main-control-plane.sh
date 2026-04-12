#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-production-env.sh"

skip_pull="no"
inventory_path=""

usage() {
  cat <<'EOF'
Usage:
  deploy-main-control-plane.sh [--inventory <path>] [--skip-pull]

Deploys the NAIM controller, Web UI, and standalone Skills Factory containers
on the main host. The controller is bound to localhost; expose browser access
through nginx or another reverse proxy.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      if [[ -z "${inventory_path}" ]]; then
        echo "error: --inventory requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    --skip-pull)
      skip_pull="yes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "${inventory_path}" ]]; then
  eval "$(naim_load_inventory_env "${inventory_path}")"
fi

controller_image="$(naim_image controller)"
web_ui_image="$(naim_image web-ui)"

remote_main_bash \
  "${NAIM_MAIN_ROOT}" \
  "${controller_image}" \
  "${web_ui_image}" \
  "${NAIM_MAIN_CONTROLLER_LOCAL_PORT}" \
  "${NAIM_MAIN_WEB_UI_LOCAL_PORT}" \
  "${skip_pull}" <<'REMOTE'
set -euo pipefail

main_root="$1"
controller_image="$2"
web_ui_image="$3"
controller_local_port="$4"
web_ui_local_port="$5"
skip_pull="$6"

sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "${main_root}"
sudo install -d -m 0750 -o "$(id -un)" -g "$(id -gn)" \
  "${main_root}/state" \
  "${main_root}/artifacts"
cd "${main_root}"

if [[ -f docker-compose.yml ]]; then
  cp docker-compose.yml "docker-compose.yml.bak-$(date +%Y%m%d-%H%M%S)"
fi

cat > docker-compose.yml <<YAML
services:
  naim-controller:
    image: ${controller_image}
    container_name: naim-controller
    restart: unless-stopped
    command:
      - serve
      - --db
      - /naim/state/controller.sqlite
      - --artifacts-root
      - /naim/artifacts
      - --listen-host
      - 0.0.0.0
      - --listen-port
      - "18080"
      - --skills-factory-upstream
      - http://naim-skills-factory:18082
    ports:
      - "127.0.0.1:${controller_local_port}:18080"
    volumes:
      - ${main_root}/state:/naim/state
      - ${main_root}/artifacts:/naim/artifacts
    networks:
      - naim-control

  naim-skills-factory:
    image: ${controller_image}
    container_name: naim-skills-factory
    restart: unless-stopped
    command:
      - serve-skills-factory
      - --db
      - /naim/state/controller.sqlite
      - --artifacts-root
      - /naim/artifacts
      - --listen-host
      - 0.0.0.0
      - --listen-port
      - "18082"
    volumes:
      - ${main_root}/state:/naim/state
      - ${main_root}/artifacts:/naim/artifacts
    networks:
      - naim-control

  naim-web-ui:
    image: ${web_ui_image}
    container_name: naim-web-ui
    restart: unless-stopped
    environment:
      NAIM_CONTROLLER_UPSTREAM: http://naim-controller:18080
    ports:
      - "127.0.0.1:${web_ui_local_port}:8080"
    depends_on:
      - naim-controller
    networks:
      - naim-control

networks:
  naim-control:
    name: naim-control
YAML

if [[ ! -f "${main_root}/state/controller.sqlite" ]]; then
  docker run --rm \
    -v "${main_root}/state:/naim/state" \
    "${controller_image}" \
    init-db --db /naim/state/controller.sqlite
fi
if [[ -f "${main_root}/state/controller.sqlite" ]]; then
  sudo chmod 0640 "${main_root}/state/controller.sqlite"
fi

if [[ "${skip_pull}" != "yes" ]]; then
  docker compose pull
fi
docker compose up -d --remove-orphans

wait_for_http() {
  local url="$1"
  local attempts="${2:-30}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${url}" >&2
  return 1
}

wait_for_http "http://127.0.0.1:${controller_local_port}/health" 45
wait_for_http "http://127.0.0.1:${web_ui_local_port}/health" 45

docker exec naim-skills-factory bash -lc \
  'exec 3<>/dev/tcp/127.0.0.1/18082
   printf "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" >&3
   response="$(cat <&3)"
   grep -F "\"service\":\"naim-skills-factory\"" <<<"${response}" >/dev/null'

echo "main control plane deployed"
echo "controller_local=http://127.0.0.1:${controller_local_port}"
echo "web_ui_local=http://127.0.0.1:${web_ui_local_port}"
REMOTE
