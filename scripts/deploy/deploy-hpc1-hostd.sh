#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-production-env.sh"

skip_pull="no"
inventory_path=""
worker_name=""

usage() {
  cat <<'EOF'
Usage:
  deploy-hpc1-hostd.sh [--inventory <path>] [--worker <name>] [--skip-pull]

Deploys a worker host agent as a Docker container. The hostd container connects
outbound to the controller, reports telemetry, and applies plane assignments on
the worker through the host Docker socket.
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
    --worker)
      worker_name="${2:-}"
      if [[ -z "${worker_name}" ]]; then
        echo "error: --worker requires a name" >&2
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
  eval "$(naim_load_inventory_env "${inventory_path}" "${worker_name}")"
fi

hostd_image="$(naim_image hostd)"
main_controller_local_url="http://127.0.0.1:${NAIM_MAIN_CONTROLLER_LOCAL_PORT}"

log_prefix="[deploy-worker-hostd:${NAIM_HOSTD_NODE}]"

echo "${log_prefix} preparing worker directories"
remote_hpc1_bash \
  "${NAIM_HOSTD_ROOT}" \
  "${NAIM_HOSTD_SHARED_ROOT}" <<'REMOTE'
set -euo pipefail
hostd_root="$1"
shared_root="$2"

sudo install -d -m 0700 -o "$(id -un)" -g "$(id -gn)" \
  "${hostd_root}" \
  "${hostd_root}/install-state"
sudo install -d -m 0750 -o "$(id -un)" -g "$(id -gn)" \
  "${hostd_root}/logs" \
  "${hostd_root}/systemd" \
  "${shared_root}" \
  "${shared_root}/artifacts" \
  "${shared_root}/models" \
  "${shared_root}/runtime" \
  "${shared_root}/state" \
  "${shared_root}/storage"
REMOTE

echo "${log_prefix} ensuring worker can pull ${hostd_image}"
compose_skip_pull="${skip_pull}"
if [[ "${skip_pull}" != "yes" ]]; then
  registry_username="$(naim_registry_username)"
  registry_password="$(naim_registry_password)"
  if [[ -n "${registry_username}" && -n "${registry_password}" ]]; then
    registry_q="$(printf '%q' "${NAIM_REGISTRY}")"
    registry_username_q="$(printf '%q' "${registry_username}")"
    hostd_image_q="$(printf '%q' "${hostd_image}")"
    printf '%s' "${registry_password}" | ssh_hpc1 "
      set -euo pipefail
      docker_config=\"\$(mktemp -d)\"
      cleanup() { rm -rf \"\${docker_config}\"; }
      trap cleanup EXIT
      export DOCKER_CONFIG=\"\${docker_config}\"
      if ! login_output=\"\$(docker login ${registry_q} -u ${registry_username_q} --password-stdin 2>&1 >/dev/null)\"; then
        printf '%s\n' \"\${login_output}\" >&2
        exit 1
      fi
      docker pull ${hostd_image_q} >/dev/null
    "
  else
    ssh_hpc1 "docker pull '${hostd_image}' >/dev/null"
  fi
  unset registry_username registry_password
  compose_skip_pull="yes"
fi

echo "${log_prefix} provisioning host record"
host_key_exists="$(
  remote_hpc1_bash "${NAIM_HOSTD_ROOT}" <<'REMOTE'
set -euo pipefail
hostd_root="$1"
if [[ -f "${hostd_root}/install-state/keys/hostd.key.b64" ]]; then
  echo yes
else
  echo no
fi
REMOTE
)"
onboarding_key="$(
  remote_main_bash \
    "${main_controller_local_url}" \
    "${NAIM_HOSTD_NODE}" \
    "${host_key_exists}" <<'REMOTE'
set -euo pipefail
controller_url="$1"
node_name="$2"
host_key_exists="$3"

sudo python3 - "${controller_url}" "${node_name}" "${host_key_exists}" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

controller_url, node_name, host_key_exists = sys.argv[1:4]

def load_host():
    url = controller_url + "/api/v1/hostd/hosts?node=" + urllib.parse.quote(node_name)
    with urllib.request.urlopen(url) as response:
        payload = json.load(response)
    items = payload.get("items", [])
    return items[0] if items else None

def create_host():
    request = urllib.request.Request(
        controller_url + "/api/v1/hostd/hosts",
        data=json.dumps({"node_name": node_name}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)

def reset_host(message):
    request = urllib.request.Request(
        controller_url + "/api/v1/hostd/hosts/" + urllib.parse.quote(node_name) + "/reset-onboarding",
        data=json.dumps({"message": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise SystemExit(
            f"failed to reset onboarding for host {node_name}: HTTP {error.code} {detail}"
        )

host = load_host()
if host is None:
    created = create_host()
    print(created.get("onboarding_key", ""))
    raise SystemExit(0)

registration_state = host.get("registration_state", "")
onboarding_state = host.get("onboarding_state", "")

if (
    registration_state == "registered"
    and onboarding_state == "completed"
    and host_key_exists == "yes"
):
    print("")
    raise SystemExit(0)

reset = reset_host("reset onboarding from production deploy")
print(reset.get("onboarding_key", ""))
PY
REMOTE
)"
unset host_key_exists

echo "${log_prefix} preparing host keypair and config"
if [[ -n "${onboarding_key}" ]]; then
  remote_hpc1_bash \
    "${hostd_image}" \
    "${NAIM_HOSTD_ROOT}" \
    "${NAIM_HOSTD_SHARED_ROOT}" \
    "${NAIM_HOSTD_CONTROLLER_URL}" \
    "${NAIM_HOSTD_NODE}" \
    "${NAIM_HOSTD_ENABLE_NVIDIA}" \
    "${onboarding_key}" <<'REMOTE'
set -euo pipefail
hostd_image="$1"
hostd_root="$2"
shared_root="$3"
controller_url="$4"
node_name="$5"
enable_nvidia="$6"
onboarding_key="$7"

docker run --rm \
  -v "${hostd_root}:${hostd_root}" \
  --entrypoint /runtime/bin/naim-node \
  "${hostd_image}" \
  install hostd \
    --controller "${controller_url}" \
    --node "${node_name}" \
    --config "${hostd_root}/naim-node.toml" \
    --state-root "${hostd_root}/install-state" \
    --log-root "${hostd_root}/logs" \
    --systemd-dir "${hostd_root}/systemd" \
    --skip-systemctl \
    --onboarding-key "${onboarding_key}" >/dev/null

perl -0pi -e 's/onboarding_key = "[^"]*"/onboarding_key = ""/' \
  "${hostd_root}/naim-node.toml"

cat > "${hostd_root}/naim-node-config.json" <<JSON
{
  "paths": {
    "storage_root": "${shared_root}/storage",
    "model_cache_root": "${shared_root}/models"
  }
}
JSON

docker_run_args=(
  --rm
  --privileged
  -v /var/run/docker.sock:/var/run/docker.sock
  -v "${hostd_root}:${hostd_root}"
  -v "${shared_root}:${shared_root}"
)
if [[ "${enable_nvidia}" == "yes" ]]; then
  docker_run_args+=(
    --runtime nvidia
    -e NVIDIA_VISIBLE_DEVICES=all
    -e NVIDIA_DRIVER_CAPABILITIES=compute,utility
    -v /usr/bin/nvidia-smi:/usr/bin/nvidia-smi:ro
  )
fi

docker run "${docker_run_args[@]}" \
  --entrypoint /runtime/bin/naim-hostd \
  "${hostd_image}" \
  report-observed-state \
    --controller "${controller_url}" \
    --node "${node_name}" \
    --config "${hostd_root}/naim-node-config.json" \
    --state-root "${shared_root}/state" \
    --host-private-key "${hostd_root}/install-state/keys/hostd.key.b64" \
    --onboarding-key "${onboarding_key}" >/dev/null
REMOTE
else
  remote_hpc1_bash \
    "${NAIM_HOSTD_ROOT}" \
    "${NAIM_HOSTD_SHARED_ROOT}" <<'REMOTE'
set -euo pipefail
hostd_root="$1"
shared_root="$2"

if [[ ! -f "${hostd_root}/install-state/keys/hostd.key.b64" ]]; then
  echo "host is registered without a local private key; rerun deploy against a controller that supports /reset-onboarding" >&2
  exit 1
fi

perl -0pi -e 's/onboarding_key = "[^"]*"/onboarding_key = ""/' \
  "${hostd_root}/naim-node.toml" 2>/dev/null || true

cat > "${hostd_root}/naim-node-config.json" <<JSON
{
  "paths": {
    "storage_root": "${shared_root}/storage",
    "model_cache_root": "${shared_root}/models"
  }
}
JSON
REMOTE
fi
unset onboarding_key

echo "${log_prefix} writing compose and starting hostd"
remote_hpc1_bash \
  "${hostd_image}" \
  "${NAIM_HOSTD_ROOT}" \
  "${NAIM_HOSTD_SHARED_ROOT}" \
  "${NAIM_HOSTD_CONTROLLER_URL}" \
  "${NAIM_HOSTD_NODE}" \
  "${NAIM_HOSTD_POLL_INTERVAL_SEC}" \
  "${NAIM_HOSTD_INVENTORY_SCAN_INTERVAL_SEC}" \
  "${NAIM_HOSTD_ENABLE_NVIDIA}" \
  "${compose_skip_pull}" <<'REMOTE'
set -euo pipefail
hostd_image="$1"
hostd_root="$2"
shared_root="$3"
controller_url="$4"
node_name="$5"
poll_interval_sec="$6"
inventory_scan_interval_sec="$7"
enable_nvidia="$8"
skip_pull="$9"

cd "${hostd_root}"
if [[ -f docker-compose.yml ]]; then
  cp docker-compose.yml "docker-compose.yml.bak-$(date +%Y%m%d-%H%M%S)"
fi

nvidia_runtime=""
nvidia_env=""
nvidia_mount=""
if [[ "${enable_nvidia}" == "yes" ]]; then
  nvidia_runtime="    runtime: nvidia"
  nvidia_env="      NVIDIA_VISIBLE_DEVICES: all
      NVIDIA_DRIVER_CAPABILITIES: compute,utility"
  nvidia_mount="      - /usr/bin/nvidia-smi:/usr/bin/nvidia-smi:ro"
fi

cat > docker-compose.yml <<YAML
services:
  naim-hostd:
    image: ${hostd_image}
    container_name: naim-hostd
    restart: unless-stopped
    privileged: true
    network_mode: host
${nvidia_runtime}
    environment:
      NAIM_NODE_CONFIG_PATH: ${hostd_root}/naim-node-config.json
      NAIM_HOSTD_PEER_ENABLED: "yes"
      NAIM_HOSTD_PEER_PORT: "29999"
      NAIM_HOSTD_DISCOVERY_MODE: udp-multicast
      NAIM_HOSTD_DISCOVERY_GROUP: 239.255.42.42
      NAIM_HOSTD_DISCOVERY_PORT: "29998"
${nvidia_env}
    command:
      - run
      - hostd
      - --foreground
      - --skip-systemctl
      - --controller
      - ${controller_url}
      - --node
      - ${node_name}
      - --config
      - ${hostd_root}/naim-node-config.json
      - --runtime-root
      - ${shared_root}/runtime
      - --state-root
      - ${shared_root}/state
      - --host-private-key
      - ${hostd_root}/install-state/keys/hostd.key.b64
      - --compose-mode
      - exec
      - --poll-interval-sec
      - "${poll_interval_sec}"
      - --inventory-scan-interval-sec
      - "${inventory_scan_interval_sec}"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
${nvidia_mount}
      - ${hostd_root}:${hostd_root}
      - ${shared_root}:${shared_root}
YAML

sudo chmod 0700 "${hostd_root}" "${hostd_root}/install-state"
if [[ -d "${hostd_root}/install-state/keys" ]]; then
  sudo chmod 0700 "${hostd_root}/install-state/keys"
fi
if [[ -f "${hostd_root}/install-state/keys/hostd.key.b64" ]]; then
  sudo chmod 0600 "${hostd_root}/install-state/keys/hostd.key.b64"
fi
for config_path in "${hostd_root}/naim-node.toml" "${hostd_root}/naim-node-config.json"; do
  if [[ -f "${config_path}" ]]; then
    sudo chmod 0640 "${config_path}"
  fi
done

rm -f "${hostd_root}/.env"
if [[ "${skip_pull}" != "yes" ]]; then
  docker compose pull
fi
docker compose up -d --remove-orphans

if [[ "${enable_nvidia}" == "yes" ]]; then
  docker exec naim-hostd nvidia-smi --query-gpu=index,memory.total --format=csv,noheader >/dev/null
fi
REMOTE

echo "${log_prefix} waiting for controller to see ${NAIM_HOSTD_NODE}"
remote_main_bash \
  "${main_controller_local_url}" \
  "${NAIM_HOSTD_NODE}" <<'REMOTE'
set -euo pipefail
controller_url="$1"
node_name="$2"

python3 - "${controller_url}" "${node_name}" <<'PY'
import json
import sys
import time
import urllib.parse
import urllib.request

controller_url, node_name = sys.argv[1:3]
url = controller_url + "/api/v1/hostd/hosts?node=" + urllib.parse.quote(node_name)
deadline = time.time() + 60
last = None
while time.time() < deadline:
    with urllib.request.urlopen(url) as response:
        payload = json.load(response)
    items = payload.get("items", [])
    if items:
        last = items[0]
        if (
            last.get("registration_state") == "registered"
            and last.get("onboarding_state") == "completed"
            and last.get("session_state") == "connected"
            and last.get("role_eligible") is True
        ):
            print(json.dumps({
                "node_name": last.get("node_name"),
                "session_state": last.get("session_state"),
                "derived_role": last.get("derived_role"),
                "role_eligible": last.get("role_eligible"),
                "capacity_summary": last.get("capacity_summary"),
            }, sort_keys=True))
            raise SystemExit(0)
    time.sleep(2)
raise SystemExit(node_name + " did not become connected and eligible; last=" + json.dumps(last))
PY
REMOTE

echo "${NAIM_HOSTD_NODE} hostd deployed"
