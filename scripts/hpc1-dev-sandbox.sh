#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

remote_host="${NAIM_HPC1_HOST:-baal@195.168.22.186}"
remote_port="${NAIM_HPC1_PORT:-2222}"
sandbox_name="${NAIM_HPC1_SANDBOX_NAME:-naim-node-dev}"
remote_base="${NAIM_HPC1_SANDBOX_BASE:-/mnt/shared-storage/backups/baal/dev-sandboxes}"
remote_root="${NAIM_HPC1_SANDBOX_ROOT:-${remote_base}/${sandbox_name}}"
remote_repo="${remote_root}/repo"
remote_build_root="${remote_root}/build-root"
remote_state_root="${remote_root}/state"
remote_logs_root="${remote_root}/logs"
remote_env_file="${remote_root}/sandbox.env"

controller_db="${remote_state_root}/controller.sqlite"
artifacts_root="${remote_state_root}/artifacts"
runtime_root="${remote_state_root}/runtime"
hostd_state_root="${remote_state_root}/hostd-state"

controller_port="${NAIM_HPC1_CONTROLLER_PORT:-28081}"
skills_port="${NAIM_HPC1_SKILLS_PORT:-28082}"
web_ui_port="${NAIM_HPC1_WEB_UI_PORT:-28083}"

hostd_node="${NAIM_HPC1_HOSTD_NODE:-local-hostd}"
hostd_prepare_mode="${NAIM_HPC1_HOSTD_PREPARE_MODE:-onboarding}"
hostd_poll_interval_sec="${NAIM_HPC1_HOSTD_POLL_INTERVAL_SEC:-2}"
hostd_compose_mode="${NAIM_HPC1_HOSTD_COMPOSE_MODE:-skip}"
hostd_advertised_address="${NAIM_HPC1_HOSTD_ADDRESS:-http://127.0.0.1:29999}"
runtime_smoke_bundle="${NAIM_HPC1_RUNTIME_SMOKE_BUNDLE:-config/v2-gpu-worker/desired-state.v2.json}"

remote_hostd_install_root="${remote_root}/install/hostd"
remote_hostd_config_path="${remote_hostd_install_root}/etc/naim-node/config.toml"
remote_hostd_layout_state_root="${remote_hostd_install_root}/var/lib/naim-node"
remote_hostd_layout_log_root="${remote_hostd_install_root}/var/log/naim-node"
remote_hostd_layout_systemd_dir="${remote_hostd_install_root}/etc/systemd/system"
remote_hostd_private_key_path="${remote_hostd_layout_state_root}/keys/hostd.key.b64"
remote_hostd_public_key_path="${remote_hostd_layout_state_root}/keys/hostd.pub.b64"

ssh_base=(ssh "${remote_host}" -p "${remote_port}")
rsync_rsh="ssh -p ${remote_port}"

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [args]

Commands:
  paths                         Show local/remote sandbox paths and ports
  setup                         Create the isolated hpc1 sandbox directories and metadata
  sync                          Sync the local repo into the remote sandbox checkout
  build [targets...]            Sync and build remotely in an isolated NAIM_BUILD_ROOT
  controller-start              Start an isolated controller + skills-factory smoke on hpc1
  controller-stop               Stop the isolated controller + skills-factory smoke
  hostd-prepare                 Create sandbox hostd layout/config and register the sandbox host
  hostd-start                   Start a background sandbox hostd poll loop against the sandbox controller
  hostd-stop                    Stop the background sandbox hostd poll loop
  runtime-smoke [state-or-dir]  Apply/start a v2 plane and verify hostd plane realization in the sandbox
  status                        Show sandbox status, ports, host registration, and recent logs
  shell                         Open an interactive shell inside the remote sandbox repo

Environment overrides:
  NAIM_HPC1_HOST
  NAIM_HPC1_PORT
  NAIM_HPC1_SANDBOX_NAME
  NAIM_HPC1_SANDBOX_BASE
  NAIM_HPC1_SANDBOX_ROOT
  NAIM_HPC1_CONTROLLER_PORT
  NAIM_HPC1_SKILLS_PORT
  NAIM_HPC1_WEB_UI_PORT
  NAIM_HPC1_HOSTD_NODE
  NAIM_HPC1_HOSTD_PREPARE_MODE
  NAIM_HPC1_HOSTD_POLL_INTERVAL_SEC
  NAIM_HPC1_HOSTD_COMPOSE_MODE
  NAIM_HPC1_HOSTD_ADDRESS
  NAIM_HPC1_RUNTIME_SMOKE_BUNDLE
EOF
}

remote_bash() {
  local script="$1"
  shift || true
  "${ssh_base[@]}" 'bash -s' -- "$@" <<EOF
set -euo pipefail
${script}
EOF
}

print_paths() {
  cat <<EOF
remote_host=${remote_host}
remote_port=${remote_port}
sandbox_name=${sandbox_name}
remote_root=${remote_root}
remote_repo=${remote_repo}
remote_build_root=${remote_build_root}
remote_state_root=${remote_state_root}
remote_logs_root=${remote_logs_root}
controller_db=${controller_db}
artifacts_root=${artifacts_root}
runtime_root=${runtime_root}
hostd_state_root=${hostd_state_root}
controller_port=${controller_port}
skills_port=${skills_port}
web_ui_port=${web_ui_port}
hostd_node=${hostd_node}
hostd_prepare_mode=${hostd_prepare_mode}
hostd_compose_mode=${hostd_compose_mode}
hostd_poll_interval_sec=${hostd_poll_interval_sec}
runtime_smoke_bundle=${runtime_smoke_bundle}
remote_hostd_install_root=${remote_hostd_install_root}
remote_hostd_config_path=${remote_hostd_config_path}
remote_hostd_layout_state_root=${remote_hostd_layout_state_root}
remote_hostd_layout_log_root=${remote_hostd_layout_log_root}
EOF
}

setup_remote() {
  remote_bash '
remote_root="$1"
remote_repo="$2"
remote_build_root="$3"
remote_state_root="$4"
remote_logs_root="$5"
remote_env_file="$6"
controller_db="$7"
artifacts_root="$8"
controller_port="$9"
skills_port="${10}"
web_ui_port="${11}"
runtime_root="${12}"
hostd_state_root="${13}"
remote_hostd_install_root="${14}"
remote_hostd_config_path="${15}"
remote_hostd_layout_state_root="${16}"
remote_hostd_layout_log_root="${17}"
remote_hostd_layout_systemd_dir="${18}"
hostd_node="${19}"
hostd_poll_interval_sec="${20}"
hostd_compose_mode="${21}"

mkdir -p \
  "$remote_repo" \
  "$remote_build_root" \
  "$remote_state_root" \
  "$remote_logs_root" \
  "$artifacts_root" \
  "$runtime_root" \
  "$hostd_state_root" \
  "$remote_hostd_install_root" \
  "$remote_hostd_layout_state_root" \
  "$remote_hostd_layout_log_root" \
  "$remote_hostd_layout_systemd_dir"

cat > "$remote_env_file" <<ENV
export NAIM_HPC1_SANDBOX_ROOT="$remote_root"
export NAIM_HPC1_REPO="$remote_repo"
export NAIM_BUILD_ROOT="$remote_build_root"
export NAIM_HPC1_STATE_ROOT="$remote_state_root"
export NAIM_HPC1_LOG_ROOT="$remote_logs_root"
export NAIM_HPC1_CONTROLLER_DB="$controller_db"
export NAIM_HPC1_ARTIFACTS_ROOT="$artifacts_root"
export NAIM_HPC1_RUNTIME_ROOT="$runtime_root"
export NAIM_HPC1_HOSTD_STATE_ROOT="$hostd_state_root"
export NAIM_HPC1_CONTROLLER_PORT="$controller_port"
export NAIM_HPC1_SKILLS_PORT="$skills_port"
export NAIM_HPC1_WEB_UI_PORT="$web_ui_port"
export NAIM_HPC1_HOSTD_NODE="$hostd_node"
export NAIM_HPC1_HOSTD_CONFIG="$remote_hostd_config_path"
export NAIM_HPC1_HOSTD_LAYOUT_STATE_ROOT="$remote_hostd_layout_state_root"
export NAIM_HPC1_HOSTD_LAYOUT_LOG_ROOT="$remote_hostd_layout_log_root"
export NAIM_HPC1_HOSTD_LAYOUT_SYSTEMD_DIR="$remote_hostd_layout_systemd_dir"
export NAIM_HPC1_HOSTD_POLL_INTERVAL_SEC="$hostd_poll_interval_sec"
export NAIM_HPC1_HOSTD_COMPOSE_MODE="$hostd_compose_mode"
ENV

cat > "$remote_root/README.txt" <<TXT
This is the isolated naim-node development sandbox for hpc1.

It is intentionally separate from the active production-like install:
  /var/lib/naim-node
  naim-node-controller.service
  naim-node-hostd.service

Do not point sandbox commands at /var/lib/naim-node or production ports.
TXT

python3 - "$controller_port" "$skills_port" "$web_ui_port" <<PY
import socket
import sys
for raw in sys.argv[1:]:
    port = int(raw)
    s = socket.socket()
    s.settimeout(0.2)
    try:
        s.connect(("127.0.0.1", port))
    except Exception:
        print(f"port {port}: free")
    else:
        print(f"port {port}: already in use")
    finally:
        s.close()
PY
' \
    "${remote_root}" \
    "${remote_repo}" \
    "${remote_build_root}" \
    "${remote_state_root}" \
    "${remote_logs_root}" \
    "${remote_env_file}" \
    "${controller_db}" \
    "${artifacts_root}" \
    "${controller_port}" \
    "${skills_port}" \
    "${web_ui_port}" \
    "${runtime_root}" \
    "${hostd_state_root}" \
    "${remote_hostd_install_root}" \
    "${remote_hostd_config_path}" \
    "${remote_hostd_layout_state_root}" \
    "${remote_hostd_layout_log_root}" \
    "${remote_hostd_layout_systemd_dir}" \
    "${hostd_node}" \
    "${hostd_poll_interval_sec}" \
    "${hostd_compose_mode}"
}

sync_remote() {
  setup_remote
  rsync -az --delete \
    --exclude '.git/' \
    --exclude 'build/' \
    --exclude 'var/' \
    --exclude '.codex/' \
    --exclude '.idea/' \
    --exclude '.vscode/' \
    --exclude 'ui/operator-react/node_modules/' \
    --exclude 'node_modules/' \
    -e "${rsync_rsh}" \
    "${repo_root}/" "${remote_host}:${remote_repo}/"
}

build_remote() {
  local targets=("$@")
  sync_remote
  if [[ "${#targets[@]}" -eq 0 ]]; then
    remote_bash '
remote_repo="$1"
remote_build_root="$2"
cd "$remote_repo"
NAIM_BUILD_ROOT="$remote_build_root" ./scripts/build-target.sh linux x64 Debug
' "${remote_repo}" "${remote_build_root}"
    return
  fi

  remote_bash '
remote_repo="$1"
remote_build_root="$2"
shift 2
cd "$remote_repo"
NAIM_BUILD_ROOT="$remote_build_root" ./scripts/configure-build.sh linux x64 Debug >/dev/null
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64)"
cmake --build "$build_dir" --target "$@"
' "${remote_repo}" "${remote_build_root}" "${targets[@]}"
}

start_controller() {
  remote_bash '
remote_repo="$1"
remote_build_root="$2"
remote_logs_root="$3"
controller_db="$4"
artifacts_root="$5"
controller_port="$6"
skills_port="$7"

mkdir -p "$remote_logs_root" "$artifacts_root"
cd "$remote_repo"
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64)"
controller_bin="$build_dir/naim-controller"

if [[ ! -x "$controller_bin" ]]; then
  echo "missing controller binary: $controller_bin" >&2
  echo "run ./scripts/hpc1-dev-sandbox.sh build first" >&2
  exit 1
fi

"$controller_bin" init-db --db "$controller_db" >/dev/null

start_service() {
  local service_name="$1"
  shift
  local pid_file="$remote_logs_root/${service_name}.pid"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "${service_name} already running with pid ${pid}"
      return
    fi
    rm -f "$pid_file"
  fi
  nohup "$@" >"$remote_logs_root/${service_name}.log" 2>&1 &
  echo $! > "$pid_file"
}

start_service skills \
  "$controller_bin" serve-skills-factory \
  --db "$controller_db" \
  --artifacts-root "$artifacts_root" \
  --listen-host 127.0.0.1 \
  --listen-port "$skills_port"

start_service controller \
  "$controller_bin" serve \
  --db "$controller_db" \
  --artifacts-root "$artifacts_root" \
  --listen-host 127.0.0.1 \
  --listen-port "$controller_port" \
  --skills-factory-upstream "http://127.0.0.1:$skills_port"

python3 - "$controller_port" <<PY
import socket
import sys
import time
port = int(sys.argv[1])
deadline = time.time() + 20
while time.time() < deadline:
    s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect(("127.0.0.1", port))
    except Exception:
        time.sleep(0.5)
    else:
        print(f"controller ready on 127.0.0.1:{port}")
        s.close()
        raise SystemExit(0)
    finally:
        try:
            s.close()
        except Exception:
            pass
raise SystemExit("controller did not become ready within 20s")
PY
' \
    "${remote_repo}" \
    "${remote_build_root}" \
    "${remote_logs_root}" \
    "${controller_db}" \
    "${artifacts_root}" \
    "${controller_port}" \
    "${skills_port}"
}

stop_controller() {
  remote_bash '
remote_logs_root="$1"
for service in controller skills; do
  pid_file="$remote_logs_root/${service}.pid"
  if [[ ! -f "$pid_file" ]]; then
    continue
  fi
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$pid_file"
done
' "${remote_logs_root}"
}

prepare_hostd() {
  setup_remote
  remote_bash '
remote_repo="$1"
remote_build_root="$2"
controller_db="$3"
controller_port="$4"
hostd_node="$5"
hostd_advertised_address="$6"
remote_hostd_config_path="$7"
remote_hostd_layout_state_root="$8"
remote_hostd_layout_log_root="$9"
remote_hostd_layout_systemd_dir="${10}"
remote_hostd_public_key_path="${11}"
runtime_root="${12}"
hostd_state_root="${13}"
hostd_prepare_mode="${14}"
remote_root="${15}"

mkdir -p \
  "$remote_hostd_layout_state_root" \
  "$remote_hostd_layout_log_root" \
  "$remote_hostd_layout_systemd_dir" \
  "$runtime_root" \
  "$hostd_state_root"

cd "$remote_repo"
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64)"
launcher_bin="$build_dir/naim-node"
controller_url="http://127.0.0.1:$controller_port"
onboarding_dir="$remote_root/onboarding"
onboarding_key_file="$onboarding_dir/${hostd_node}.key"

if [[ ! -x "$launcher_bin" ]]; then
  echo "missing launcher binary: $launcher_bin" >&2
  echo "run ./scripts/hpc1-dev-sandbox.sh build first" >&2
  exit 1
fi

mkdir -p "$onboarding_dir"
onboarding_key=""
if [[ "$hostd_prepare_mode" == "onboarding" ]]; then
  onboarding_payload="$(python3 - "$controller_url" "$hostd_node" <<PY
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

controller_url, node_name = sys.argv[1:3]
query_url = controller_url + "/api/v1/hostd/hosts?node=" + urllib.parse.quote(node_name)
with urllib.request.urlopen(query_url) as response:
    payload = json.load(response)
items = payload.get("items", [])
if not items:
    request = urllib.request.Request(
        controller_url + "/api/v1/hostd/hosts",
        data=json.dumps({"node_name": node_name}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        created = json.load(response)
    print(json.dumps({
        "status": "created",
        "registration_state": "provisioned",
        "onboarding_state": created.get("onboarding_state", "pending"),
        "onboarding_key": created.get("onboarding_key", ""),
    }))
else:
    item = items[0]
    print(json.dumps({
        "status": "existing",
        "registration_state": item.get("registration_state", ""),
        "onboarding_state": item.get("onboarding_state", ""),
    }))
PY
)"
  registration_state="$(python3 - "$onboarding_payload" <<PY
import json
import sys
print(json.loads(sys.argv[1]).get("registration_state", ""))
PY
)"
  onboarding_state="$(python3 - "$onboarding_payload" <<PY
import json
import sys
print(json.loads(sys.argv[1]).get("onboarding_state", ""))
PY
)"
  onboarding_key="$(python3 - "$onboarding_payload" <<PY
import json
import sys
print(json.loads(sys.argv[1]).get("onboarding_key", ""))
PY
)"
  if [[ -n "$onboarding_key" ]]; then
    printf "%s" "$onboarding_key" > "$onboarding_key_file"
  elif [[ "$registration_state" == "provisioned" && "$onboarding_state" == "pending" ]]; then
    if [[ -f "$onboarding_key_file" ]]; then
      onboarding_key="$(cat "$onboarding_key_file")"
    else
      echo "host $hostd_node is provisioned but onboarding key is unavailable; use a fresh node name or clear sandbox state" >&2
      exit 1
    fi
  fi
fi

install_args=(
  install hostd
  --controller "$controller_url"
  --node "$hostd_node"
  --config "$remote_hostd_config_path"
  --state-root "$remote_hostd_layout_state_root"
  --log-root "$remote_hostd_layout_log_root"
  --systemd-dir "$remote_hostd_layout_systemd_dir"
  --skip-systemctl
)
if [[ -n "$onboarding_key" ]]; then
  install_args+=(--onboarding-key "$onboarding_key")
fi
"$launcher_bin" "${install_args[@]}" >/dev/null

if [[ "$hostd_prepare_mode" != "onboarding" ]]; then
  "$launcher_bin" connect-hostd \
    --db "$controller_db" \
    --node "$hostd_node" \
    --address "$hostd_advertised_address" \
    --public-key "$remote_hostd_public_key_path" >/dev/null
fi

echo "hostd prepared node=$hostd_node mode=$hostd_prepare_mode"
' \
    "${remote_repo}" \
    "${remote_build_root}" \
    "${controller_db}" \
    "${controller_port}" \
    "${hostd_node}" \
    "${hostd_advertised_address}" \
    "${remote_hostd_config_path}" \
    "${remote_hostd_layout_state_root}" \
    "${remote_hostd_layout_log_root}" \
    "${remote_hostd_layout_systemd_dir}" \
    "${remote_hostd_public_key_path}" \
    "${runtime_root}" \
    "${hostd_state_root}" \
    "${hostd_prepare_mode}" \
    "${remote_root}"
}

start_hostd() {
  start_controller
  prepare_hostd
  remote_bash '
remote_repo="$1"
remote_build_root="$2"
remote_logs_root="$3"
controller_port="$4"
hostd_node="$5"
runtime_root="$6"
hostd_state_root="$7"
remote_hostd_config_path="$8"
remote_hostd_private_key_path="$9"
hostd_compose_mode="${10}"
hostd_poll_interval_sec="${11}"
controller_db="${12}"

mkdir -p "$remote_logs_root"
cd "$remote_repo"
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64)"
launcher_bin="$build_dir/naim-node"
controller_bin="$build_dir/naim-controller"
pid_file="$remote_logs_root/hostd.pid"

if [[ ! -x "$launcher_bin" ]] || [[ ! -x "$controller_bin" ]]; then
  echo "missing sandbox binaries; run ./scripts/hpc1-dev-sandbox.sh build first" >&2
  exit 1
fi

if [[ -f "$pid_file" ]]; then
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "hostd already running with pid ${pid}"
  else
    rm -f "$pid_file"
  fi
fi

if [[ ! -f "$pid_file" ]]; then
  NAIM_CONFIG="$remote_hostd_config_path" \
  NAIM_NODE_CONFIG_PATH="$remote_repo/config/naim-node-config.json" \
    nohup "$launcher_bin" run hostd \
      --controller "http://127.0.0.1:$controller_port" \
      --node "$hostd_node" \
      --runtime-root "$runtime_root" \
      --state-root "$hostd_state_root" \
      --host-private-key "$remote_hostd_private_key_path" \
      --compose-mode "$hostd_compose_mode" \
      --poll-interval-sec "$hostd_poll_interval_sec" \
      --foreground \
      --skip-systemctl >"$remote_logs_root/hostd.log" 2>&1 &
  echo $! > "$pid_file"
fi

python3 - "$controller_bin" "$controller_db" "$hostd_node" <<PY
import subprocess
import sys
import time

controller_bin, db_path, node_name = sys.argv[1:4]
deadline = time.time() + 25
while time.time() < deadline:
    output = subprocess.run(
        [controller_bin, "show-hostd-hosts", "--db", db_path, "--node", node_name],
        check=False,
        capture_output=True,
        text=True,
    )
    if output.returncode == 0 and "\"session_state\": \"connected\"" in output.stdout:
        print(f"hostd connected node={node_name}")
        raise SystemExit(0)
    time.sleep(0.5)
raise SystemExit(f"hostd did not connect for node={node_name}")
PY
' \
    "${remote_repo}" \
    "${remote_build_root}" \
    "${remote_logs_root}" \
    "${controller_port}" \
    "${hostd_node}" \
    "${runtime_root}" \
    "${hostd_state_root}" \
    "${remote_hostd_config_path}" \
    "${remote_hostd_private_key_path}" \
    "${hostd_compose_mode}" \
    "${hostd_poll_interval_sec}" \
    "${controller_db}"
}

stop_hostd() {
  remote_bash '
remote_logs_root="$1"
pid_file="$remote_logs_root/hostd.pid"
if [[ -f "$pid_file" ]]; then
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$pid_file"
fi
' "${remote_logs_root}"
}

runtime_smoke() {
  local bundle_arg="${1:-${runtime_smoke_bundle}}"
  start_hostd
  remote_bash '
remote_repo="$1"
remote_build_root="$2"
controller_db="$3"
artifacts_root="$4"
controller_port="$5"
hostd_node="$6"
hostd_state_root="$7"
bundle_arg="$8"

cd "$remote_repo"
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64)"
controller_bin="$build_dir/naim-controller"
hostd_bin="$build_dir/naim-hostd"

if [[ ! -x "$controller_bin" ]] || [[ ! -x "$hostd_bin" ]]; then
  echo "missing sandbox binaries; run ./scripts/hpc1-dev-sandbox.sh build first" >&2
  exit 1
fi

desired_state_path="$bundle_arg"
if [[ ! "$desired_state_path" = /* ]]; then
  desired_state_path="$remote_repo/$desired_state_path"
fi
if [[ -d "$desired_state_path" ]]; then
  if [[ -f "$desired_state_path/desired-state.v2.json" ]]; then
    desired_state_path="$desired_state_path/desired-state.v2.json"
  else
    echo "runtime smoke expects desired-state.v2.json; legacy bundle dirs are not supported: $desired_state_path" >&2
    exit 1
  fi
fi
if [[ ! -f "$desired_state_path" ]]; then
  echo "runtime smoke desired-state not found: $desired_state_path" >&2
  exit 1
fi

plane_name="$(python3 - "$desired_state_path" <<PY
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as source:
    print(json.load(source)["plane_name"])
PY
)"

"$controller_bin" apply-state-file \
  --state "$desired_state_path" \
  --db "$controller_db" \
  --artifacts-root "$artifacts_root" >/dev/null
"$controller_bin" start-plane --db "$controller_db" --plane "$plane_name" >/dev/null

python3 - "$controller_bin" "$hostd_bin" "$controller_db" "$hostd_node" "$hostd_state_root" "$plane_name" <<PY
import pathlib
import subprocess
import sys
import time
import json

controller_bin, hostd_bin, db_path, node_name, state_root, plane_name = sys.argv[1:7]
plane_state = pathlib.Path(state_root) / node_name / "planes" / plane_name / "applied-state.json"
deadline = time.time() + 40

while time.time() < deadline:
    host_output = subprocess.run(
        [controller_bin, "show-hostd-hosts", "--db", db_path, "--node", node_name],
        check=False,
        capture_output=True,
        text=True,
    )
    obs_output = subprocess.run(
        [controller_bin, "show-host-observations", "--db", db_path, "--node", node_name],
        check=False,
        capture_output=True,
        text=True,
    )
    assignment_output = subprocess.run(
        [controller_bin, "show-host-assignments", "--db", db_path, "--node", node_name],
        check=False,
        capture_output=True,
        text=True,
    )
    local_state_output = subprocess.run(
        [hostd_bin, "show-local-state", "--node", node_name, "--state-root", state_root],
        check=False,
        capture_output=True,
        text=True,
    )
    plane_output = subprocess.run(
        [controller_bin, "show-plane", "--db", db_path, "--plane", plane_name],
        check=False,
        capture_output=True,
        text=True,
    )

    connected = host_output.returncode == 0 and "\"session_state\": \"connected\"" in host_output.stdout
    observed = (
        obs_output.returncode == 0
        and "status=applied" in obs_output.stdout
        and "applied_generation=" in obs_output.stdout
    )
    assigned = assignment_output.returncode == 0 and f"plane={plane_name}" in assignment_output.stdout and "status=applied" in assignment_output.stdout
    running = plane_output.returncode == 0 and "state=running" in plane_output.stdout
    realized = False
    plane_state_summary = ""
    if plane_state.exists():
        try:
            plane_state_json = json.loads(plane_state.read_text(encoding="utf-8"))
            plane_name_matches = plane_state_json.get("plane_name") == plane_name
            plane_instances = [
                item.get("name", "")
                for item in plane_state_json.get("instances", [])
                if plane_name in item.get("name", "")
            ]
            plane_disks = [
                item.get("name", "")
                for item in plane_state_json.get("disks", [])
                if plane_name in item.get("name", "")
            ]
            realized = plane_name_matches and bool(plane_instances) and bool(plane_disks)
            if realized:
                plane_state_summary = (
                    f"plane_state={plane_state} "
                    f"instances={len(plane_instances)} disks={len(plane_disks)}"
                )
        except Exception:
            realized = False

    if connected and observed and assigned and realized and running:
        print(f"runtime smoke ok plane={plane_name} node={node_name}")
        if plane_state_summary:
            print(plane_state_summary)
        print(local_state_output.stdout.strip())
        raise SystemExit(0)

    time.sleep(1.0)

raise SystemExit(f"runtime smoke timed out plane={plane_name} node={node_name}")
PY
' \
    "${remote_repo}" \
    "${remote_build_root}" \
    "${controller_db}" \
    "${artifacts_root}" \
    "${controller_port}" \
    "${hostd_node}" \
    "${hostd_state_root}" \
    "${bundle_arg}"
}

status_remote() {
  remote_bash '
remote_root="$1"
remote_repo="$2"
remote_logs_root="$3"
controller_port="$4"
skills_port="$5"
controller_db="$6"
hostd_node="$7"
hostd_state_root="$8"
remote_build_root="$9"

printf "remote_root=%s\nremote_repo=%s\n" "$remote_root" "$remote_repo"
for service in controller skills hostd; do
  pid_file="$remote_logs_root/${service}.pid"
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
      printf "%s_pid=%s alive=yes\n" "$service" "$pid"
    else
      printf "%s_pid=%s alive=no\n" "$service" "${pid:-missing}"
    fi
  else
    printf "%s_pid=missing alive=no\n" "$service"
  fi
done
python3 - "$controller_port" "$skills_port" <<PY
import socket
import sys
for raw in sys.argv[1:]:
    port = int(raw)
    s = socket.socket()
    s.settimeout(0.2)
    try:
        s.connect(("127.0.0.1", port))
    except Exception:
        print(f"port {port}: closed")
    else:
        print(f"port {port}: listening")
    finally:
        s.close()
PY

cd "$remote_repo"
build_dir="$(NAIM_BUILD_ROOT="$remote_build_root" ./scripts/print-build-dir.sh linux x64 2>/dev/null || true)"
if [[ -n "${build_dir}" ]] && [[ -x "$build_dir/naim-controller" ]]; then
  echo "--- host registry ---"
  "$build_dir/naim-controller" show-hostd-hosts --db "$controller_db" --node "$hostd_node" || true
  echo "--- host observations ---"
  "$build_dir/naim-controller" show-host-observations --db "$controller_db" --node "$hostd_node" || true
fi
if [[ -n "${build_dir}" ]] && [[ -x "$build_dir/naim-hostd" ]]; then
  echo "--- local state ---"
  "$build_dir/naim-hostd" show-local-state --node "$hostd_node" --state-root "$hostd_state_root" || true
fi

for log in "$remote_logs_root/controller.log" "$remote_logs_root/skills.log" "$remote_logs_root/hostd.log"; do
  if [[ -f "$log" ]]; then
    echo "--- ${log} ---"
    tail -n 20 "$log"
  fi
done
' \
    "${remote_root}" \
    "${remote_repo}" \
    "${remote_logs_root}" \
    "${controller_port}" \
    "${skills_port}" \
    "${controller_db}" \
    "${hostd_node}" \
    "${hostd_state_root}" \
    "${remote_build_root}"
}

open_shell() {
  "${ssh_base[@]}" -t "cd '${remote_repo}' && bash --login"
}

command="${1:-}"
shift || true

case "${command}" in
  paths)
    print_paths
    ;;
  setup)
    setup_remote
    ;;
  sync)
    sync_remote
    ;;
  build)
    build_remote "$@"
    ;;
  controller-start)
    start_controller
    ;;
  controller-stop)
    stop_controller
    ;;
  hostd-prepare)
    prepare_hostd
    ;;
  hostd-start)
    start_hostd
    ;;
  hostd-stop)
    stop_hostd
    ;;
  runtime-smoke)
    runtime_smoke "$@"
    ;;
  status)
    status_remote
    ;;
  shell)
    open_shell
    ;;
  *)
    usage
    exit 1
    ;;
esac
