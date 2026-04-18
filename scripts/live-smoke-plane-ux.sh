#!/usr/bin/env bash
set -euo pipefail

MAIN_SSH="${MAIN_SSH:-baal@51.68.181.52}"
HPC1_SSH="${HPC1_SSH:-baal@195.168.22.186}"
HPC1_SSH_PORT="${HPC1_SSH_PORT:-2222}"
PLANE_NAME="${PLANE_NAME:-ux-plane-smoke}"
CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:18084}"
CONTROLLER_DB="${CONTROLLER_DB:-/opt/naim/control-plane/state/controller.sqlite}"
TIMEOUT_SEC="${TIMEOUT_SEC:-900}"

MODEL_ID="Qwen/Qwen3.5-9B-Q8_0"
MODEL_PATH="/mnt/array/naim/storage/gguf/Qwen/Qwen3.5-9B-Q8_0/Qwen3.5-9B-Q8_0.gguf"

ssh_main() {
  ssh "${MAIN_SSH}" "$@"
}

ssh_hpc1() {
  ssh -p "${HPC1_SSH_PORT}" "${HPC1_SSH}" "$@"
}

create_token() {
  ssh_main "sudo -n python3 - <<'PY'
import datetime, secrets, sqlite3
token = 'codex-ux-plane-smoke-' + secrets.token_urlsafe(32)
expires = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
con = sqlite3.connect('${CONTROLLER_DB}')
con.execute(
    'insert or ignore into users(id, username, role, password_hash) values (?, ?, ?, ?)',
    (1, 'codex-live-admin', 'admin', ''),
)
con.execute(
    'insert into auth_sessions(token, user_id, session_kind, plane_name, expires_at, last_used_at) values (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
    (token, 1, 'web', '', expires),
)
con.commit()
con.close()
print(token)
PY"
}

revoke_token() {
  local token="${1:-}"
  if [[ -z "${token}" ]]; then
    return 0
  fi
  ssh_main "sudo -n python3 - <<'PY'
import datetime, sqlite3
token = '''${token}'''
revoked_at = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
con = sqlite3.connect('${CONTROLLER_DB}')
con.execute('update auth_sessions set revoked_at=? where token=?', (revoked_at, token))
con.commit()
con.close()
PY" >/dev/null || true
}

api() {
  local method="$1"
  local path="$2"
  local data_path="${3:-}"
  if [[ -n "${data_path}" ]]; then
    ssh_main "curl -fsS -X '${method}' -H 'X-Naim-Session-Token: ${TOKEN}' -H 'Content-Type: application/json' --data-binary @- '${CONTROLLER_URL}${path}'" <"${data_path}"
    return
  fi
  ssh_main "curl -fsS -X '${method}' -H 'X-Naim-Session-Token: ${TOKEN}' '${CONTROLLER_URL}${path}'"
}

plane_exists() {
  ssh_main "sudo -n sqlite3 '${CONTROLLER_DB}' \"select count(*) from planes where name='${PLANE_NAME}';\"" | grep -qx "1"
}

wait_for_plane_deleted() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if ! plane_exists; then
      return 0
    fi
    sleep 3
  done
  echo "timed out waiting for ${PLANE_NAME} deletion" >&2
  return 1
}

cleanup_plane() {
  if plane_exists; then
    echo "[live-smoke] cleanup existing ${PLANE_NAME}"
    api POST "/api/v1/planes/${PLANE_NAME}/stop" >/dev/null || true
    sleep 5
    api DELETE "/api/v1/planes/${PLANE_NAME}" >/dev/null || true
    wait_for_plane_deleted || true
  fi
}

wait_for_generation_applied() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    local row
    row="$(ssh_main "sudo -n sqlite3 -separator '|' '${CONTROLLER_DB}' \"select generation, applied_generation, state from planes where name='${PLANE_NAME}';\"")"
    if [[ -n "${row}" ]]; then
      IFS='|' read -r generation applied state <<<"${row}"
      echo "[live-smoke] plane state=${state} generation=${generation} applied=${applied}"
      if [[ "${generation}" == "${applied}" ]]; then
        return 0
      fi
    fi
    sleep 5
  done
  echo "timed out waiting for ${PLANE_NAME} generation convergence" >&2
  return 1
}

wait_for_interaction_ready() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    local status
    status="$(api GET "/api/v1/planes/${PLANE_NAME}/interaction/status" || true)"
    if [[ -n "${status}" ]]; then
      if python3 - "${status}" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(f"[live-smoke] interaction ready={payload.get('ready')} reason={payload.get('reason')} degraded={payload.get('degraded')}")
if payload.get("ready") is True and payload.get("degraded") is False:
    raise SystemExit(0)
raise SystemExit(1)
PY
      then
        return 0
      fi
    fi
    sleep 10
  done
  echo "timed out waiting for ${PLANE_NAME} interaction readiness" >&2
  return 1
}

assert_preflight() {
  echo "[live-smoke] preflight"
  ssh_main "sudo -n python3 - <<'PY'
import json, sqlite3, sys
con = sqlite3.connect('${CONTROLLER_DB}')
con.row_factory = sqlite3.Row
hosts = {row['node_name']: row for row in con.execute('select * from registered_hosts')}
for node, role in [('hpc1', 'worker'), ('storage1', 'storage')]:
    row = hosts.get(node)
    if row is None:
        raise SystemExit(f'missing host {node}')
    if row['session_state'] != 'connected' or row['derived_role'] != role:
        raise SystemExit(
            'bad host {}: session={} role={}'.format(
                node, row['session_state'], row['derived_role']
            )
        )
links = {
    (row['observer_node_name'], row['peer_node_name']): row
    for row in con.execute('select * from host_peer_links')
}
for key in [('hpc1', 'storage1'), ('storage1', 'hpc1')]:
    row = links.get(key)
    if row is None or not row['tcp_reachable']:
        raise SystemExit(f'missing direct LAN link {key}')
job = con.execute(
    'select status from model_library_download_jobs where model_id=? and node_name=? and target_paths_json like ?',
    ('${MODEL_ID}', 'storage1', '%Qwen3.5-9B-Q8_0.gguf%'),
).fetchone()
if job is None or job['status'] != 'completed':
    raise SystemExit('completed Qwen3.5-9B-Q8_0 model-library entry not found')
print('preflight ok')
PY"
  ssh_hpc1 "docker ps --format '{{.Names}} {{.Status}}' | grep -F 'lt-cypher-ai' >/dev/null"
}

write_payloads() {
  local create_json edit_json
  create_json="$(mktemp)"
  edit_json="$(mktemp)"
  cat >"${create_json}" <<JSON
{
  "desired_state_v2": {
    "version": 2,
    "plane_name": "${PLANE_NAME}",
    "plane_mode": "llm",
    "protected": false,
    "placement": {"execution_node": "hpc1"},
    "topology": {
      "nodes": [{"name": "hpc1", "execution_mode": "mixed", "gpu_memory_mb": {"1": 24576}}]
    },
    "runtime": {
      "engine": "llama.cpp",
      "distributed_backend": "llama_rpc",
      "workers": 1,
      "max_model_len": 8192,
      "max_num_seqs": 4,
      "gpu_memory_utilization": 0.85
    },
    "network": {
      "gateway_port": 18284,
      "inference_port": 18294,
      "server_name": "ux-plane-smoke.local"
    },
    "resources": {
      "worker": {
        "placement_mode": "manual",
        "share_mode": "exclusive",
        "gpu_fraction": 1,
        "memory_cap_mb": 24576
      },
      "shared_disk_gb": 32
    },
    "model": {
      "source": {"type": "library", "ref": "${MODEL_ID}"},
      "served_model_name": "ux-plane-smoke-qwen",
      "target_filename": "Qwen3.5-9B-Q8_0.gguf",
      "materialization": {
        "mode": "prepare_on_worker",
        "local_path": "${MODEL_PATH}",
        "source_node_name": "storage1",
        "source_paths": ["${MODEL_PATH}"],
        "source_format": "gguf",
        "source_quantization": "Q8_0",
        "desired_output_format": "gguf",
        "quantization": "Q8_0",
        "keep_source": false,
        "writeback": {"enabled": false, "if_missing": true, "target_node_name": "storage1"}
      }
    },
    "interaction": {
      "system_prompt": "You are a concise smoke-test assistant.",
      "thinking_enabled": false,
      "default_response_language": "ru",
      "supported_response_languages": ["en", "de", "uk", "ru"],
      "follow_user_language": true
    },
    "infer": {
      "replicas": 1,
      "image": "chainzano.com/naim/infer-runtime:5ac368d"
    },
    "worker": {
      "image": "chainzano.com/naim/worker-runtime:5ac368d",
      "gpu_device": "1",
      "assignments": [{"node": "hpc1", "gpu_device": "1"}]
    },
    "skills": {"enabled": false, "image": "chainzano.com/naim/skills-runtime:5ac368d"},
    "app": {"enabled": false}
  }
}
JSON
  python3 - "${create_json}" "${edit_json}" <<'PY'
import json, pathlib, sys
create_path, edit_path = map(pathlib.Path, sys.argv[1:3])
payload = json.loads(create_path.read_text())
state = payload["desired_state_v2"]
state["runtime"]["max_num_seqs"] = 5
state["network"]["server_name"] = "ux-plane-smoke-edited.local"
edit_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY
  CREATE_JSON="${create_json}"
  EDIT_JSON="${edit_json}"
}

TOKEN="$(create_token)"
trap 'revoke_token "${TOKEN:-}"; rm -f "${CREATE_JSON:-}" "${EDIT_JSON:-}"' EXIT

assert_preflight
cleanup_plane
write_payloads

echo "[live-smoke] create ${PLANE_NAME}"
api POST "/api/v1/planes" "${CREATE_JSON}" >/dev/null

echo "[live-smoke] edit ${PLANE_NAME}"
api PUT "/api/v1/planes/${PLANE_NAME}" "${EDIT_JSON}" >/dev/null

echo "[live-smoke] start ${PLANE_NAME}"
api POST "/api/v1/planes/${PLANE_NAME}/start" >/dev/null
wait_for_generation_applied
wait_for_interaction_ready

echo "[live-smoke] verify runtime containers"
ssh_hpc1 "docker ps --format '{{.Names}} {{.Image}} {{.Status}}' | grep -F 'naim-${PLANE_NAME}-hpc1' | grep -F 'chainzano.com/naim/'"
ssh_hpc1 "docker ps --format '{{.Names}} {{.Status}}' | grep -F 'lt-cypher-ai' >/dev/null"

echo "[live-smoke] stop ${PLANE_NAME}"
api POST "/api/v1/planes/${PLANE_NAME}/stop" >/dev/null
wait_for_generation_applied

echo "[live-smoke] delete ${PLANE_NAME}"
api DELETE "/api/v1/planes/${PLANE_NAME}" >/dev/null
wait_for_plane_deleted

if ssh_hpc1 "docker ps -a --format '{{.Names}}' | grep -F 'naim-${PLANE_NAME}-'"; then
  echo "leftover ${PLANE_NAME} containers found" >&2
  exit 1
fi

echo "[live-smoke] ok"
