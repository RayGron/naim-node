#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
build_dir="$("${script_dir}/print-build-dir.sh")"

skip_build=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) skip_build=1 ;;
    *)
      echo "usage: $0 [--skip-build]" >&2
      exit 1
      ;;
  esac
done

if [[ "${skip_build}" -eq 0 ]]; then
  "${script_dir}/build-target.sh" Debug >/dev/null
fi

command -v curl >/dev/null 2>&1 || {
  echo "isolated-browsing-live: curl is required" >&2
  exit 1
}
command -v python3 >/dev/null 2>&1 || {
  echo "isolated-browsing-live: python3 is required" >&2
  exit 1
}

next_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-120}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

mkdir -p "${repo_root}/var"
work_root="$(mktemp -d "${repo_root}/var/live-isolated-browsing.XXXXXX")"
db_path="${work_root}/controller.sqlite"
controller_log="${work_root}/controller.log"
browsing_log="${work_root}/browsing.log"
browsing_status_path="${work_root}/browsing-runtime-status.json"
browsing_state_root="${work_root}/browsing-state"
artifacts_root="${work_root}/artifacts"
desired_state_path="${work_root}/maglev.desired-state.v2.json"
request_body_path="${work_root}/maglev-upsert.json"
controller_port="$(next_port)"
browsing_port="$(next_port)"
plane_name="maglev"
auth_token="live-web-session"
controller_pid=""
browsing_pid=""

cleanup() {
  stop_pid() {
    local pid="$1"
    if [[ -z "${pid}" ]]; then
      return 0
    fi
    kill "${pid}" >/dev/null 2>&1 || true
    for _ in $(seq 1 10); do
      if ! kill -0 "${pid}" >/dev/null 2>&1; then
        wait "${pid}" >/dev/null 2>&1 || true
        return 0
      fi
      sleep 0.1
    done
    kill -9 "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  }

  stop_pid "${browsing_pid}"
  stop_pid "${controller_pid}"
  rm -rf "${work_root}"
}
trap cleanup EXIT

mkdir -p "${browsing_state_root}" "${artifacts_root}"

cat >"${desired_state_path}" <<EOF
{
  "version": 2,
  "plane_name": "${plane_name}",
  "plane_mode": "llm",
  "model": {
    "source": {
      "type": "local",
      "path": "/models/qwen"
    },
    "materialization": {
      "mode": "reference",
      "local_path": "/models/qwen"
    },
    "served_model_name": "qwen-maglev"
  },
  "runtime": {
    "engine": "llama.cpp",
    "distributed_backend": "llama_rpc",
    "workers": 1
  },
  "topology": {
    "nodes": [
      {
        "name": "local-hostd",
        "execution_mode": "mixed",
        "gpu_memory_mb": {
          "0": 24576
        }
      }
    ]
  },
  "infer": {
    "replicas": 1
  },
  "worker": {
    "assignments": [
      {
        "node": "local-hostd",
        "gpu_device": "0"
      }
    ]
  },
  "network": {
    "gateway_port": 18184,
    "inference_port": 18194,
    "server_name": "maglev.internal"
  },
  "browsing": {
    "enabled": true,
    "policy": {
      "browser_session_enabled": true,
      "allowed_domains": ["example.com", "openai.com", "reddit.com", "old.reddit.com", "x.com", "twitter.com"],
      "blocked_domains": ["localhost", "internal"],
      "max_search_results": 4,
      "max_fetch_bytes": 16384
    },
    "publish": [
      {
        "host_ip": "127.0.0.1",
        "host_port": ${browsing_port},
        "container_port": 18130
      }
    ]
  },
  "app": {
    "enabled": false
  }
}
EOF

python3 - "${desired_state_path}" "${request_body_path}" "${artifacts_root}" <<'PY'
import json
import sys

desired_state_path = sys.argv[1]
request_body_path = sys.argv[2]
artifacts_root = sys.argv[3]
with open(desired_state_path, "r", encoding="utf-8") as source:
    desired_state = json.load(source)
with open(request_body_path, "w", encoding="utf-8") as output:
    json.dump({"desired_state_v2": desired_state, "artifacts_root": artifacts_root}, output)
PY

echo "isolated-browsing-live: init controller db"
"${build_dir}/naim-controller" init-db --db "${db_path}" >/dev/null
python3 - "${db_path}" "${auth_token}" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
auth_token = sys.argv[2]
conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "INSERT INTO users(username, role, password_hash) VALUES (?, 'admin', '')",
        ("live-admin",),
    )
    conn.execute(
        """
        INSERT INTO auth_sessions(token, user_id, session_kind, plane_name, expires_at, last_used_at)
        VALUES (?, 1, 'web', '', datetime('now', '+1 day'), datetime('now'))
        """,
        (auth_token,),
    )
    conn.execute(
        """
        INSERT INTO registered_hosts(
          node_name,
          public_key_base64,
          transport_mode,
          execution_mode,
          registration_state,
          derived_role,
          role_reason,
          session_state,
          session_expires_at,
          last_session_at,
          last_heartbeat_at,
          capabilities_json,
          status_message
        ) VALUES (
          'local-hostd',
          'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
          'out',
          'mixed',
          'registered',
          'worker',
          'live smoke connected hostd',
          'connected',
          datetime('now', '+1 day'),
          datetime('now'),
          datetime('now'),
          '{"capacity_summary":{"gpu_count":1,"storage_root":"/tmp/naim","storage_total_bytes":200000000000,"storage_free_bytes":150000000000,"total_memory_bytes":68719476736}}',
          'registered by live smoke'
        )
        """
    )
    conn.commit()
finally:
    conn.close()
PY

echo "isolated-browsing-live: start controller"
"${build_dir}/naim-controller" serve \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" \
  --listen-host 127.0.0.1 \
  --listen-port "${controller_port}" >"${controller_log}" 2>&1 &
controller_pid="$!"
wait_for_http "http://127.0.0.1:${controller_port}/health"

echo "isolated-browsing-live: create maglev via plane API"
create_payload="$(
  curl -fsS -X POST \
    -H "X-Naim-Session-Token: ${auth_token}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${request_body_path}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes"
)"
printf '%s' "${create_payload}" | grep -F '"status":"ok"' >/dev/null
printf '%s' "${create_payload}" | grep -F '"action":"upsert-plane-state"' >/dev/null
curl -fsS \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes" | grep -F "\"name\":\"${plane_name}\"" >/dev/null

echo "isolated-browsing-live: start maglev"
curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/start" \
  | grep -F '"action":"start-plane"' >/dev/null

echo "isolated-browsing-live: browsing should be configured but not ready before runtime boot"
pre_status_path="${work_root}/pre-status.json"
pre_status_code="$(
  curl -sS -o "${pre_status_path}" -w '%{http_code}' \
    -H "X-Naim-Session-Token: ${auth_token}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/status"
)"
if [[ "${pre_status_code}" == "404" ]]; then
  echo "isolated-browsing-live: controller binary does not expose /api/v1/planes/<plane>/webgateway/* yet" >&2
  echo "isolated-browsing-live: rebuild naim-controller before rerunning this smoke check" >&2
  exit 1
fi
if [[ "${pre_status_code}" != "200" ]]; then
  echo "isolated-browsing-live: unexpected pre-runtime browsing status code: ${pre_status_code}" >&2
  cat "${pre_status_path}" >&2
  exit 1
fi
pre_status="$(cat "${pre_status_path}")"
printf '%s' "${pre_status}" | grep -F '"webgateway_enabled":true' >/dev/null
printf '%s' "${pre_status}" | grep -F '"webgateway_ready":false' >/dev/null
printf '%s' "${pre_status}" | grep -F '"plane_name":"maglev"' >/dev/null

echo "isolated-browsing-live: start browsing runtime"
NAIM_PLANE_NAME="${plane_name}" \
NAIM_INSTANCE_NAME="webgateway-${plane_name}" \
NAIM_INSTANCE_ROLE="webgateway" \
NAIM_NODE_NAME="local-hostd" \
NAIM_CONTROL_ROOT="/naim/shared/control/${plane_name}" \
NAIM_CONTROLLER_URL="http://127.0.0.1:${controller_port}" \
NAIM_WEBGATEWAY_RUNTIME_STATUS_PATH="${browsing_status_path}" \
NAIM_WEBGATEWAY_STATE_ROOT="${browsing_state_root}" \
NAIM_WEBGATEWAY_PORT="${browsing_port}" \
NAIM_WEBGATEWAY_POLICY_JSON='{"browser_session_enabled":true,"rendered_browser_enabled":true,"allowed_domains":["example.com","openai.com","reddit.com","old.reddit.com","x.com","twitter.com"],"blocked_domains":["localhost","internal"],"max_search_results":4,"max_fetch_bytes":16384}' \
  "${build_dir}/naim-webgatewayd" >"${browsing_log}" 2>&1 &
browsing_pid="$!"
wait_for_http "http://127.0.0.1:${browsing_port}/health"

echo "isolated-browsing-live: verify controller-owned browsing status"
status_payload="$(
  curl -fsS \
    -H "X-Naim-Session-Token: ${auth_token}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/status"
)"
printf '%s' "${status_payload}" | grep -F '"webgateway_ready":true' >/dev/null
printf '%s' "${status_payload}" | grep -F "\"webgateway_target\":\"http://127.0.0.1:${browsing_port}\"" >/dev/null
printf '%s' "${status_payload}" | grep -F '"browser_session_enabled":true' >/dev/null
printf '%s' "${status_payload}" | grep -F '"rendered_browser_enabled":true' >/dev/null

echo "isolated-browsing-live: verify maglev dashboard exposes browsing state"
dashboard_payload="$(
  curl -fsS \
    -H "X-Naim-Session-Token: ${auth_token}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/dashboard"
)"
printf '%s' "${dashboard_payload}" | grep -F '"webgateway_enabled":true' >/dev/null
printf '%s' "${dashboard_payload}" | grep -F '"webgateway_ready":true' >/dev/null

echo "isolated-browsing-live: verify maglev interaction status exposes browsing capability"
interaction_payload="$(
  curl -fsS \
    -H "X-Naim-Session-Token: ${auth_token}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/interaction/status"
)"
printf '%s' "${interaction_payload}" | grep -F '"interaction_enabled":true' >/dev/null
printf '%s' "${interaction_payload}" | grep -F '"webgateway_enabled":true' >/dev/null
printf '%s' "${interaction_payload}" | grep -F '"webgateway_ready":true' >/dev/null
printf '%s' "${interaction_payload}" | grep -F '"browser_session_enabled":true' >/dev/null

echo "isolated-browsing-live: verify search proxy"
search_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"query":"OpenAI API","limit":2,"domains":["openai.com"]}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/search")"
printf '%s' "${search_payload}" | grep -F '"query":"OpenAI API"' >/dev/null
printf '%s' "${search_payload}" | grep -F '"results":[' >/dev/null
printf '%s' "${search_payload}" | grep -F '"backend":"' >/dev/null

echo "isolated-browsing-live: verify sanitized fetch proxy"
fetch_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"url":"https://example.com"}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/fetch")"
printf '%s' "${fetch_payload}" | grep -F '"content_type":"text/html' >/dev/null
printf '%s' "${fetch_payload}" | grep -F 'Example Domain' >/dev/null
printf '%s' "${fetch_payload}" | grep -F '"backend":"' >/dev/null
printf '%s' "${fetch_payload}" | grep -F '"injection_flags":[' >/dev/null

echo "isolated-browsing-live: verify broker fallback for external fetches"
for external_url in "https://old.reddit.com/r/OpenAI/" "https://www.reddit.com/r/OpenAI/"; do
  external_body="${work_root}/external-fetch-$(printf '%s' "${external_url}" | tr '/:.' '_').json"
  external_status="$(
    curl -sS -o "${external_body}" -w '%{http_code}' \
      -X POST \
      -H "X-Naim-Session-Token: ${auth_token}" \
      -H 'Content-Type: application/json' \
      --data "{\"url\":\"${external_url}\"}" \
      "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/fetch"
  )"
  case "${external_status}" in
    200|502) ;;
    *) echo "isolated-browsing-live: unexpected external fetch status ${external_status} for ${external_url}" >&2; cat "${external_body}" >&2; exit 1 ;;
  esac
done

echo "isolated-browsing-live: verify unsafe target rejection"
unsafe_body="${work_root}/unsafe-fetch.json"
unsafe_status="$(
  curl -sS -o "${unsafe_body}" -w '%{http_code}' \
    -X POST \
    -H "X-Naim-Session-Token: ${auth_token}" \
    -H 'Content-Type: application/json' \
    --data '{"url":"http://127.0.0.1"}' \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/fetch"
)"
test "${unsafe_status}" = "502"
grep -F '"code":"unsafe_url"' "${unsafe_body}" >/dev/null

echo "isolated-browsing-live: verify browser session lifecycle"
session_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"confirmed":true,"url":"https://example.com"}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions")"
session_id="$(printf '%s' "${session_payload}" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
test -n "${session_id}"
test -d "${browsing_state_root}/${session_id}"

read_payload="$(curl -fsS \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${session_id}")"
printf '%s' "${read_payload}" | grep -F "\"session_id\":\"${session_id}\"" >/dev/null

extract_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"action":"extract"}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${session_id}/actions")"
if ! printf '%s' "${extract_payload}" | grep -F 'sanitized extract from active rendered browser session' >/dev/null && \
   ! printf '%s' "${extract_payload}" | grep -F 'sanitized extract from current session URL' >/dev/null; then
  echo "isolated-browsing-live: unexpected extract observation" >&2
  printf '%s\n' "${extract_payload}" >&2
  exit 1
fi

unsupported_body="${work_root}/unsupported-action.json"
unsupported_status="$(
  curl -sS -o "${unsupported_body}" -w '%{http_code}' \
    -X POST \
    -H "X-Naim-Session-Token: ${auth_token}" \
    -H 'Content-Type: application/json' \
    --data '{"action":"click","confirmed":true}' \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${session_id}/actions"
)"
test "${unsupported_status}" = "501"
grep -F '"code":"browser_action_not_supported"' "${unsupported_body}" >/dev/null

curl -fsS -X DELETE \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${session_id}" \
  | grep -F "\"session_id\":\"${session_id}\"" >/dev/null
test ! -d "${browsing_state_root}/${session_id}"

echo "isolated-browsing-live: verify broker session snapshot path"
snapshot_session_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"confirmed":true,"url":"https://example.com"}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions")"
snapshot_session_id="$(printf '%s' "${snapshot_session_payload}" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
test -n "${snapshot_session_id}"
printf '%s' "${snapshot_session_payload}" | grep -F '"backend":"' >/dev/null

snapshot_payload="$(curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  -H 'Content-Type: application/json' \
  --data '{"action":"snapshot"}' \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${snapshot_session_id}/actions")"
printf '%s' "${snapshot_payload}" | grep -F '"backend":"' >/dev/null
curl -fsS -X DELETE \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/webgateway/sessions/${snapshot_session_id}" \
  | grep -F "\"session_id\":\"${snapshot_session_id}\"" >/dev/null
test ! -d "${browsing_state_root}/${snapshot_session_id}"
test -f "${browsing_state_root}/audit.log"
test -f "${browsing_status_path}"

echo "isolated-browsing-live: OK"
