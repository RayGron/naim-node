#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
read -r host_os host_arch < <("${script_dir}/detect-host-target.sh")
build_dir="$("${script_dir}/print-build-dir.sh" "${host_os}" "${host_arch}")"

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
  "${script_dir}/configure-build.sh" "${host_os}" "${host_arch}" Debug >/dev/null
  cmake --build "${build_dir}" --target naim-controller naim-webgatewayd -j 8 >/dev/null
fi

command -v curl >/dev/null 2>&1 || {
  echo "maglev-web-live: curl is required" >&2
  exit 1
}
command -v python3 >/dev/null 2>&1 || {
  echo "maglev-web-live: python3 is required" >&2
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
work_root="$(mktemp -d "${repo_root}/var/live-maglev-web.XXXXXX")"
db_path="${work_root}/controller.sqlite"
controller_log="${work_root}/controller.log"
browsing_log="${work_root}/browsing.log"
gateway_log="${work_root}/gateway.log"
browsing_status_path="${work_root}/browsing-runtime-status.json"
browsing_state_root="${work_root}/browsing-state"
artifacts_root="${work_root}/artifacts"
desired_state_path="${work_root}/maglev.desired-state.v2.json"
request_body_path="${work_root}/maglev-upsert.json"
controller_port="$(next_port)"
browsing_port="$(next_port)"
gateway_port="$(next_port)"
plane_name="maglev"
auth_token="live-maglev-web"
controller_pid=""
browsing_pid=""
gateway_pid=""

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

  stop_pid "${gateway_pid}"
  stop_pid "${browsing_pid}"
  stop_pid "${controller_pid}"
  if [[ "${MAGLEV_KEEP_WORK_ROOT:-0}" == "1" ]]; then
    echo "[maglev-web-live] keeping work root ${work_root}" >&2
    return
  fi
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
  "interaction": {
    "default_response_language": "en",
    "follow_user_language": true,
    "supported_response_languages": ["en", "ru"]
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
    "gateway_port": ${gateway_port},
    "inference_port": 18194,
    "server_name": "maglev.internal"
  },
  "browsing": {
    "enabled": true,
    "policy": {
      "browser_session_enabled": true,
      "allowed_domains": ["example.com", "openai.com", "reddit.com", "old.reddit.com", "x.com", "twitter.com"],
      "blocked_domains": ["localhost", "internal"],
      "max_search_results": 5,
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

echo "maglev-web-live: init controller db"
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

echo "maglev-web-live: start gateway stub"
python3 - "${gateway_port}" >"${gateway_log}" 2>&1 <<'PY' &
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])

class Handler(BaseHTTPRequestHandler):
    def _write_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._write_json(200, {"status": "ok"})
            return
        if self.path == "/v1/models":
            self._write_json(
                200,
                {"object": "list", "data": [{"id": "stub-model", "object": "model"}]},
            )
            return
        self._write_json(404, {"status": "error", "error": {"code": "not_found"}})

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self._write_json(404, {"status": "error", "error": {"code": "not_found"}})
            return
        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length) or b"{}")
        messages = payload.get("messages", [])
        system_message = ""
        user_message = ""
        for message in messages:
            if message.get("role") == "system" and not system_message:
                system_message = str(message.get("content", ""))
            if message.get("role") == "user":
                user_message = str(message.get("content", ""))
        content = "SYSTEM:\n" + system_message + "\n\nUSER:\n" + user_message
        response = {
            "id": "chatcmpl-stub",
            "object": "chat.completion",
            "model": "stub-model",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
        }
        self._write_json(200, response)

    def log_message(self, fmt, *args):
        return

server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
server.serve_forever()
PY
gateway_pid="$!"
wait_for_http "http://127.0.0.1:${gateway_port}/health"

echo "maglev-web-live: start controller"
"${build_dir}/naim-controller" serve \
  --db "${db_path}" \
  --artifacts-root "${artifacts_root}" \
  --listen-host 127.0.0.1 \
  --listen-port "${controller_port}" >"${controller_log}" 2>&1 &
controller_pid="$!"
wait_for_http "http://127.0.0.1:${controller_port}/health"

echo "maglev-web-live: create maglev via plane API"
create_payload="$(
  curl -fsS -X POST \
    -H "X-Naim-Session-Token: ${auth_token}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${request_body_path}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes"
)"
printf '%s' "${create_payload}" | grep -F '"status":"ok"' >/dev/null

echo "maglev-web-live: start maglev"
curl -fsS -X POST \
  -H "X-Naim-Session-Token: ${auth_token}" \
  "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/start" >/dev/null

echo "maglev-web-live: start browsing runtime"
NAIM_PLANE_NAME="${plane_name}" \
NAIM_INSTANCE_NAME="webgateway-${plane_name}" \
NAIM_INSTANCE_ROLE="webgateway" \
NAIM_NODE_NAME="local-hostd" \
NAIM_CONTROL_ROOT="/naim/shared/control/${plane_name}" \
NAIM_CONTROLLER_URL="http://127.0.0.1:${controller_port}" \
NAIM_WEBGATEWAY_RUNTIME_STATUS_PATH="${browsing_status_path}" \
NAIM_WEBGATEWAY_STATE_ROOT="${browsing_state_root}" \
NAIM_WEBGATEWAY_PORT="${browsing_port}" \
NAIM_WEBGATEWAY_POLICY_JSON='{"browser_session_enabled":true,"rendered_browser_enabled":true,"allowed_domains":["example.com","openai.com","reddit.com","old.reddit.com","x.com","twitter.com"],"blocked_domains":["localhost","internal"],"max_search_results":5,"max_fetch_bytes":16384}' \
  "${build_dir}/naim-webgatewayd" >"${browsing_log}" 2>&1 &
browsing_pid="$!"
wait_for_http "http://127.0.0.1:${browsing_port}/health"

echo "maglev-web-live: wait for interaction readiness"
status_path="${work_root}/interaction-status.json"
ready="no"
for _ in $(seq 1 60); do
  status_code="$(
    curl -sS -o "${status_path}" -w '%{http_code}' \
      -H "X-Naim-Session-Token: ${auth_token}" \
      "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/interaction/status"
  )"
  if [[ "${status_code}" == "200" ]]; then
    ready="$(python3 - "${status_path}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as source:
    payload = json.load(source)
print("yes" if payload.get("ready") else "no")
PY
)"
    if [[ "${ready}" == "yes" ]]; then
      break
    fi
  fi
  sleep 0.5
done

if [[ "${ready}" != "yes" ]]; then
  echo "maglev-web-live: inject synthetic host observation"
  python3 - "${db_path}" "${desired_state_path}" "${gateway_port}" <<'PY'
import json
import sqlite3
import sys

db_path = sys.argv[1]
desired_state_path = sys.argv[2]
gateway_port = int(sys.argv[3])

with open(desired_state_path, "r", encoding="utf-8") as source:
    desired_state = json.load(source)

runtime_status = {
    "plane_name": "maglev",
    "instance_name": "infer-maglev",
    "instance_role": "infer",
    "node_name": "local-hostd",
    "runtime_backend": "llama-rpc-head",
    "active_model_id": "/models/qwen",
    "active_served_model_name": "qwen-maglev",
    "cached_local_model_path": "/models/qwen",
    "model_path": "/models/qwen",
    "gateway_listen": f"127.0.0.1:{gateway_port}",
    "gateway_ready": True,
    "inference_ready": True,
    "active_model_ready": True,
    "gateway_plan_ready": True,
    "launch_ready": True,
    "ready": True,
    "replica_groups_expected": 1,
    "replica_groups_ready": 1,
    "replica_groups_degraded": 0,
    "api_endpoints_expected": 1,
    "api_endpoints_ready": 1,
    "registry_entries": 1,
}

instance_runtime = [
    {
        "instance_name": "infer-maglev",
        "instance_role": "infer",
        "node_name": "local-hostd",
        "model_path": "/models/qwen",
        "gpu_device": "0",
        "runtime_phase": "ready",
        "started_at": "",
        "last_activity_at": "",
        "runtime_pid": 1234,
        "engine_pid": 1235,
        "ready": True,
    }
]

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        """
        INSERT INTO host_observations(
            node_name, plane_name, applied_generation, last_assignment_id, status,
            status_message, observed_state_json, runtime_status_json, instance_runtime_json,
            gpu_telemetry_json, disk_telemetry_json, network_telemetry_json, cpu_telemetry_json,
            heartbeat_at, updated_at
        ) VALUES(
            'local-hostd', 'maglev', 1, NULL, 'applied',
            '', ?, ?, ?,
            '', '', '', '',
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        ON CONFLICT(node_name) DO UPDATE SET
            plane_name=excluded.plane_name,
            applied_generation=excluded.applied_generation,
            status=excluded.status,
            observed_state_json=excluded.observed_state_json,
            runtime_status_json=excluded.runtime_status_json,
            instance_runtime_json=excluded.instance_runtime_json,
            heartbeat_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            json.dumps(desired_state),
            json.dumps(runtime_status),
            json.dumps(instance_runtime),
        ),
    )
    conn.commit()
finally:
    conn.close()
PY

  for _ in $(seq 1 40); do
    status_code="$(
      curl -sS -o "${status_path}" -w '%{http_code}' \
        -H "X-Naim-Session-Token: ${auth_token}" \
        "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/interaction/status"
    )"
    if [[ "${status_code}" == "200" ]]; then
      ready="$(python3 - "${status_path}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as source:
    payload = json.load(source)
print("yes" if payload.get("ready") else "no")
PY
)"
      if [[ "${ready}" == "yes" ]]; then
        break
      fi
    fi
    sleep 0.5
  done
fi

if [[ "${ready}" != "yes" ]]; then
  echo "maglev-web-live: interaction status never became ready" >&2
  cat "${status_path}" >&2
  exit 1
fi

invoke_interaction() {
  local name="$1"
  local request_file="$2"
  local response_file="${work_root}/${name}.response.json"
  local status_code
  status_code="$(
    curl -sS -o "${response_file}" -w '%{http_code}' -X POST \
    -H "X-Naim-Session-Token: ${auth_token}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${request_file}" \
    "http://127.0.0.1:${controller_port}/api/v1/planes/${plane_name}/interaction/chat/completions"
  )"
  if [[ "${status_code}" != "200" ]]; then
    echo "maglev-web-live: interaction request ${name} failed with HTTP ${status_code}" >&2
    cat "${response_file}" >&2
    exit 1
  fi
  printf '%s\n' "${response_file}"
}

assert_json() {
  local response_file="$1"
  local mode="$2"
  python3 - "${response_file}" "${mode}" <<'PY'
import json
import sys

path = sys.argv[1]
mode = sys.argv[2]
with open(path, "r", encoding="utf-8") as source:
    payload = json.load(source)

browsing = payload.get("webgateway", payload.get("browsing", {}))
content = payload["choices"][0]["message"]["content"]

def ensure(condition, message):
    if not condition:
        raise SystemExit(message)

if mode == "toggle_enable":
    ensure(browsing.get("mode") == "enabled", "toggle_enable: mode should be enabled")
    ensure(browsing.get("decision") == "not_needed", "toggle_enable: decision should be not_needed")
    ensure(browsing.get("lookup_state") == "enabled_toggle_only", "toggle_enable: lookup_state should be enabled_toggle_only")
    ensure(browsing.get("indicator", {}).get("compact") == "web:on", "toggle_enable: compact indicator should be web:on")
    ensure(browsing.get("trace", [{}])[0].get("compact") == "web:on", "toggle_enable: trace should start with web:on")
    ensure("WebGateway state: enabled_toggle_only." in content, "toggle_enable: assistant should receive toggle-only browsing state")
elif mode == "search_intent":
    ensure(browsing.get("mode") == "enabled", "search_intent: mode should stay enabled")
    ensure(browsing.get("decision") == "search_and_fetch", "search_intent: decision should be search_and_fetch")
    searches = browsing.get("searches", [])
    sources = browsing.get("sources", [])
    ensure(searches, "search_intent: search attempt should be recorded")
    if not sources:
        ensure(
            browsing.get("lookup_state") == "attempted_no_evidence",
            "search_intent: lookup_state should expose attempted_no_evidence when no sources were attached",
        )
        ensure(
            browsing.get("indicator", {}).get("compact") == "web:search none",
            "search_intent: compact indicator should show search none when no evidence was attached",
        )
        ensure(
            browsing.get("reason") == "search_returned_no_sources",
            "search_intent: empty evidence must explain that search found no usable sources",
        )
        ensure(
            "Controller attempted a web lookup for this request" in content,
            "search_intent: assistant should receive the attempted-search fallback instruction",
        )
    else:
        ensure(
            browsing.get("lookup_state") == "evidence_attached",
            "search_intent: lookup_state should be evidence_attached when sources were attached",
        )
        ensure(
            browsing.get("indicator", {}).get("compact") == "web:search ok",
            "search_intent: compact indicator should show search ok when evidence was attached",
        )
elif mode == "direct_fetch":
    ensure(browsing.get("decision") == "direct_fetch", "direct_fetch: decision should be direct_fetch")
    ensure(browsing.get("lookup_state") == "evidence_attached", "direct_fetch: lookup_state should be evidence_attached")
    ensure(browsing.get("indicator", {}).get("compact") == "web:fetch ok", "direct_fetch: compact indicator should be web:fetch ok")
    sources = browsing.get("sources", [])
    ensure(sources and "example.com" in sources[0].get("url", ""), "direct_fetch: expected example.com source")
    ensure("https://example.com" in content, "direct_fetch: assistant should receive fetched source URL")
elif mode == "disable_override":
    ensure(browsing.get("mode") == "disabled", "disable_override: mode should be disabled")
    ensure(browsing.get("decision") == "disabled", "disable_override: decision should be disabled")
    ensure(browsing.get("lookup_state") == "disabled_by_user", "disable_override: lookup_state should be disabled_by_user")
    ensure(browsing.get("indicator", {}).get("compact") == "web:off user", "disable_override: compact indicator should be web:off user")
elif mode == "enabled_not_needed":
    ensure(browsing.get("mode") == "enabled", "enabled_not_needed: mode should stay enabled")
    ensure(browsing.get("decision") == "not_needed", "enabled_not_needed: decision should be not_needed")
    ensure(browsing.get("lookup_state") == "enabled_not_needed", "enabled_not_needed: lookup_state should be enabled_not_needed")
    ensure(browsing.get("indicator", {}).get("compact") == "web:on idle", "enabled_not_needed: compact indicator should be web:on idle")
else:
    raise SystemExit(f"unknown mode: {mode}")
PY
}

assert_direct_fetch_domain() {
  local response_file="$1"
  local expected_fragment="$2"
  python3 - "${response_file}" "${expected_fragment}" <<'PY'
import json
import sys

path = sys.argv[1]
expected = sys.argv[2]
with open(path, "r", encoding="utf-8") as source:
    payload = json.load(source)

browsing = payload.get("webgateway", payload.get("browsing", {}))
sources = browsing.get("sources", [])

if browsing.get("decision") != "direct_fetch":
    raise SystemExit(f"expected direct_fetch decision, got {browsing.get('decision')!r}")
if browsing.get("lookup_state") != "evidence_attached":
    raise SystemExit(f"expected evidence_attached, got {browsing.get('lookup_state')!r}")
if not sources:
    raise SystemExit("expected at least one attached source")
if not sources[0].get("backend"):
    raise SystemExit("expected source backend to be populated")
if expected not in sources[0].get("url", ""):
    raise SystemExit(f"expected {expected!r} in source url {sources[0].get('url', '')!r}")
PY
}

echo "maglev-web-live: test natural-language web enable"
toggle_request="${work_root}/toggle-enable.json"
cat >"${toggle_request}" <<'EOF'
{"messages":[{"role":"user","content":"Enable web for this chat."}]}
EOF
toggle_response="$(invoke_interaction "toggle-enable" "${toggle_request}")"
assert_json "${toggle_response}" "toggle_enable"

echo "maglev-web-live: test direct safe fetch"
fetch_request="${work_root}/direct-fetch.json"
cat >"${fetch_request}" <<'EOF'
{"messages":[
  {"role":"user","content":"Enable web for this chat."},
  {"role":"user","content":"Use the web and check https://example.com for me."}
]}
EOF
fetch_response="$(invoke_interaction "direct-fetch" "${fetch_request}")"
assert_json "${fetch_response}" "direct_fetch"

echo "maglev-web-live: test enabled web with no lookup needed"
offline_request="${work_root}/enabled-not-needed.json"
cat >"${offline_request}" <<'EOF'
{"messages":[
  {"role":"user","content":"Enable web for this chat."},
  {"role":"user","content":"Explain TCP handshakes."}
]}
EOF
offline_response="$(invoke_interaction "enabled-not-needed" "${offline_request}")"
assert_json "${offline_response}" "enabled_not_needed"

trap - EXIT
cleanup
echo "maglev-web-live: OK"
