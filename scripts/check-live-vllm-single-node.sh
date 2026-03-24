#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

plane_name="qwen35-9b-min"
recreate_plane="${COMET_RECREATE_PLANE:-no}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --recreate)
      recreate_plane="yes"
      shift
      ;;
    --*)
      echo "error: unknown argument: $1" >&2
      exit 1
      ;;
    *)
      plane_name="$1"
      shift
      ;;
  esac
done
controller_url="${COMET_CONTROLLER_URL:-http://127.0.0.1:18080}"
web_ui_url="${COMET_WEB_UI_URL:-http://127.0.0.1:18081}"

docker_cmd=(docker)
if ! docker info >/dev/null 2>&1; then
  docker_cmd=(sudo -n docker)
fi
if ! "${docker_cmd[@]}" info >/dev/null 2>&1; then
  echo "error: docker is not available for live validation" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  if [[ "${worker_stopped_for_validation:-no}" == "yes" ]]; then
    "${docker_cmd[@]}" start "${worker_container}" >/dev/null 2>&1 || true
  fi
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

status_json="${tmp_dir}/status.json"
models_json="${tmp_dir}/models.json"
chat_request_json="${tmp_dir}/chat-request.json"
chat_response_json="${tmp_dir}/chat-response.json"
stream_request_json="${tmp_dir}/stream-request.json"
stream_response_txt="${tmp_dir}/stream-response.txt"
long_request_json="${tmp_dir}/long-request.json"
long_response_json="${tmp_dir}/long-response.json"
failure_chat_response_json="${tmp_dir}/failure-chat-response.json"

resolve_container_name() {
  local service_name="$1"
  local container_name
  container_name="$("${docker_cmd[@]}" ps --format '{{.Names}}' | grep -F "${service_name}" | head -n1 || true)"
  if [[ -z "${container_name}" ]]; then
    echo "error: unable to find running container for ${service_name}" >&2
    exit 1
  fi
  printf '%s\n' "${container_name}"
}

infer_container=""
worker_container=""
worker_stopped_for_validation="no"

echo "[check-live-vllm] ensuring plane ${plane_name} is running"
if [[ "${recreate_plane}" == "yes" ]]; then
  echo "[check-live-vllm] recreating plane ${plane_name}"
  curl -fsS -X DELETE "${controller_url}/api/v1/planes/${plane_name}" >/dev/null || true
fi
"${repo_root}/scripts/run-plane.sh" "${plane_name}" >/dev/null
infer_container="$(resolve_container_name "infer-${plane_name}")"
worker_container="$(resolve_container_name "worker-${plane_name}")"

echo "[check-live-vllm] checking interaction status"
curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/status" >"${status_json}"
python3 - <<'PY' "${status_json}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("ready") is True, payload
runtime_status = payload.get("runtime_status") or {}
assert runtime_status.get("runtime_backend") == "worker-vllm", runtime_status
print("status=ok")
PY

echo "[check-live-vllm] checking model listing"
curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/models" >"${models_json}"
python3 - <<'PY' "${models_json}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
models = payload.get("data") or []
assert models, payload
print("models=" + ",".join(item.get("id", "") for item in models))
PY

echo "[check-live-vllm] checking worker upstream contract"
"${docker_cmd[@]}" inspect "${infer_container}" >/dev/null
if "${docker_cmd[@]}" inspect "${infer_container}" --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -q '^COMET_INFER_VLLM_UPSTREAM_URL='; then
  echo "error: infer container still has COMET_INFER_VLLM_UPSTREAM_URL pinned" >&2
  exit 1
fi
worker_upstream_path="$(
  python3 - <<'PY' "${status_json}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
control_root = (payload.get("runtime_status") or {}).get("control_root", "")
assert control_root, payload
print(control_root.rstrip("/") + "/worker-upstream.json")
PY
)"
"${docker_cmd[@]}" exec "${infer_container}" test -f "${worker_upstream_path}"
"${docker_cmd[@]}" exec "${infer_container}" python3 - <<'PY' "${worker_upstream_path}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert payload.get("ready") is True, payload
assert payload.get("base_url", "").startswith("http://"), payload
print("worker_upstream=ok")
PY

echo "[check-live-vllm] checking basic chat completion"
python3 - <<'PY' "${chat_request_json}"
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "model": "qwen3.5-9b",
    "messages": [{"role": "user", "content": "Reply with PONG only."}],
    "max_tokens": 16,
}))
PY
curl -fsS \
  -H 'Content-Type: application/json' \
  --data-binary "@${chat_request_json}" \
  "${controller_url}/api/v1/planes/${plane_name}/interaction/chat/completions" \
  >"${chat_response_json}"
python3 - <<'PY' "${chat_response_json}"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
content = payload["choices"][0]["message"]["content"].strip()
assert content == "PONG", content
print("chat=PONG")
PY

echo "[check-live-vllm] checking streaming chat completion"
python3 - <<'PY' "${stream_request_json}"
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "model": "qwen3.5-9b",
    "messages": [{"role": "user", "content": "Reply with PONG only."}],
    "max_tokens": 16,
    "stream": True,
}))
PY
curl -sN --max-time 20 \
  -H 'Content-Type: application/json' \
  --data-binary "@${stream_request_json}" \
  "${controller_url}/api/v1/planes/${plane_name}/interaction/chat/completions/stream" \
  >"${stream_response_txt}"
python3 - <<'PY' "${stream_response_txt}"
import json
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
assert "event: complete" in text, text
assert "data: [DONE]" in text, text
deltas = []
for match in re.finditer(r"event: delta\ndata: (.+)", text):
    payload = json.loads(match.group(1))
    deltas.append(payload.get("delta", ""))
joined = "".join(deltas).strip()
assert joined == "PONG", joined
print("stream=PONG")
PY

echo "[check-live-vllm] checking missing-worker failure handling"
"${docker_cmd[@]}" stop "${worker_container}" >/dev/null
worker_stopped_for_validation="yes"
deadline=$((SECONDS + 60))
status_payload=""
while (( SECONDS < deadline )); do
  status_payload="$(curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/status")"
  ready="$(
    python3 - <<'PY' "${status_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
print("yes" if payload.get("ready") else "no")
PY
)"
  if [[ "${ready}" == "no" ]]; then
    break
  fi
  sleep 2
done
python3 - <<'PY' "${status_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
assert payload.get("ready") is False, payload
runtime_status = payload.get("runtime_status") or {}
assert runtime_status.get("inference_ready") is False, runtime_status
print("missing_worker_status=ok")
PY
failure_code="$(
  curl -sS -o "${failure_chat_response_json}" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    --data-binary "@${chat_request_json}" \
    "${controller_url}/api/v1/planes/${plane_name}/interaction/chat/completions"
)"
python3 - <<'PY' "${failure_code}" "${failure_chat_response_json}"
import pathlib
import sys

code = int(sys.argv[1])
body = pathlib.Path(sys.argv[2]).read_text()
assert code >= 500, (code, body)
assert "unavailable" in body.lower() or "failed" in body.lower(), body
print(f"missing_worker_chat=http_{code}")
PY
"${docker_cmd[@]}" start "${worker_container}" >/dev/null
worker_stopped_for_validation="no"
deadline=$((SECONDS + 120))
status_payload=""
while (( SECONDS < deadline )); do
  if status_payload="$(curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/status" 2>/dev/null)"; then
    ready="$(
      python3 - <<'PY' "${status_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
print("yes" if payload.get("ready") else "no")
PY
)"
    if [[ "${ready}" == "yes" ]]; then
      break
    fi
  fi
  sleep 2
done
python3 - <<'PY' "${status_payload}"
import json
import sys

payload = json.loads(sys.argv[1])
assert payload.get("ready") is True, payload
print("worker_recovery=ok")
PY

echo "[check-live-vllm] checking long-form semantic completion"
python3 - <<'PY' "${long_request_json}"
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "model": "qwen3.5-9b",
    "messages": [{
        "role": "user",
        "content": (
            "Write a detailed guide in English about how to design and operate a single-node GPU "
            "inference service for LLMs. The answer must be around 1200 words, structured with "
            "sections and checklists. Split across multiple messages if needed, but finish the "
            "full artifact completely."
        ),
    }],
}))
PY
curl -fsS \
  -H 'Content-Type: application/json' \
  --data-binary "@${long_request_json}" \
  "${controller_url}/api/v1/planes/${plane_name}/interaction/chat/completions" \
  >"${long_response_json}"
python3 - <<'PY' "${long_response_json}"
import json
import pathlib
import re
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
session = payload["session"]
text = payload["choices"][0]["message"]["content"]
words = re.findall(r"\b\w+\b", text)
assert len(words) >= 900, len(words)
assert session["segment_count"] >= 2, session
assert session["continuation_count"] >= 1, session
assert session["stop_reason"] == "semantic_completion_marker", session
assert session["marker_seen"] is True, session
print(
    "long_form="
    + f"words={len(words)} segments={session['segment_count']} continuations={session['continuation_count']}"
)
PY

echo "[check-live-vllm] checking web UI health"
curl -fsS -o /dev/null "${web_ui_url}/"
echo "[check-live-vllm] web_ui=ok"

echo "[check-live-vllm] success"
