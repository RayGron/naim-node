#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run-plane.sh [plane-name] [--controller-url <url>] [--artifacts-root <path>] [--wait-seconds <n>] [--cpu]

Loads config/<plane-name>/desired-state.json into the local controller, starts the plane,
and waits until interaction/status reports ready.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

plane_name="qwen35-9b-min"
controller_url="http://127.0.0.1:18080"
artifacts_root="/var/lib/comet-node/artifacts"
wait_seconds="900"
force_cpu="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --controller-url)
      controller_url="${2:-}"
      shift 2
      ;;
    --artifacts-root)
      artifacts_root="${2:-}"
      shift 2
      ;;
    --wait-seconds)
      wait_seconds="${2:-}"
      shift 2
      ;;
    --cpu)
      force_cpu="yes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
    *)
      plane_name="$1"
      shift
      ;;
  esac
done

desired_state_path="${repo_root}/config/${plane_name}/desired-state.json"
if [[ ! -f "${desired_state_path}" ]]; then
  echo "error: desired state not found: ${desired_state_path}" >&2
  exit 1
fi

config_summary="$(
  python3 - <<'PY' "${repo_root}/config/comet-node-config.json"
import json
import pathlib
import sys

config_path = pathlib.Path(sys.argv[1])
payload = json.loads(config_path.read_text())
paths = payload.get("paths", {})
storage_root = paths.get("storage_root", "/var/lib/comet")
model_cache_root = paths.get("model_cache_root")
if not model_cache_root:
    model_cache_root = str(pathlib.Path(storage_root).parent / "models")
print(storage_root)
print(model_cache_root)
PY
)"
storage_root="$(printf '%s\n' "${config_summary}" | sed -n '1p')"
model_cache_root="$(printf '%s\n' "${config_summary}" | sed -n '2p')"

wait_for_http() {
  local url="$1"
  local attempts="$2"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

download_to_cache_if_needed() {
  local source_url="$1"
  local target_path="$2"

  mkdir -p "$(dirname -- "${target_path}")"
  if [[ -s "${target_path}" ]]; then
    echo "[run-plane] reusing cached model ${target_path}"
    return
  fi

  echo "[run-plane] downloading model to shared cache ${target_path}"
  curl -fL --progress-bar --output "${target_path}.part" "${source_url}"
  mv "${target_path}.part" "${target_path}"
}

tmp_dir="$(mktemp -d "${repo_root}/var/run-plane.XXXXXX")"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

echo "[run-plane] waiting for controller at ${controller_url}"
if ! wait_for_http "${controller_url}/health" 60; then
  echo "error: controller is not reachable at ${controller_url}" >&2
  exit 1
fi

python3 - <<'PY' \
  "${desired_state_path}" \
  "${tmp_dir}/desired-state.runtime.json" \
  "${model_cache_root}" \
  "${plane_name}" \
  "${force_cpu}"
import json
import pathlib
import re
import sys
from urllib.parse import urlparse

desired_state_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
model_cache_root = pathlib.Path(sys.argv[3])
plane_name = sys.argv[4]
force_cpu = sys.argv[5] == "yes"

state = json.loads(desired_state_path.read_text())
bootstrap = state.get("bootstrap_model") or {}

def sanitize(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)

single_source_url = bootstrap.get("source_url")
source_urls = bootstrap.get("source_urls") or []
if single_source_url and not bootstrap.get("local_path"):
    filename = bootstrap.get("target_filename")
    if not filename:
      parsed = urlparse(single_source_url)
      filename = pathlib.Path(parsed.path).name
    model_dir_name = sanitize(bootstrap.get("model_id") or plane_name)
    bootstrap["local_path"] = str(model_cache_root / model_dir_name / filename)

if force_cpu:
    state.setdefault("inference", {})["llama_gpu_layers"] = 0

state["bootstrap_model"] = bootstrap
output_path.write_text(json.dumps(state, indent=2) + "\n")
PY

bootstrap_local_path="$(
  python3 - <<'PY' "${tmp_dir}/desired-state.runtime.json"
import json
import sys
state = json.load(open(sys.argv[1]))
bootstrap = state.get("bootstrap_model") or {}
print(bootstrap.get("local_path", ""))
print("yes" if bootstrap.get("source_urls") else "no")
print(state.get("gateway", {}).get("listen_port", ""))
PY
)"
bootstrap_cached_path="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '1p')"
bootstrap_is_multipart="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '2p')"
gateway_port="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '3p')"

if [[ "${bootstrap_is_multipart}" != "yes" ]] && [[ -n "${bootstrap_cached_path}" ]]; then
  source_url="$(
    python3 - <<'PY' "${desired_state_path}"
import json
import sys
state = json.load(open(sys.argv[1]))
bootstrap = state.get("bootstrap_model") or {}
print(bootstrap.get("source_url", ""))
PY
)"
  if [[ -n "${source_url}" ]]; then
    download_to_cache_if_needed "${source_url}" "${bootstrap_cached_path}"
  fi
fi

payload_path="${tmp_dir}/apply-plane.json"
python3 - <<'PY' \
  "${tmp_dir}/desired-state.runtime.json" \
  "${payload_path}" \
  "${artifacts_root}"
import json
import pathlib
import sys

desired_state = json.loads(pathlib.Path(sys.argv[1]).read_text())
payload = {
    "desired_state": desired_state,
    "artifacts_root": sys.argv[3],
}
pathlib.Path(sys.argv[2]).write_text(json.dumps(payload))
PY

echo "[run-plane] applying ${plane_name}"
curl -fsS \
  -H 'Content-Type: application/json' \
  -X POST \
  --data-binary "@${payload_path}" \
  "${controller_url}/api/v1/planes" >/dev/null

echo "[run-plane] starting ${plane_name}"
curl -fsS \
  -X POST \
  "${controller_url}/api/v1/planes/${plane_name}/start" >/dev/null

echo "[run-plane] waiting for ${plane_name} readiness"
deadline=$((SECONDS + wait_seconds))
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

if [[ -z "${status_payload}" ]]; then
  echo "error: failed to read interaction status for ${plane_name}" >&2
  exit 1
fi

ready="$(
  python3 - <<'PY' "${status_payload}"
import json
import sys
payload = json.loads(sys.argv[1])
print("yes" if payload.get("ready") else "no")
PY
)"
if [[ "${ready}" != "yes" ]]; then
  echo "error: plane ${plane_name} did not become ready within ${wait_seconds}s" >&2
  printf '%s\n' "${status_payload}" >&2
  exit 1
fi

models_payload="$(curl -fsS "${controller_url}/api/v1/planes/${plane_name}/interaction/models")"

echo "plane=${plane_name}"
echo "ready=yes"
echo "controller_url=${controller_url}"
if [[ -n "${gateway_port}" ]]; then
  echo "gateway_url=http://127.0.0.1:${gateway_port}"
fi
echo "storage_root=${storage_root}"
echo "model_cache_root=${model_cache_root}"
if [[ "${force_cpu}" == "yes" ]]; then
  echo "runtime_mode=cpu"
fi
echo "models=${models_payload}"
