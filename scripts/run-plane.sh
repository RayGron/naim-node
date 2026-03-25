#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run-plane.sh [plane-name] [--controller-url <url>] [--artifacts-root <path>] [--wait-seconds <n>] [--cpu] [--no-wait]

Loads config/<plane-name>/desired-state.json into the local controller, starts the plane,
and waits until interaction/status reports ready. The desired state is taken directly from the repo.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

plane_name="qwen35-9b-min"
controller_url="http://127.0.0.1:18080"
artifacts_root="/var/lib/comet-node/artifacts"
wait_seconds="900"
force_cpu="no"
wait_for_ready="yes"

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
    --no-wait)
      wait_for_ready="no"
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

resolve_docker() {
  if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    echo "docker"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n docker version >/dev/null 2>&1; then
    echo "sudo -n docker"
    return 0
  fi
  echo ""
}

resolve_sudo_prefix() {
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    echo "sudo -n"
    return 0
  fi
  echo ""
}

hostd_report_observation() {
  local sudo_prefix="$1"
  local hostd_bin="${repo_root}/build/linux/x64/comet-hostd"
  local db_path="${COMET_NODE_CONTROLLER_DB:-/var/lib/comet-node/controller.sqlite}"
  local node_name="${COMET_NODE_NAME:-local-hostd}"
  local state_root="${COMET_NODE_HOSTD_STATE_ROOT:-/var/lib/comet-node/hostd-state}"
  if [[ ! -x "${hostd_bin}" || ! -f "${db_path}" ]]; then
    return 0
  fi
  if [[ -n "${sudo_prefix}" ]]; then
    ${sudo_prefix} "${hostd_bin}" report-observed-state \
      --db "${db_path}" \
      --node "${node_name}" \
      --state-root "${state_root}" >/dev/null 2>&1 || true
    return 0
  fi
  "${hostd_bin}" report-observed-state \
    --db "${db_path}" \
    --node "${node_name}" \
    --state-root "${state_root}" >/dev/null 2>&1 || true
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
sudo_prefix="$(resolve_sudo_prefix)"
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
runtime_engine = (state.get("inference") or {}).get("runtime_engine", "llama.cpp")

def sanitize(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)

single_source_url = bootstrap.get("source_url")
source_urls = bootstrap.get("source_urls") or []
if single_source_url and not bootstrap.get("local_path"):
    filename = bootstrap.get("target_filename")
    if not filename:
      parsed = urlparse(single_source_url)
      filename = pathlib.Path(parsed.path).name
    existing_path = None
    for candidate in sorted(model_cache_root.rglob(filename)):
        if candidate.is_file():
            existing_path = candidate
            break
    if existing_path is not None:
        bootstrap["local_path"] = str(existing_path)
    else:
        model_dir_name = sanitize(bootstrap.get("model_id") or plane_name)
        bootstrap["local_path"] = str(model_cache_root / model_dir_name / filename)

if runtime_engine == "vllm" and not bootstrap.get("local_path") and not single_source_url and not source_urls:
    model_id = bootstrap.get("model_id") or plane_name
    model_dir = model_cache_root / "vllm"
    for part in model_id.split("/"):
        model_dir /= sanitize(part)
    bootstrap["local_path"] = str(model_dir)

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
print((state.get("inference") or {}).get("runtime_engine", "llama.cpp"))
print(bootstrap.get("model_id", ""))
print(bootstrap.get("source_url", ""))
PY
)"
bootstrap_cached_path="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '1p')"
bootstrap_is_multipart="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '2p')"
gateway_port="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '3p')"
runtime_engine="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '4p')"
bootstrap_model_id="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '5p')"
bootstrap_source_url="$(printf '%s\n' "${bootstrap_local_path}" | sed -n '6p')"

if [[ "${bootstrap_is_multipart}" != "yes" ]] && [[ -n "${bootstrap_cached_path}" ]]; then
  if [[ "${runtime_engine}" == "vllm" ]] && [[ -z "${bootstrap_source_url}" ]]; then
    docker_cmd="$(resolve_docker)"
    if [[ -z "${docker_cmd}" ]]; then
      echo "error: docker is required to prepare a vLLM model cache" >&2
      exit 1
    fi
    if ! ${docker_cmd} image inspect comet/worker-vllm-runtime:dev >/dev/null 2>&1; then
      echo "error: comet/worker-vllm-runtime:dev is missing. Run ./scripts/install-single-node.sh --with-vllm-worker first." >&2
      exit 1
    fi
    if [[ -f "${bootstrap_cached_path}/config.json" ]]; then
      echo "[run-plane] reusing cached vLLM model ${bootstrap_cached_path}"
    else
      echo "[run-plane] downloading Hugging Face model ${bootstrap_model_id} to shared cache ${bootstrap_cached_path}"
      mkdir -p "$(dirname -- "${bootstrap_cached_path}")"
      model_cache_mount="$(dirname -- "${model_cache_root}")"
      model_cache_target="${bootstrap_cached_path}"
      ${docker_cmd} run --rm \
        -v "${model_cache_mount}:${model_cache_mount}" \
        comet/worker-vllm-runtime:dev \
        python3 -c "from huggingface_hub import snapshot_download; snapshot_download(repo_id='${bootstrap_model_id}', local_dir='${model_cache_target}', local_dir_use_symlinks=False)"
    fi
  elif [[ -n "${bootstrap_source_url}" ]]; then
    download_to_cache_if_needed "${bootstrap_source_url}" "${bootstrap_cached_path}"
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

if [[ "${wait_for_ready}" != "yes" ]]; then
  echo "plane=${plane_name}"
  echo "started=yes"
  echo "controller_url=${controller_url}"
  if [[ -n "${gateway_port}" ]]; then
    echo "gateway_url=http://127.0.0.1:${gateway_port}"
  fi
  echo "storage_root=${storage_root}"
  echo "model_cache_root=${model_cache_root}"
  if [[ "${force_cpu}" == "yes" ]]; then
    echo "runtime_mode=cpu"
  fi
  exit 0
fi

echo "[run-plane] waiting for ${plane_name} readiness"
deadline=$((SECONDS + wait_seconds))
status_payload=""
while (( SECONDS < deadline )); do
  hostd_report_observation "${sudo_prefix}"
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

hostd_report_observation "${sudo_prefix}"
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
