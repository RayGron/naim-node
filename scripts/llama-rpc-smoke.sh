#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build/local-debug}"
SMOKE_ROOT="${SMOKE_ROOT:-/tmp/comet-llama-rpc-smoke}"
MODEL_URL="${MODEL_URL:-https://huggingface.co/ggml-org/models/resolve/main/tinyllamas/stories260K.gguf}"
MODEL_PATH="$SMOKE_ROOT/model/stories260K.gguf"
CONTROL_ROOT="$SMOKE_ROOT/control"
INFER_CONFIG="$SMOKE_ROOT/infer-runtime.json"
WORKER_STATUS="$CONTROL_ROOT/worker-group/worker-smoke-a.json"
WORKER_LOG="$SMOKE_ROOT/worker.log"
INFER_LOG="$SMOKE_ROOT/infer.log"
RPC_SERVER_BIN="${COMET_RPC_SERVER_BIN:-$BUILD_DIR/bin/rpc-server}"
LLAMA_SERVER_BIN="${COMET_LLAMA_SERVER_BIN:-$BUILD_DIR/bin/llama-server}"
WORKER_BIN="$BUILD_DIR/comet-workerd"
INFER_BIN="$BUILD_DIR/comet-inferctl"

cleanup() {
  set +e
  if [[ -n "${INFER_PID:-}" ]]; then kill "$INFER_PID" >/dev/null 2>&1 || true; fi
  if [[ -n "${WORKER_PID:-}" ]]; then kill "$WORKER_PID" >/dev/null 2>&1 || true; fi
  wait >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$SMOKE_ROOT/model" "$CONTROL_ROOT/worker-group" "$SMOKE_ROOT/worker-private" "$SMOKE_ROOT/infer-logs"

if [[ ! -f "$MODEL_PATH" ]]; then
  curl -L --fail -o "$MODEL_PATH" "$MODEL_URL"
fi

cat > "$CONTROL_ROOT/active-model.json" <<EOF
{
  "model_id": "ggml-org/tinyllamas-stories260K",
  "served_model_name": "stories260k-rpc",
  "cached_runtime_model_path": "$MODEL_PATH",
  "cached_local_model_path": "$MODEL_PATH",
  "runtime_model_path": "$MODEL_PATH",
  "model_path": "$MODEL_PATH"
}
EOF

cat > "$CONTROL_ROOT/gateway-plan.json" <<'EOF'
{"version":1,"status":"applied"}
EOF

cat > "$INFER_CONFIG" <<EOF
{
  "plane": {"name": "llama-rpc-smoke", "control_root": "$CONTROL_ROOT"},
  "control": {"root": "$CONTROL_ROOT", "controller_url": "http://127.0.0.1:8080"},
  "gpu_nodes": [],
  "serving_workers": [],
  "inference": {
    "primary_infer_node": "local-hostd",
    "runtime_engine": "llama.cpp",
    "distributed_backend": "llama_rpc",
    "data_parallel_mode": "off",
    "data_parallel_lb_mode": "external",
    "api_server_count": 1,
    "worker_group_id": "smoke-group",
    "worker_selection_policy": "prefer-free-then-share",
    "net_if": "lo0",
    "models_root": "$SMOKE_ROOT/model",
    "model_cache_dir": "$SMOKE_ROOT/model",
    "gguf_cache_dir": "$SMOKE_ROOT/model",
    "infer_log_dir": "$SMOKE_ROOT/infer-logs",
    "api_port": 19082,
    "llama_port": 19081,
    "llama_ctx_size": 512,
    "llama_threads": 2,
    "llama_gpu_layers": 0,
    "rendezvous_port": 29500
  },
  "worker_group": {
    "group_id": "smoke-group",
    "infer_instance_name": "infer-llama-rpc-smoke",
    "distributed_backend": "llama_rpc",
    "rendezvous_host": "infer-llama-rpc-smoke",
    "rendezvous_port": 29500,
    "expected_workers": 1,
    "worker_selection_policy": "prefer-free-then-share",
    "members": [
      {
        "name": "worker-smoke-a",
        "node_name": "local-hostd",
        "gpu_device": "",
        "rank": 0,
        "replica_group_id": "rpc-group-0",
        "replica_index": 0,
        "replica_size": 1,
        "replica_leader": true,
        "data_parallel_rank": 0,
        "data_parallel_size": 1,
        "data_parallel_size_local": 1,
        "data_parallel_start_rank": 0,
        "data_parallel_api_endpoint": false,
        "data_parallel_head_address": "",
        "data_parallel_rpc_port": 0,
        "rpc_port": 50052,
        "rpc_endpoint": "127.0.0.1:50052",
        "colocated_with_primary_infer": true,
        "gpu_fraction": 1.0,
        "share_mode": "exclusive",
        "priority": 100,
        "preemptible": false,
        "enabled": true,
        "leader": true
      }
    ]
  },
  "gateway": {"listen_host": "127.0.0.1", "listen_port": 19080, "server_name": "llama-rpc-smoke.local"}
}
EOF

: > "$WORKER_LOG"
: > "$INFER_LOG"

COMET_RPC_SERVER_BIN="$RPC_SERVER_BIN" \
COMET_PLANE_NAME=llama-rpc-smoke \
COMET_INSTANCE_NAME=worker-smoke-a \
COMET_INSTANCE_ROLE=worker \
COMET_NODE_NAME=local-hostd \
COMET_CONTROL_ROOT="$CONTROL_ROOT" \
COMET_WORKER_RUNTIME_STATUS_PATH="$WORKER_STATUS" \
COMET_WORKER_BOOT_MODE=llama-rpc \
COMET_DISTRIBUTED_BACKEND=llama_rpc \
COMET_WORKER_RPC_HOST=127.0.0.1 \
COMET_WORKER_RPC_PORT=50052 \
COMET_WORKER_RPC_ENDPOINT=127.0.0.1:50052 \
COMET_WORKER_THREADS=2 \
COMET_WORKER_CTX_SIZE=512 \
COMET_PRIVATE_DISK_PATH="$SMOKE_ROOT/worker-private" \
"$WORKER_BIN" >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!

COMET_LLAMA_SERVER_BIN="$LLAMA_SERVER_BIN" \
COMET_CONTROL_ROOT="$CONTROL_ROOT" \
COMET_PLANE_NAME=llama-rpc-smoke \
COMET_INSTANCE_NAME=infer-llama-rpc-smoke \
COMET_INSTANCE_ROLE=infer \
COMET_NODE_NAME=local-hostd \
"$INFER_BIN" launch-runtime --config "$INFER_CONFIG" --backend auto >"$INFER_LOG" 2>&1 &
INFER_PID=$!

python3 - <<'PY'
import json, time, urllib.request

base = "http://127.0.0.1:19080"
for path in ["/health", "/v1/models"]:
    for _ in range(60):
        try:
            with urllib.request.urlopen(base + path, timeout=2) as r:
                print(path, r.status)
                print(r.read(400).decode("utf-8", "ignore"))
                break
        except Exception:
            time.sleep(1)
    else:
        raise SystemExit(f"timeout waiting for {path}")

req = urllib.request.Request(
    base + "/v1/chat/completions",
    data=json.dumps(
        {
            "model": "stories260k-rpc",
            "messages": [{"role": "user", "content": "Say hello in one short sentence."}],
            "max_tokens": 16,
            "temperature": 0.1,
        }
    ).encode("utf-8"),
    headers={"content-type": "application/json"},
)
with urllib.request.urlopen(req, timeout=30) as r:
    body = json.load(r)
    print("chat-ok", body["choices"][0]["message"]["content"])
PY

echo
echo "WORKER STATUS"
cat "$WORKER_STATUS"
echo
echo "INFER STATUS"
cat "$CONTROL_ROOT/runtime-status.json"
