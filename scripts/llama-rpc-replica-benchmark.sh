#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build/local-debug}"
MODEL_PATH="${MODEL_PATH:-/mnt/shared-storage/models/gguf/Qwen/Qwen3.5-9B/Qwen3.5-9B-Q4_K_M.gguf}"
SERVED_MODEL="${SERVED_MODEL:-qwen3.5-9b-q4km-rpc}"
THREADS="${THREADS:-8}"
CTX_SIZE="${CTX_SIZE:-8192}"
GPU_LAYERS="${GPU_LAYERS:-99}"
MAX_NUM_SEQS="${MAX_NUM_SEQS:-16}"
GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.9}"
GPU_LIST="${GPU_LIST:-0,2,3}"
REPLICA_COUNT="${REPLICA_COUNT:-3}"
BASE_PORT="${BASE_PORT:-19080}"
RPC_BASE_PORT="${RPC_BASE_PORT:-19100}"
WARMUP_CONCURRENCY="${WARMUP_CONCURRENCY:-1}"
WARMUP_REQUESTS_PER_WORKER="${WARMUP_REQUESTS_PER_WORKER:-3}"
WARMUP_MAX_TOKENS="${WARMUP_MAX_TOKENS:-64}"
FIXED_CONCURRENCY="${FIXED_CONCURRENCY:-12}"
FIXED_REQUESTS_PER_WORKER="${FIXED_REQUESTS_PER_WORKER:-3}"
FIXED_MAX_TOKENS="${FIXED_MAX_TOKENS:-128}"
SCALED_CONCURRENCY="${SCALED_CONCURRENCY:-$((12 * REPLICA_COUNT))}"
SCALED_REQUESTS_PER_WORKER="${SCALED_REQUESTS_PER_WORKER:-3}"
SCALED_MAX_TOKENS="${SCALED_MAX_TOKENS:-128}"

if [[ -w "/mnt/shared-storage/backups/${USER:-$(id -un)}" ]]; then
  BENCH_ROOT_DEFAULT="/mnt/shared-storage/backups/${USER:-$(id -un)}/tmp/llama-rpc-replicas"
elif [[ -w "/mnt/shared-storage/backups/baal" ]]; then
  BENCH_ROOT_DEFAULT="/mnt/shared-storage/backups/baal/tmp/llama-rpc-replicas"
else
  BENCH_ROOT_DEFAULT="/tmp/llama-rpc-replicas"
fi
BENCH_ROOT="${BENCH_ROOT:-$BENCH_ROOT_DEFAULT}"

RPC_SERVER_BIN="${COMET_RPC_SERVER_BIN:-$BUILD_DIR/bin/rpc-server}"
LLAMA_SERVER_BIN="${COMET_LLAMA_SERVER_BIN:-$BUILD_DIR/bin/llama-server}"
WORKER_BIN="$BUILD_DIR/comet-workerd"
INFER_BIN="$BUILD_DIR/comet-inferctl"
BENCH_BIN="$ROOT_DIR/scripts/benchmark-openai-multi-base.py"

cleanup() {
  set +e
  if [[ -f "$BENCH_ROOT/infer.pids" ]]; then
    while read -r pid; do
      kill "$pid" >/dev/null 2>&1 || true
    done < "$BENCH_ROOT/infer.pids"
  fi
  if [[ -f "$BENCH_ROOT/workers.pids" ]]; then
    while read -r pid; do
      kill "$pid" >/dev/null 2>&1 || true
    done < "$BENCH_ROOT/workers.pids"
  fi
  wait >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$BENCH_ROOT"
rm -rf "$BENCH_ROOT"/*

IFS=',' read -r -a GPUS <<< "$GPU_LIST"
if (( ${#GPUS[@]} < REPLICA_COUNT )); then
  echo "GPU_LIST must provide at least REPLICA_COUNT entries" >&2
  exit 1
fi

python3 - <<'PY' \
  "$BENCH_ROOT" "$MODEL_PATH" "$SERVED_MODEL" "$CTX_SIZE" "$THREADS" "$GPU_LAYERS" \
  "$REPLICA_COUNT" "$BASE_PORT" "$RPC_BASE_PORT" "$MAX_NUM_SEQS" "$GPU_MEMORY_UTILIZATION"
import json
import os
import sys

(
    bench_root,
    model_path,
    served_model,
    ctx_size,
    threads,
    gpu_layers,
    replica_count,
    base_port,
    rpc_base_port,
    max_num_seqs,
    gpu_memory_utilization,
) = sys.argv[1:]
ctx_size = int(ctx_size)
threads = int(threads)
gpu_layers = int(gpu_layers)
replica_count = int(replica_count)
base_port = int(base_port)
rpc_base_port = int(rpc_base_port)
max_num_seqs = int(max_num_seqs)
gpu_memory_utilization = float(gpu_memory_utilization)

for idx in range(replica_count):
    replica_root = os.path.join(bench_root, f"replica-{idx}")
    control_root = os.path.join(replica_root, "control")
    os.makedirs(os.path.join(control_root, "worker-group"), exist_ok=True)
    os.makedirs(os.path.join(replica_root, "worker-private"), exist_ok=True)
    os.makedirs(os.path.join(replica_root, "infer-logs"), exist_ok=True)
    with open(os.path.join(control_root, "active-model.json"), "w") as fh:
        json.dump(
            {
                "model_id": "Qwen/Qwen3.5-9B",
                "served_model_name": served_model,
                "cached_runtime_model_path": model_path,
                "cached_local_model_path": model_path,
                "runtime_model_path": model_path,
                "model_path": model_path,
            },
            fh,
        )
    with open(os.path.join(control_root, "gateway-plan.json"), "w") as fh:
        json.dump({"version": 1, "status": "applied"}, fh)

    gateway_port = base_port + idx * 100
    llama_port = gateway_port + 1
    api_port = gateway_port + 2
    rpc_port = rpc_base_port + idx * 100

    config = {
        "plane": {"name": f"llama-rpc-replica-{idx}", "control_root": control_root},
        "control": {"root": control_root, "controller_url": "http://127.0.0.1:18080"},
        "gpu_nodes": [],
        "serving_workers": [],
        "inference": {
            "primary_infer_node": "local-hostd",
            "runtime_engine": "llama.cpp",
            "distributed_backend": "llama_rpc",
            "data_parallel_mode": "off",
            "data_parallel_lb_mode": "external",
            "api_server_count": 1,
            "worker_group_id": f"llama-rpc-group-{idx}",
            "worker_selection_policy": "prefer-free-then-share",
            "net_if": "lo",
            "models_root": os.path.join(replica_root, "models"),
            "model_cache_dir": os.path.join(replica_root, "models", "cache"),
            "gguf_cache_dir": os.path.dirname(model_path),
            "infer_log_dir": os.path.join(replica_root, "infer-logs"),
            "api_port": api_port,
            "llama_port": llama_port,
            "max_model_len": ctx_size,
            "max_num_seqs": max_num_seqs,
            "gpu_memory_utilization": gpu_memory_utilization,
            "llama_ctx_size": ctx_size,
            "llama_threads": threads,
            "llama_gpu_layers": gpu_layers,
            "rendezvous_port": 29500 + idx,
        },
        "worker_group": {
            "group_id": f"llama-rpc-group-{idx}",
            "infer_instance_name": f"infer-llama-rpc-replica-{idx}",
            "distributed_backend": "llama_rpc",
            "rendezvous_host": f"infer-llama-rpc-replica-{idx}",
            "rendezvous_port": 29500 + idx,
            "expected_workers": 1,
            "worker_selection_policy": "prefer-free-then-share",
            "members": [
                {
                    "name": f"worker-{idx}",
                    "instance_name": f"worker-{idx}",
                    "node_name": "local-hostd",
                    "gpu_device": "",
                    "rank": 0,
                    "replica_group_id": f"llama-rpc-group-{idx}",
                    "replica_index": 0,
                    "replica_size": 1,
                    "replica_leader": True,
                    "data_parallel_rank": 0,
                    "data_parallel_size": 1,
                    "data_parallel_size_local": 1,
                    "data_parallel_start_rank": 0,
                    "data_parallel_api_endpoint": False,
                    "data_parallel_head_address": "",
                    "data_parallel_rpc_port": rpc_port,
                    "rpc_port": rpc_port,
                    "rpc_endpoint": f"127.0.0.1:{rpc_port}",
                    "colocated_with_primary_infer": True,
                    "gpu_fraction": 1.0,
                    "share_mode": "exclusive",
                    "priority": 100,
                    "preemptible": False,
                    "enabled": True,
                    "leader": True,
                }
            ],
        },
        "gateway": {
            "listen_host": "127.0.0.1",
            "listen_port": gateway_port,
            "server_name": f"llama-rpc-replica-{idx}.local",
        },
    }
    with open(os.path.join(replica_root, "infer-runtime.json"), "w") as fh:
        json.dump(config, fh)
PY

: > "$BENCH_ROOT/infer.pids"
: > "$BENCH_ROOT/workers.pids"

for ((idx=0; idx<REPLICA_COUNT; idx++)); do
  replica_root="$BENCH_ROOT/replica-$idx"
  gateway_port=$((BASE_PORT + idx * 100))
  rpc_port=$((RPC_BASE_PORT + idx * 100))

  CUDA_VISIBLE_DEVICES="${GPUS[$idx]}" \
  COMET_RPC_SERVER_BIN="$RPC_SERVER_BIN" \
  COMET_PLANE_NAME="llama-rpc-replica-$idx" \
  COMET_INSTANCE_NAME="worker-$idx" \
  COMET_INSTANCE_ROLE=worker \
  COMET_NODE_NAME=local-hostd \
  COMET_CONTROL_ROOT="$replica_root/control" \
  COMET_WORKER_RUNTIME_STATUS_PATH="$replica_root/control/worker-group/worker-$idx.json" \
  COMET_WORKER_BOOT_MODE=llama-rpc \
  COMET_DISTRIBUTED_BACKEND=llama_rpc \
  COMET_WORKER_RPC_HOST=127.0.0.1 \
  COMET_WORKER_RPC_PORT="$rpc_port" \
  COMET_WORKER_RPC_ENDPOINT="127.0.0.1:$rpc_port" \
  COMET_WORKER_THREADS="$THREADS" \
  COMET_WORKER_CTX_SIZE="$CTX_SIZE" \
  COMET_PRIVATE_DISK_PATH="$replica_root/worker-private" \
  "$WORKER_BIN" > "$replica_root/worker.log" 2>&1 &
  echo $! >> "$BENCH_ROOT/workers.pids"

  CUDA_VISIBLE_DEVICES='' \
  COMET_INFER_PROFILES_PATH="$ROOT_DIR/runtime/infer/runtime-profiles.json" \
  COMET_LLAMA_SERVER_BIN="$LLAMA_SERVER_BIN" \
  COMET_CONTROL_ROOT="$replica_root/control" \
  COMET_PLANE_NAME="llama-rpc-replica-$idx" \
  COMET_INSTANCE_NAME="infer-llama-rpc-replica-$idx" \
  COMET_INSTANCE_ROLE=infer \
  COMET_NODE_NAME=local-hostd \
  "$INFER_BIN" launch-runtime --config "$replica_root/infer-runtime.json" --backend auto \
    > "$replica_root/infer.log" 2>&1 &
  echo $! >> "$BENCH_ROOT/infer.pids"
done

declare -a BASE_URL_ARGS=()
for ((idx=0; idx<REPLICA_COUNT; idx++)); do
  gateway_port=$((BASE_PORT + idx * 100))
  base_url="http://127.0.0.1:$gateway_port"
  for path in /health /v1/models; do
    python3 - <<'PY' "$base_url" "$path"
import sys
import time
import urllib.request

base_url, path = sys.argv[1], sys.argv[2]
last_error = None
for _ in range(180):
    try:
        with urllib.request.urlopen(base_url + path, timeout=3) as response:
            print(path, response.status)
            break
    except Exception as exc:
        last_error = exc
        time.sleep(1)
else:
    raise SystemExit(f"timeout waiting for {base_url}{path}: {last_error}")
PY
  done
  BASE_URL_ARGS+=(--base-url "$base_url")
done

python3 "$BENCH_BIN" \
  "${BASE_URL_ARGS[@]}" \
  --model "$SERVED_MODEL" \
  --concurrency "$WARMUP_CONCURRENCY" \
  --requests-per-worker "$WARMUP_REQUESTS_PER_WORKER" \
  --max-tokens "$WARMUP_MAX_TOKENS" \
  --timeout 300 \
  --unique-prompts | tee "$BENCH_ROOT/warmup.json"

python3 "$BENCH_BIN" \
  "${BASE_URL_ARGS[@]}" \
  --model "$SERVED_MODEL" \
  --concurrency "$FIXED_CONCURRENCY" \
  --requests-per-worker "$FIXED_REQUESTS_PER_WORKER" \
  --max-tokens "$FIXED_MAX_TOKENS" \
  --timeout 180 \
  --unique-prompts | tee "$BENCH_ROOT/fixed.json"

python3 "$BENCH_BIN" \
  "${BASE_URL_ARGS[@]}" \
  --model "$SERVED_MODEL" \
  --concurrency "$SCALED_CONCURRENCY" \
  --requests-per-worker "$SCALED_REQUESTS_PER_WORKER" \
  --max-tokens "$SCALED_MAX_TOKENS" \
  --timeout 180 \
  --unique-prompts | tee "$BENCH_ROOT/scaled.json"
