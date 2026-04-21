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

RPC_SERVER_BIN="${NAIM_RPC_SERVER_BIN:-$BUILD_DIR/bin/rpc-server}"
LLAMA_SERVER_BIN="${NAIM_LLAMA_SERVER_BIN:-$BUILD_DIR/bin/llama-server}"
WORKER_BIN="$BUILD_DIR/naim-workerd"
INFER_BIN="$BUILD_DIR/naim-inferctl"
BENCH_BIN="$ROOT_DIR/scripts/benchmark-openai-multi-base.sh"
DEVTOOL_BIN="$ROOT_DIR/scripts/naim-devtool.sh"

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

"$DEVTOOL_BIN" prepare-llama-rpc-replicas \
  --bench-root "$BENCH_ROOT" \
  --model-path "$MODEL_PATH" \
  --served-model "$SERVED_MODEL" \
  --ctx-size "$CTX_SIZE" \
  --threads "$THREADS" \
  --gpu-layers "$GPU_LAYERS" \
  --replica-count "$REPLICA_COUNT" \
  --base-port "$BASE_PORT" \
  --rpc-base-port "$RPC_BASE_PORT" \
  --max-num-seqs "$MAX_NUM_SEQS" \
  --gpu-memory-utilization "$GPU_MEMORY_UTILIZATION"

: > "$BENCH_ROOT/infer.pids"
: > "$BENCH_ROOT/workers.pids"

for ((idx=0; idx<REPLICA_COUNT; idx++)); do
  replica_root="$BENCH_ROOT/replica-$idx"
  gateway_port=$((BASE_PORT + idx * 100))
  rpc_port=$((RPC_BASE_PORT + idx * 100))

  CUDA_VISIBLE_DEVICES="${GPUS[$idx]}" \
  NAIM_RPC_SERVER_BIN="$RPC_SERVER_BIN" \
  NAIM_PLANE_NAME="llama-rpc-replica-$idx" \
  NAIM_INSTANCE_NAME="worker-$idx" \
  NAIM_INSTANCE_ROLE=worker \
  NAIM_NODE_NAME=local-hostd \
  NAIM_CONTROL_ROOT="$replica_root/control" \
  NAIM_WORKER_RUNTIME_STATUS_PATH="$replica_root/control/worker-group/worker-$idx.json" \
  NAIM_WORKER_BOOT_MODE=llama-rpc \
  NAIM_DISTRIBUTED_BACKEND=llama_rpc \
  NAIM_WORKER_RPC_HOST=127.0.0.1 \
  NAIM_WORKER_RPC_PORT="$rpc_port" \
  NAIM_WORKER_RPC_ENDPOINT="127.0.0.1:$rpc_port" \
  NAIM_WORKER_THREADS="$THREADS" \
  NAIM_WORKER_CTX_SIZE="$CTX_SIZE" \
  NAIM_PRIVATE_DISK_PATH="$replica_root/worker-private" \
  "$WORKER_BIN" > "$replica_root/worker.log" 2>&1 &
  echo $! >> "$BENCH_ROOT/workers.pids"

  CUDA_VISIBLE_DEVICES='' \
  NAIM_INFER_PROFILES_PATH="$ROOT_DIR/runtime/infer/runtime-profiles.json" \
  NAIM_LLAMA_SERVER_BIN="$LLAMA_SERVER_BIN" \
  NAIM_CONTROL_ROOT="$replica_root/control" \
  NAIM_PLANE_NAME="llama-rpc-replica-$idx" \
  NAIM_INSTANCE_NAME="infer-llama-rpc-replica-$idx" \
  NAIM_INSTANCE_ROLE=infer \
  NAIM_NODE_NAME=local-hostd \
  "$INFER_BIN" launch-runtime --config "$replica_root/infer-runtime.json" --backend auto \
    > "$replica_root/infer.log" 2>&1 &
  echo $! >> "$BENCH_ROOT/infer.pids"
done

declare -a BASE_URLS=()
for ((idx=0; idx<REPLICA_COUNT; idx++)); do
  gateway_port=$((BASE_PORT + idx * 100))
  base_url="http://127.0.0.1:$gateway_port"
  for path in /health /v1/models; do
    ready=no
    for _ in $(seq 1 180); do
      if curl -fsS --max-time 3 "${base_url}${path}" >/dev/null 2>&1; then
        printf '%s %s\n' "${path}" "200"
        ready=yes
        break
      fi
      sleep 1
    done
    if [[ "${ready}" != "yes" ]]; then
      echo "timeout waiting for ${base_url}${path}" >&2
      exit 1
    fi
  done
  BASE_URLS+=("$base_url")
done
base_urls_csv="$(IFS=,; echo "${BASE_URLS[*]}")"

"$BENCH_BIN" \
  --base-urls "$base_urls_csv" \
  --model "$SERVED_MODEL" \
  --concurrency "$WARMUP_CONCURRENCY" \
  --requests-per-worker "$WARMUP_REQUESTS_PER_WORKER" \
  --max-tokens "$WARMUP_MAX_TOKENS" \
  --timeout 300 \
  --unique-prompts | tee "$BENCH_ROOT/warmup.json"

"$BENCH_BIN" \
  --base-urls "$base_urls_csv" \
  --model "$SERVED_MODEL" \
  --concurrency "$FIXED_CONCURRENCY" \
  --requests-per-worker "$FIXED_REQUESTS_PER_WORKER" \
  --max-tokens "$FIXED_MAX_TOKENS" \
  --timeout 180 \
  --unique-prompts | tee "$BENCH_ROOT/fixed.json"

"$BENCH_BIN" \
  --base-urls "$base_urls_csv" \
  --model "$SERVED_MODEL" \
  --concurrency "$SCALED_CONCURRENCY" \
  --requests-per-worker "$SCALED_REQUESTS_PER_WORKER" \
  --max-tokens "$SCALED_MAX_TOKENS" \
  --timeout 180 \
  --unique-prompts | tee "$BENCH_ROOT/scaled.json"
