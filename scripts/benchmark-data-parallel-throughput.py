#!/usr/bin/env python3
import argparse
import json
import threading
import time
import urllib.request
from dataclasses import dataclass


@dataclass
class RunStats:
    requests: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    failures: int = 0


def make_url(controller_url: str, plane_name: str) -> str:
    base = controller_url.rstrip("/")
    return f"{base}/api/v1/planes/{plane_name}/interaction/chat/completions"


def request_once(url: str, model: str, prompt: str, max_tokens: int, timeout: int) -> dict:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected status {response.status}")
        return json.loads(response.read().decode("utf-8"))


def worker_thread(
    url: str,
    model: str,
    prompt: str,
    max_tokens: int,
    timeout: int,
    requests_per_worker: int,
    stats: RunStats,
    lock: threading.Lock,
) -> None:
    local = RunStats()
    for _ in range(requests_per_worker):
        try:
            payload = request_once(url, model, prompt, max_tokens, timeout)
        except Exception:
            local.failures += 1
            continue
        usage = payload.get("usage") or {}
        local.requests += 1
        local.prompt_tokens += int(usage.get("prompt_tokens") or 0)
        local.completion_tokens += int(usage.get("completion_tokens") or 0)
        local.total_tokens += int(usage.get("total_tokens") or 0)

    with lock:
        stats.requests += local.requests
        stats.prompt_tokens += local.prompt_tokens
        stats.completion_tokens += local.completion_tokens
        stats.total_tokens += local.total_tokens
        stats.failures += local.failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark aggregate throughput for comet-node interaction endpoint."
    )
    parser.add_argument("--controller", required=True, help="Controller base URL")
    parser.add_argument("--plane", required=True, help="Plane name")
    parser.add_argument("--model", required=True, help="Model name to send in requests")
    parser.add_argument(
        "--prompt",
        default="Reply with a concise paragraph about throughput benchmarking.",
        help="Prompt text sent to each request",
    )
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--requests-per-worker", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    url = make_url(args.controller, args.plane)
    stats = RunStats()
    lock = threading.Lock()
    threads = []
    started_at = time.time()

    for _ in range(max(1, args.concurrency)):
        thread = threading.Thread(
            target=worker_thread,
            args=(
                url,
                args.model,
                args.prompt,
                max(1, args.max_tokens),
                max(1, args.timeout),
                max(1, args.requests_per_worker),
                stats,
                lock,
            ),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    elapsed = max(0.001, time.time() - started_at)
    report = {
        "controller": args.controller,
        "plane": args.plane,
        "model": args.model,
        "concurrency": args.concurrency,
        "requests_per_worker": args.requests_per_worker,
        "elapsed_sec": round(elapsed, 3),
        "successful_requests": stats.requests,
        "failed_requests": stats.failures,
        "prompt_tokens": stats.prompt_tokens,
        "completion_tokens": stats.completion_tokens,
        "total_tokens": stats.total_tokens,
        "aggregate_tokens_per_sec": round(stats.total_tokens / elapsed, 3),
        "aggregate_completion_tokens_per_sec": round(stats.completion_tokens / elapsed, 3),
    }
    print(json.dumps(report, indent=2))
    return 0 if stats.requests > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
