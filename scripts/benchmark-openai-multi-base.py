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


def request_once(base_url: str, model: str, prompt: str, max_tokens: int, timeout: int) -> dict:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + "/v1/chat/completions",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected status {response.status}")
        return json.loads(response.read().decode("utf-8"))


def build_prompt(base_prompt: str, request_index: int, unique_prompts: bool) -> str:
    if not unique_prompts:
        return base_prompt
    return (
        f"{base_prompt}\n\n"
        f"Unique request marker: {request_index}.\n"
        "Use the marker only as context disambiguation and do not mention it in the answer."
    )


def worker_thread(
    base_urls: list[str],
    model: str,
    base_prompt: str,
    max_tokens: int,
    timeout: int,
    requests_per_worker: int,
    unique_prompts: bool,
    next_request_index: list[int],
    index_lock: threading.Lock,
    stats: RunStats,
    stats_lock: threading.Lock,
) -> None:
    local = RunStats()
    for _ in range(requests_per_worker):
        with index_lock:
            request_index = next_request_index[0]
            next_request_index[0] += 1
        prompt = build_prompt(base_prompt, request_index, unique_prompts)
        base_url = base_urls[request_index % len(base_urls)]
        try:
            payload = request_once(base_url, model, prompt, max_tokens, timeout)
        except Exception:
            local.failures += 1
            continue
        usage = payload.get("usage") or {}
        local.requests += 1
        local.prompt_tokens += int(usage.get("prompt_tokens") or 0)
        local.completion_tokens += int(usage.get("completion_tokens") or 0)
        local.total_tokens += int(usage.get("total_tokens") or 0)

    with stats_lock:
        stats.requests += local.requests
        stats.prompt_tokens += local.prompt_tokens
        stats.completion_tokens += local.completion_tokens
        stats.total_tokens += local.total_tokens
        stats.failures += local.failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark aggregate throughput across multiple OpenAI-compatible base URLs."
    )
    parser.add_argument(
        "--base-url",
        dest="base_urls",
        action="append",
        required=True,
        help="Base URL to target. Pass multiple times to shard requests across replicas.",
    )
    parser.add_argument("--model", required=True, help="Model name to send in requests")
    parser.add_argument(
        "--prompt",
        default="Reply with a concise paragraph about throughput benchmarking.",
        help="Base prompt text sent to requests",
    )
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--requests-per-worker", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--unique-prompts",
        action="store_true",
        help="Append a unique marker to every request prompt to defeat prefix-sharing effects.",
    )
    args = parser.parse_args()

    stats = RunStats()
    stats_lock = threading.Lock()
    index_lock = threading.Lock()
    next_request_index = [0]
    threads = []
    started_at = time.time()

    for _ in range(max(1, args.concurrency)):
        thread = threading.Thread(
            target=worker_thread,
            args=(
                args.base_urls,
                args.model,
                args.prompt,
                max(1, args.max_tokens),
                max(1, args.timeout),
                max(1, args.requests_per_worker),
                args.unique_prompts,
                next_request_index,
                index_lock,
                stats,
                stats_lock,
            ),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    elapsed = max(0.001, time.time() - started_at)
    report = {
        "base_urls": args.base_urls,
        "model": args.model,
        "concurrency": args.concurrency,
        "requests_per_worker": args.requests_per_worker,
        "unique_prompts": args.unique_prompts,
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
