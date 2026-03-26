#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def request(method, url, payload=None, timeout=60):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(body) if body else None
            return response.status, dict(response.headers.items()), parsed
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        parsed = json.loads(body) if body else None
        return exc.code, dict(exc.headers.items()), parsed


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def assert_error_envelope(payload, expected_code=None):
    assert_true(isinstance(payload, dict), f"expected JSON object error payload, got: {payload!r}")
    assert_true(payload.get("status") == "error", f"expected status=error, got: {payload}")
    assert_true(isinstance(payload.get("error"), dict), f"expected error object, got: {payload}")
    assert_true(bool(payload.get("request_id")), f"expected request_id, got: {payload}")
    if expected_code is not None:
        assert_true(
            payload["error"].get("code") == expected_code,
            f"expected error.code={expected_code}, got: {payload}",
        )


def parse_sse(url, payload, timeout=180):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    events = []
    with urllib.request.urlopen(req, timeout=timeout) as response:
        event_name = None
        data_lines = []
        while True:
            raw_line = response.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            if line.startswith("event: "):
                event_name = line[len("event: ") :]
            elif line.startswith("data: "):
                data_lines.append(line[len("data: ") :])
            elif line == "":
                data_blob = "\n".join(data_lines)
                events.append((event_name, data_blob))
                if data_blob == "[DONE]":
                    break
                event_name = None
                data_lines = []
    return events


def maybe_run_maglev(controller_url, plane_name, maglev_repo):
    maglev_binary = os.path.join(
        maglev_repo,
        "target",
        "linux-x64",
        "debug",
        "maglev",
    )
    if not os.path.isfile(maglev_binary):
        return "skipped: maglev binary not found"

    config = {
        "defaultBackendMode": "openai_compat",
        "defaultOpenAiCompatProfile": "comet-node-contract",
        "openaiCompat": {
            "requestTimeoutMs": 120000,
            "structuredRequestTimeoutMs": 180000,
            "chatResponseProfile": {"temperature": 0.2, "maxTokens": 128},
            "jsonResponseProfile": {"temperature": 0.2, "maxTokens": [512, 1024]},
            "promptProfiles": {
                "chat": {"systemPrompt": "Reply directly and briefly."},
                "taskPlan": {
                    "systemPrompt": "Return JSON only. Schema: {\"summary\":string,\"steps\":[]}. No markdown."
                },
                "edit": {
                    "systemPrompt": "Return JSON only. Schema: []. No markdown."
                },
                "commit": {
                    "systemPrompt": "Return JSON only. Schema: {\"title\":string,\"body\":string|null}."
                },
                "deploy": {
                    "systemPrompt": "Return JSON only. Schema: {\"host\":string,\"repoPath\":string,\"branch\":string,\"restartCommand\":string|null}."
                },
                "status": {
                    "systemPrompt": "Return JSON only. Schema: {\"message\":string}."
                },
                "repair": {"systemPrompt": "Repair malformed JSON. Return JSON only."},
            },
        },
        "openaiCompatProfiles": {
            "comet-node-contract": {
                "apiBaseUrl": f"{controller_url}/api/v1/planes/{plane_name}/interaction",
                "model": plane_name,
                "chatModel": plane_name,
            }
        },
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(config, handle, indent=2)
        config_path = handle.name
    try:
        result = subprocess.run(
            [
                maglev_binary,
                "--config-file",
                config_path,
                "--endpoint-profile",
                "comet-node-contract",
                "--task",
                "Какая модель сейчас активна?",
            ],
            cwd=maglev_repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=180,
            check=False,
        )
        assert_true(result.returncode == 0, f"maglev smoke failed:\n{result.stdout}")
        assert_true(result.stdout.strip(), "maglev smoke returned empty output")
        return "ok"
    finally:
        os.unlink(config_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--controller-url", default="http://127.0.0.1:18080")
    parser.add_argument("--plane", required=True)
    parser.add_argument("--compute-plane")
    parser.add_argument("--maglev-repo", default="")
    args = parser.parse_args()

    controller_url = args.controller_url.rstrip("/")
    plane = args.plane

    status_url = f"{controller_url}/api/v1/planes/{plane}/interaction/status"
    models_url = f"{controller_url}/api/v1/planes/{plane}/interaction/models"
    chat_url = f"{controller_url}/api/v1/planes/{plane}/interaction/chat/completions"
    stream_url = f"{controller_url}/api/v1/planes/{plane}/interaction/chat/completions/stream"

    status_code, headers, payload = request("GET", status_url)
    assert_true(status_code == 200, f"status endpoint failed: {status_code} {payload}")
    assert_true(payload.get("ready") in {True, False}, f"status missing ready: {payload}")
    assert_true(payload.get("degraded") in {True, False}, f"status missing degraded: {payload}")
    assert_true(isinstance(payload.get("reason"), str) and payload["reason"], f"status missing reason: {payload}")
    assert_true(bool(payload.get("request_id")), f"status missing request_id: {payload}")
    assert_true(isinstance(payload.get("comet"), dict), f"status missing comet metadata: {payload}")
    assert_true(headers.get("X-Comet-Request-Id") or headers.get("x-comet-request-id"), "status header missing request id")

    status_code, headers, payload = request("GET", models_url)
    assert_true(status_code == 200, f"models endpoint failed: {status_code} {payload}")
    assert_true(isinstance(payload.get("data"), list), f"models payload missing data list: {payload}")
    assert_true(bool(payload.get("request_id")), f"models missing request_id: {payload}")
    assert_true(isinstance(payload.get("comet"), dict), f"models missing comet metadata: {payload}")
    assert_true(headers.get("X-Comet-Request-Id") or headers.get("x-comet-request-id"), "models header missing request id")

    active_model = payload["data"][0]["id"]

    status_code, headers, payload = request(
        "POST",
        chat_url,
        {
            "model": active_model,
            "messages": [{"role": "user", "content": "Назови активную модель одним коротким предложением."}],
        },
        timeout=180,
    )
    assert_true(status_code == 200, f"chat completion failed: {status_code} {payload}")
    assert_true(bool(payload.get("request_id")), f"chat missing request_id: {payload}")
    assert_true(isinstance(payload.get("session"), dict), f"chat missing session metadata: {payload}")
    assert_true(isinstance(payload.get("comet"), dict), f"chat missing comet metadata: {payload}")
    assert_true("choices" in payload and payload["choices"], f"chat missing choices: {payload}")
    assert_true(headers.get("X-Comet-Request-Id") or headers.get("x-comet-request-id"), "chat header missing request id")

    status_code, _, payload = request(
        "POST",
        chat_url,
        {
            "model": active_model + "-mismatch",
            "messages": [{"role": "user", "content": "Hi"}],
        },
    )
    assert_true(status_code == 409, f"model mismatch expected 409, got {status_code}: {payload}")
    assert_error_envelope(payload, "model_mismatch")

    stream_events = parse_sse(
        stream_url,
        {
            "messages": [{"role": "user", "content": "Кратко объясни, что ты умеешь."}],
        },
    )
    event_names = [name for name, _ in stream_events if name]
    assert_true("session_started" in event_names, f"stream missing session_started: {event_names}")
    assert_true("segment_started" in event_names, f"stream missing segment_started: {event_names}")
    assert_true(any(name == "delta" for name in event_names), f"stream missing delta: {event_names}")
    assert_true(
        any(name in {"session_complete", "session_failed"} for name in event_names),
        f"stream missing terminal session event: {event_names}",
    )
    assert_true(
        event_names[-1] in {"complete", "error"} or stream_events[-1][1] == "[DONE]",
        f"stream missing terminal compatibility event: {stream_events[-3:]}",
    )
    assert_true(stream_events[-1][1] == "[DONE]", f"stream missing DONE marker: {stream_events[-3:]}")

    status_code, _, payload = request(
        "POST",
        chat_url,
        {
            "messages": [{"role": "user", "content": "Return exactly {\"message\":\"ok\"}."}],
            "response_format": {"type": "json_object"},
        },
        timeout=180,
    )
    assert_true(status_code == 200, f"structured output request failed: {status_code} {payload}")
    assert_true(payload.get("structured_output", {}).get("valid") is True, f"structured output not marked valid: {payload}")
    assert_true(isinstance(payload.get("structured_output", {}).get("json"), dict), f"structured output missing parsed json: {payload}")

    status_code, _, payload = request(
        "POST",
        chat_url,
        {
            "messages": [{"role": "user", "content": "Hi"}],
            "response_format": {"type": "json_schema"},
        },
    )
    assert_true(status_code == 400, f"unsupported response_format expected 400, got {status_code}: {payload}")
    assert_error_envelope(payload, "unsupported_response_format")

    status_code, _, payload = request(
        "POST",
        chat_url,
        {
            "messages": [{"role": "user", "content": "Hi"}],
            "tools": [{"type": "function", "function": {"name": "x", "parameters": {}}}],
        },
    )
    assert_true(status_code == 400, f"unsupported tools expected 400, got {status_code}: {payload}")
    assert_error_envelope(payload, "unsupported_field")

    status_code, headers, payload = request("POST", f"{controller_url}/api/v1/planes/does-not-exist/interaction/chat/completions", {"messages": [{"role": "user", "content": "Hi"}]})
    assert_true(status_code == 404, f"unknown plane expected 404, got {status_code}: {payload}")
    assert_true(headers.get("X-Comet-Request-Id") or headers.get("x-comet-request-id"), "unknown plane response missing request id header")
    assert_error_envelope(payload, "plane_not_found")

    if args.compute_plane:
      status_code, _, payload = request(
          "POST",
          f"{controller_url}/api/v1/planes/{args.compute_plane}/interaction/chat/completions",
          {"messages": [{"role": "user", "content": "Hi"}]},
      )
      assert_true(status_code == 409, f"compute plane expected 409, got {status_code}: {payload}")
      assert_error_envelope(payload, "interaction_disabled")

    if args.maglev_repo:
        maglev_result = maybe_run_maglev(controller_url, plane, args.maglev_repo)
        print(f"maglev_smoke={maglev_result}")

    print("external inference contract checks: ok")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"external inference contract checks failed: {exc}", file=sys.stderr)
        sys.exit(1)
