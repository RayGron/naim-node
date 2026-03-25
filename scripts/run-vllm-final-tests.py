#!/usr/bin/env python3
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
import pathlib
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def run_cmd(args, check=True, capture_output=True, text=True, input_text=None):
    return subprocess.run(
        args,
        check=check,
        capture_output=capture_output,
        text=text,
        input=input_text,
    )


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


class Campaign:
    def __init__(self, repo_root: pathlib.Path, keep_final: bool):
        self.repo_root = repo_root
        self.keep_final = keep_final
        self.controller_url = os.environ.get("COMET_CONTROLLER_URL", "http://127.0.0.1:18080")
        self.controller_db = pathlib.Path(os.environ.get("COMET_NODE_CONTROLLER_DB", "/var/lib/comet-node/controller.sqlite"))
        self.runtime_root = pathlib.Path(os.environ.get("COMET_SPLIT_HOSTD_RUNTIME_ROOT", "/var/lib/comet-node/runtime"))
        self.hostd_poll_interval = os.environ.get("COMET_SPLIT_HOSTD_POLL_INTERVAL_SEC", "2")
        self.controller_public_key_path = pathlib.Path(
            os.environ.get("COMET_NODE_CONTROLLER_PUBLIC_KEY", "/var/lib/comet-node/keys/controller.pub.b64")
        )
        self.host_private_key_path = pathlib.Path(
            os.environ.get("COMET_NODE_HOST_PRIVATE_KEY", "/var/lib/comet-node/keys/hostd.key.b64")
        )
        self.host_public_key_path = pathlib.Path("/var/lib/comet-node/keys/hostd.pub.b64")
        self.config = json.loads((repo_root / "config/comet-node-config.json").read_text())
        self.storage_root = pathlib.Path(self.config["paths"]["storage_root"])
        self.model_cache_root = pathlib.Path(self.config["paths"]["model_cache_root"])
        self.artifacts_root = os.environ.get("COMET_NODE_ARTIFACTS_ROOT", "/var/lib/comet-node/artifacts")
        self.build_dir = repo_root / "build/linux/x64"
        self.hostd_bin = self.build_dir / "comet-hostd"
        self.launcher_bin = self.build_dir / "comet-node"
        self.controller_bin = self.build_dir / "comet-controller"
        self.report_root = repo_root / "var/reports/vllm-phase-final" / time.strftime("%Y%m%d-%H%M%S")
        self.report_root.mkdir(parents=True, exist_ok=True)
        latest_link = repo_root / "var/reports/vllm-phase-final/latest"
        latest_link.parent.mkdir(parents=True, exist_ok=True)
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(self.report_root.name)
        self.summary = []
        self.split_hostd_pids = {}
        self.planes_to_cleanup = []
        self.docker_cmd = self.resolve_docker()
        self.sudo_prefix = self.resolve_sudo_prefix()
        self.lock_handle = None
        self.acquire_lock()
        self.controller_fingerprint = hashlib.sha256(
            base64.b64decode(self.controller_public_key_path.read_text().strip())
        ).hexdigest()

    def acquire_lock(self):
        lock_path = pathlib.Path(os.environ.get("COMET_VLLM_FINAL_TEST_LOCK", "/tmp/comet-vllm-final-tests.lock"))
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("w")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another vLLM final test campaign is already running: {lock_path}") from exc
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        self.lock_handle = handle

    def resolve_docker(self):
        for candidate in (["docker"], ["sudo", "-n", "docker"]):
            result = run_cmd(candidate + ["info"], check=False)
            if result.returncode == 0:
                return candidate
        raise RuntimeError("docker is not available")

    def resolve_sudo_prefix(self):
        result = run_cmd(["sudo", "-n", "true"], check=False)
        return ["sudo", "-n"] if result.returncode == 0 else []

    def request_json(self, method: str, path: str, payload=None, timeout=120):
        url = urllib.parse.urljoin(self.controller_url, path)
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}

    def controller_snapshot(self):
        return {
            "planes": self.request_json("GET", "/api/v1/planes", timeout=30),
            "host_assignments": self.request_json("GET", "/api/v1/host-assignments", timeout=30),
            "host_observations": self.request_json("GET", "/api/v1/host-observations", timeout=30),
            "host_health": self.request_json("GET", "/api/v1/host-health", timeout=30),
        }

    def request_raw(self, method: str, url: str, payload=None, timeout=600):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8"), response.status, dict(response.headers)

    def wait_for_controller(self):
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                body, _, _ = self.request_raw("GET", urllib.parse.urljoin(self.controller_url, "/health"), timeout=10)
                if body:
                    return
            except Exception:
                time.sleep(2)
        raise RuntimeError(f"controller did not become reachable at {self.controller_url}")

    def model_local_path(self, model_id: str) -> pathlib.Path:
        target = self.model_cache_root / "vllm"
        for part in model_id.split("/"):
            safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in part)
            target /= safe
        return target

    def ensure_model_cache(self, state: dict):
        inference = state.get("inference") or {}
        bootstrap = state.get("bootstrap_model") or {}
        if inference.get("runtime_engine") != "vllm":
            raise RuntimeError(f"plane {state.get('plane_name')} is not a vLLM plane")
        model_id = bootstrap.get("model_id")
        if not model_id:
            raise RuntimeError(f"plane {state.get('plane_name')} is missing bootstrap_model.model_id")
        local_path = pathlib.Path(bootstrap.get("local_path") or self.model_local_path(model_id))
        state.setdefault("bootstrap_model", {})["local_path"] = str(local_path)
        ready_marker = local_path / ".comet-model-ready"
        try:
            if ready_marker.exists():
                return
        except PermissionError:
            if local_path.exists():
                return
            raise
        local_path.parent.mkdir(parents=True, exist_ok=True)
        mount_root = self.model_cache_root.parent
        code = (
            "import os\n"
            "from huggingface_hub import snapshot_download\n"
            "snapshot_download("
            "repo_id=os.environ['MODEL_ID'], "
            "local_dir=os.environ['MODEL_DIR'], "
            "local_dir_use_symlinks=False)"
        )
        subprocess.run(
            self.docker_cmd
            + [
                "run",
                "--rm",
                "-e",
                f"MODEL_ID={model_id}",
                "-e",
                f"MODEL_DIR={local_path}",
                "-v",
                f"{mount_root}:{mount_root}",
                "comet/worker-vllm-runtime:dev",
                "python3",
                "-c",
                code,
            ],
            check=True,
        )
        ready_marker.touch()

    def hostd_state_root(self, node_name: str) -> pathlib.Path:
        if node_name == "local-hostd":
            return pathlib.Path("/var/lib/comet-node/hostd-state")
        return pathlib.Path(f"/var/lib/comet-node/{node_name}-state")

    def apply_next_assignment(self, node_name: str):
        subprocess.run(
            [
                *(self.sudo_prefix or []),
                str(self.hostd_bin),
                "apply-next-assignment",
                "--db",
                str(self.controller_db),
                "--node",
                node_name,
                "--runtime-root",
                str(self.runtime_root),
                "--state-root",
                str(self.hostd_state_root(node_name)),
                "--compose-mode",
                "exec",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def report_observed_state(self, node_name: str):
        subprocess.run(
            [
                *(self.sudo_prefix or []),
                str(self.hostd_bin),
                "report-observed-state",
                "--db",
                str(self.controller_db),
                "--node",
                node_name,
                "--state-root",
                str(self.hostd_state_root(node_name)),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def pump_nodes(self, node_names):
        for node_name in node_names:
            self.apply_next_assignment(node_name)
        for node_name in node_names:
            self.report_observed_state(node_name)

    def delete_plane(self, plane_name: str):
        try:
            self.request_raw(
                "DELETE",
                urllib.parse.urljoin(self.controller_url, f"/api/v1/planes/{plane_name}"),
                timeout=60,
            )
        except Exception:
            pass

    def active_assignments_for_planes(self, plane_names):
        plane_names = set(plane_names)
        payload = self.request_json("GET", "/api/v1/host-assignments", timeout=30)
        assignments = payload.get("assignments", [])
        active_statuses = {"pending", "claimed", "applying", "running", "queued"}
        return [
            item
            for item in assignments
            if item.get("plane_name") in plane_names and item.get("status") in active_statuses
        ]

    def wait_assignments_clear(self, plane_names, timeout=3600, node_names=None):
        plane_names = [item for item in plane_names if item]
        if not plane_names:
            return
        deadline = time.time() + timeout
        last_active = []
        while time.time() < deadline:
            if node_names:
                self.pump_nodes(node_names)
            last_active = self.active_assignments_for_planes(plane_names)
            if not last_active:
                return
            time.sleep(2)
        raise RuntimeError(f"assignments did not clear for planes {plane_names}: {last_active}")

    def capture_failure_artifacts(self, test_id: str, plane_names=None):
        plane_names = plane_names or []
        target = self.report_root / "artifacts" / test_id / f"failure-{time.strftime('%H%M%S')}"
        target.mkdir(parents=True, exist_ok=True)
        try:
            (target / "controller-snapshot.json").write_text(json.dumps(self.controller_snapshot(), indent=2) + "\n")
        except Exception as exc:
            (target / "controller-snapshot.error.txt").write_text(str(exc) + "\n")
        for plane_name in plane_names:
            if not plane_name:
                continue
            try:
                status = self.request_json("GET", f"/api/v1/planes/{plane_name}/interaction/status", timeout=30)
                (target / f"{plane_name}-interaction-status.json").write_text(json.dumps(status, indent=2) + "\n")
            except Exception as exc:
                (target / f"{plane_name}-interaction-status.error.txt").write_text(str(exc) + "\n")
        command_specs = {
            "docker-ps.txt": [*self.docker_cmd, "ps", "-a", "--format", "table {{.Names}}\t{{.Status}}\t{{.Image}}"],
            "nvidia-smi.txt": [*(self.sudo_prefix or []), "nvidia-smi"],
            "controller-journal.txt": [*(self.sudo_prefix or []), "journalctl", "-u", "comet-node-controller.service", "-n", "400", "--no-pager"],
            "hostd-journal.txt": [*(self.sudo_prefix or []), "journalctl", "-u", "comet-node-hostd.service", "-n", "400", "--no-pager"],
            "processes.txt": [*(self.sudo_prefix or []), "ps", "-eo", "pid=,etimes=,args="],
        }
        for filename, args in command_specs.items():
            result = run_cmd(args, check=False)
            content = result.stdout if result.returncode == 0 else (result.stdout + "\n" + result.stderr)
            (target / filename).write_text(content)

    def wait_plane_absent(self, plane_name: str, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            payload = self.request_json("GET", "/api/v1/planes", timeout=30)
            if all(item.get("name") != plane_name for item in payload.get("items", [])):
                return
            time.sleep(2)
        raise RuntimeError(f"plane {plane_name} did not disappear from controller state")

    def apply_plane(self, state: dict):
        self.ensure_model_cache(state)
        payload = {"desired_state": state, "artifacts_root": self.artifacts_root}
        self.request_json("POST", "/api/v1/planes", payload=payload, timeout=120)
        self.request_raw(
            "POST",
            urllib.parse.urljoin(self.controller_url, f"/api/v1/planes/{state['plane_name']}/start"),
            timeout=60,
        )
        self.planes_to_cleanup.append(state["plane_name"])

    def wait_ready(self, plane_name: str, timeout=7200, require_workers=None, require_primary=None, node_names=None):
        deadline = time.time() + timeout
        last_payload = None
        while time.time() < deadline:
            if node_names:
                self.pump_nodes(node_names)
            try:
                payload = self.request_json(
                    "GET",
                    f"/api/v1/planes/{plane_name}/interaction/status",
                    timeout=30,
                )
                last_payload = payload
            except Exception:
                time.sleep(2)
                continue
            if payload.get("ready") is True:
                if require_workers is not None and payload.get("worker_group_ready") != require_workers:
                    time.sleep(2)
                    continue
                if require_primary is not None and payload.get("primary_infer_node") != require_primary:
                    time.sleep(2)
                    continue
                return payload
            time.sleep(2)
        raise RuntimeError(f"plane {plane_name} did not become ready: {last_payload}")

    def interaction_request_raw(self, method: str, url: str, payload=None, timeout=600, max_attempts=4):
        last_error = None
        for attempt in range(max_attempts):
            try:
                return self.request_raw(method, url, payload=payload, timeout=timeout)
            except urllib.error.HTTPError as exc:
                if exc.code not in {502, 503, 504} or attempt + 1 == max_attempts:
                    raise
                last_error = exc
            except urllib.error.URLError as exc:
                if attempt + 1 == max_attempts:
                    raise
                last_error = exc
            time.sleep(2 + attempt)
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"interaction request failed without a response: {url}")

    def chat_completion(self, plane_name: str, payload: dict, timeout=1800):
        url = urllib.parse.urljoin(
            self.controller_url, f"/api/v1/planes/{plane_name}/interaction/chat/completions"
        )
        started = time.time()
        body, status, _ = self.interaction_request_raw("POST", url, payload=payload, timeout=timeout)
        elapsed_ms = (time.time() - started) * 1000
        parsed = json.loads(body)
        return parsed, elapsed_ms, status

    def stream_completion(self, plane_name: str, payload: dict, timeout=1800):
        url = urllib.parse.urljoin(
            self.controller_url, f"/api/v1/planes/{plane_name}/interaction/chat/completions/stream"
        )
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        start = time.time()
        first_delta = None
        deltas = []
        complete_payload = None
        last_error = None
        for attempt in range(4):
            event_name = None
            data_lines = []
            deltas = []
            complete_payload = None
            first_delta = None
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
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
                            if event_name and data_lines:
                                data_blob = "\n".join(data_lines)
                                if event_name == "delta":
                                    payload_json = json.loads(data_blob)
                                    delta = payload_json.get("delta", "")
                                    if delta and first_delta is None:
                                        first_delta = (time.time() - start) * 1000
                                    deltas.append(delta)
                                elif event_name == "complete" and data_blob != "[DONE]":
                                    try:
                                        complete_payload = json.loads(data_blob)
                                    except json.JSONDecodeError:
                                        complete_payload = {"raw": data_blob}
                            event_name = None
                            data_lines = []
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in {502, 503, 504} or attempt == 3:
                    raise
                last_error = exc
                time.sleep(2 + attempt)
            except urllib.error.URLError as exc:
                if attempt == 3:
                    raise
                last_error = exc
                time.sleep(2 + attempt)
        else:
            if last_error is not None:
                raise last_error
        total_ms = (time.time() - start) * 1000
        return {
            "ttft_ms": first_delta,
            "total_latency_ms": total_ms,
            "content": "".join(deltas).strip(),
            "complete_payload": complete_payload,
        }

    def sample_gpu(self, gpu_ids, stop_event, sink):
        gpu_ids = {str(item) for item in gpu_ids}
        while not stop_event.is_set():
            result = run_cmd(
                [
                    *(self.sudo_prefix or []),
                    "nvidia-smi",
                    "--query-gpu=index,utilization.gpu,memory.used",
                    "--format=csv,noheader,nounits",
                ],
                check=False,
            )
            if result.returncode == 0:
                stamp = time.time()
                for line in result.stdout.splitlines():
                    parts = [part.strip() for part in line.split(",")]
                    if len(parts) != 3:
                        continue
                    index, util, mem = parts
                    if index not in gpu_ids:
                        continue
                    try:
                        sink.append(
                            {
                                "ts": stamp,
                                "gpu": index,
                                "util": float(util),
                                "memory_mb": float(mem),
                            }
                        )
                    except ValueError:
                        continue
            stop_event.wait(1.0)

    def gpu_summary(self, samples):
        by_gpu = {}
        for item in samples:
            gpu = item["gpu"]
            by_gpu.setdefault(gpu, {"util": [], "memory_mb": []})
            by_gpu[gpu]["util"].append(item["util"])
            by_gpu[gpu]["memory_mb"].append(item["memory_mb"])
        summary = {}
        for gpu, values in by_gpu.items():
            summary[gpu] = {
                "util_avg": round(statistics.mean(values["util"]), 2),
                "util_peak": round(max(values["util"]), 2),
                "memory_peak_mb": round(max(values["memory_mb"]), 2),
            }
        return summary

    def chat_probe(self, plane_name: str, payload: dict, gpu_ids):
        samples = []
        stop_event = threading.Event()
        sampler = threading.Thread(target=self.sample_gpu, args=(gpu_ids, stop_event, samples), daemon=True)
        sampler.start()
        try:
            response, elapsed_ms, status = self.chat_completion(plane_name, payload)
        finally:
            stop_event.set()
            sampler.join(timeout=2)
        usage = response.get("usage") or {}
        content = response["choices"][0]["message"]["content"].strip()
        tokens = usage.get("completion_tokens")
        tokens_per_sec = round(tokens / (elapsed_ms / 1000), 2) if tokens else None
        return {
            "status": status,
            "content": content,
            "elapsed_ms": round(elapsed_ms, 2),
            "usage": usage,
            "completion_tokens": tokens,
            "tokens_per_sec": tokens_per_sec,
            "gpu": self.gpu_summary(samples),
        }

    def stream_probe(self, plane_name: str, payload: dict, gpu_ids):
        samples = []
        stop_event = threading.Event()
        sampler = threading.Thread(target=self.sample_gpu, args=(gpu_ids, stop_event, samples), daemon=True)
        sampler.start()
        try:
            response = self.stream_completion(plane_name, payload)
        finally:
            stop_event.set()
            sampler.join(timeout=2)
        response["gpu"] = self.gpu_summary(samples)
        return response

    def load_state(self, relative_path: str):
        return json.loads((self.repo_root / relative_path).read_text())

    def infer_name(self, state: dict):
        return next(instance["name"] for instance in state["instances"] if instance["role"] == "infer")

    def worker_instances(self, state: dict):
        return [instance for instance in state["instances"] if instance["role"] == "worker"]

    def unique_node_order(self, members):
        seen = []
        for member in members:
            node_name = member["node_name"]
            if node_name not in seen:
                seen.append(node_name)
        return seen

    def make_single_worker_plane(self, base_state: dict, plane_name: str, gpu_device: str, gateway_port: int, api_port: int, rendezvous_port: int, node_name="local-hostd", execution_mode="mixed"):
        state = copy.deepcopy(base_state)
        infer_name = f"infer-{plane_name}"
        worker_name = f"worker-{plane_name}"
        shared_disk_name = f"plane-{plane_name}-shared"
        control_root = f"/comet/shared/control/{plane_name}"
        private_infer = f"{infer_name}-private"
        private_worker = f"{worker_name}-private"
        served_model_name = state["bootstrap_model"]["served_model_name"]
        state["plane_name"] = plane_name
        state["plane_shared_disk_name"] = shared_disk_name
        state["control_root"] = control_root
        state["inference"]["primary_infer_node"] = node_name if execution_mode != "infer-only" else "infer-hostd"
        state["inference"]["worker_group_id"] = f"{plane_name}-workers"
        state["inference"]["api_port"] = api_port
        state["inference"]["rendezvous_port"] = rendezvous_port
        state["gateway"]["listen_port"] = gateway_port
        state["gateway"]["server_name"] = f"{plane_name}.local"
        state["worker_group"]["group_id"] = f"{plane_name}-workers"
        state["worker_group"]["infer_instance_name"] = infer_name
        state["worker_group"]["rendezvous_host"] = infer_name
        state["worker_group"]["rendezvous_port"] = rendezvous_port
        state["worker_group"]["expected_workers"] = 1
        member = state["worker_group"]["members"][0]
        member["name"] = worker_name
        member["node_name"] = node_name if execution_mode != "infer-only" else "worker-hostd-a"
        member["gpu_device"] = str(gpu_device)
        member["rank"] = 0
        member["leader"] = True
        state["runtime_gpu_nodes"] = [
            {
                "name": worker_name,
                "node_name": member["node_name"],
                "gpu_device": str(gpu_device),
                "gpu_fraction": member.get("gpu_fraction", 1.0),
                "memory_cap_mb": member.get("memory_cap_mb"),
            }
        ]
        if execution_mode == "infer-only":
            state["nodes"] = [
                {
                    "name": "infer-hostd",
                    "platform": "linux",
                    "execution_mode": "infer-only",
                    "gpu_devices": [],
                    "gpu_memory_mb": {},
                },
                {
                    "name": "worker-hostd-a",
                    "platform": "linux",
                    "execution_mode": "worker-only",
                    "gpu_devices": [str(gpu_device)],
                    "gpu_memory_mb": {str(gpu_device): 97887},
                },
            ]
        else:
            state["nodes"] = [
                {
                    "name": node_name,
                    "platform": "linux",
                    "execution_mode": execution_mode,
                    "gpu_devices": ["0", "1"],
                    "gpu_memory_mb": {"0": 97887, "1": 97887},
                }
            ]
        if len(state["nodes"]) > 1:
            state.pop("placement_target", None)
        state["disks"] = [
            {
                "name": shared_disk_name,
                "kind": "plane-shared",
                "plane_name": plane_name,
                "owner_name": plane_name,
                "node_name": state["nodes"][0]["name"],
                "host_path": f"/var/lib/comet/disks/planes/{plane_name}/shared",
                "container_path": "/comet/shared",
                "size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "plane-shared"),
            }
        ]
        if execution_mode == "infer-only":
            state["disks"].append(
                {
                    "name": shared_disk_name,
                    "kind": "plane-shared",
                    "plane_name": plane_name,
                    "owner_name": plane_name,
                    "node_name": "worker-hostd-a",
                    "host_path": f"/var/lib/comet/disks/planes/{plane_name}/shared",
                    "container_path": "/comet/shared",
                    "size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "plane-shared"),
                }
            )
        state["disks"].extend(
            [
                {
                    "name": private_infer,
                    "kind": "infer-private",
                    "plane_name": plane_name,
                    "owner_name": infer_name,
                    "node_name": state["nodes"][0]["name"],
                    "host_path": f"/var/lib/comet/disks/instances/{infer_name}/private",
                    "container_path": "/comet/private",
                    "size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "infer-private"),
                },
                {
                    "name": private_worker,
                    "kind": "worker-private",
                    "plane_name": plane_name,
                    "owner_name": worker_name,
                    "node_name": member["node_name"],
                    "host_path": f"/var/lib/comet/disks/instances/{worker_name}/private",
                    "container_path": "/comet/private",
                    "size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "worker-private"),
                },
            ]
        )
        state["instances"] = [
            {
                "name": infer_name,
                "role": "infer",
                "plane_name": plane_name,
                "node_name": state["nodes"][0]["name"],
                "image": "comet/infer-runtime:dev",
                "command": "/runtime/infer/entrypoint.sh",
                "private_disk_name": private_infer,
                "shared_disk_name": shared_disk_name,
                "environment": {
                    "COMET_PLANE_NAME": plane_name,
                    "COMET_INSTANCE_NAME": infer_name,
                    "COMET_INSTANCE_ROLE": "infer",
                    "COMET_NODE_NAME": state["nodes"][0]["name"],
                    "COMET_INFER_BOOT_MODE": "launch-runtime",
                    "COMET_INFER_RUNTIME_BACKEND": "worker-vllm",
                    "COMET_CONTROLLER_URL": "http://controller.internal:8080",
                    "COMET_CONTROL_ROOT": control_root,
                    "COMET_INFER_RUNTIME_CONFIG": f"{control_root}/infer-runtime.json",
                    "COMET_INFERENCE_PORT": str(api_port),
                    "COMET_GATEWAY_PORT": str(gateway_port),
                    "COMET_SHARED_DISK_PATH": "/comet/shared",
                    "COMET_PRIVATE_DISK_PATH": "/comet/private",
                },
                "labels": {
                    "comet.plane": plane_name,
                    "comet.role": "infer",
                    "comet.node": state["nodes"][0]["name"],
                },
                "private_disk_size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "infer-private"),
            },
            {
                "name": worker_name,
                "role": "worker",
                "plane_name": plane_name,
                "node_name": member["node_name"],
                "image": "comet/worker-vllm-runtime:dev",
                "command": "/runtime/worker/entrypoint.sh",
                "private_disk_name": private_worker,
                "shared_disk_name": shared_disk_name,
                "environment": {
                    "COMET_PLANE_NAME": plane_name,
                    "COMET_INSTANCE_NAME": worker_name,
                    "COMET_INSTANCE_ROLE": "worker",
                    "COMET_NODE_NAME": member["node_name"],
                    "COMET_GPU_DEVICE": str(gpu_device),
                    "COMET_WORKER_BOOT_MODE": "vllm-openai",
                    "COMET_CONTROL_ROOT": control_root,
                    "COMET_SHARED_DISK_PATH": "/comet/shared",
                    "COMET_PRIVATE_DISK_PATH": "/comet/private",
                    "COMET_WORKER_RUNTIME_STATUS_PATH": "/comet/private/worker-runtime-status.json",
                },
                "labels": {
                    "comet.plane": plane_name,
                    "comet.role": "worker",
                    "comet.node": member["node_name"],
                    "comet.placement": "manual",
                    "comet.placement.mode": "manual",
                    "comet.placement.action": "manual",
                    "comet.requested.node": member["node_name"],
                    "comet.requested.gpu": str(gpu_device),
                },
                "gpu_device": str(gpu_device),
                "gpu_fraction": member.get("gpu_fraction", 1.0),
                "memory_cap_mb": member.get("memory_cap_mb"),
                "private_disk_size_gb": next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "worker-private"),
            },
        ]
        state["bootstrap_model"]["served_model_name"] = served_model_name
        return state

    def make_split_plane(self, base_state: dict, plane_name: str, gateway_port: int, api_port: int, rendezvous_port: int):
        state = copy.deepcopy(base_state)
        infer_name = f"infer-{plane_name}"
        shared_disk_name = f"plane-{plane_name}-shared"
        control_root = f"/comet/shared/control/{plane_name}"
        state["plane_name"] = plane_name
        state["plane_shared_disk_name"] = shared_disk_name
        state["control_root"] = control_root
        state["inference"]["primary_infer_node"] = "infer-hostd"
        state["inference"]["worker_group_id"] = f"{plane_name}-workers"
        state["inference"]["api_port"] = api_port
        state["inference"]["rendezvous_port"] = rendezvous_port
        state["gateway"]["listen_port"] = gateway_port
        state["gateway"]["server_name"] = f"{plane_name}.local"
        state["worker_group"]["group_id"] = f"{plane_name}-workers"
        state["worker_group"]["infer_instance_name"] = infer_name
        state["worker_group"]["rendezvous_host"] = infer_name
        state["worker_group"]["rendezvous_port"] = rendezvous_port
        worker_members = []
        nodes = [
            {
                "name": "infer-hostd",
                "platform": "linux",
                "execution_mode": "infer-only",
                "gpu_devices": [],
                "gpu_memory_mb": {},
            }
        ]
        worker_nodes = []
        for index, member in enumerate(state["worker_group"]["members"]):
            node_name = f"worker-hostd-{chr(ord('a') + index)}"
            gpu_device = str(index)
            worker_name = f"worker-{plane_name}-{chr(ord('a') + index)}"
            member["name"] = worker_name
            member["node_name"] = node_name
            member["gpu_device"] = gpu_device
            member["rank"] = index
            member["leader"] = index == 0
            worker_members.append(member)
            worker_nodes.append(
                {
                    "name": node_name,
                    "platform": "linux",
                    "execution_mode": "worker-only",
                    "gpu_devices": [gpu_device],
                    "gpu_memory_mb": {gpu_device: 97887},
                }
            )
        nodes.extend(worker_nodes)
        state["nodes"] = nodes
        state.pop("placement_target", None)
        state["runtime_gpu_nodes"] = [
            {
                "name": member["name"],
                "node_name": member["node_name"],
                "gpu_device": member["gpu_device"],
                "gpu_fraction": member.get("gpu_fraction", 1.0),
                "memory_cap_mb": member.get("memory_cap_mb"),
            }
            for member in worker_members
        ]
        shared_size = next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "plane-shared")
        infer_private_size = next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "infer-private")
        worker_private_size = next(d["size_gb"] for d in base_state["disks"] if d["kind"] == "worker-private")
        state["disks"] = []
        for node in nodes:
            state["disks"].append(
                {
                    "name": shared_disk_name,
                    "kind": "plane-shared",
                    "plane_name": plane_name,
                    "owner_name": plane_name,
                    "node_name": node["name"],
                    "host_path": f"/var/lib/comet/disks/planes/{plane_name}/shared",
                    "container_path": "/comet/shared",
                    "size_gb": shared_size,
                }
            )
        infer_private = f"{infer_name}-private"
        state["disks"].append(
            {
                "name": infer_private,
                "kind": "infer-private",
                "plane_name": plane_name,
                "owner_name": infer_name,
                "node_name": "infer-hostd",
                "host_path": f"/var/lib/comet/disks/instances/{infer_name}/private",
                "container_path": "/comet/private",
                "size_gb": infer_private_size,
            }
        )
        for member in worker_members:
            state["disks"].append(
                {
                    "name": f"{member['name']}-private",
                    "kind": "worker-private",
                    "plane_name": plane_name,
                    "owner_name": member["name"],
                    "node_name": member["node_name"],
                    "host_path": f"/var/lib/comet/disks/instances/{member['name']}/private",
                    "container_path": "/comet/private",
                    "size_gb": worker_private_size,
                }
            )
        state["instances"] = [
            {
                "name": infer_name,
                "role": "infer",
                "plane_name": plane_name,
                "node_name": "infer-hostd",
                "image": "comet/infer-runtime:dev",
                "command": "/runtime/infer/entrypoint.sh",
                "private_disk_name": infer_private,
                "shared_disk_name": shared_disk_name,
                "environment": {
                    "COMET_PLANE_NAME": plane_name,
                    "COMET_INSTANCE_NAME": infer_name,
                    "COMET_INSTANCE_ROLE": "infer",
                    "COMET_NODE_NAME": "infer-hostd",
                    "COMET_INFER_BOOT_MODE": "launch-runtime",
                    "COMET_INFER_RUNTIME_BACKEND": "worker-vllm",
                    "COMET_CONTROLLER_URL": "http://controller.internal:8080",
                    "COMET_CONTROL_ROOT": control_root,
                    "COMET_INFER_RUNTIME_CONFIG": f"{control_root}/infer-runtime.json",
                    "COMET_INFERENCE_PORT": str(api_port),
                    "COMET_GATEWAY_PORT": str(gateway_port),
                    "COMET_SHARED_DISK_PATH": "/comet/shared",
                    "COMET_PRIVATE_DISK_PATH": "/comet/private",
                },
                "labels": {
                    "comet.plane": plane_name,
                    "comet.role": "infer",
                    "comet.node": "infer-hostd",
                },
                "private_disk_size_gb": infer_private_size,
            }
        ]
        for member in worker_members:
            state["instances"].append(
                {
                    "name": member["name"],
                    "role": "worker",
                    "plane_name": plane_name,
                    "node_name": member["node_name"],
                    "image": "comet/worker-vllm-runtime:dev",
                    "command": "/runtime/worker/entrypoint.sh",
                    "private_disk_name": f"{member['name']}-private",
                    "shared_disk_name": shared_disk_name,
                    "environment": {
                        "COMET_PLANE_NAME": plane_name,
                        "COMET_INSTANCE_NAME": member["name"],
                        "COMET_INSTANCE_ROLE": "worker",
                        "COMET_NODE_NAME": member["node_name"],
                        "COMET_GPU_DEVICE": member["gpu_device"],
                        "COMET_WORKER_BOOT_MODE": "vllm-openai",
                        "COMET_CONTROL_ROOT": control_root,
                        "COMET_SHARED_DISK_PATH": "/comet/shared",
                        "COMET_PRIVATE_DISK_PATH": "/comet/private",
                        "COMET_WORKER_RUNTIME_STATUS_PATH": "/comet/private/worker-runtime-status.json",
                    },
                    "labels": {
                        "comet.plane": plane_name,
                        "comet.role": "worker",
                        "comet.node": member["node_name"],
                        "comet.placement": "manual",
                        "comet.placement.mode": "manual",
                        "comet.placement.action": "manual",
                        "comet.requested.node": member["node_name"],
                        "comet.requested.gpu": member["gpu_device"],
                    },
                    "gpu_device": member["gpu_device"],
                    "gpu_fraction": member.get("gpu_fraction", 1.0),
                    "memory_cap_mb": member.get("memory_cap_mb"),
                    "private_disk_size_gb": worker_private_size,
                }
            )
        return state

    def matching_hostd_loop_pids(self, node_name: str):
        result = run_cmd([*(self.sudo_prefix or []), "ps", "-eo", "pid=,args="], check=True)
        target = f"comet-node run hostd --foreground --skip-systemctl --node {node_name}"
        pids = []
        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            pid_text, _, args = line.partition(" ")
            if target not in args:
                continue
            if "run-vllm-final-tests.py" in args or "python3 -" in args:
                continue
            try:
                pids.append(int(pid_text))
            except ValueError:
                continue
        return pids

    def terminate_existing_hostd_loops(self, node_name: str):
        pids = self.matching_hostd_loop_pids(node_name)
        if pids:
            subprocess.run([*(self.sudo_prefix or []), "kill", *[str(pid) for pid in pids]], check=False)

    def stop_existing_hostd_loop(self, node_name: str):
        self.terminate_existing_hostd_loops(node_name)
        deadline = time.time() + 30
        while time.time() < deadline:
            if not self.matching_hostd_loop_pids(node_name):
                return
            time.sleep(1)
        raise RuntimeError(f"stale hostd loop for {node_name} is still running")

    def register_host(self, node_name: str, execution_mode: str):
        subprocess.run(
            [
                *(self.sudo_prefix or []),
                str(self.launcher_bin),
                "connect-hostd",
                "--db",
                str(self.controller_db),
                "--node",
                node_name,
                "--public-key",
                str(self.host_public_key_path),
                "--controller-fingerprint",
                self.controller_fingerprint,
                "--execution-mode",
                execution_mode,
            ],
            check=True,
        )

    def start_hostd_loop(self, node_name: str, execution_mode: str):
        self.stop_existing_hostd_loop(node_name)
        self.register_host(node_name, execution_mode)
        state_root = pathlib.Path(f"/var/lib/comet-node/{node_name}-state")
        log_dir = self.report_root / "artifacts" / "split-hostd-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{node_name}.log"
        subprocess.run([*(self.sudo_prefix or []), "mkdir", "-p", str(state_root)], check=True)
        command = (
            "set -euo pipefail\n"
            f"nohup '{self.launcher_bin}' run hostd "
            f"--foreground --skip-systemctl --node '{node_name}' "
            f"--controller '{self.controller_url}' "
            f"--controller-fingerprint '{self.controller_fingerprint}' "
            f"--host-private-key '{self.host_private_key_path}' "
            f"--runtime-root '{self.runtime_root}' "
            f"--state-root '{state_root}' "
            f"--compose-mode exec "
            f"--poll-interval-sec '{self.hostd_poll_interval}' "
            f">'{log_path}' 2>&1 &\n"
            "echo $!\n"
        )
        result = run_cmd([*(self.sudo_prefix or []), "bash", "-lc", command], check=True)
        self.split_hostd_pids[node_name] = int(result.stdout.strip())

    def setup_split_hostds(self):
        self.start_hostd_loop("infer-hostd", "infer-only")
        self.start_hostd_loop("worker-hostd-a", "worker-only")
        self.start_hostd_loop("worker-hostd-b", "worker-only")

    def cleanup_split_hostds(self):
        for node_name in ("infer-hostd", "worker-hostd-a", "worker-hostd-b"):
            self.terminate_existing_hostd_loops(node_name)
        for node_name, pid in list(self.split_hostd_pids.items()):
            subprocess.run([*(self.sudo_prefix or []), "kill", str(pid)], check=False)
            self.split_hostd_pids.pop(node_name, None)

    def no_vllm_processes(self):
        result = run_cmd(
            [
                *(self.sudo_prefix or []),
                "nvidia-smi",
                "--query-compute-apps=process_name",
                "--format=csv,noheader",
            ],
            check=False,
        )
        if result.returncode != 0:
            return True
        return "VLLM::EngineCore" not in result.stdout

    def wait_gpu_idle(self, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.no_vllm_processes():
                return
            time.sleep(2)
        raise RuntimeError("GPU workloads are still running after cleanup")

    def delete_other_planes(self, keep=None, node_names=None):
        keep = set(keep or [])
        payload = self.request_json("GET", "/api/v1/planes", timeout=30)
        for item in payload.get("items", []):
            name = item.get("name")
            if name and name not in keep:
                self.delete_plane(name)
                try:
                    self.wait_plane_absent(name, timeout=900)
                    self.wait_assignments_clear([name], timeout=900, node_names=node_names)
                except Exception:
                    pass

    def save_report(self, test_id: str, payload: dict):
        target_json = self.report_root / f"{test_id}.json"
        target_md = self.report_root / f"{test_id}.md"
        target_json.write_text(json.dumps(payload, indent=2) + "\n")
        lines = [
            f"# {payload['title']}",
            "",
            f"- started_at: {payload['started_at']}",
            f"- finished_at: {payload['finished_at']}",
            f"- status: {payload['status']}",
        ]
        metrics = payload.get("metrics") or {}
        if metrics:
            lines.append("")
            lines.append("## Metrics")
            for key, value in metrics.items():
                lines.append(f"- {key}: {value}")
        issues = payload.get("issues") or []
        if issues:
            lines.append("")
            lines.append("## Issues")
            for issue in issues:
                lines.append(f"- {issue}")
        if payload.get("notes"):
            lines.append("")
            lines.append("## Notes")
            for note in payload["notes"]:
                lines.append(f"- {note}")
        target_md.write_text("\n".join(lines) + "\n")

    def summarize_text_variants(self, outputs):
        normalized = [" ".join(item.split()) for item in outputs]
        variants = {}
        for item in normalized:
            variants[item] = variants.get(item, 0) + 1
        return variants

    def test_1_local_122b(self):
        test_id = "test-1-qwen35-122b-a10b-local-distributed"
        started_at = now_iso()
        payload = {
            "title": "Test 1: Qwen3.5-122B-A10B local distributed runtime",
            "started_at": started_at,
            "status": "running",
            "issues": [],
            "notes": [],
        }
        try:
            self.delete_other_planes()
            self.wait_gpu_idle()
            state = self.load_state("config/qwen35-122b-a10b/desired-state.json")
            artifacts_dir = self.report_root / "artifacts" / test_id
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "desired-state.json").write_text(json.dumps(state, indent=2) + "\n")
            start_ts = time.time()
            self.apply_plane(state)
            status = self.wait_ready(
                state["plane_name"],
                timeout=10800,
                require_workers=2,
                require_primary="local-hostd",
                node_names=["local-hostd"],
            )
            startup_ms = round((time.time() - start_ts) * 1000, 2)
            model_id = self.request_json("GET", f"/api/v1/planes/{state['plane_name']}/interaction/models")["data"][0]["id"]
            prompts = {
                "short": "Reply with one sentence that explains why redundant backups are useful.",
                "medium": "In 5 bullet points, explain how distributed inference can improve reliability, but mention at least one tradeoff.",
                "long": "Write a compact 8-point checklist for bringing up a distributed vLLM service across two GPUs. Keep it practical and concrete.",
            }
            speed = {}
            for label, prompt in prompts.items():
                req = {"model": model_id, "messages": [{"role": "user", "content": prompt}], "max_tokens": 256, "temperature": 0}
                speed[label] = {
                    "stream": self.stream_probe(state["plane_name"], {**req, "stream": True}, ["0", "1"]),
                    "chat": self.chat_probe(state["plane_name"], req, ["0", "1"]),
                }
            consistency_outputs = []
            consistency_metrics = []
            consistency_prompt = "State exactly three advantages of using a single distributed runtime across two GPUs."
            for _ in range(5):
                req = {"model": model_id, "messages": [{"role": "user", "content": consistency_prompt}], "max_tokens": 160, "temperature": 0}
                result = self.chat_probe(state["plane_name"], req, ["0", "1"])
                consistency_outputs.append(result["content"])
                consistency_metrics.append(result)
            variants = self.summarize_text_variants(consistency_outputs)
            memory_messages = [
                {"role": "user", "content": "Memorize these facts exactly: project codename Helios, city Trieste, codeword amber-741, checksum K9L2, review date 2041-11-07, tree linden. Reply only with STORED."},
                {"role": "assistant", "content": "STORED"},
                {"role": "user", "content": "Briefly explain why phased rollouts reduce operational risk."},
                {"role": "assistant", "content": "They limit blast radius, expose issues early, and keep rollback smaller."},
                {"role": "user", "content": "Now give two reasons operators like telemetry dashboards."},
                {"role": "assistant", "content": "They show health quickly and help pinpoint regressions."},
                {"role": "user", "content": "What are the exact codeword and checksum from the first turn? Reply as codeword=<...>; checksum=<...>."},
            ]
            context_result = self.chat_probe(
                state["plane_name"],
                {"model": model_id, "messages": memory_messages, "max_tokens": 64, "temperature": 0},
                ["0", "1"],
            )
            context_pass = "amber-741" in context_result["content"] and "K9L2" in context_result["content"]
            payload["status"] = "ok"
            payload["finished_at"] = now_iso()
            payload["metrics"] = {
                "startup_ms": startup_ms,
                "worker_group_ready": status.get("worker_group_ready"),
                "model": model_id,
                "speed_short_ttft_ms": speed["short"]["stream"]["ttft_ms"],
                "speed_short_total_ms": speed["short"]["chat"]["elapsed_ms"],
                "speed_short_tokens_per_sec": speed["short"]["chat"]["tokens_per_sec"],
                "speed_medium_ttft_ms": speed["medium"]["stream"]["ttft_ms"],
                "speed_medium_total_ms": speed["medium"]["chat"]["elapsed_ms"],
                "speed_medium_tokens_per_sec": speed["medium"]["chat"]["tokens_per_sec"],
                "speed_long_ttft_ms": speed["long"]["stream"]["ttft_ms"],
                "speed_long_total_ms": speed["long"]["chat"]["elapsed_ms"],
                "speed_long_tokens_per_sec": speed["long"]["chat"]["tokens_per_sec"],
                "consistency_unique_variants": len(variants),
                "context_recall_pass": context_pass,
            }
            payload["artifacts"] = {
                "speed": speed,
                "consistency_outputs": consistency_outputs,
                "consistency_variants": variants,
                "context_result": context_result,
                "status": status,
            }
            if len(variants) > 1:
                payload["issues"].append("Deterministic consistency was weaker than expected across 5 temperature=0 runs.")
            if not context_pass:
                payload["issues"].append("The model failed the exact recall check after the multi-turn context sequence.")
        except Exception as exc:
            payload["status"] = "failed"
            payload["finished_at"] = now_iso()
            payload["issues"].append(str(exc))
            self.capture_failure_artifacts(test_id, [state.get("plane_name")])
        self.save_report(test_id, payload)
        self.summary.append({"id": test_id, "title": payload["title"], "status": payload["status"], "issues": payload["issues"]})

    def test_2_dual_35b_dialog(self):
        test_id = "test-2-qwen35-35b-a3b-dual-infer"
        started_at = now_iso()
        payload = {
            "title": "Test 2: dual Qwen3.5-35B-A3B local planes with dialogue",
            "started_at": started_at,
            "status": "running",
            "issues": [],
            "notes": [],
        }
        plane_a = "qwen35-35b-a3b-a"
        plane_b = "qwen35-35b-a3b-b"
        try:
            self.delete_other_planes(node_names=["local-hostd"])
            self.wait_gpu_idle()
            base_state = self.load_state("config/qwen35-35b-a3b/desired-state.json")
            state_a = self.make_single_worker_plane(base_state, plane_a, "0", 18110, 18111, 29531)
            state_b = self.make_single_worker_plane(base_state, plane_b, "1", 18112, 18113, 29532)
            artifacts_dir = self.report_root / "artifacts" / test_id
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "plane-a.json").write_text(json.dumps(state_a, indent=2) + "\n")
            (artifacts_dir / "plane-b.json").write_text(json.dumps(state_b, indent=2) + "\n")
            start_ts = time.time()
            self.apply_plane(state_a)
            self.apply_plane(state_b)
            status_a = self.wait_ready(
                plane_a,
                timeout=7200,
                require_workers=1,
                require_primary="local-hostd",
                node_names=["local-hostd"],
            )
            status_b = self.wait_ready(
                plane_b,
                timeout=7200,
                require_workers=1,
                require_primary="local-hostd",
                node_names=["local-hostd"],
            )
            startup_ms = round((time.time() - start_ts) * 1000, 2)
            model_a = self.request_json("GET", f"/api/v1/planes/{plane_a}/interaction/models")["data"][0]["id"]
            model_b = self.request_json("GET", f"/api/v1/planes/{plane_b}/interaction/models")["data"][0]["id"]
            probe_prompt = "Explain in two short sentences why isolated planes help operational safety."
            probe_a = self.chat_probe(plane_a, {"model": model_a, "messages": [{"role": "user", "content": probe_prompt}], "max_tokens": 96, "temperature": 0}, ["0"])
            probe_b = self.chat_probe(plane_b, {"model": model_b, "messages": [{"role": "user", "content": probe_prompt}], "max_tokens": 96, "temperature": 0}, ["1"])
            consistency_outputs_a = []
            consistency_outputs_b = []
            for _ in range(5):
                req = {"model": model_a, "messages": [{"role": "user", "content": "Name two benefits of worker isolation."}], "max_tokens": 80, "temperature": 0}
                consistency_outputs_a.append(self.chat_probe(plane_a, req, ["0"])["content"])
                req_b = {"model": model_b, "messages": [{"role": "user", "content": "Name two benefits of worker isolation."}], "max_tokens": 80, "temperature": 0}
                consistency_outputs_b.append(self.chat_probe(plane_b, req_b, ["1"])["content"])
            rnd = random.Random(20260325)
            topics = [
                {"topic": "whether cities should convert rooftops into pollinator gardens", "keywords": ["cities", "rooftops", "pollinator", "gardens"]},
                {"topic": "whether small software teams should prefer fewer services with stronger interfaces", "keywords": ["software", "teams", "services", "interfaces"]},
                {"topic": "whether public libraries should host community repair workshops", "keywords": ["libraries", "community", "repair", "workshops"]},
            ]
            chosen = rnd.choice(topics)
            transcript = []
            dialogue_metrics = []
            for turn in range(12):
                speaker = "A" if turn % 2 == 0 else "B"
                plane = plane_a if speaker == "A" else plane_b
                model = model_a if speaker == "A" else model_b
                system_prompt = (
                    "You are Agent A. Discuss the topic constructively in 2-3 sentences and keep continuity."
                    if speaker == "A"
                    else "You are Agent B. Discuss the topic critically but cooperatively in 2-3 sentences and keep continuity."
                )
                prompt = (
                    f"Topic: {chosen['topic']}.\n"
                    "Continue the discussion naturally. Keep the topic stable and respond to the latest point.\n\n"
                    f"Transcript so far:\n{os.linesep.join(transcript) if transcript else '[start of dialogue]'}"
                )
                result = self.chat_probe(
                    plane,
                    {
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt},
                        ],
                        "max_tokens": 160,
                        "temperature": 0.6,
                    },
                    ["0"] if speaker == "A" else ["1"],
                )
                transcript.append(f"{speaker}: {result['content']}")
                dialogue_metrics.append({"turn": turn + 1, "speaker": speaker, **result})
            keyword_hits = sum(
                1
                for item in transcript
                if any(keyword.lower() in item.lower() for keyword in chosen["keywords"])
            )
            payload["status"] = "ok"
            payload["finished_at"] = now_iso()
            payload["metrics"] = {
                "startup_ms": startup_ms,
                "plane_a_model": model_a,
                "plane_b_model": model_b,
                "plane_a_total_ms": probe_a["elapsed_ms"],
                "plane_a_tokens_per_sec": probe_a["tokens_per_sec"],
                "plane_b_total_ms": probe_b["elapsed_ms"],
                "plane_b_tokens_per_sec": probe_b["tokens_per_sec"],
                "plane_a_consistency_variants": len(self.summarize_text_variants(consistency_outputs_a)),
                "plane_b_consistency_variants": len(self.summarize_text_variants(consistency_outputs_b)),
                "dialogue_turns": len(dialogue_metrics),
                "dialogue_keyword_hits": keyword_hits,
                "dialogue_topic": chosen["topic"],
            }
            payload["artifacts"] = {
                "status_a": status_a,
                "status_b": status_b,
                "probe_a": probe_a,
                "probe_b": probe_b,
                "consistency_outputs_a": consistency_outputs_a,
                "consistency_outputs_b": consistency_outputs_b,
                "dialogue": dialogue_metrics,
                "transcript": transcript,
            }
            if len(self.summarize_text_variants(consistency_outputs_a)) > 1:
                payload["issues"].append("Plane A showed more than one deterministic answer variant.")
            if len(self.summarize_text_variants(consistency_outputs_b)) > 1:
                payload["issues"].append("Plane B showed more than one deterministic answer variant.")
            if keyword_hits < 8:
                payload["issues"].append("The dialogue drifted away from the chosen topic too often.")
        except Exception as exc:
            payload["status"] = "failed"
            payload["finished_at"] = now_iso()
            payload["issues"].append(str(exc))
            self.capture_failure_artifacts(test_id, [plane_a, plane_b])
        self.save_report(test_id, payload)
        self.summary.append({"id": test_id, "title": payload["title"], "status": payload["status"], "issues": payload["issues"]})

    def test_3_split_122b(self):
        test_id = "test-3-qwen35-122b-a10b-split-distributed"
        started_at = now_iso()
        payload = {
            "title": "Test 3: Qwen3.5-122B-A10B split-host simulated distributed runtime",
            "started_at": started_at,
            "status": "running",
            "issues": [],
            "notes": ["This is a split-host simulation on hpc1, so network latency is intra-host bridge latency, not a real LAN measurement."],
        }
        plane_name = "qwen35-122b-a10b-split"
        try:
            self.delete_other_planes(node_names=["infer-hostd", "worker-hostd-a", "worker-hostd-b"])
            self.wait_gpu_idle()
            self.setup_split_hostds()
            base_state = self.load_state("config/qwen35-122b-a10b/desired-state.json")
            state = self.make_split_plane(base_state, plane_name, 18120, 18121, 29541)
            artifacts_dir = self.report_root / "artifacts" / test_id
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "desired-state.json").write_text(json.dumps(state, indent=2) + "\n")
            start_ts = time.time()
            self.apply_plane(state)
            status = self.wait_ready(
                plane_name,
                timeout=10800,
                require_workers=2,
                require_primary="infer-hostd",
                node_names=["infer-hostd", "worker-hostd-a", "worker-hostd-b"],
            )
            startup_ms = round((time.time() - start_ts) * 1000, 2)
            model_id = self.request_json("GET", f"/api/v1/planes/{plane_name}/interaction/models")["data"][0]["id"]
            prompts = {
                "short": "Reply in one sentence: why is a split infer/worker topology operationally useful?",
                "medium": "Give 4 concise bullets on the cost of distributed inference coordination.",
                "long": "Write a practical 8-step runbook for debugging a split infer/worker rollout across two GPUs.",
            }
            speed = {}
            for label, prompt in prompts.items():
                req = {"model": model_id, "messages": [{"role": "user", "content": prompt}], "max_tokens": 256, "temperature": 0}
                speed[label] = {
                    "stream": self.stream_probe(plane_name, {**req, "stream": True}, ["0", "1"]),
                    "chat": self.chat_probe(plane_name, req, ["0", "1"]),
                }
            context_messages = [
                {"role": "user", "content": "Memorize these exact tokens: orchard-55, checksum T7Q9, city Brno, protocol amber. Reply only with STORED."},
                {"role": "assistant", "content": "STORED"},
                {"role": "user", "content": "Explain in two short sentences what rendezvous metadata is used for."},
                {"role": "assistant", "content": "It helps workers find each other and agree on the distributed launch contract."},
                {"role": "user", "content": "Give me the exact token and checksum from the first turn as token=<...>; checksum=<...>."},
            ]
            context_result = self.chat_probe(
                plane_name,
                {"model": model_id, "messages": context_messages, "max_tokens": 64, "temperature": 0},
                ["0", "1"],
            )
            context_pass = "orchard-55" in context_result["content"] and "T7Q9" in context_result["content"]
            local_reference_path = self.report_root / "test-1-qwen35-122b-a10b-local-distributed.json"
            local_reference = json.loads(local_reference_path.read_text()) if local_reference_path.exists() else {}
            payload["status"] = "ok"
            payload["finished_at"] = now_iso()
            payload["metrics"] = {
                "startup_ms": startup_ms,
                "model": model_id,
                "worker_group_ready": status.get("worker_group_ready"),
                "speed_short_ttft_ms": speed["short"]["stream"]["ttft_ms"],
                "speed_short_total_ms": speed["short"]["chat"]["elapsed_ms"],
                "speed_medium_ttft_ms": speed["medium"]["stream"]["ttft_ms"],
                "speed_medium_total_ms": speed["medium"]["chat"]["elapsed_ms"],
                "speed_long_ttft_ms": speed["long"]["stream"]["ttft_ms"],
                "speed_long_total_ms": speed["long"]["chat"]["elapsed_ms"],
                "context_recall_pass": context_pass,
                "local_vs_split_short_total_delta_ms": (
                    speed["short"]["chat"]["elapsed_ms"] - (local_reference.get("artifacts", {}).get("speed", {}).get("short", {}).get("chat", {}).get("elapsed_ms", 0))
                    if local_reference
                    else None
                ),
            }
            payload["artifacts"] = {
                "status": status,
                "speed": speed,
                "context_result": context_result,
            }
            if not context_pass:
                payload["issues"].append("The split-host run lost an exact token during the context recall check.")
        except Exception as exc:
            payload["status"] = "failed"
            payload["finished_at"] = now_iso()
            payload["issues"].append(str(exc))
            self.capture_failure_artifacts(test_id, [plane_name])
        self.save_report(test_id, payload)
        self.summary.append({"id": test_id, "title": payload["title"], "status": payload["status"], "issues": payload["issues"]})

    def test_4_split_dual_35b(self):
        test_id = "test-4-qwen35-35b-a3b-split-dual-dialog"
        started_at = now_iso()
        payload = {
            "title": "Test 4: dual Qwen3.5-35B-A3B split-host simulated dialogue",
            "started_at": started_at,
            "status": "running",
            "issues": [],
            "notes": ["This is a split-host simulation on hpc1, so network latency is intra-host bridge latency, not a real LAN measurement."],
        }
        plane_a = "qwen35-35b-a3b-split-a"
        plane_b = "qwen35-35b-a3b-split-b"
        try:
            self.delete_other_planes(node_names=["infer-hostd", "worker-hostd-a", "worker-hostd-b"])
            self.wait_gpu_idle()
            self.setup_split_hostds()
            base_state = self.load_state("config/qwen35-35b-a3b/desired-state.json")
            state_a = self.make_single_worker_plane(base_state, plane_a, "0", 18130, 18131, 29551, execution_mode="infer-only")
            state_b = self.make_single_worker_plane(base_state, plane_b, "1", 18132, 18133, 29552, execution_mode="infer-only")
            state_b["worker_group"]["members"][0]["node_name"] = "worker-hostd-b"
            state_b["runtime_gpu_nodes"][0]["node_name"] = "worker-hostd-b"
            state_b["nodes"][1]["name"] = "worker-hostd-b"
            state_b["disks"][1]["node_name"] = "worker-hostd-b"
            state_b["disks"][3]["node_name"] = "worker-hostd-b"
            state_b["instances"][1]["node_name"] = "worker-hostd-b"
            state_b["instances"][1]["environment"]["COMET_NODE_NAME"] = "worker-hostd-b"
            state_b["instances"][1]["labels"]["comet.node"] = "worker-hostd-b"
            state_b["instances"][1]["labels"]["comet.requested.node"] = "worker-hostd-b"
            artifacts_dir = self.report_root / "artifacts" / test_id
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "plane-a.json").write_text(json.dumps(state_a, indent=2) + "\n")
            (artifacts_dir / "plane-b.json").write_text(json.dumps(state_b, indent=2) + "\n")
            start_ts = time.time()
            self.apply_plane(state_a)
            self.apply_plane(state_b)
            status_a = self.wait_ready(
                plane_a,
                timeout=7200,
                require_workers=1,
                require_primary="infer-hostd",
                node_names=["infer-hostd", "worker-hostd-a", "worker-hostd-b"],
            )
            status_b = self.wait_ready(
                plane_b,
                timeout=7200,
                require_workers=1,
                require_primary="infer-hostd",
                node_names=["infer-hostd", "worker-hostd-a", "worker-hostd-b"],
            )
            startup_ms = round((time.time() - start_ts) * 1000, 2)
            model_a = self.request_json("GET", f"/api/v1/planes/{plane_a}/interaction/models")["data"][0]["id"]
            model_b = self.request_json("GET", f"/api/v1/planes/{plane_b}/interaction/models")["data"][0]["id"]
            probe_a = self.chat_probe(plane_a, {"model": model_a, "messages": [{"role": "user", "content": "In two short sentences, explain why a split infer/worker setup can simplify node roles."}], "max_tokens": 96, "temperature": 0}, ["0"])
            probe_b = self.chat_probe(plane_b, {"model": model_b, "messages": [{"role": "user", "content": "In two short sentences, explain why a split infer/worker setup can simplify node roles."}], "max_tokens": 96, "temperature": 0}, ["1"])
            rnd = random.Random(20260325)
            topics = [
                {"topic": "whether urban rail systems should prioritize overnight freight slots", "keywords": ["urban", "rail", "freight", "slots"]},
                {"topic": "whether local clinics should adopt shared procurement for equipment", "keywords": ["clinics", "shared", "procurement", "equipment"]},
                {"topic": "whether universities should publish reproducibility checklists with papers", "keywords": ["universities", "reproducibility", "checklists", "papers"]},
            ]
            chosen = rnd.choice(topics)
            transcript = []
            dialogue_metrics = []
            for turn in range(12):
                speaker = "A" if turn % 2 == 0 else "B"
                plane = plane_a if speaker == "A" else plane_b
                model = model_a if speaker == "A" else model_b
                gpu_ids = ["0"] if speaker == "A" else ["1"]
                system_prompt = (
                    "You are Agent A. Discuss the topic constructively in 2-3 sentences and keep continuity."
                    if speaker == "A"
                    else "You are Agent B. Discuss the topic critically but cooperatively in 2-3 sentences and keep continuity."
                )
                prompt = (
                    f"Topic: {chosen['topic']}.\n"
                    "Continue the discussion naturally. Keep the topic stable and respond to the latest point.\n\n"
                    f"Transcript so far:\n{os.linesep.join(transcript) if transcript else '[start of dialogue]'}"
                )
                result = self.chat_probe(
                    plane,
                    {
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt},
                        ],
                        "max_tokens": 160,
                        "temperature": 0.6,
                    },
                    gpu_ids,
                )
                transcript.append(f"{speaker}: {result['content']}")
                dialogue_metrics.append({"turn": turn + 1, "speaker": speaker, **result})
            keyword_hits = sum(
                1
                for item in transcript
                if any(keyword.lower() in item.lower() for keyword in chosen["keywords"])
            )
            payload["status"] = "ok"
            payload["finished_at"] = now_iso()
            payload["metrics"] = {
                "startup_ms": startup_ms,
                "plane_a_total_ms": probe_a["elapsed_ms"],
                "plane_b_total_ms": probe_b["elapsed_ms"],
                "plane_a_tokens_per_sec": probe_a["tokens_per_sec"],
                "plane_b_tokens_per_sec": probe_b["tokens_per_sec"],
                "dialogue_turns": len(dialogue_metrics),
                "dialogue_keyword_hits": keyword_hits,
                "dialogue_topic": chosen["topic"],
            }
            payload["artifacts"] = {
                "status_a": status_a,
                "status_b": status_b,
                "probe_a": probe_a,
                "probe_b": probe_b,
                "dialogue": dialogue_metrics,
                "transcript": transcript,
            }
            if keyword_hits < 8:
                payload["issues"].append("The split-host dialogue drifted away from the chosen topic too often.")
        except Exception as exc:
            payload["status"] = "failed"
            payload["finished_at"] = now_iso()
            payload["issues"].append(str(exc))
            self.capture_failure_artifacts(test_id, [plane_a, plane_b])
        self.save_report(test_id, payload)
        self.summary.append({"id": test_id, "title": payload["title"], "status": payload["status"], "issues": payload["issues"]})

    def write_summary(self):
        summary = {
            "generated_at": now_iso(),
            "report_root": str(self.report_root),
            "tests": self.summary,
        }
        (self.report_root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        lines = ["# vLLM Final Test Campaign", "", f"- generated_at: {summary['generated_at']}", ""]
        for item in self.summary:
            lines.append(f"## {item['id']}")
            lines.append(f"- title: {item['title']}")
            lines.append(f"- status: {item['status']}")
            if item["issues"]:
                lines.append("- issues:")
                for issue in item["issues"]:
                    lines.append(f"  - {issue}")
            lines.append("")
        (self.report_root / "summary.md").write_text("\n".join(lines))

    def finalize(self):
        if not self.keep_final:
            for plane in reversed(self.planes_to_cleanup):
                self.delete_plane(plane)
            self.cleanup_split_hostds()
        if self.lock_handle is not None:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self.lock_handle.close()
            self.lock_handle = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=str(pathlib.Path(__file__).resolve().parents[1]))
    parser.add_argument("--no-keep-final", action="store_true")
    parser.add_argument("--tests", default="1,2,3,4")
    args = parser.parse_args()

    campaign = Campaign(pathlib.Path(args.repo_root), keep_final=not args.no_keep_final)
    selected = {item.strip() for item in args.tests.split(",") if item.strip()}
    try:
        campaign.wait_for_controller()
        if "1" in selected:
            campaign.test_1_local_122b()
        if "2" in selected:
            campaign.test_2_dual_35b_dialog()
        if "3" in selected:
            campaign.test_3_split_122b()
        if "4" in selected:
            campaign.test_4_split_dual_35b()
        campaign.write_summary()
    finally:
        campaign.finalize()


if __name__ == "__main__":
    main()
