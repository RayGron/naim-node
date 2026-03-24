#!/usr/bin/env python3
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


STOP_REQUESTED = False


def env(name: str, default: str = "") -> str:
    value = os.environ.get(name, "")
    return value if value else default


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "")
    if not value:
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name, "")
    if not value:
        return default
    return float(value)


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name, "")
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json_or_empty(path: Path) -> dict:
    if not path.is_file():
        return {}
    return json.loads(path.read_text())


def active_model_path(control_root: str) -> Path:
    return Path(control_root) / "active-model.json"


def worker_upstream_path(control_root: str) -> Path:
    return Path(control_root) / "worker-upstream.json"


def worker_group_dir(control_root: str) -> Path:
    return Path(control_root) / "worker-group"


def worker_group_member_path(control_root: str, instance_name: str) -> Path:
    return worker_group_dir(control_root) / f"{instance_name}.json"


def status_path(private_disk_path: str) -> Path:
    return Path(
        env(
            "COMET_WORKER_RUNTIME_STATUS_PATH",
            str(Path(private_disk_path) / "worker-runtime-status.json"),
        )
    )


def ready_path() -> Path:
    return Path("/tmp/comet-ready")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def set_ready(ready: bool) -> None:
    path = ready_path()
    if ready:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ready\n")
        return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def resolve_model_ref(control_root: str) -> tuple[str, dict]:
    explicit_model = env("COMET_WORKER_MODEL_PATH")
    active_model = load_json_or_empty(active_model_path(control_root))
    candidates = [
        explicit_model,
        active_model.get("cached_runtime_model_path", ""),
        active_model.get("runtime_model_path", ""),
        active_model.get("cached_local_model_path", ""),
        active_model.get("local_model_path", ""),
        active_model.get("source_model_id", ""),
        active_model.get("model_id", ""),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        expanded = Path(os.path.expanduser(candidate))
        if expanded.exists():
            return str(expanded), active_model
    for candidate in candidates:
        if candidate:
            return candidate, active_model
    raise RuntimeError("unable to resolve a model path or model id for vLLM worker")


def resolve_served_model_name(active_model: dict) -> str:
    return env(
        "COMET_VLLM_SERVED_MODEL_NAME",
        active_model.get("served_model_name")
        or active_model.get("model_id")
        or env("COMET_PLANE_NAME", "comet-vllm-model"),
    )


def resolve_active_model_id(active_model: dict, served_model_name: str) -> str:
    return (
        active_model.get("model_id")
        or active_model.get("source_model_id")
        or served_model_name
    )


def resolve_advertised_base_url(port: int) -> str:
    explicit = env("COMET_WORKER_ADVERTISED_BASE_URL")
    if explicit:
        return explicit.rstrip("/")
    instance_name = env("COMET_INSTANCE_NAME", "worker")
    return f"http://{instance_name}:{port}"


def build_status(
    *,
    phase: str,
    ready: bool,
    started_at: str,
    last_activity_at: str,
    active_model_id: str,
    model_ref: str,
    served_model_name: str,
    port: int,
    runtime_pid: int,
) -> dict:
    return {
        "plane_name": env("COMET_PLANE_NAME"),
        "control_root": env("COMET_CONTROL_ROOT"),
        "controller_url": "",
        "primary_infer_node": "",
        "instance_name": env("COMET_INSTANCE_NAME", "worker"),
        "instance_role": env("COMET_INSTANCE_ROLE", "worker"),
        "node_name": env("COMET_NODE_NAME"),
        "runtime_backend": "vllm-worker",
        "runtime_phase": phase,
        "enabled_gpu_nodes": 1 if env("COMET_GPU_DEVICE") else 0,
        "registry_entries": 0,
        "supervisor_pid": os.getpid(),
        "runtime_pid": runtime_pid,
        "engine_pid": runtime_pid,
        "aliases": [],
        "active_model_id": active_model_id,
        "active_served_model_name": served_model_name,
        "active_runtime_profile": "vllm-openai",
        "cached_local_model_path": model_ref,
        "model_path": model_ref,
        "gpu_device": env("COMET_GPU_DEVICE", env("COMET_WORKER_GPU_DEVICE")),
        "gateway_listen": "",
        "upstream_models_url": f"http://127.0.0.1:{port}/v1/models",
        "inference_health_url": f"http://127.0.0.1:{port}/health",
        "gateway_health_url": "",
        "started_at": started_at,
        "last_activity_at": last_activity_at,
        "ready": ready,
        "active_model_ready": True,
        "gateway_plan_ready": False,
        "inference_ready": ready,
        "gateway_ready": False,
        "launch_ready": ready,
    }


def build_worker_upstream_contract(
    *,
    control_root: str,
    ready: bool,
    phase: str,
    node_name: str,
    instance_name: str,
    active_model_id: str,
    served_model_name: str,
    port: int,
    last_activity_at: str,
) -> dict:
    base_url = resolve_advertised_base_url(port)
    return {
        "plane_name": env("COMET_PLANE_NAME"),
        "control_root": control_root,
        "node_name": node_name,
        "instance_name": instance_name,
        "runtime_backend": "vllm-worker",
        "ready": ready,
        "phase": phase,
        "active_model_id": active_model_id,
        "served_model_name": served_model_name,
        "port": port,
        "base_url": base_url,
        "health_url": f"{base_url}/health",
        "models_url": f"{base_url}/v1/models",
        "last_activity_at": last_activity_at,
    }


def build_worker_group_member_contract(
    *,
    control_root: str,
    ready: bool,
    phase: str,
    node_name: str,
    instance_name: str,
    active_model_id: str,
    served_model_name: str,
    port: int,
    last_activity_at: str,
) -> dict:
    return {
        "plane_name": env("COMET_PLANE_NAME"),
        "control_root": control_root,
        "group_id": env("COMET_WORKER_GROUP_ID", "default-worker-group"),
        "distributed_backend": env("COMET_DISTRIBUTED_BACKEND", "vllm"),
        "worker_selection_policy": env(
            "COMET_WORKER_SELECTION_POLICY", "prefer-free-then-share"
        ),
        "node_name": node_name,
        "instance_name": instance_name,
        "runtime_backend": "vllm-worker",
        "rank": env_int("COMET_WORKER_GROUP_RANK", 0),
        "world_size": env_int("COMET_WORKER_GROUP_WORLD_SIZE", 1),
        "leader": env_bool("COMET_WORKER_GROUP_LEADER", False),
        "gpu_device": env("COMET_GPU_DEVICE", env("COMET_WORKER_GPU_DEVICE")),
        "ready": ready,
        "phase": phase,
        "active_model_id": active_model_id,
        "served_model_name": served_model_name,
        "port": port,
        "base_url": resolve_advertised_base_url(port),
        "health_url": f"{resolve_advertised_base_url(port)}/health",
        "models_url": f"{resolve_advertised_base_url(port)}/v1/models",
        "rendezvous_port": env_int("COMET_RENDEZVOUS_PORT", 29500),
        "last_activity_at": last_activity_at,
    }


def probe(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False


def build_command(model_ref: str, served_model_name: str, port: int) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--model",
        model_ref,
        "--served-model-name",
        served_model_name,
        "--tensor-parallel-size",
        str(env_int("COMET_VLLM_TENSOR_PARALLEL_SIZE", 1)),
        "--pipeline-parallel-size",
        str(env_int("COMET_VLLM_PIPELINE_PARALLEL_SIZE", 1)),
        "--max-model-len",
        str(env_int("COMET_VLLM_MAX_MODEL_LEN", 8192)),
        "--max-num-seqs",
        str(env_int("COMET_VLLM_MAX_NUM_SEQS", 16)),
        "--gpu-memory-utilization",
        str(env_float("COMET_VLLM_GPU_MEMORY_UTILIZATION", 0.9)),
    ]
    download_dir = env("COMET_VLLM_DOWNLOAD_DIR")
    if download_dir:
        command.extend(["--download-dir", download_dir])
    if env_bool("COMET_VLLM_ENFORCE_EAGER", False):
        command.append("--enforce-eager")
    if env_bool("COMET_VLLM_TRUST_REMOTE_CODE", False):
        command.append("--trust-remote-code")
    return command


def signal_handler(_signum, _frame) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def main() -> int:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    plane_name = env("COMET_PLANE_NAME", "unknown")
    instance_name = env("COMET_INSTANCE_NAME", "worker")
    control_root = env("COMET_CONTROL_ROOT")
    private_disk_path = env("COMET_PRIVATE_DISK_PATH", "/comet/private")
    port = env_int("COMET_VLLM_PORT", env_int("COMET_INFERENCE_PORT", 8000))

    if not control_root:
        raise RuntimeError("COMET_CONTROL_ROOT is required for vLLM worker mode")

    model_ref, active_model = resolve_model_ref(control_root)
    served_model_name = resolve_served_model_name(active_model)
    active_model_id = resolve_active_model_id(active_model, served_model_name)
    started_at = utc_now_iso()
    status_file = status_path(private_disk_path)
    upstream_file = worker_upstream_path(control_root)
    worker_group_member_file = worker_group_member_path(control_root, instance_name)
    health_url = f"http://127.0.0.1:{port}/health"
    child_env = os.environ.copy()
    instance_name = env("COMET_INSTANCE_NAME", "worker")
    node_name = env("COMET_NODE_NAME")

    gpu_device = env("COMET_GPU_DEVICE", env("COMET_WORKER_GPU_DEVICE"))
    if gpu_device:
        child_env.setdefault("CUDA_VISIBLE_DEVICES", gpu_device)
        child_env.setdefault("NVIDIA_VISIBLE_DEVICES", gpu_device)

    command = build_command(model_ref, served_model_name, port)
    print(
        f"[comet-worker] launching vLLM plane={plane_name} instance={instance_name} "
        f"model={model_ref} served_model={served_model_name} port={port}",
        flush=True,
    )
    print(f"[comet-worker] command={' '.join(command)}", flush=True)

    child = subprocess.Popen(command, env=child_env)
    set_ready(False)
    write_json(
        status_file,
        build_status(
            phase="starting",
            ready=False,
            started_at=started_at,
            last_activity_at="",
            active_model_id=active_model_id,
            model_ref=model_ref,
            served_model_name=served_model_name,
            port=port,
            runtime_pid=child.pid,
        ),
    )
    write_json(
        worker_group_member_file,
        build_worker_group_member_contract(
            control_root=control_root,
            ready=False,
            phase="starting",
            node_name=node_name,
            instance_name=instance_name,
            active_model_id=active_model_id,
            served_model_name=served_model_name,
            port=port,
            last_activity_at="",
        ),
    )
    if env_bool("COMET_WORKER_GROUP_LEADER", False):
        write_json(
            upstream_file,
            build_worker_upstream_contract(
                control_root=control_root,
                ready=False,
                phase="starting",
                node_name=node_name,
                instance_name=instance_name,
                active_model_id=active_model_id,
                served_model_name=served_model_name,
                port=port,
                last_activity_at="",
            ),
        )

    while child.poll() is None and not STOP_REQUESTED:
        healthy = probe(health_url)
        now = utc_now_iso()
        set_ready(healthy)
        write_json(
            status_file,
            build_status(
                phase="running" if healthy else "starting",
                ready=healthy,
                started_at=started_at,
                last_activity_at=now,
                active_model_id=active_model_id,
                model_ref=model_ref,
                served_model_name=served_model_name,
                port=port,
                runtime_pid=child.pid,
            ),
        )
        write_json(
            worker_group_member_file,
            build_worker_group_member_contract(
                control_root=control_root,
                ready=healthy,
                phase="running" if healthy else "starting",
                node_name=node_name,
                instance_name=instance_name,
                active_model_id=active_model_id,
                served_model_name=served_model_name,
                port=port,
                last_activity_at=now,
            ),
        )
        if env_bool("COMET_WORKER_GROUP_LEADER", False):
            write_json(
                upstream_file,
                build_worker_upstream_contract(
                    control_root=control_root,
                    ready=healthy,
                    phase="running" if healthy else "starting",
                    node_name=node_name,
                    instance_name=instance_name,
                    active_model_id=active_model_id,
                    served_model_name=served_model_name,
                    port=port,
                    last_activity_at=now,
                ),
            )
        time.sleep(2)

    if STOP_REQUESTED and child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=env_int("COMET_WORKER_GRACEFUL_STOP_TIMEOUT_SEC", 15))
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()

    set_ready(False)
    write_json(
        status_file,
        build_status(
            phase="stopped" if child.returncode == 0 else "failed",
            ready=False,
            started_at=started_at,
            last_activity_at=utc_now_iso(),
            active_model_id=active_model_id,
            model_ref=model_ref,
            served_model_name=served_model_name,
            port=port,
            runtime_pid=child.pid,
        ),
    )
    write_json(
        worker_group_member_file,
        build_worker_group_member_contract(
            control_root=control_root,
            ready=False,
            phase="stopped" if child.returncode == 0 else "failed",
            node_name=node_name,
            instance_name=instance_name,
            active_model_id=active_model_id,
            served_model_name=served_model_name,
            port=port,
            last_activity_at=utc_now_iso(),
        ),
    )
    if env_bool("COMET_WORKER_GROUP_LEADER", False):
        write_json(
            upstream_file,
            build_worker_upstream_contract(
                control_root=control_root,
                ready=False,
                phase="stopped" if child.returncode == 0 else "failed",
                node_name=node_name,
                instance_name=instance_name,
                active_model_id=active_model_id,
                served_model_name=served_model_name,
                port=port,
                last_activity_at=utc_now_iso(),
            ),
        )
    return 0 if child.returncode is None else child.returncode


if __name__ == "__main__":
    sys.exit(main())
