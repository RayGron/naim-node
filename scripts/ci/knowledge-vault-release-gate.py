#!/usr/bin/env python3
import argparse
import contextlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


PRIVATE_MARKERS = [
    "RESTRICTED_RELEASE_GATE_SECRET",
    "restricted.example.internal",
    "ghp_release_gate_secret",
    "sk-release-gate-secret",
]

LEAK_PATTERNS = [
    ("fixture secret marker", "RESTRICTED_RELEASE_GATE_SECRET"),
    ("fixture private hostname", "restricted.example.internal"),
    ("fixture GitHub token", "ghp_release_gate_secret"),
    ("fixture OpenAI-style token", "sk-release-gate-secret"),
    ("SSH private key", "-----BEGIN OPENSSH PRIVATE KEY-----"),
    ("PEM private key", "-----BEGIN PRIVATE KEY-----"),
    ("private home path", "/home/"),
    ("private WSL path", "/mnt/e/"),
    ("private shared-storage path", "/mnt/shared-storage/"),
    ("private SSH inventory user", "baal@"),
    ("private production address", "195.168."),
    ("private control-plane address", "51.68."),
]


def log(message):
    print(f"[knowledge-release-gate] {message}", flush=True)


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def free_port():
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request_json(port, method, path, payload=None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
      data = json.dumps(payload).encode("utf-8")
      headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: {error.code} {body}") from error


class KnowledgeRuntime:
    def __init__(self, binary, root, name):
        self.binary = Path(binary)
        self.root = Path(root)
        self.name = name
        self.port = free_port()
        self.store_path = self.root / "store"
        self.status_path = self.root / "status.json"
        self.ready_path = self.root / "ready"
        self.log_path = self.root / "knowledged.log"
        self.process = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update(
            {
                "NAIM_KNOWLEDGE_SERVICE_ID": "kv_release_gate",
                "NAIM_NODE_NAME": self.name,
                "NAIM_KNOWLEDGE_STORE_PATH": str(self.store_path),
                "NAIM_KNOWLEDGE_STATUS_PATH": str(self.status_path),
                "NAIM_READY_FILE": str(self.ready_path),
                "NAIM_KNOWLEDGE_LISTEN_HOST": "127.0.0.1",
                "NAIM_KNOWLEDGE_PORT": str(self.port),
            }
        )
        log_handle = open(self.log_path, "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(self.binary)],
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 20
        last_error = ""
        while time.time() < deadline:
            if self.process.poll() is not None:
                break
            try:
                request_json(self.port, "GET", "/health")
                return self
            except Exception as error:
                last_error = str(error)
                time.sleep(0.1)
        output = self.log_path.read_text(encoding="utf-8", errors="replace") if self.log_path.exists() else ""
        raise RuntimeError(f"{self.name} did not become ready: {last_error}\n{output}")

    def __exit__(self, exc_type, exc, traceback):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.process = None

    def get(self, path):
        return request_json(self.port, "GET", path)

    def post(self, path, payload):
        return request_json(self.port, "POST", path, payload)

    def put(self, path, payload):
        return request_json(self.port, "PUT", path, payload)


def run_binary(path):
    log(f"running {path.name}")
    subprocess.run([str(path)], check=True)


def ingest_fixture(runtime):
    public = runtime.post(
        "/v1/source-ingest",
        {
            "source_kind": "document",
            "source_ref": "fixture://release-gate/public",
            "content": (
                "Release gate public knowledge covers store timing, query lifecycle, "
                "replica merge, capsule build, repair, markdown export, and restore."
            ),
            "scope_ids": ["scope.public"],
            "metadata": {"title": "Release Gate Public Fixture"},
        },
    )
    private = runtime.post(
        "/v1/source-ingest",
        {
            "source_kind": "document",
            "source_ref": "fixture://release-gate/private",
            "content": (
                "RESTRICTED_RELEASE_GATE_SECRET restricted.example.internal "
                "ghp_release_gate_secret sk-release-gate-secret"
            ),
            "scope_ids": ["scope.private"],
            "metadata": {"title": "Private Fixture"},
        },
    )
    expect(public.get("status") == "accepted", "public ingest should be accepted")
    expect(private.get("status") == "accepted", "private ingest should be accepted")
    return public, private


def assert_scope_and_query_lifecycle(runtime, public, private, artifact_dir):
    public_block_id = public["source_block_id"]
    private_block_id = private["source_block_id"]

    search = runtime.post("/v1/search", {"query": "release gate", "scope_id": "scope.public"})
    expect(search.get("results"), "public search should return results")
    expect(
        any(item.get("block_id") == public_block_id for item in search["results"]),
        "public search should include public fixture",
    )

    redacted = runtime.post("/v1/search", {"query": "restricted", "scope_id": "scope.public"})
    expect(redacted.get("results"), "out-of-scope search should return a redacted stub")
    redacted_text = json.dumps(redacted, sort_keys=True)
    expect("out_of_scope" in redacted_text, "out-of-scope search should mark redaction reason")
    for marker in PRIVATE_MARKERS:
        expect(marker not in redacted_text, f"redacted search leaked {marker}")

    runtime.post(
        "/v1/catalog",
        {
            "object_id": public_block_id,
            "shard_id": "release-gate-shard",
            "scope_ids": ["scope.public"],
            "hints": ["release gate", "query lifecycle", "restore"],
            "shard_health": {"status": "healthy", "detail": {}},
        },
    )
    route = runtime.post(
        "/v1/query-route",
        {"query": "release gate", "scope_ids": ["scope.public"]},
    )
    expect(route.get("shard_requests"), "query route should plan at least one shard request")

    capsule = runtime.post(
        "/v1/capsules",
        {
            "plane_id": "release-gate-plane",
            "capsule_id": "release-gate-capsule",
            "included": [public_block_id],
        },
    )
    expect(capsule.get("capsule_id") == "release-gate-capsule", "capsule should build")
    expect(
        runtime.get("/v1/capsules/release-gate-capsule").get("status") == "valid",
        "capsule should validate after build",
    )

    context = runtime.post(
        "/v1/context",
        {
            "query": "release gate",
            "scope_id": "scope.public",
            "plane_id": "release-gate-plane",
            "capsule_id": "release-gate-capsule",
            "token_budget": 1200,
        },
    )
    expect(context.get("context"), "query lifecycle should return context entries")

    runtime.post(
        "/v1/replica-merges/schedule",
        {
            "plane_id": "release-gate-plane",
            "capsule_id": "release-gate-capsule",
            "cadence": "daily",
        },
    )
    runtime.post(
        "/v1/overlays",
        {
            "plane_id": "release-gate-plane",
            "capsule_id": "release-gate-capsule",
            "change_type": "claim_add",
            "proposed_blocks": [
                {
                    "knowledge_id": "release-gate.merge",
                    "title": "Release Gate Merge",
                    "body": "Merged by release gate fixture.",
                    "scope_ids": ["scope.public"],
                }
            ],
        },
    )
    merge = runtime.post("/v1/replica-merges/run-due", {"plane_id": "release-gate-plane", "force": True})
    expect(merge.get("jobs"), "replica merge should run")
    expect(merge["jobs"][0].get("status") == "completed", "replica merge should complete")

    repair = runtime.post("/v1/repair", {"apply": True, "full_rebuild": True})
    expect(repair.get("report_id"), "repair should persist a report")

    markdown = runtime.post("/v1/markdown-export", {"scope_ids": ["scope.public"]})
    expect(markdown.get("files"), "public markdown export should produce files")
    export_path = artifact_dir / "public-markdown-export.json"
    write_json(export_path, markdown)
    export_text = json.dumps(markdown, sort_keys=True)
    expect(private_block_id not in export_text, "public markdown export leaked private block id")
    for marker in PRIVATE_MARKERS:
        expect(marker not in export_text, f"public markdown export leaked {marker}")

    graph = runtime.post(
        "/v1/graph-neighborhood",
        {"knowledge_ids": [public.get("knowledge_id", "knowledge.release-gate-public-fixture")], "depth": 1},
    )
    expect("nodes" in graph and "edges" in graph, "graph-neighborhood should return graph payload")
    return {
        "public_block_id": public_block_id,
        "private_block_id": private_block_id,
        "artifact_paths": [str(export_path)],
    }


def create_backup(store_path, backup_path):
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(backup_path, "w:gz") as archive:
        archive.add(store_path, arcname="store")


def restore_backup(backup_path, restore_root):
    restore_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(backup_path, "r:gz") as archive:
        archive.extractall(restore_root)
    return restore_root / "store"


def run_backup_restore_fixture(binary, work_root, artifact_dir):
    log("running backup/restore fixture")
    source_root = work_root / "backup-source"
    restore_root = work_root / "backup-restore"
    backup_path = artifact_dir / "knowledge-vault-fixture-backup.tar.gz"
    with KnowledgeRuntime(binary, source_root, "release-gate-source") as runtime:
        public, private = ingest_fixture(runtime)
        lifecycle = assert_scope_and_query_lifecycle(runtime, public, private, artifact_dir)
    create_backup(source_root / "store", backup_path)
    restored_store = restore_backup(backup_path, restore_root)
    expect(restored_store.exists(), "restored store should exist")
    with KnowledgeRuntime(binary, restore_root, "release-gate-restored") as restored:
        status = restored.get("/v1/status")
        expect(status.get("storage_engine") == "rocksdb", "restored store should report rocksdb")
        public_search = restored.post("/v1/search", {"query": "release gate", "scope_id": "scope.public"})
        expect(public_search.get("results"), "restored store should preserve public search data")
        redacted = restored.post("/v1/search", {"query": "restricted", "scope_id": "scope.public"})
        redacted_text = json.dumps(redacted, sort_keys=True)
        expect("out_of_scope" in redacted_text, "restored store should preserve scope redaction")
        for marker in PRIVATE_MARKERS:
            expect(marker not in redacted_text, f"restored redaction leaked {marker}")
    return {
        "status": "passed",
        "backup_path": str(backup_path),
        "public_block_id": lifecycle["public_block_id"],
        "private_block_id": lifecycle["private_block_id"],
        "public_artifacts": lifecycle["artifact_paths"],
    }


def benchmark_operation(name, timings, fn):
    started = time.perf_counter()
    result = fn()
    timings[name] = round((time.perf_counter() - started) * 1000, 3)
    return result


def run_benchmark(binary, work_root, output_path, item_count):
    log(f"running benchmark baseline with {item_count} items")
    timings = {}
    with KnowledgeRuntime(binary, work_root / "benchmark-store", "release-gate-benchmark") as runtime:
        blocks = []

        def ingest_all():
            for index in range(item_count):
                payload = runtime.post(
                    "/v1/source-ingest",
                    {
                        "source_kind": "document",
                        "source_ref": f"fixture://benchmark/{index}",
                        "content": (
                            f"Benchmark record {index} for store query merge capsule repair timing. "
                            "This public fixture contains no private inventory value."
                        ),
                        "scope_ids": ["scope.benchmark"],
                        "metadata": {"title": f"Benchmark Record {index}"},
                    },
                )
                blocks.append(payload["source_block_id"])

        benchmark_operation("store_ingest_ms", timings, ingest_all)
        benchmark_operation(
            "query_search_ms",
            timings,
            lambda: runtime.post("/v1/search", {"query": "benchmark repair", "scope_id": "scope.benchmark"}),
        )
        benchmark_operation(
            "capsule_build_ms",
            timings,
            lambda: runtime.post(
                "/v1/capsules",
                {
                    "plane_id": "benchmark-plane",
                    "capsule_id": "benchmark-capsule",
                    "included": blocks[: min(10, len(blocks))],
                },
            ),
        )
        runtime.post(
            "/v1/overlays",
            {
                "plane_id": "benchmark-plane",
                "capsule_id": "benchmark-capsule",
                "change_type": "claim_add",
                "proposed_blocks": [
                    {
                        "knowledge_id": "benchmark.merge",
                        "title": "Benchmark Merge",
                        "body": "Benchmark merge timing fixture.",
                        "scope_ids": ["scope.benchmark"],
                    }
                ],
            },
        )
        benchmark_operation(
            "merge_trigger_ms",
            timings,
            lambda: runtime.post(
                "/v1/replica-merges/trigger",
                {"plane_id": "benchmark-plane", "capsule_id": "benchmark-capsule"},
            ),
        )
        benchmark_operation(
            "repair_rebuild_ms",
            timings,
            lambda: runtime.post("/v1/repair", {"apply": True, "full_rebuild": True}),
        )

    payload = {
        "status": "passed",
        "item_count": item_count,
        "storage_engine": "rocksdb",
        "timings_ms": timings,
        "captured_at_unix": int(time.time()),
    }
    write_json(output_path, payload)
    return payload


def scan_artifacts(paths):
    log("running security leakage scan")
    findings = []
    for path_text in paths:
        path = Path(path_text)
        if not path.exists() or not path.is_file():
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        for label, pattern in LEAK_PATTERNS:
            if pattern in content:
                findings.append({"path": str(path), "kind": label, "pattern": pattern})
    return {
        "status": "passed" if not findings else "failed",
        "scanned_files": [Path(item).name for item in paths if Path(item).exists()],
        "checks": [label for label, _ in LEAK_PATTERNS],
        "findings": findings,
    }


def run_gate(args):
    build_dir = Path(args.build_dir).resolve()
    work_root = Path(args.work_root).resolve() if args.work_root else Path(tempfile.mkdtemp(prefix="naim-kv-gate-"))
    artifact_dir = Path(args.artifact_dir).resolve() if args.artifact_dir else work_root / "artifacts"
    benchmark_output = (
        Path(args.benchmark_output).resolve()
        if args.benchmark_output
        else artifact_dir / "knowledge-vault-benchmark-baseline.json"
    )
    report_path = Path(args.report).resolve() if args.report else artifact_dir / "knowledge-vault-release-gate.json"
    if work_root.exists():
        shutil.rmtree(work_root)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    required_binaries = {
        "contract_tests": build_dir / "naim-knowledge-contract-tests",
        "store_tests": build_dir / "naim-knowledge-store-tests",
        "service_tests": build_dir / "naim-knowledge-vault-service-tests",
        "runtime": build_dir / "naim-knowledged",
    }
    missing = [name for name, path in required_binaries.items() if not os.access(path, os.X_OK)]
    if missing:
        rendered = ", ".join(f"{name}={required_binaries[name]}" for name in missing)
        raise RuntimeError(f"missing built Knowledge Vault release-gate binaries: {rendered}")

    started = time.perf_counter()
    run_binary(required_binaries["contract_tests"])
    run_binary(required_binaries["store_tests"])
    run_binary(required_binaries["service_tests"])
    backup_restore = run_backup_restore_fixture(required_binaries["runtime"], work_root, artifact_dir)
    benchmark = run_benchmark(required_binaries["runtime"], work_root, benchmark_output, args.benchmark_items)
    public_artifacts = [benchmark_output, *backup_restore["public_artifacts"]]
    security = scan_artifacts(public_artifacts)
    expect(security["status"] == "passed", f"security leakage scan failed: {security['findings']}")

    report = {
        "status": "passed",
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "contract_tests": "passed",
        "scope_redaction_and_query_lifecycle": "passed",
        "backup_restore": {
            "status": backup_restore["status"],
            "backup_artifact": Path(backup_restore["backup_path"]).name,
            "restored_public_block": True,
            "restored_private_scope_redaction": True,
        },
        "benchmark_baseline": benchmark,
        "security_scan": security,
    }
    write_json(report_path, report)
    log(f"release gate passed; report={report_path}")
    return report


def main():
    parser = argparse.ArgumentParser(description="Run the Knowledge Vault release gate.")
    parser.add_argument("--build-dir", required=True)
    parser.add_argument("--work-root")
    parser.add_argument("--artifact-dir")
    parser.add_argument("--benchmark-output")
    parser.add_argument("--benchmark-items", type=int, default=25)
    parser.add_argument("--report")
    args = parser.parse_args()

    try:
        run_gate(args)
    except Exception as error:
        print(f"knowledge-vault release gate failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
