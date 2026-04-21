#!/usr/bin/env bash

naim_live_plane_name() {
  python3 - "$1" <<'PYJSON'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as source:
    print(json.load(source)["plane_name"])
PYJSON
}

naim_live_seed_connected_hostd() {
  local db_path="$1"
  local node_name="$2"
  local gpu_count="${3:-2}"
  local total_memory_bytes="${4:-137438953472}"
  python3 - "$db_path" "$node_name" "$gpu_count" "$total_memory_bytes" <<'PYJSON'
import json
import sqlite3
import sys

db_path, node_name, gpu_count, total_memory_bytes = sys.argv[1:5]
capabilities = {
    "capacity_summary": {
        "gpu_count": int(gpu_count),
        "storage_root": "/tmp/naim",
        "storage_total_bytes": 200_000_000_000,
        "storage_free_bytes": 150_000_000_000,
        "total_memory_bytes": int(total_memory_bytes),
    }
}
conn = sqlite3.connect(db_path)
try:
    conn.execute(
        """
        INSERT INTO registered_hosts(
          node_name,
          public_key_base64,
          transport_mode,
          execution_mode,
          registration_state,
          derived_role,
          role_reason,
          session_state,
          session_expires_at,
          last_session_at,
          last_heartbeat_at,
          capabilities_json,
          status_message
        ) VALUES (?, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=', 'out', 'mixed', 'registered', 'worker',
                  'live v2 connected hostd', 'connected', datetime('now', '+1 day'),
                  datetime('now'), datetime('now'), ?, 'registered by live v2 fixture')
        ON CONFLICT(node_name) DO UPDATE SET
          public_key_base64=excluded.public_key_base64,
          transport_mode=excluded.transport_mode,
          execution_mode=excluded.execution_mode,
          registration_state=excluded.registration_state,
          derived_role=excluded.derived_role,
          role_reason=excluded.role_reason,
          session_state=excluded.session_state,
          session_expires_at=excluded.session_expires_at,
          last_session_at=excluded.last_session_at,
          last_heartbeat_at=excluded.last_heartbeat_at,
          capabilities_json=excluded.capabilities_json,
          status_message=excluded.status_message
        """,
        (node_name, json.dumps(capabilities)),
    )
    conn.commit()
finally:
    conn.close()
PYJSON
}

naim_live_apply_v2_state() {
  local build_dir="$1"
  local db_path="$2"
  local artifacts_root="$3"
  local state_path="$4"
  local plane_name
  plane_name="$(naim_live_plane_name "$state_path")"
  "${build_dir}/naim-controller" apply-state-file     --state "$state_path"     --db "$db_path"     --artifacts-root "$artifacts_root" >/dev/null
  "${build_dir}/naim-controller" start-plane --db "$db_path" --plane "$plane_name" >/dev/null
}

naim_live_write_compute_state() {
  local output_path="$1"
  local plane_name="$2"
  local worker_count="${3:-2}"
  python3 - "$output_path" "$plane_name" "$worker_count" <<'PYJSON'
import json
import sys

output_path, plane_name, worker_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
nodes = []
assignments = []
for index in range(worker_count):
    node = "node-a" if index == 0 else "node-b"
    if node not in [item["name"] for item in nodes]:
        nodes.append({"name": node, "execution_mode": "mixed", "gpu_memory_mb": {"0": 24576, "1": 24576}})
    assignments.append({"node": node, "gpu_device": "0"})
state = {
    "version": 2,
    "plane_name": plane_name,
    "plane_mode": "compute",
    "runtime": {"engine": "custom", "workers": worker_count},
    "topology": {"nodes": nodes},
    "worker": {
        "image": "nvidia/cuda:12.9.1-runtime-ubuntu24.04",
        "assignments": assignments,
        "start": {"type": "command", "command": "sleep infinity"},
        "storage": {"size_gb": 1, "mount_path": "/naim/private"},
    },
    "resources": {
        "worker": {
            "placement_mode": "auto",
            "share_mode": "exclusive",
            "gpu_fraction": 1.0,
            "memory_cap_mb": 4096,
        },
        "shared_disk_gb": 1,
    },
    "app": {"enabled": False},
}
with open(output_path, "w", encoding="utf-8") as output:
    json.dump(state, output, indent=2)
    output.write("\n")
PYJSON
}

naim_live_write_llm_state() {
  local output_path="$1"
  local plane_name="$2"
  local gguf_path="$3"
  local gateway_port="${4:-18184}"
  local inference_port="${5:-18194}"
  python3 - "$output_path" "$plane_name" "$gguf_path" "$gateway_port" "$inference_port" <<'PYJSON'
import json
import sys

output_path, plane_name, gguf_path, gateway_port, inference_port = sys.argv[1:6]
state = {
    "version": 2,
    "plane_name": plane_name,
    "plane_mode": "llm",
    "model": {
        "source": {"type": "local", "path": gguf_path, "ref": "local/final-live"},
        "materialization": {"mode": "reference", "local_path": gguf_path},
        "served_model_name": plane_name,
    },
    "runtime": {
        "engine": "llama.cpp",
        "distributed_backend": "llama_rpc",
        "workers": 1,
        "max_model_len": 1024,
        "max_num_seqs": 1,
        "gpu_memory_utilization": 0.5,
    },
    "topology": {
        "nodes": [{"name": "local-hostd", "execution_mode": "mixed", "gpu_memory_mb": {"0": 24576}}]
    },
    "infer": {"replicas": 1},
    "worker": {"assignments": [{"node": "local-hostd", "gpu_device": "0"}]},
    "network": {
        "gateway_port": int(gateway_port),
        "inference_port": int(inference_port),
        "server_name": f"{plane_name}.internal",
    },
    "app": {"enabled": False},
    "resources": {
        "worker": {
            "placement_mode": "auto",
            "share_mode": "exclusive",
            "gpu_fraction": 1.0,
            "memory_cap_mb": 4096,
        },
        "shared_disk_gb": 1,
    },
}
with open(output_path, "w", encoding="utf-8") as output:
    json.dump(state, output, indent=2)
    output.write("\n")
PYJSON
}


naim_live_seed_admin_session() {
  local db_path="$1"
  local token="$2"
  python3 - "$db_path" "$token" <<'PYJSON'
import sqlite3
import sys

db_path, token = sys.argv[1:3]
conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "INSERT OR IGNORE INTO users(id, username, role, password_hash) VALUES (1, 'live-admin', 'admin', '')"
    )
    conn.execute(
        """
        INSERT INTO auth_sessions(token, user_id, session_kind, plane_name, expires_at, last_used_at)
        VALUES (?, 1, 'web', '', datetime('now', '+1 day'), datetime('now'))
        ON CONFLICT(token) DO UPDATE SET
          user_id=excluded.user_id,
          session_kind=excluded.session_kind,
          plane_name=excluded.plane_name,
          expires_at=excluded.expires_at,
          last_used_at=excluded.last_used_at
        """,
        (token,),
    )
    conn.commit()
finally:
    conn.close()
PYJSON
}
