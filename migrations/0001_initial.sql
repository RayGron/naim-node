PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE planes (
    name TEXT PRIMARY KEY,
    shared_disk_name TEXT NOT NULL,
    control_root TEXT NOT NULL DEFAULT '',
    artifacts_root TEXT NOT NULL DEFAULT '',
    plane_mode TEXT NOT NULL DEFAULT 'compute',
    bootstrap_model_json TEXT NOT NULL DEFAULT '',
    interaction_settings_json TEXT NOT NULL DEFAULT '',
    desired_state_json TEXT NOT NULL DEFAULT '',
    inference_config_json TEXT NOT NULL DEFAULT '',
    gateway_config_json TEXT NOT NULL DEFAULT '',
    runtime_gpu_nodes_json TEXT NOT NULL DEFAULT '',
    generation INTEGER NOT NULL DEFAULT 1,
    applied_generation INTEGER NOT NULL DEFAULT 0,
    rebalance_iteration INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nodes (
    name TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE registered_hosts (
    node_name TEXT PRIMARY KEY,
    advertised_address TEXT NOT NULL DEFAULT '',
    public_key_base64 TEXT NOT NULL DEFAULT '',
    controller_public_key_fingerprint TEXT NOT NULL DEFAULT '',
    transport_mode TEXT NOT NULL DEFAULT 'out',
    registration_state TEXT NOT NULL DEFAULT 'registered',
    session_state TEXT NOT NULL DEFAULT 'disconnected',
    session_token TEXT NOT NULL DEFAULT '',
    session_expires_at TEXT NOT NULL DEFAULT '',
    session_host_sequence INTEGER NOT NULL DEFAULT 0,
    session_controller_sequence INTEGER NOT NULL DEFAULT 0,
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    status_message TEXT NOT NULL DEFAULT '',
    last_session_at TEXT NOT NULL DEFAULT '',
    last_heartbeat_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE host_peer_links (
    observer_node_name TEXT NOT NULL,
    peer_node_name TEXT NOT NULL,
    peer_endpoint TEXT NOT NULL DEFAULT '',
    local_interface TEXT NOT NULL DEFAULT '',
    remote_address TEXT NOT NULL DEFAULT '',
    seen_udp INTEGER NOT NULL DEFAULT 0,
    tcp_reachable INTEGER NOT NULL DEFAULT 0,
    rtt_ms INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL DEFAULT '',
    last_probe_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(observer_node_name, peer_node_name)
);

CREATE INDEX idx_host_peer_links_peer
    ON host_peer_links(peer_node_name, observer_node_name);

CREATE TABLE file_transfer_tickets (
    ticket_id TEXT PRIMARY KEY,
    source_node_name TEXT NOT NULL,
    requester_node_name TEXT NOT NULL,
    source_paths_json TEXT NOT NULL DEFAULT '[]',
    expires_at TEXT NOT NULL,
    max_chunk_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_validated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE plane_nodes (
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    PRIMARY KEY (plane_name, node_name),
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE node_gpus (
    node_name TEXT NOT NULL,
    gpu_device TEXT NOT NULL,
    memory_mb INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (node_name, gpu_device),
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE virtual_disks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    host_path TEXT NOT NULL,
    container_path TEXT NOT NULL,
    size_gb INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (name, node_name),
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE disk_runtime_state (
    disk_name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    image_path TEXT NOT NULL DEFAULT '',
    filesystem_type TEXT NOT NULL DEFAULT '',
    loop_device TEXT NOT NULL DEFAULT '',
    mount_point TEXT NOT NULL DEFAULT '',
    runtime_state TEXT NOT NULL DEFAULT '',
    attached_at TEXT NOT NULL DEFAULT '',
    mounted_at TEXT NOT NULL DEFAULT '',
    last_verified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status_message TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (disk_name, node_name),
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE instances (
    name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    image TEXT NOT NULL,
    command TEXT NOT NULL,
    private_disk_name TEXT NOT NULL,
    shared_disk_name TEXT NOT NULL,
    gpu_device TEXT,
    placement_mode TEXT NOT NULL DEFAULT 'manual',
    share_mode TEXT NOT NULL DEFAULT 'exclusive',
    gpu_fraction REAL NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 100,
    preemptible INTEGER NOT NULL DEFAULT 0,
    memory_cap_mb INTEGER,
    private_disk_size_gb INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE instance_dependencies (
    instance_name TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    PRIMARY KEY (instance_name, dependency_name),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE instance_environment (
    instance_name TEXT NOT NULL,
    env_key TEXT NOT NULL,
    env_value TEXT NOT NULL,
    PRIMARY KEY (instance_name, env_key),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE instance_labels (
    instance_name TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (instance_name, label_key),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE instance_published_ports (
    instance_name TEXT NOT NULL,
    host_ip TEXT NOT NULL,
    host_port INTEGER NOT NULL,
    container_port INTEGER NOT NULL,
    PRIMARY KEY (instance_name, host_ip, host_port, container_port),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE interaction_sessions (
    session_id TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    owner_kind TEXT NOT NULL DEFAULT 'anonymous',
    owner_user_id INTEGER,
    auth_session_kind TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'active',
    last_used_at TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL DEFAULT '',
    archive_path TEXT NOT NULL DEFAULT '',
    archive_codec TEXT NOT NULL DEFAULT '',
    archive_sha256 TEXT NOT NULL DEFAULT '',
    context_state_json TEXT NOT NULL DEFAULT '{}',
    latest_prompt_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_context_tokens INTEGER NOT NULL DEFAULT 0,
    compression_state TEXT NOT NULL DEFAULT 'none',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_interaction_sessions_owner
    ON interaction_sessions(plane_name, owner_kind, owner_user_id, updated_at DESC);

CREATE TABLE interaction_messages (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    kind TEXT NOT NULL,
    content_json TEXT NOT NULL DEFAULT 'null',
    usage_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, seq),
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE interaction_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    turn_range_start INTEGER NOT NULL DEFAULT 0,
    turn_range_end INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE interaction_archives (
    session_id TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    owner_kind TEXT NOT NULL DEFAULT 'anonymous',
    owner_user_id INTEGER,
    archive_path TEXT NOT NULL DEFAULT '',
    archive_codec TEXT NOT NULL DEFAULT '',
    archive_sha256 TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL DEFAULT '',
    restore_state TEXT NOT NULL DEFAULT 'archived',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE host_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    desired_generation INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    assignment_type TEXT NOT NULL,
    desired_state_json TEXT NOT NULL,
    artifacts_root TEXT NOT NULL,
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    progress_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE node_availability_overrides (
    node_name TEXT PRIMARY KEY,
    availability TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rollout_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plane_name TEXT NOT NULL DEFAULT '',
    desired_generation INTEGER NOT NULL,
    step INTEGER NOT NULL,
    worker_name TEXT NOT NULL,
    action TEXT NOT NULL,
    target_node_name TEXT NOT NULL,
    target_gpu_device TEXT NOT NULL,
    victim_worker_names_json TEXT NOT NULL DEFAULT '[]',
    reason TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE host_observations (
    node_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    applied_generation INTEGER,
    last_assignment_id INTEGER,
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    observed_state_json TEXT NOT NULL DEFAULT '',
    runtime_status_json TEXT NOT NULL DEFAULT '',
    instance_runtime_json TEXT NOT NULL DEFAULT '',
    gpu_telemetry_json TEXT NOT NULL DEFAULT '',
    disk_telemetry_json TEXT NOT NULL DEFAULT '',
    network_telemetry_json TEXT NOT NULL DEFAULT '',
    cpu_telemetry_json TEXT NOT NULL DEFAULT '',
    heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plane_name TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    worker_name TEXT NOT NULL DEFAULT '',
    assignment_id INTEGER,
    rollout_action_id INTEGER,
    category TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_log_created_at
    ON event_log(created_at DESC, id DESC);
CREATE INDEX idx_event_log_plane_name
    ON event_log(plane_name);
CREATE INDEX idx_event_log_node_name
    ON event_log(node_name);
CREATE INDEX idx_event_log_worker_name
    ON event_log(worker_name);
CREATE INDEX idx_event_log_category
    ON event_log(category);

CREATE TABLE scheduler_plane_runtime (
    plane_name TEXT PRIMARY KEY,
    active_action TEXT NOT NULL DEFAULT '',
    active_worker_name TEXT NOT NULL DEFAULT '',
    phase TEXT NOT NULL DEFAULT '',
    action_generation INTEGER NOT NULL DEFAULT 0,
    stable_samples INTEGER NOT NULL DEFAULT 0,
    rollback_attempt_count INTEGER NOT NULL DEFAULT 0,
    source_node_name TEXT NOT NULL DEFAULT '',
    source_gpu_device TEXT NOT NULL DEFAULT '',
    target_node_name TEXT NOT NULL DEFAULT '',
    target_gpu_device TEXT NOT NULL DEFAULT '',
    previous_state_json TEXT NOT NULL DEFAULT '',
    status_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE scheduler_worker_runtime (
    worker_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    last_move_at TEXT NOT NULL DEFAULT '',
    last_eviction_at TEXT NOT NULL DEFAULT '',
    last_verified_generation INTEGER,
    last_scheduler_phase TEXT NOT NULL DEFAULT '',
    last_status_message TEXT NOT NULL DEFAULT '',
    manual_intervention_required INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE scheduler_node_runtime (
    node_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    last_move_at TEXT NOT NULL DEFAULT '',
    last_verified_generation INTEGER,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
