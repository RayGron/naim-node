const state = {
  dashboard: null,
  hostHealth: null,
  diskState: null,
  rolloutActions: null,
  rebalancePlan: null,
  recentEvents: [],
  streamConnected: false,
  apiHealthy: false,
  lastRefreshAt: null,
  errorMessage: "",
};

const knownEventNames = [
  "bundle.imported",
  "bundle.applied",
  "host-observation.reported",
  "host-assignment.claimed",
  "host-assignment.applied",
  "host-assignment.failed",
  "host-assignment.retried",
  "rollout-action.status-updated",
  "rollout-action.eviction-enqueued",
  "rollout-action.retry-placement-applied",
  "scheduler.rebalance-materialized",
  "scheduler.rollback-planned",
  "scheduler.rollback-applied",
  "scheduler.move-verified",
  "scheduler.manual-intervention-required",
  "node-availability.updated",
];

const refreshButton = document.getElementById("refresh-button");
const summaryGrid = document.getElementById("summary-grid");
const nodeGrid = document.getElementById("node-grid");
const assignmentGrid = document.getElementById("assignment-grid");
const rolloutList = document.getElementById("rollout-list");
const rebalanceList = document.getElementById("rebalance-list");
const diskList = document.getElementById("disk-list");
const eventStream = document.getElementById("event-stream");
const apiStatusChip = document.getElementById("api-status-chip");
const streamStatusChip = document.getElementById("stream-status-chip");
const lastRefreshValue = document.getElementById("last-refresh-value");
const emptyStateTemplate = document.getElementById("empty-state-template");

function emptyStateNode(message) {
  const node = emptyStateTemplate.content.firstElementChild.cloneNode(true);
  node.textContent = message;
  return node;
}

function formatTime(value) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (part) => String(part).padStart(2, "0");
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function formatBool(value) {
  return value ? "yes" : "no";
}

function createStatusDot(className) {
  const dot = document.createElement("span");
  dot.className = `status-dot ${className}`;
  return dot;
}

function setChipStatus(chip, className, text) {
  chip.innerHTML = "";
  chip.append(createStatusDot(className));
  const label = document.createElement("span");
  label.textContent = text;
  chip.append(label);
}

function fetchJson(path) {
  return fetch(path, {
    headers: {
      Accept: "application/json",
    },
  }).then(async (response) => {
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.error?.message || payload?.status || response.statusText);
    }
    return payload;
  });
}

function nodeIndicatorClass(node) {
  if (node.health === "stale" || node.status === "failed") {
    return "status-critical";
  }
  if (node.runtime_launch_ready === true) {
    return "status-good";
  }
  if (node.runtime_phase && node.runtime_launch_ready !== true) {
    return "status-booting";
  }
  if (node.availability && node.availability !== "active") {
    return "status-warn";
  }
  if (node.health === "unknown") {
    return "status-booting";
  }
  return "status-warn";
}

function severityTagClass(severity) {
  if (severity === "error") {
    return "critical";
  }
  if (severity === "warning") {
    return "warning";
  }
  return "";
}

function renderSummary() {
  summaryGrid.innerHTML = "";
  const dashboard = state.dashboard;
  if (!dashboard || !dashboard.plane) {
    summaryGrid.append(emptyStateNode("No plane state loaded."));
    return;
  }

  const cards = [
    {
      label: "Plane",
      value: dashboard.plane.plane_name,
      meta: `generation ${dashboard.desired_generation ?? "n/a"}`,
    },
    {
      label: "Nodes Ready",
      value: `${dashboard.runtime.ready_nodes}/${dashboard.plane.node_count}`,
      meta: `${dashboard.runtime.degraded_gpu_telemetry_nodes} telemetry degraded`,
    },
    {
      label: "Assignments",
      value: dashboard.assignments.total,
      meta: `${dashboard.assignments.pending} pending, ${dashboard.assignments.failed} failed`,
    },
    {
      label: "Rollout",
      value: dashboard.rollout.total_actions,
      meta: `${dashboard.rollout.loop_status} / ${dashboard.rollout.loop_reason}`,
    },
    {
      label: "Events",
      value: state.recentEvents.length,
      meta: `latest ${state.recentEvents[0]?.category || "none"}`,
    },
  ];

  for (const card of cards) {
    const element = document.createElement("article");
    element.className = "summary-card";
    element.innerHTML = `
      <p class="summary-label">${card.label}</p>
      <p class="summary-value">${card.value}</p>
      <p class="summary-meta">${card.meta}</p>
    `;
    summaryGrid.append(element);
  }
}

function renderNodes() {
  nodeGrid.innerHTML = "";
  const nodes = state.dashboard?.nodes || [];
  if (!nodes.length) {
    nodeGrid.append(emptyStateNode("No node data available."));
    return;
  }

  for (const node of nodes) {
    const card = document.createElement("article");
    card.className = "node-card";
    card.append(
      (() => {
        const row = document.createElement("div");
        row.className = "node-topline";
        const title = document.createElement("h3");
        title.className = "node-title";
        title.textContent = node.node_name;
        const pill = document.createElement("div");
        pill.className = "pill";
        pill.append(createStatusDot(nodeIndicatorClass(node)));
        const pillText = document.createElement("span");
        pillText.textContent = `${node.health || "unknown"} / ${node.availability || "active"}`;
        pill.append(pillText);
        row.append(title, pill);
        return row;
      })(),
    );

    const metrics = document.createElement("div");
    metrics.className = "node-metrics";
    const rows = [
      ["Status", node.status || "n/a"],
      ["Runtime", node.runtime_phase || "n/a"],
      ["Launch ready", node.runtime_launch_ready === null ? "n/a" : formatBool(node.runtime_launch_ready)],
      ["Applied gen", node.applied_generation ?? "n/a"],
      ["GPU count", node.gpu_count ?? "n/a"],
      ["Desired instances", node.desired_instance_count ?? "n/a"],
      ["Desired disks", node.desired_disk_count ?? "n/a"],
    ];
    for (const [label, value] of rows) {
      const row = document.createElement("div");
      row.className = "metric-row";
      row.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
      metrics.append(row);
    }
    card.append(metrics);
    nodeGrid.append(card);
  }
}

function renderAssignments() {
  assignmentGrid.innerHTML = "";
  const nodes = state.dashboard?.assignments?.by_node || [];
  if (!nodes.length) {
    assignmentGrid.append(emptyStateNode("No assignment activity."));
    return;
  }
  for (const item of nodes) {
    const card = document.createElement("article");
    card.className = "assignment-card";
    card.innerHTML = `
      <div class="card-topline">
        <h3 class="card-title">${item.node_name}</h3>
        <span class="tag">${item.latest_status}</span>
      </div>
      <div class="card-metrics">
        <div class="metric-row"><span>Latest assignment</span><strong>#${item.latest_assignment_id}</strong></div>
        <div class="metric-row"><span>Pending</span><strong>${item.pending}</strong></div>
        <div class="metric-row"><span>Claimed</span><strong>${item.claimed}</strong></div>
        <div class="metric-row"><span>Failed</span><strong>${item.failed}</strong></div>
      </div>
    `;
    assignmentGrid.append(card);
  }
}

function renderRollout() {
  rolloutList.innerHTML = "";
  const actions = state.rolloutActions?.actions || [];
  if (!actions.length) {
    rolloutList.append(emptyStateNode("No rollout actions for the current generation."));
  } else {
    for (const action of actions) {
      const card = document.createElement("article");
      card.className = "rollout-card";
      card.innerHTML = `
        <div class="card-topline">
          <h3 class="card-title">${action.worker_name || "worker n/a"}</h3>
          <span class="tag ${action.status === "ready-to-retry" ? "warning" : ""}">${action.status}</span>
        </div>
        <div class="card-metrics">
          <div class="metric-row"><span>Action</span><strong>${action.action}</strong></div>
          <div class="metric-row"><span>Target</span><strong>${action.target_node_name || "n/a"}:${action.target_gpu_device || "n/a"}</strong></div>
          <div class="metric-row"><span>Victims</span><strong>${(action.victim_worker_names || []).join(", ") || "none"}</strong></div>
        </div>
      `;
      rolloutList.append(card);
    }
  }
}

function renderRebalance() {
  rebalanceList.innerHTML = "";
  const entries = state.rebalancePlan?.rebalance_plan || [];
  if (!entries.length) {
    rebalanceList.append(emptyStateNode("No rebalance recommendations."));
    return;
  }
  for (const entry of entries.slice(0, 8)) {
    const card = document.createElement("article");
    card.className = "rebalance-card";
    const gateReason = entry.gate_reason ? `<div class="metric-row"><span>Gate</span><strong>${entry.gate_reason}</strong></div>` : "";
    card.innerHTML = `
      <div class="card-topline">
        <h3 class="card-title">${entry.worker_name}</h3>
        <span class="tag ${entry.decision === "defer" ? "warning" : entry.decision === "hold" ? "" : ""}">${entry.decision}</span>
      </div>
      <div class="card-metrics">
        <div class="metric-row"><span>Class</span><strong>${entry.class}</strong></div>
        <div class="metric-row"><span>State</span><strong>${entry.state}</strong></div>
        <div class="metric-row"><span>Target</span><strong>${entry.target_node_name || "n/a"}:${entry.target_gpu_device || "n/a"}</strong></div>
        <div class="metric-row"><span>Action</span><strong>${entry.action || "n/a"}</strong></div>
        <div class="metric-row"><span>Score</span><strong>${entry.score ?? "n/a"}</strong></div>
        ${gateReason}
      </div>
    `;
    rebalanceList.append(card);
  }
}

function diskUsageText(item) {
  if (item.telemetry?.used_bytes != null && item.telemetry?.total_bytes != null) {
    return `${Math.round((item.telemetry.used_bytes / Math.max(item.telemetry.total_bytes, 1)) * 100)}%`;
  }
  if (item.used_percent != null) {
    return `${item.used_percent}%`;
  }
  return "n/a";
}

function renderDisks() {
  diskList.innerHTML = "";
  const items = state.diskState?.items || [];
  if (!items.length) {
    diskList.append(emptyStateNode("No disk state available."));
    return;
  }
  for (const item of items.slice(0, 12)) {
    const card = document.createElement("article");
    card.className = "disk-card";
    card.innerHTML = `
      <div class="card-topline">
        <h3 class="card-title">${item.disk_name}</h3>
        <span class="tag">${item.realized_state || item.desired_state || "unknown"}</span>
      </div>
      <div class="card-metrics">
        <div class="metric-row"><span>Node</span><strong>${item.node_name || "n/a"}</strong></div>
        <div class="metric-row"><span>Desired</span><strong>${item.desired_state || "n/a"}</strong></div>
        <div class="metric-row"><span>Usage</span><strong>${diskUsageText(item)}</strong></div>
        <div class="metric-row"><span>Mount</span><strong class="mono">${item.mount_point || "n/a"}</strong></div>
      </div>
    `;
    diskList.append(card);
  }
}

function renderEvents() {
  eventStream.innerHTML = "";
  const items = state.recentEvents || [];
  if (!items.length) {
    eventStream.append(emptyStateNode("No recent events."));
    return;
  }
  for (const event of items.slice(0, 20)) {
    const card = document.createElement("article");
    card.className = `event-card severity-${event.severity || "info"}`;
    const tags = [];
    tags.push(`<span class="tag">${event.category}.${event.event_type}</span>`);
    if (event.node_name) {
      tags.push(`<span class="tag">${event.node_name}</span>`);
    }
    if (event.worker_name) {
      tags.push(`<span class="tag">${event.worker_name}</span>`);
    }
    if (event.severity === "warning") {
      tags.push(`<span class="tag warning">warning</span>`);
    }
    if (event.severity === "error") {
      tags.push(`<span class="tag critical">critical</span>`);
    }
    card.innerHTML = `
      <div class="card-topline">
        <h3 class="card-title">${event.message || `${event.category}.${event.event_type}`}</h3>
        <span class="note">${formatTime(event.created_at)}</span>
      </div>
      <div class="note mono">${event.plane_name || "plane:n/a"}</div>
      <div class="event-meta">${tags.join("")}</div>
    `;
    eventStream.append(card);
  }
}

function renderStatus() {
  const dashboard = state.dashboard;
  const planeName = dashboard?.plane?.plane_name || "no-plane";
  if (state.apiHealthy) {
    setChipStatus(apiStatusChip, "status-good", `API ready / ${planeName}`);
  } else if (state.errorMessage) {
    setChipStatus(apiStatusChip, "status-critical", `API error / ${state.errorMessage}`);
  } else {
    setChipStatus(apiStatusChip, "status-booting", "API pending");
  }

  if (state.streamConnected) {
    setChipStatus(streamStatusChip, "status-good", "Realtime stream connected");
  } else {
    setChipStatus(streamStatusChip, "status-booting", "Realtime stream idle");
  }
  lastRefreshValue.textContent = state.lastRefreshAt ? formatTime(state.lastRefreshAt) : "never";
}

function renderAll() {
  renderSummary();
  renderNodes();
  renderAssignments();
  renderRollout();
  renderRebalance();
  renderDisks();
  renderEvents();
  renderStatus();
}

async function refreshData() {
  try {
    const [dashboard, hostHealth, diskState, rolloutActions, rebalancePlan] = await Promise.all([
      fetchJson("/api/v1/dashboard"),
      fetchJson("/api/v1/host-health"),
      fetchJson("/api/v1/disk-state"),
      fetchJson("/api/v1/rollout-actions"),
      fetchJson("/api/v1/rebalance-plan"),
    ]);
    state.dashboard = dashboard;
    state.hostHealth = hostHealth;
    state.diskState = diskState;
    state.rolloutActions = rolloutActions;
    state.rebalancePlan = rebalancePlan;
    state.recentEvents = dashboard.recent_events || [];
    state.apiHealthy = true;
    state.errorMessage = "";
    state.lastRefreshAt = new Date().toISOString();
    renderAll();
  } catch (error) {
    state.apiHealthy = false;
    state.errorMessage = error instanceof Error ? error.message : String(error);
    renderStatus();
  }
}

function upsertEvent(event) {
  if (!event || typeof event !== "object") {
    return;
  }
  const existingIndex = state.recentEvents.findIndex((item) => item.id === event.id);
  if (existingIndex >= 0) {
    state.recentEvents.splice(existingIndex, 1);
  }
  state.recentEvents.unshift(event);
  state.recentEvents = state.recentEvents.slice(0, 20);
  renderEvents();
}

function connectEventStream() {
  const source = new EventSource("/api/v1/events/stream");
  source.onopen = () => {
    state.streamConnected = true;
    renderStatus();
  };
  source.onerror = () => {
    state.streamConnected = false;
    renderStatus();
  };
  for (const eventName of knownEventNames) {
    source.addEventListener(eventName, (message) => {
      try {
        const payload = JSON.parse(message.data);
        upsertEvent(payload);
      } catch (_error) {
        return;
      }
      void refreshData();
    });
  }
}

refreshButton.addEventListener("click", () => {
  void refreshData();
});

void refreshData();
connectEventStream();
setInterval(() => {
  void refreshData();
}, 15000);
