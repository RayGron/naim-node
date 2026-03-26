import { useEffect, useRef, useState, startTransition } from "react";

const REFRESH_DEBOUNCE_MS = 350;
const AUTO_REFRESH_MS = 5000;
const EVENT_LIMIT = 24;
const CHAT_LANGUAGE_OPTIONS = [
  { value: "en", label: "English" },
  { value: "de", label: "Deutsch" },
  { value: "uk", label: "Українська" },
  { value: "ru", label: "Русский" },
];

function fetchJson(path, init = {}) {
  return fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init.headers || {}),
    },
  }).then(async (response) => {
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message =
        payload?.error?.message ||
        payload?.message ||
        payload?.status ||
        response.statusText;
      throw new Error(message);
    }
    return payload;
  });
}

function planePath(planeName, suffix = "") {
  const encoded = encodeURIComponent(planeName);
  return suffix
    ? `/api/v1/planes/${encoded}/${suffix}`
    : `/api/v1/planes/${encoded}`;
}

function modelLibraryPath(suffix = "") {
  return suffix ? `/api/v1/model-library/${suffix}` : "/api/v1/model-library";
}

function interactionPath(planeName, suffix) {
  return planePath(planeName, `interaction/${suffix}`);
}

function queryPath(path, query) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null && value !== "") {
      params.set(key, String(value));
    }
  }
  const rendered = params.toString();
  return rendered ? `${path}?${rendered}` : path;
}

function parseEventStreamChunk(chunk) {
  const lines = chunk.split("\n");
  let event = "message";
  const data = [];
  for (const line of lines) {
    if (line.startsWith("event:")) {
      event = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice(5).trimStart());
    }
  }
  return { event, data: data.join("\n") };
}

async function consumeEventStream(response, onFrame) {
  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error("Streaming response body is unavailable.");
  }
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      if (frame.trim()) {
        onFrame(parseEventStreamChunk(frame));
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) {
    onFrame(parseEventStreamChunk(buffer));
  }
}

function ActionIcon({ kind }) {
  if (kind === "view") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path
          d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <circle
          cx="12"
          cy="12"
          r="3.2"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
        />
      </svg>
    );
  }
  if (kind === "edit") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path
          d="M4 20l4.2-1 9.1-9.1a1.9 1.9 0 0 0 0-2.7l-.5-.5a1.9 1.9 0 0 0-2.7 0L5 15.8 4 20Z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M13.5 7.5l3 3"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
        />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        d="M5 7h14"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
      <path
        d="M9 7V5.8A1.8 1.8 0 0 1 10.8 4h2.4A1.8 1.8 0 0 1 15 5.8V7"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M7 7l.8 11a2 2 0 0 0 2 1.8h4.4a2 2 0 0 0 2-1.8L17 7"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M10 11v5M14 11v5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
    </svg>
  );
}

function formatTimestamp(value) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (part) => String(part).padStart(2, "0");
  return [
    pad(date.getDate()),
    pad(date.getMonth() + 1),
    date.getFullYear(),
  ].join("/") + ` ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function yesNo(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  return value ? "yes" : "no";
}

function compactBytes(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let amount = value;
  let unitIndex = 0;
  while (amount >= 1024 && unitIndex < units.length - 1) {
    amount /= 1024;
    unitIndex += 1;
  }
  return `${amount.toFixed(amount >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function normalizeChatLanguage(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return CHAT_LANGUAGE_OPTIONS.some((option) => option.value === normalized)
    ? normalized
    : "";
}

function supportedChatLanguageOptions(desiredState, interactionStatus) {
  const allowed = Array.isArray(desiredState?.interaction?.supported_response_languages) &&
    desiredState.interaction.supported_response_languages.length > 0
    ? desiredState.interaction.supported_response_languages
    : Array.isArray(interactionStatus?.supported_response_languages) &&
        interactionStatus.supported_response_languages.length > 0
      ? interactionStatus.supported_response_languages
      : CHAT_LANGUAGE_OPTIONS.map((option) => option.value);
  const allowedSet = new Set(allowed.map((item) => normalizeChatLanguage(item)).filter(Boolean));
  return CHAT_LANGUAGE_OPTIONS.filter((option) => allowedSet.has(option.value));
}

function eventSeverityClass(severity) {
  if (severity === "error") {
    return "is-critical";
  }
  if (severity === "warning") {
    return "is-warning";
  }
  return "is-healthy";
}

function alertSeverityClass(severity) {
  if (severity === "critical") {
    return "is-critical";
  }
  if (severity === "warning") {
    return "is-warning";
  }
  if (severity === "booting") {
    return "is-booting";
  }
  return "is-healthy";
}

function runtimeIndicatorClass(value, fallbackHealth) {
  if (value === true) {
    return "is-healthy";
  }
  if (value === false) {
    return "is-warning";
  }
  if (fallbackHealth === "stale" || fallbackHealth === "failed") {
    return "is-critical";
  }
  return "is-booting";
}

function planeStateClass(state) {
  if (state === "failed" || state === "degraded") {
    return "is-critical";
  }
  if (state === "stopping" || state === "starting" || state === "stopped") {
    return "is-warning";
  }
  return "is-healthy";
}

function planeDisplayState(plane) {
  if (!plane) {
    return "unknown";
  }
  if ((plane.failed_assignments ?? 0) > 0) {
    return "failed bootstrap";
  }
  if (plane.staged_update) {
    return "staged update";
  }
  return plane.state || "unknown";
}

function planeDisplayStateClass(plane) {
  const label = planeDisplayState(plane);
  if (label === "failed bootstrap") {
    return "is-critical";
  }
  if (label === "staged update") {
    return "is-warning";
  }
  return planeStateClass(label);
}

function bootstrapModelTargetPath(desiredState) {
  const bootstrapModel = desiredState?.bootstrap_model;
  if (!bootstrapModel) {
    return "n/a";
  }
  if (bootstrapModel.materialization_mode === "reference" && bootstrapModel.local_path) {
    return bootstrapModel.local_path;
  }
  if (Array.isArray(bootstrapModel.source_urls) && bootstrapModel.source_urls.length > 1) {
    return `${desiredState?.inference?.model_cache_dir || desiredState?.inference?.gguf_cache_dir || "/comet/shared/models/cache"}/multipart`;
  }
  const targetFilename =
    bootstrapModel.target_filename ||
    bootstrapModel.local_path?.split("/").pop() ||
    bootstrapModel.source_url?.split("/").pop() ||
    "model.gguf";
  return `${desiredState?.inference?.gguf_cache_dir || "/comet/shared/models/gguf"}/${targetFilename}`;
}

function bootstrapModelSourceLabel(bootstrapModel) {
  if (!bootstrapModel) {
    return "not configured";
  }
  if (bootstrapModel.materialization_mode === "reference" && bootstrapModel.local_path) {
    return `reference: ${bootstrapModel.local_path}`;
  }
  if (Array.isArray(bootstrapModel.source_urls) && bootstrapModel.source_urls.length > 0) {
    return `${bootstrapModel.source_urls.length} remote part${bootstrapModel.source_urls.length === 1 ? "" : "s"}`;
  }
  return bootstrapModel.local_path || bootstrapModel.source_url || "not configured";
}

function modelLibraryItemSummary(item) {
  if (!item) {
    return "unknown";
  }
  const kind = item.kind || "artifact";
  const size = compactBytes(item.size_bytes);
  const parts = item.part_count > 1 ? ` / ${item.part_count} parts` : "";
  return `${kind} / ${size}${parts}`;
}

function interactionReasonMessage(status) {
  const reason = status?.reason || "";
  const failureDetail = status?.failure_detail || "";
  if (reason === "plane_mode_compute") {
    return "Interaction is disabled for compute planes.";
  }
  if (reason === "plane_not_running") {
    return "Start the plane to enable chat interaction.";
  }
  if (reason === "unsupported_local_runtime") {
    return failureDetail || "This plane cannot be materialized on the local host.";
  }
  if (reason === "no_observation") {
    return "Waiting for a fresh infer-node observation.";
  }
  if (reason === "runtime_start_failed") {
    return failureDetail || "Runtime startup failed on the infer node.";
  }
  if (reason === "runtime_status_missing") {
    return "Runtime status is not available yet.";
  }
  if (reason === "active_model_missing") {
    return "No active model is loaded yet.";
  }
  if (reason === "gateway_not_ready") {
    return "Gateway is not ready yet.";
  }
  if (reason === "inference_not_ready") {
    return "Inference runtime is not ready yet.";
  }
  if (reason === "gateway_target_missing") {
    return "Controller could not resolve an interaction target.";
  }
  return "Interaction is available when the LLM plane is running and launch-ready.";
}

function observedStateForObservation(observation) {
  return observation?.observed_state || {};
}

function observedInstancesForObservation(observation) {
  const state = observedStateForObservation(observation);
  return Array.isArray(state.instances) ? state.instances : [];
}

function deriveObservedRuntime(observationItems, nodeItems) {
  const observedInstances = [];
  const nodeRuntime = new Map();

  for (const observation of observationItems) {
    const instances = observedInstancesForObservation(observation);
    const hasObservedRuntime =
      observation?.runtime_status?.available === true ||
      observation?.instance_runtimes?.available === true ||
      observation?.applied_generation !== null && observation?.applied_generation !== undefined ||
      instances.length > 0;
    const isHealthyNode =
      observation?.status !== "stale" &&
      observation?.status !== "failed" &&
      observation?.status !== "error";
    const runtimeLaunchReady = hasObservedRuntime ? isHealthyNode : false;
    const runtimePhase = observation?.runtime_status?.snapshot?.phase || (hasObservedRuntime ? "applied" : "pending");
    observedInstances.push(...instances);
    nodeRuntime.set(observation.node_name, {
      runtimeLaunchReady,
      runtimePhase,
      observedInstanceCount: instances.length,
      appliedGeneration: observation?.applied_generation,
      observationStatus: observation?.status || "unknown",
    });
  }

  let readyNodes = 0;
  let notReadyNodes = 0;
  for (const node of nodeItems) {
    const derived = nodeRuntime.get(node.node_name);
    const ready =
      node.runtime_launch_ready === true ||
      (node.runtime_launch_ready === null || node.runtime_launch_ready === undefined
        ? derived?.runtimeLaunchReady === true
        : false);
    if (ready) {
      readyNodes += 1;
    } else {
      notReadyNodes += 1;
    }
  }

  return {
    observedInstances,
    readyNodes,
    notReadyNodes,
    nodeRuntime,
  };
}

function instanceRole(instance) {
  return instance?.kind || instance?.role || "instance";
}

function statusDot(className) {
  return <span className={`status-dot ${className}`} aria-hidden="true" />;
}

function EmptyState({ title, detail }) {
  return (
    <div className="empty-state">
      <div className="empty-state-title">{title}</div>
      {detail ? <div className="empty-state-detail">{detail}</div> : null}
    </div>
  );
}

function OnboardingCard({ onCreatePlane }) {
  return (
    <section className="onboarding-card">
      <div className="section-label">First plane</div>
      <h3>Create a plane from the Web UI</h3>
      <p className="onboarding-copy">
        The platform is running. Create your first plane, paste a desired-state template, then
        stage and start it from the operator workflow.
      </p>
      <div className="onboarding-steps">
        <div>1. Open the plane editor.</div>
        <div>2. Paste or adjust the desired-state JSON.</div>
        <div>3. Save the plane, then start it from Dashboard.</div>
      </div>
      <div className="toolbar">
        <button
          className="ghost-button"
          type="button"
          onClick={onCreatePlane}
        >
          New plane
        </button>
      </div>
    </section>
  );
}

function buildNewPlaneTemplate() {
  return JSON.stringify(
    {
      plane_name: "new-plane",
      plane_shared_disk_name: "plane-new-plane-shared",
      control_root: "/comet/shared/control/new-plane",
      plane_mode: "compute",
      placement_target: "local",
      bootstrap_model: {
        model_id: "model-id",
        served_model_name: "model-id",
        materialization_mode: "copy",
        local_path: "/abs/path/to/model.gguf",
        source_url: null,
        target_filename: "model.gguf",
        sha256: null,
      },
      interaction: {
        system_prompt:
          "You are Cypher AI. Reply concisely and directly. If preferred_language is provided, always reply in that language. If preferred_language is absent, reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese.",
        default_response_language: "ru",
        supported_response_languages: ["en", "de", "uk", "ru"],
        follow_user_language: true,
        completion_policy: {
          response_mode: "normal",
          max_tokens: 512,
          max_continuations: 0,
          max_total_completion_tokens: 512,
          max_elapsed_time_ms: 30000,
        },
        long_completion_policy: {
          response_mode: "very_long",
          max_tokens: 1024,
          max_continuations: 6,
          max_total_completion_tokens: 6144,
          max_elapsed_time_ms: 120000,
          semantic_goal:
            "complete the requested artifact fully before emitting [[TASK_COMPLETE]]",
        },
      },
      inference: {
        primary_infer_node: "local-hostd",
        net_if: "eth0",
        models_root: "/comet/shared/models",
        gguf_cache_dir: "/comet/shared/models/gguf",
        infer_log_dir: "/comet/shared/logs/infer",
        llama_port: 8000,
        llama_ctx_size: 4096,
        llama_threads: 4,
        llama_gpu_layers: 50,
        inference_healthcheck_retries: 120,
        inference_healthcheck_interval_sec: 5,
      },
      gateway: {
        listen_host: "0.0.0.0",
        listen_port: 8080,
        server_name: "new-plane.local",
      },
      runtime_gpu_nodes: [],
      nodes: [
        {
          name: "local-hostd",
          platform: "linux",
          gpu_devices: ["0"],
          gpu_memory_mb: {
            "0": 24576,
          },
        },
      ],
      disks: [
        {
          name: "plane-new-plane-shared",
          kind: "plane-shared",
          plane_name: "new-plane",
          owner_name: "",
          node_name: "local-hostd",
          host_path: "/comet/disks/new-plane/shared",
          container_path: "/comet/shared",
          size_gb: 40,
        },
        {
          name: "infer-new-plane-private",
          kind: "infer-private",
          plane_name: "new-plane",
          owner_name: "infer-new-plane",
          node_name: "local-hostd",
          host_path: "/comet/disks/new-plane/infer-private",
          container_path: "/comet/infer-private",
          size_gb: 20,
        },
        {
          name: "worker-new-plane-private",
          kind: "worker-private",
          plane_name: "new-plane",
          owner_name: "worker-a",
          node_name: "local-hostd",
          host_path: "/comet/disks/new-plane/worker-private",
          container_path: "/comet/worker-private",
          size_gb: 10,
        },
      ],
      instances: [
        {
          name: "infer-new-plane",
          role: "infer",
          plane_name: "new-plane",
          node_name: "local-hostd",
          image: "comet/infer-runtime:dev",
          command: "",
          private_disk_name: "infer-new-plane-private",
          shared_disk_name: "plane-new-plane-shared",
          depends_on: [],
          environment: {},
          labels: {},
          gpu_device: null,
          placement_mode: "manual",
          share_mode: "exclusive",
          gpu_fraction: 0,
          priority: 100,
          preemptible: false,
          memory_cap_mb: null,
          private_disk_size_gb: 20,
        },
        {
          name: "worker-a",
          role: "worker",
          plane_name: "new-plane",
          node_name: "local-hostd",
          image: "comet/worker-runtime:dev",
          command: "",
          private_disk_name: "worker-new-plane-private",
          shared_disk_name: "plane-new-plane-shared",
          depends_on: ["infer-new-plane"],
          environment: {},
          labels: {},
          gpu_device: "0",
          placement_mode: "manual",
          share_mode: "shared",
          gpu_fraction: 0.25,
          priority: 100,
          preemptible: true,
          memory_cap_mb: 4096,
          private_disk_size_gb: 10,
        },
      ],
    },
    null,
    2,
  );
}

function PlaneEditorDialog({ dialog, setDialog, onClose, onSave }) {
  if (!dialog.open) {
    return null;
  }

  const readOnly = dialog.mode === "view";
  const title =
    dialog.mode === "new"
      ? "New plane"
      : dialog.mode === "edit"
        ? `Edit plane ${dialog.planeName}`
        : `View plane ${dialog.planeName}`;

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-card plane-editor-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="plane-editor-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-header">
          <div>
            <div className="section-label">Plane registry</div>
            <h2 id="plane-editor-title">{title}</h2>
          </div>
          <button className="ghost-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>
        <p className="editor-copy">
          {readOnly
            ? "Read-only desired state JSON for the selected plane."
            : "Edit desired state JSON and stage it in the controller. Runtime changes are applied only after Start plane."}
        </p>
        {dialog.error ? <div className="error-banner">{dialog.error}</div> : null}
        <label className="field-label" htmlFor="plane-editor-json">
          Desired state JSON
        </label>
        <textarea
          id="plane-editor-json"
          className="editor-textarea"
          value={dialog.text}
          onChange={(event) =>
            setDialog((current) => ({
              ...current,
              text: event.target.value,
            }))
          }
          readOnly={readOnly}
          spellCheck="false"
        />
        <div className="toolbar">
          {readOnly ? (
            <button
              className="ghost-button"
              type="button"
              onClick={() =>
                setDialog((current) => ({
                  ...current,
                  mode: "edit",
                  error: "",
                }))
              }
            >
              Edit plane
            </button>
          ) : (
            <button
              className="ghost-button"
              type="button"
              onClick={onSave}
              disabled={dialog.busy}
            >
              {dialog.mode === "new" ? "Create plane" : "Save staged changes"}
            </button>
          )}
        </div>
      </section>
    </div>
  );
}

function SummaryCard({ label, value, meta }) {
  return (
    <article className="summary-card">
      <div className="summary-label">{label}</div>
      <div className="summary-value">{value}</div>
      <div className="summary-meta">{meta}</div>
    </article>
  );
}

function progressPercent(value) {
  return Math.max(0, Math.min(100, value));
}

function inferProgressOperationKind(pendingOperation, planeRecord, inFlightAssignments) {
  if (pendingOperation?.kind) {
    return pendingOperation.kind;
  }
  if (inFlightAssignments <= 0 || !planeRecord) {
    return "";
  }
  if (planeRecord.state === "running") {
    return "start";
  }
  if (planeRecord.state === "stopped") {
    return "stop";
  }
  return "apply";
}

function buildOperationProgressModel({
  pendingOperation,
  selectedPlaneName,
  planeRecord,
  inFlightAssignments,
  failedAssignments,
  failedAssignmentMessage,
  observedInstanceCount,
  desiredInstanceCount,
  readyNodes,
  structuredProgress,
}) {
  if (structuredProgress) {
    const bytesDone =
      structuredProgress.bytes_done !== null && structuredProgress.bytes_done !== undefined
        ? compactBytes(structuredProgress.bytes_done)
        : null;
    const bytesTotal =
      structuredProgress.bytes_total !== null && structuredProgress.bytes_total !== undefined
        ? compactBytes(structuredProgress.bytes_total)
        : null;
    const transfer =
      bytesDone && bytesTotal ? `${bytesDone} / ${bytesTotal}` : bytesDone || bytesTotal || "";
    return {
      kind: pendingOperation?.kind || structuredProgress.phase || "apply",
      label: structuredProgress.title || "Applying plane state",
      progress: progressPercent(structuredProgress.percent ?? 0),
      detail: [structuredProgress.detail || "", transfer].filter(Boolean).join(" · "),
      complete: structuredProgress.phase === "completed",
      failed: structuredProgress.phase === "failed",
    };
  }

  const kind = inferProgressOperationKind(pendingOperation, planeRecord, inFlightAssignments);
  if (!kind) {
    return null;
  }

  const noAssignmentsInFlight = inFlightAssignments === 0;
  if (failedAssignments > 0) {
    const label =
      kind === "start"
        ? "Starting plane"
        : kind === "stop"
          ? "Stopping plane"
          : kind === "delete"
            ? "Deleting plane"
            : "Applying bundle";
    return {
      kind,
      label,
      progress: 100,
      detail: failedAssignmentMessage || "Host assignment failed and requires operator action.",
      complete: false,
      failed: true,
    };
  }

  if (kind === "start") {
    let progress = 18;
    let detail = "Controller accepted the start request.";
    if (planeRecord?.state === "running") {
      progress = 42;
      detail = "Plane state switched to running.";
    }
    if (inFlightAssignments > 0) {
      progress = 72;
      detail = "Host assignments are being applied.";
    }
    if (noAssignmentsInFlight && readyNodes > 0 && observedInstanceCount >= desiredInstanceCount) {
      progress = 100;
      detail = "Plane start converged on the node.";
    } else if (noAssignmentsInFlight && planeRecord?.state === "running") {
      progress = 84;
      detail = "Plane is running, waiting for final observation convergence.";
    }
    return {
      kind,
      label: "Starting plane",
      progress: progressPercent(progress),
      detail,
      complete: progress >= 100,
      failed: false,
    };
  }

  if (kind === "stop") {
    let progress = 18;
    let detail = "Controller accepted the stop request.";
    if (planeRecord?.state === "stopped") {
      progress = 42;
      detail = "Plane state switched to stopped.";
    }
    if (inFlightAssignments > 0) {
      progress = 72;
      detail = "Host assignments are removing runtime state.";
    }
    if (noAssignmentsInFlight && observedInstanceCount === 0) {
      progress = 100;
      detail = "Plane stop converged on the node.";
    } else if (noAssignmentsInFlight && planeRecord?.state === "stopped") {
      progress = 84;
      detail = "Plane is stopped, waiting for final observation convergence.";
    }
    return {
      kind,
      label: "Stopping plane",
      progress: progressPercent(progress),
      detail,
      complete: progress >= 100,
      failed: false,
    };
  }

  if (kind === "delete") {
    let progress = 18;
    let detail = "Controller accepted the delete request.";
    const targetPlaneMissing =
      pendingOperation?.planeName &&
      pendingOperation.planeName !== selectedPlaneName;
    if (targetPlaneMissing && noAssignmentsInFlight) {
      return {
        kind,
        label: "Deleting plane",
        progress: 100,
        detail: "Plane cleanup converged and the plane was removed from the registry.",
        complete: true,
        failed: false,
      };
    }
    if (planeRecord?.state === "deleting") {
      progress = 46;
      detail = "Plane state switched to deleting.";
    }
    if (inFlightAssignments > 0) {
      progress = 74;
      detail = "Host assignments are removing plane runtime and storage state.";
    }
    if (!planeRecord && noAssignmentsInFlight) {
      progress = 100;
      detail = "Plane cleanup converged and the plane was removed from the registry.";
    } else if (noAssignmentsInFlight && planeRecord?.state === "deleting") {
      progress = 88;
      detail = "Cleanup assignments finished, waiting for final registry removal.";
    }
    return {
      kind,
      label: "Deleting plane",
      progress: progressPercent(progress),
      detail,
      complete: progress >= 100,
      failed: false,
    };
  }

  let progress = 22;
  let detail = "Bundle apply request accepted by the controller.";
  if (planeRecord) {
    progress = 46;
    detail = "Plane record exists and desired state is persisted.";
  }
  if (inFlightAssignments > 0) {
    progress = 74;
    detail = "Host assignments are materializing the new plane state.";
  }
  if (noAssignmentsInFlight && readyNodes > 0 && observedInstanceCount >= desiredInstanceCount) {
    progress = 100;
    detail = "Bundle apply converged on the node.";
  } else if (noAssignmentsInFlight && planeRecord) {
    progress = 86;
    detail = "Desired state is applied, waiting for final observation convergence.";
  }
  return {
    kind,
    label: "Applying bundle",
    progress: progressPercent(progress),
    detail,
    complete: progress >= 100,
    failed: false,
  };
}

function isRoutineEvent(event) {
  if (!event) {
    return false;
  }
  const category = event.category || "";
  const eventType = event.event_type || "";
  const severity = event.severity || "info";
  if (severity !== "info") {
    return false;
  }
  if (category === "host-observation" && eventType === "reported") {
    return true;
  }
  if (category === "host-registry" && eventType === "session-opened") {
    return true;
  }
  return false;
}

function nodeStatusLabel(runtimeLaunchReady, runtimePhase, health) {
  if (runtimeLaunchReady) {
    return "ready";
  }
  if (runtimePhase === "starting" || runtimePhase === "stopping" || runtimePhase === "pending") {
    return runtimePhase;
  }
  return health || "unknown";
}

function App() {
  const initialPlane = new URLSearchParams(window.location.search).get("plane") || "";
  const initialPage = new URLSearchParams(window.location.search).get("page") || "dashboard";
  const [planes, setPlanes] = useState([]);
  const [selectedPlane, setSelectedPlane] = useState(initialPlane);
  const [selectedPage, setSelectedPage] = useState(
    ["dashboard", "planes", "models"].includes(initialPage) ? initialPage : "dashboard",
  );
  const [planeDetail, setPlaneDetail] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [hostObservations, setHostObservations] = useState(null);
  const [diskState, setDiskState] = useState(null);
  const [rolloutState, setRolloutState] = useState(null);
  const [rebalancePlan, setRebalancePlan] = useState(null);
  const [events, setEvents] = useState([]);
  const [interactionStatus, setInteractionStatus] = useState(null);
  const [modelLibrary, setModelLibrary] = useState({ items: [], roots: [], jobs: [] });
  const [selectedTab, setSelectedTab] = useState("status");
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLanguage, setChatLanguage] = useState("ru");
  const [chatBusy, setChatBusy] = useState(false);
  const [chatError, setChatError] = useState("");
  const [interactionPaneWidth, setInteractionPaneWidth] = useState(34);
  const [draggingDivider, setDraggingDivider] = useState(false);
  const [loading, setLoading] = useState(true);
  const [actionBusy, setActionBusy] = useState("");
  const [modelLibraryBusy, setModelLibraryBusy] = useState("");
  const [modelDownloadForm, setModelDownloadForm] = useState({
    modelId: "",
    targetRoot: "",
    targetSubdir: "",
    sourceUrls: "",
  });
  const [apiError, setApiError] = useState("");
  const [apiHealthy, setApiHealthy] = useState(false);
  const [streamHealthy, setStreamHealthy] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState("");
  const [lastEventName, setLastEventName] = useState("none");
  const [pendingOperation, setPendingOperation] = useState(null);
  const [planeDialog, setPlaneDialog] = useState({
    open: false,
    mode: "new",
    planeName: "",
    text: "",
    busy: false,
    error: "",
  });

  const refreshTimerRef = useRef(null);
  const eventSourceRef = useRef(null);
  const chatAbortRef = useRef(null);
  const interactionSplitRef = useRef(null);
  const chatTranscriptRef = useRef(null);
  const chatTranscriptEndRef = useRef(null);

  async function loadPlanes(preferredPlane = selectedPlane) {
    const payload = await fetchJson("/api/v1/planes");
    const items = Array.isArray(payload.items) ? payload.items : [];
    setPlanes(items);
    const planeExists =
      preferredPlane && items.some((item) => item.name === preferredPlane);
    const nextPlane = planeExists
      ? preferredPlane
      : items.length > 0
        ? items[0].name
        : "";
    if (nextPlane !== selectedPlane) {
      startTransition(() => {
        setSelectedPlane(nextPlane);
      });
    }
    return nextPlane;
  }

  async function refreshModelLibrary() {
    const payload = await fetchJson(modelLibraryPath());
    setModelLibrary({
      items: Array.isArray(payload.items) ? payload.items : [],
      roots: Array.isArray(payload.roots) ? payload.roots : [],
      jobs: Array.isArray(payload.jobs) ? payload.jobs : [],
    });
  }

  async function refreshAll(planeOverride) {
    setLoading(true);
    setApiError("");
    try {
      const planeName = await loadPlanes(planeOverride ?? selectedPlane);
      if (!planeName) {
        setPlaneDetail(null);
        setDashboard(null);
        setHostObservations(null);
        setDiskState(null);
        setRolloutState(null);
        setRebalancePlan(null);
        setEvents([]);
        setInteractionStatus(null);
        setApiHealthy(true);
        setLastRefreshAt(new Date().toISOString());
        return;
      }

      const [
        planePayload,
        dashboardPayload,
        hostObservationsPayload,
        diskPayload,
        rolloutPayload,
        rebalancePayload,
        eventsPayload,
        interactionPayload,
      ] = await Promise.all([
        fetchJson(planePath(planeName)),
        fetchJson(planePath(planeName, "dashboard")),
        fetchJson(
          queryPath("/api/v1/host-observations", {
            plane: planeName,
            stale_after: 30,
          }),
        ),
        fetchJson(queryPath("/api/v1/disk-state", { plane: planeName })),
        fetchJson(queryPath("/api/v1/rollout-actions", { plane: planeName })),
        fetchJson(
          queryPath("/api/v1/rebalance-plan", {
            plane: planeName,
            stale_after: 30,
          }),
        ),
        fetchJson(
          queryPath("/api/v1/events", {
            plane: planeName,
            limit: EVENT_LIMIT,
          }),
        ),
        fetchJson(interactionPath(planeName, "status")),
      ]);

      setPlaneDetail(planePayload);
      setDashboard(dashboardPayload);
      setHostObservations(hostObservationsPayload);
      setDiskState(diskPayload);
      setRolloutState(rolloutPayload);
      setRebalancePlan(rebalancePayload);
      setEvents(Array.isArray(eventsPayload.events) ? eventsPayload.events : []);
      setInteractionStatus(interactionPayload);
      setApiHealthy(true);
      setLastRefreshAt(new Date().toISOString());
    } catch (error) {
      setApiHealthy(false);
      setApiError(error.message || String(error));
    } finally {
      setLoading(false);
    }
  }

  async function deleteModelLibraryEntry(entry) {
    if (!entry?.path) {
      return;
    }
    const confirmed = window.confirm(
      `Delete model artifact ${entry.name || entry.path}? This removes the underlying data from disk.`,
    );
    if (!confirmed) {
      return;
    }
    setModelLibraryBusy(`delete:${entry.path}`);
    try {
      await fetchJson(modelLibraryPath(), {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: entry.path }),
      });
      await refreshModelLibrary();
      await refreshAll(selectedPlane);
    } finally {
      setModelLibraryBusy("");
    }
  }

  async function enqueueModelLibraryDownload() {
    const sourceUrls = modelDownloadForm.sourceUrls
      .split(/\r?\n/)
      .map((value) => value.trim())
      .filter(Boolean);
    if (!modelDownloadForm.targetRoot.trim() || sourceUrls.length === 0) {
      return;
    }
    setModelLibraryBusy("download");
    try {
      await fetchJson(modelLibraryPath("download"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model_id: modelDownloadForm.modelId.trim() || undefined,
          target_root: modelDownloadForm.targetRoot.trim(),
          target_subdir: modelDownloadForm.targetSubdir.trim() || undefined,
          source_urls: sourceUrls,
        }),
      });
      setModelDownloadForm((current) => ({
        ...current,
        sourceUrls: "",
      }));
      await refreshModelLibrary();
      await refreshAll(selectedPlane);
    } finally {
      setModelLibraryBusy("");
    }
  }

  function scheduleRefresh(planeName) {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
    }
    refreshTimerRef.current = setTimeout(() => {
      refreshTimerRef.current = null;
      refreshAll(planeName);
    }, REFRESH_DEBOUNCE_MS);
  }

  async function executePlaneAction(action, planeName = selectedPlane) {
    if (!planeName) {
      return;
    }
    setActionBusy(action);
    setApiError("");
    try {
      const request =
        action === "delete"
          ? fetchJson(planePath(planeName), { method: "DELETE" })
          : fetchJson(planePath(planeName, action), { method: "POST" });
      await request;
      setPendingOperation({
        kind: action,
        planeName,
        startedAt: new Date().toISOString(),
      });
      await refreshAll(planeName);
    } catch (error) {
      setApiError(error.message || String(error));
    } finally {
      setActionBusy("");
    }
  }

  async function openPlaneDialog(mode, planeName = "") {
    setApiError("");
    try {
      if (mode === "new") {
        setPlaneDialog({
          open: true,
          mode,
          planeName: "",
          text: buildNewPlaneTemplate(),
          busy: false,
          error: "",
        });
        return;
      }
      const payload =
        planeName === selectedPlane && planeDetail
          ? planeDetail
          : await fetchJson(planePath(planeName));
      setPlaneDialog({
        open: true,
        mode,
        planeName,
        text: JSON.stringify(payload.desired_state || {}, null, 2),
        busy: false,
        error: "",
      });
    } catch (error) {
      setApiError(error.message || String(error));
    }
  }

  async function savePlaneDialog() {
    setPlaneDialog((current) => ({ ...current, busy: true, error: "" }));
    try {
      const desiredState = JSON.parse(planeDialog.text);
      if (!desiredState?.plane_name) {
        throw new Error("desired_state.plane_name is required.");
      }
      if (planeDialog.mode === "edit" && desiredState.plane_name !== planeDialog.planeName) {
        throw new Error("Plane rename is not supported in edit mode.");
      }
      const method = planeDialog.mode === "edit" ? "PUT" : "POST";
      const path =
        planeDialog.mode === "edit"
          ? planePath(planeDialog.planeName)
          : "/api/v1/planes";
      await fetchJson(path, {
        method,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
            desired_state: desiredState,
        }),
      });
      setPlaneDialog({
        open: false,
        mode: "new",
        planeName: "",
        text: "",
        busy: false,
        error: "",
      });
      startTransition(() => {
        setSelectedPlane(desiredState.plane_name);
      });
      await refreshAll(desiredState.plane_name);
    } catch (error) {
      setPlaneDialog((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  function stopChatStream() {
    if (chatAbortRef.current) {
      chatAbortRef.current.abort();
      chatAbortRef.current = null;
    }
    setChatBusy(false);
  }

  function handleChatInputKeyDown(event) {
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }
    event.preventDefault();
    if (!chatBusy && chatInput.trim()) {
      sendChatPrompt();
    }
  }

  async function sendChatPrompt() {
    if (!selectedPlane || !chatInput.trim() || chatBusy) {
      return;
    }
    const prompt = chatInput.trim();
    const userMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: prompt,
    };
    const assistantId = `assistant-${Date.now() + 1}`;
    const nextMessages = [...chatMessages, userMessage];
    setChatMessages([
      ...nextMessages,
      {
        id: assistantId,
        role: "assistant",
        content: "",
        metrics: null,
        error: "",
        session: null,
      },
    ]);
    setChatInput("");
    setChatError("");
    setChatBusy(true);

    const controller = new AbortController();
    chatAbortRef.current = controller;
    const startedAt = performance.now();
    try {
      const response = await fetch(interactionPath(selectedPlane, "chat/completions/stream"), {
        method: "POST",
        headers: {
          Accept: "text/event-stream",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          stream: true,
          preferred_language: chatLanguage,
          messages: nextMessages.map((item) => ({
            role: item.role,
            content: item.content,
          })),
        }),
        signal: controller.signal,
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(
          payload?.message || payload?.error?.message || response.statusText,
        );
      }

      await consumeEventStream(response, ({ event, data }) => {
        if (data === "[DONE]") {
          return;
        }
        if (!data) {
          return;
        }
        const payload = JSON.parse(data);
        if (event === "segment_started") {
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    session: {
                      ...(item.session || {}),
                      segmentCount: Math.max(
                        item.session?.segmentCount || 0,
                        (payload.segment_index ?? 0) + 1,
                      ),
                      status: "in_progress",
                    },
                  }
                : item,
            ),
          );
          return;
        }
        if (event === "continuation_started") {
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    session: {
                      ...(item.session || {}),
                      continuationCount: payload.continuation_index ?? 0,
                      status: "in_progress",
                      stopReason: payload.reason || "",
                    },
                  }
                : item,
            ),
          );
          return;
        }
        if (event === "delta") {
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? { ...item, content: `${item.content}${payload.delta || ""}` }
                : item,
            ),
          );
          return;
        }
        if (event === "segment_complete") {
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    session: {
                      ...(item.session || {}),
                      lastSegmentFinishReason: payload.finish_reason || "",
                      segmentCount: Math.max(
                        item.session?.segmentCount || 0,
                        (payload.segment_index ?? 0) + 1,
                      ),
                    },
                  }
                : item,
            ),
          );
          return;
        }
        if (event === "session_complete" || event === "complete") {
          const session = payload.session || payload;
          const latencyMs =
            payload.latency_ms ??
            Math.max(1, Math.round(performance.now() - startedAt));
          const usage = payload.usage || session.usage || {};
          const completionTokens = usage.completion_tokens ?? 0;
          const totalTokens = usage.total_tokens ?? 0;
          const tokensPerSecond =
            latencyMs > 0 ? (completionTokens / (latencyMs / 1000)) : 0;
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    metrics: {
                      latencyMs,
                      completionTokens,
                      totalTokens,
                      tokensPerSecond,
                    },
                    session: {
                      ...(item.session || {}),
                      status:
                        payload.completion_status ||
                        session.status ||
                        "completed",
                      stopReason:
                        payload.stop_reason ||
                        session.stop_reason ||
                        "",
                      continuationCount:
                        payload.continuation_count ??
                        session.continuation_count ??
                        0,
                      segmentCount:
                        payload.segment_count ??
                        session.segment_count ??
                        item.session?.segmentCount ??
                        1,
                    },
                  }
                : item,
            ),
          );
          return;
        }
        if (event === "session_failed") {
          const message = payload.message || "Semantic completion session failed.";
          setChatError(message);
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    error: message,
                    session: {
                      ...(item.session || {}),
                      status: payload.status || "failed",
                      stopReason: payload.message || "",
                      continuationCount: payload.continuation_count ?? 0,
                      segmentCount: payload.segment_count ?? item.session?.segmentCount ?? 0,
                    },
                  }
                : item,
            ),
          );
        }
      });
    } catch (error) {
      if (controller.signal.aborted) {
        setChatMessages((current) =>
          current.map((item) =>
            item.id === assistantId
              ? { ...item, error: "Stream stopped by operator." }
              : item,
          ),
        );
      } else {
        const message = error.message || String(error);
        setChatError(message);
        setChatMessages((current) =>
          current.map((item) =>
            item.id === assistantId ? { ...item, error: message } : item,
          ),
        );
      }
    } finally {
      if (chatAbortRef.current === controller) {
        chatAbortRef.current = null;
      }
      setChatBusy(false);
    }
  }

  useEffect(() => {
    refreshAll(initialPlane);
    refreshModelLibrary().catch(() => {});
    return () => {
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      refreshAll(selectedPlane);
    }, AUTO_REFRESH_MS);
    return () => clearInterval(timer);
  }, [selectedPlane]);

  useEffect(() => {
    if (!selectedPlane) {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      setStreamHealthy(false);
      return;
    }

    const source = new EventSource(
      queryPath("/api/v1/events/stream", {
        plane: selectedPlane,
        limit: EVENT_LIMIT,
      }),
    );
    eventSourceRef.current = source;
    setStreamHealthy(false);

    source.onopen = () => {
      setStreamHealthy(true);
    };
    source.onerror = () => {
      setStreamHealthy(false);
    };
    source.onmessage = (event) => {
      setLastEventName(event.type || "message");
      scheduleRefresh(selectedPlane);
    };

    return () => {
      source.close();
      if (eventSourceRef.current === source) {
        eventSourceRef.current = null;
      }
    };
  }, [selectedPlane]);

  useEffect(() => {
    setSelectedTab("status");
    setChatMessages([]);
    setChatError("");
    if (chatAbortRef.current) {
      chatAbortRef.current.abort();
      chatAbortRef.current = null;
    }
    setChatBusy(false);
  }, [selectedPlane]);

  useEffect(() => {
    if (!draggingDivider) {
      return undefined;
    }
    function handleMouseMove(event) {
      const container = interactionSplitRef.current;
      if (!container) {
        return;
      }
      const bounds = container.getBoundingClientRect();
      const offset = event.clientX - bounds.left;
      const nextWidth = Math.min(58, Math.max(24, (offset / bounds.width) * 100));
      setInteractionPaneWidth(nextWidth);
    }
    function handleMouseUp() {
      setDraggingDivider(false);
    }
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [draggingDivider]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (selectedPlane) {
      params.set("plane", selectedPlane);
    } else {
      params.delete("plane");
    }
    params.set("page", selectedPage);
    const next = params.toString()
      ? `${window.location.pathname}?${params.toString()}`
      : window.location.pathname;
    window.history.replaceState({}, "", next);
  }, [selectedPlane, selectedPage]);

  const desiredState = planeDetail?.desired_state || null;
  const planeRecord =
    planeDetail?.planes?.find((item) => item.name === selectedPlane) || null;
  const planeMode =
    dashboard?.plane?.plane_mode ||
    desiredState?.plane_mode ||
    planeRecord?.plane_mode ||
    "compute";
  const llmPlane = planeMode === "llm";
  const chatLanguageOptions = supportedChatLanguageOptions(desiredState, interactionStatus);
  const interactionReady = interactionStatus?.ready === true;
  const nodeItems = dashboard?.nodes || [];
  const observationItems = hostObservations?.observations || [];
  const assignmentItems = dashboard?.assignments?.by_node || [];
  const rolloutItems = rolloutState?.actions || [];
  const rebalanceItems = rebalancePlan?.rebalance_plan || [];
  const diskItems = diskState?.items || [];
  const instances = desiredState?.instances || [];
  const observedRuntime = deriveObservedRuntime(observationItems, nodeItems);
  const observedInstances = observedRuntime.observedInstances;
  const displayedInstances = observedInstances.length > 0 ? observedInstances : instances;
  const inferItems = displayedInstances.filter((item) => instanceRole(item) === "infer");
  const workerItems = displayedInstances.filter((item) => instanceRole(item) === "worker");
  const alertSummary = dashboard?.alerts || {
    critical: 0,
    warning: 0,
    booting: 0,
    total: 0,
    items: [],
  };
  const alertItems = Array.isArray(alertSummary.items) ? alertSummary.items : [];
  const runtimeSummary = dashboard?.runtime || {};
  const assignmentSummary = dashboard?.assignments || {};
  const structuredProgress = assignmentSummary.latest_progress || null;
  const inFlightAssignments =
    (assignmentSummary.pending ?? 0) + (assignmentSummary.claimed ?? 0);
  const failedAssignments = assignmentSummary.failed ?? 0;
  const readyNodes =
    observedRuntime.readyNodes > 0 || observationItems.length > 0
      ? observedRuntime.readyNodes
      : runtimeSummary.ready_nodes ?? 0;
  const notReadyNodes =
    observedRuntime.readyNodes > 0 || observationItems.length > 0
      ? observedRuntime.notReadyNodes
      : runtimeSummary.not_ready_nodes ?? 0;
  const displayedInstanceCount =
    observedInstances.length > 0 ? observedInstances.length : dashboard?.plane?.instance_count ?? 0;
  const missingRuntimeNodes = observationItems.filter(
    (observation) =>
      observation?.runtime_status?.available !== true &&
      observation?.instance_runtimes?.available !== true,
  ).length;
  const filteredEvents = events.filter((event) => !isRoutineEvent(event));
  const failedAssignmentMessage =
    alertItems.find((item) => item.kind === "failed-assignment")?.detail ||
    filteredEvents.find(
      (event) => event.category === "host-assignment" && event.event_type === "failed",
    )?.message ||
    "";
  const operationProgress = buildOperationProgressModel({
    pendingOperation,
    selectedPlaneName: selectedPlane,
    planeRecord,
    inFlightAssignments,
    failedAssignments,
    failedAssignmentMessage,
    observedInstanceCount: observedInstances.length,
    desiredInstanceCount: instances.length,
    readyNodes,
    structuredProgress,
  });
  const selectedPlaneDeleting = planeRecord?.state === "deleting";
  const currentPlaneDisplayState = planeRecord ? planeDisplayState(planeRecord) : "";
  const currentPlaneDisplayClass = planeRecord ? planeDisplayStateClass(planeRecord) : "is-booting";
  const activeModelCount = Array.isArray(modelLibrary.items) ? modelLibrary.items.length : 0;
  const activeModelJobs = Array.isArray(modelLibrary.jobs)
    ? modelLibrary.jobs.filter((job) => {
        const status = String(job?.status || "").toLowerCase();
        return status !== "" && status !== "completed" && status !== "complete" && status !== "failed";
      }).length
    : 0;
  const modelsNavClass = apiError
    ? "is-critical"
    : activeModelJobs > 0
      ? "is-warning"
      : activeModelCount > 0
        ? "is-healthy"
        : "is-booting";
  const modelsNavLabel = activeModelJobs > 0
    ? `${activeModelJobs} active`
    : activeModelCount > 0
      ? `${activeModelCount} tracked`
      : "empty";
  const planesNavLabel = selectedPlane
    ? currentPlaneDisplayState || "selected"
    : planes.length > 0
      ? `${planes.length} total`
      : "empty";
  const planesNavMeta = selectedPlane || `${planes.length} registered`;
  const modelsNavMeta = activeModelJobs > 0
    ? `${activeModelJobs} download job${activeModelJobs === 1 ? "" : "s"}`
    : `${activeModelCount} discovered model${activeModelCount === 1 ? "" : "s"}`;
  const dashboardNavLabel = selectedPlane ? "focused" : "idle";
  const dashboardNavClass = selectedPlane ? currentPlaneDisplayClass : "is-booting";

  function renderPlanesRegistry() {
    return (
      <section className="panel page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Planes</div>
            <h2>Plane registry</h2>
          </div>
          <button
            className="ghost-button"
            type="button"
            onClick={() => openPlaneDialog("new")}
          >
            New plane
          </button>
        </div>
        <div className="page-copy">
          Select a plane to inspect it on Dashboard, or manage its lifecycle directly from the
          editor and action controls.
        </div>
        <div className="plane-list">
          {planes.length === 0 ? (
            <OnboardingCard onCreatePlane={() => openPlaneDialog("new")} />
          ) : (
            planes.map((plane) => {
              const selected = plane.name === selectedPlane;
              const displayState = planeDisplayState(plane);
              const displayStateClass = planeDisplayStateClass(plane);
              return (
                <article
                  key={plane.name}
                  className={`plane-card ${selected ? "is-selected" : ""}`}
                >
                  <button
                    className="plane-card-main"
                    type="button"
                    aria-label={`plane ${plane.name} ${displayState} generation ${plane.generation ?? "n/a"}`}
                    onClick={() => {
                      setSelectedPlane(plane.name);
                      setSelectedPage("dashboard");
                      refreshAll(plane.name);
                    }}
                  >
                    <div className="plane-card-top">
                      <div className="plane-name">{plane.name}</div>
                      <div className={`pill ${displayStateClass}`}>
                        {statusDot(displayStateClass)}
                        <span>{displayState}</span>
                      </div>
                    </div>
                    <div className="plane-card-meta">
                      <span>gen {plane.generation ?? "n/a"}</span>
                      <span>applied {plane.applied_generation ?? 0}</span>
                      <span>{plane.instance_count ?? 0} instances</span>
                      <span>{plane.node_count ?? 0} nodes</span>
                    </div>
                  </button>
                  <div className="plane-card-actions">
                    <button
                      className="ghost-button compact-button icon-button"
                      type="button"
                      aria-label={`View plane ${plane.name}`}
                      title={`View plane ${plane.name}`}
                      onClick={() => {
                        setSelectedPlane(plane.name);
                        setSelectedPage("dashboard");
                        refreshAll(plane.name);
                      }}
                    >
                      <ActionIcon kind="view" />
                    </button>
                    <button
                      className="ghost-button compact-button icon-button"
                      type="button"
                      aria-label={`Edit plane ${plane.name}`}
                      title={`Edit plane ${plane.name}`}
                      onClick={() => openPlaneDialog("edit", plane.name)}
                    >
                      <ActionIcon kind="edit" />
                    </button>
                    <button
                      className="ghost-button compact-button danger-button icon-button"
                      type="button"
                      disabled={actionBusy !== ""}
                      aria-label={`Delete plane ${plane.name}`}
                      title={`Delete plane ${plane.name}`}
                      onClick={async () => {
                        const confirmed = window.confirm(
                          `Delete plane ${plane.name}? This will stop it, remove related infer and worker runtime, and then remove it from the controller registry.`,
                        );
                        if (!confirmed) {
                          return;
                        }
                        startTransition(() => {
                          setSelectedPlane(plane.name);
                          setSelectedPage("planes");
                        });
                        await executePlaneAction("delete", plane.name);
                      }}
                    >
                      <ActionIcon kind="delete" />
                    </button>
                  </div>
                </article>
              );
            })
          )}
        </div>
      </section>
    );
  }

  function renderModelsLibrary() {
    return (
      <section className="panel page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Models</div>
            <h2>Model library</h2>
          </div>
          <div className="toolbar">
            <button
              className="ghost-button"
              type="button"
              disabled={modelLibraryBusy !== ""}
              onClick={() => refreshModelLibrary()}
            >
              Refresh models
            </button>
          </div>
        </div>
        <div className="page-copy">
          Manage discovered model artifacts and queue new downloads, including multipart model
          sources.
        </div>
        <div className="models-page-grid">
          <div className="subpanel">
            <div className="subpanel-header">
              <div>
                <div className="section-label">Catalog</div>
                <h3>Discovered artifacts</h3>
              </div>
              <div className={`tag ${modelsNavClass}`}>
                {statusDot(modelsNavClass)}
                <span>{modelsNavLabel}</span>
              </div>
            </div>
            <div className="list-column model-library-list model-library-list-expanded">
              {(modelLibrary.items || []).length === 0 ? (
                <EmptyState
                  title="No discovered models"
                  detail="Add a model URL below or point a plane at a local_path to seed library roots."
                />
              ) : (
                modelLibrary.items.map((item) => (
                  <article className="list-card" key={item.path}>
                    <div className="card-row">
                      <strong>{item.name}</strong>
                      <span className="tag">{modelLibraryItemSummary(item)}</span>
                    </div>
                    <div className="list-detail">
                      <div>{item.path}</div>
                      <div>root {item.root || "n/a"}</div>
                      {Array.isArray(item.referenced_by) && item.referenced_by.length > 0 ? (
                        <div>used by {item.referenced_by.join(", ")}</div>
                      ) : (
                        <div>not referenced by any plane</div>
                      )}
                    </div>
                    <div className="toolbar">
                      <button
                        className="ghost-button compact-button danger-button"
                        type="button"
                        disabled={modelLibraryBusy !== "" || item.deletable === false}
                        onClick={() => deleteModelLibraryEntry(item)}
                      >
                        Delete
                      </button>
                    </div>
                  </article>
                ))
              )}
            </div>
          </div>
          <div className="models-page-side">
            <div className="subpanel">
              <div className="subpanel-header">
                <div>
                  <div className="section-label">Queue</div>
                  <h3>Download jobs</h3>
                </div>
              </div>
              {(modelLibrary.jobs || []).length > 0 ? (
                <div className="list-column model-library-jobs model-library-jobs-expanded">
                  {modelLibrary.jobs.map((job) => (
                    <article className="list-card" key={job.id}>
                      <div className="card-row">
                        <strong>{job.model_id || job.id}</strong>
                        <span className="tag">{job.status}</span>
                      </div>
                      <div className="list-detail">
                        <div>{job.target_root}{job.target_subdir ? `/${job.target_subdir}` : ""}</div>
                        <div>{compactBytes(job.bytes_done)} / {compactBytes(job.bytes_total)}</div>
                        {job.current_item ? <div>{job.current_item}</div> : null}
                        {job.error_message ? <div>{job.error_message}</div> : null}
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <EmptyState
                  title="No queued downloads"
                  detail="Multipart and single-file model downloads will appear here while they are running."
                />
              )}
            </div>
            <div className="subpanel">
              <div className="subpanel-header">
                <div>
                  <div className="section-label">Add model</div>
                  <h3>Queue download</h3>
                </div>
              </div>
              <label className="field-label" htmlFor="model-target-root">
                Target root
              </label>
              <input
                id="model-target-root"
                className="text-input"
                type="text"
                value={modelDownloadForm.targetRoot}
                onChange={(event) =>
                  setModelDownloadForm((current) => ({ ...current, targetRoot: event.target.value }))
                }
                placeholder="/abs/path/to/model/library"
                spellCheck="false"
              />
              <label className="field-label" htmlFor="model-target-subdir">
                Target subdir
              </label>
              <input
                id="model-target-subdir"
                className="text-input"
                type="text"
                value={modelDownloadForm.targetSubdir}
                onChange={(event) =>
                  setModelDownloadForm((current) => ({ ...current, targetSubdir: event.target.value }))
                }
                placeholder="Qwen/Qwen3.5-122B-A10B-FP8"
                spellCheck="false"
              />
              <label className="field-label" htmlFor="model-id-input">
                Model id
              </label>
              <input
                id="model-id-input"
                className="text-input"
                type="text"
                value={modelDownloadForm.modelId}
                onChange={(event) =>
                  setModelDownloadForm((current) => ({ ...current, modelId: event.target.value }))
                }
                placeholder="Qwen/Qwen3.5-122B-A10B-FP8"
                spellCheck="false"
              />
              <label className="field-label" htmlFor="model-source-urls">
                Source URL(s)
              </label>
              <textarea
                id="model-source-urls"
                className="editor-textarea model-source-textarea"
                value={modelDownloadForm.sourceUrls}
                onChange={(event) =>
                  setModelDownloadForm((current) => ({ ...current, sourceUrls: event.target.value }))
                }
                placeholder="One URL per line. Multipart models are supported."
              />
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={
                    modelLibraryBusy !== "" ||
                    !modelDownloadForm.targetRoot.trim() ||
                    !modelDownloadForm.sourceUrls.trim()
                  }
                  onClick={enqueueModelLibraryDownload}
                >
                  Download model
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>
    );
  }

  useEffect(() => {
    const nextDefault =
      normalizeChatLanguage(desiredState?.interaction?.default_response_language) ||
      normalizeChatLanguage(interactionStatus?.default_response_language) ||
      "ru";
    const options = supportedChatLanguageOptions(desiredState, interactionStatus);
    const allowed = options.some((option) => option.value === nextDefault)
      ? nextDefault
      : options[0]?.value || "ru";
    setChatLanguage((current) => (options.some((option) => option.value === current) ? current : allowed));
  }, [desiredState, interactionStatus]);

  useEffect(() => {
    if (modelDownloadForm.targetRoot || !Array.isArray(modelLibrary.roots) || modelLibrary.roots.length === 0) {
      return;
    }
    setModelDownloadForm((current) => ({
      ...current,
      targetRoot: modelLibrary.roots[0],
    }));
  }, [modelDownloadForm.targetRoot, modelLibrary.roots]);

  useEffect(() => {
    if (!llmPlane && selectedTab === "interaction") {
      setSelectedTab("status");
    }
  }, [llmPlane, selectedTab]);

  useEffect(() => {
    if (selectedTab !== "interaction") {
      return;
    }
    const transcript = chatTranscriptRef.current;
    if (!transcript) {
      return;
    }
    const scrollToBottom = () => {
      if (chatTranscriptEndRef.current) {
        chatTranscriptEndRef.current.scrollIntoView({
          behavior: chatBusy ? "auto" : "smooth",
          block: "end",
        });
        return;
      }
      transcript.scrollTo({
        top: transcript.scrollHeight,
        behavior: chatBusy ? "auto" : "smooth",
      });
    };
    const frame = window.requestAnimationFrame(scrollToBottom);
    return () => window.cancelAnimationFrame(frame);
  }, [chatMessages, chatBusy, chatError, selectedTab]);

  useEffect(() => {
    if (!operationProgress?.complete || operationProgress?.failed) {
      return undefined;
    }
    const timer = setTimeout(() => {
      setPendingOperation(null);
    }, 2500);
    return () => clearTimeout(timer);
  }, [operationProgress?.complete]);

  return (
    <div className="app-shell">
      <div className="starfield" aria-hidden="true" />
      <header className="hero">
        <div className="hero-copy">
          <div className="eyebrow">Comet Operator Interface</div>
          <h1>Constellation Control</h1>
          <p className="hero-text">
            Multi-plane control surface for lifecycle, rollout pressure, runtime
            readiness, and live controller telemetry.
          </p>
        </div>
        <div className="hero-meta">
          <div className="status-chip">
            {statusDot(apiHealthy ? "is-healthy" : apiError ? "is-critical" : "is-booting")}
            <span>{apiHealthy ? "API ready" : apiError ? "API error" : "API pending"}</span>
          </div>
          <div className="status-chip">
            {statusDot(streamHealthy ? "is-healthy" : selectedPlane ? "is-warning" : "is-booting")}
            <span>{streamHealthy ? "Stream live" : selectedPlane ? "Stream reconnecting" : "Stream idle"}</span>
          </div>
          <div className="meta-card">
            <span className="meta-label">Last refresh</span>
            <span className="meta-value">{formatTimestamp(lastRefreshAt)}</span>
          </div>
          <div className="meta-card">
            <span className="meta-label">Last event</span>
            <span className="meta-value">{lastEventName}</span>
          </div>
        </div>
      </header>

      <main className="main-grid">
        <aside className="panel side-menu">
          <div className="section-label">Navigation</div>
          <div className="side-menu-list" role="navigation" aria-label="Operator sections">
            <button
              className={`side-menu-item ${selectedPage === "dashboard" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("dashboard")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Dashboard</span>
                <span className="side-menu-meta">{selectedPlane || "Plane detail and live status"}</span>
              </div>
              <span className={`tag ${dashboardNavClass}`}>
                {statusDot(dashboardNavClass)}
                <span>{dashboardNavLabel}</span>
              </span>
            </button>
            <button
              className={`side-menu-item ${selectedPage === "planes" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("planes")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Planes</span>
                <span className="side-menu-meta">{planesNavMeta}</span>
              </div>
              <span className={`tag ${selectedPlane ? currentPlaneDisplayClass : "is-booting"}`}>
                {statusDot(selectedPlane ? currentPlaneDisplayClass : "is-booting")}
                <span>{planesNavLabel}</span>
              </span>
            </button>
            <button
              className={`side-menu-item ${selectedPage === "models" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("models")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Models</span>
                <span className="side-menu-meta">{modelsNavMeta}</span>
              </div>
              <span className={`tag ${modelsNavClass}`}>
                {statusDot(modelsNavClass)}
                <span>{modelsNavLabel}</span>
              </span>
            </button>
          </div>
        </aside>

        {selectedPage === "planes" ? (
          renderPlanesRegistry()
        ) : selectedPage === "models" ? (
          renderModelsLibrary()
        ) : (
          <section className="panel plane-overview">
          <div className="panel-header">
            <div>
              <div className="section-label">Plane detail</div>
              <h2>{selectedPlane || "No plane selected"}</h2>
            </div>
            <div className="toolbar">
              <button
                className="ghost-button"
                type="button"
                onClick={() => executePlaneAction("start")}
                disabled={
                  !selectedPlane ||
                  actionBusy !== "" ||
                  planeRecord?.state === "running" ||
                  selectedPlaneDeleting
                }
              >
                Start plane
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => executePlaneAction("stop")}
                disabled={
                  !selectedPlane ||
                  actionBusy !== "" ||
                  planeRecord?.state === "stopped" ||
                  selectedPlaneDeleting
                }
              >
                Stop plane
              </button>
            </div>
          </div>

          {selectedPlane && planeRecord && dashboard ? (
            <div className="tab-strip" role="tablist" aria-label="Plane detail tabs">
              <button
                className={`tab-button ${selectedTab === "status" ? "is-active" : ""}`}
                type="button"
                role="tab"
                aria-selected={selectedTab === "status"}
                onClick={() => setSelectedTab("status")}
              >
                <span className="tab-button-title">Status</span>
                <span className="tab-button-meta">Plane, nodes, rollout, events</span>
              </button>
              {llmPlane ? (
                <button
                  className={`tab-button ${selectedTab === "interaction" ? "is-active" : ""}`}
                  type="button"
                  role="tab"
                  aria-selected={selectedTab === "interaction"}
                  onClick={() => setSelectedTab("interaction")}
                >
                  <span className="tab-button-title">Interaction</span>
                  <span className="tab-button-meta">Chat with the running model</span>
                </button>
              ) : null}
            </div>
          ) : null}

          {apiError ? <div className="error-banner">{apiError}</div> : null}

          {!selectedPlane || !planeRecord || !dashboard ? (
            <EmptyState
              title={loading ? "Loading plane state" : "No plane selected"}
              detail="Select a plane to inspect nodes, instances, disks, rollout state, and events."
            />
          ) : (
            <>
              {selectedTab === "status" ? (
                <>
              <div className="summary-grid">
                <SummaryCard
                  label="Nodes"
                  value={dashboard.plane?.node_count ?? 0}
                  meta={`${readyNodes} ready / ${notReadyNodes} not ready`}
                />
                <SummaryCard
                  label="Instances"
                  value={displayedInstanceCount}
                  meta={`${inferItems.length} infer / ${workerItems.length} worker`}
                />
                <SummaryCard
                  label="Rollout"
                  value={dashboard.rollout?.total_actions ?? 0}
                  meta={`${dashboard.rollout?.loop_status ?? "n/a"} / ${dashboard.rollout?.loop_reason ?? "n/a"}`}
                />
                <SummaryCard
                  label="Alerts"
                  value={alertSummary.total ?? 0}
                  meta={`${alertSummary.critical ?? 0} critical / ${alertSummary.warning ?? 0} warning / ${alertSummary.booting ?? 0} booting`}
                />
              </div>

              {operationProgress ? (
                <section className="action-progress-card">
                  <div className="card-row">
                    <div>
                      <div className="section-label">Current action</div>
                      <strong>{operationProgress.label}</strong>
                    </div>
                    <span
                      className={`tag ${operationProgress.failed ? "is-critical" : operationProgress.complete ? "is-healthy" : "is-booting"}`}
                    >
                      {statusDot(
                        operationProgress.failed
                          ? "is-critical"
                          : operationProgress.complete
                            ? "is-healthy"
                            : "is-booting",
                      )}
                      <span>{operationProgress.progress}%</span>
                    </span>
                  </div>
                  <div className="progress-track" aria-hidden="true">
                    <div
                      className={`progress-fill ${operationProgress.failed ? "is-failed" : operationProgress.complete ? "is-complete" : ""}`}
                      style={{ width: `${operationProgress.progress}%` }}
                    />
                  </div>
                  <div className="progress-detail">{operationProgress.detail}</div>
                </section>
              ) : null}

              <div className="panel-grid">
                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Plane config</h3>
                    <span className="subpanel-meta">Desired config, applied config, and bootstrap model</span>
                  </div>
                  <div className="list-card">
                    <div className="metric-grid compact-metric-grid">
                      <div className="metric-row"><span>Lifecycle state</span><strong>{planeRecord.state || "n/a"}</strong></div>
                      <div className="metric-row"><span>Plane mode</span><strong>{planeMode}</strong></div>
                      <div className="metric-row"><span>Desired generation</span><strong>{planeRecord.generation ?? "n/a"}</strong></div>
                      <div className="metric-row"><span>Applied generation</span><strong>{planeRecord.applied_generation ?? 0}</strong></div>
                      <div className="metric-row"><span>Pending restart</span><strong>{planeRecord.staged_update ? "yes" : "no"}</strong></div>
                      <div className="metric-row"><span>Shared disk</span><strong>{desiredState?.plane_shared_disk_name || "n/a"}</strong></div>
                      <div className="metric-row"><span>Control root</span><strong>{desiredState?.control_root || "n/a"}</strong></div>
                      <div className="metric-row"><span>Bootstrap source</span><strong>{bootstrapModelSourceLabel(desiredState?.bootstrap_model)}</strong></div>
                      <div className="metric-row"><span>Bootstrap target</span><strong>{bootstrapModelTargetPath(desiredState)}</strong></div>
                    </div>
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Operational watch</h3>
                    <span className="subpanel-meta">Live rollout, failure, and readiness signals</span>
                  </div>
                  <div className="list-column">
                    <article className="list-card">
                      <div className="card-row">
                        <strong>Runtime state</strong>
                        <span className={`tag ${notReadyNodes > 0 ? "is-warning" : "is-healthy"}`}>
                          {statusDot(notReadyNodes > 0 ? "is-warning" : "is-healthy")}
                          <span>{notReadyNodes > 0 ? "degraded" : "stable"}</span>
                        </span>
                      </div>
                      <div className="metric-grid compact-metric-grid">
                        <div className="metric-row"><span>Observed nodes</span><strong>{runtimeSummary.observed_nodes ?? observationItems.length}</strong></div>
                        <div className="metric-row"><span>Ready nodes</span><strong>{readyNodes}</strong></div>
                        <div className="metric-row"><span>Observed instances</span><strong>{observedInstances.length}</strong></div>
                        <div className="metric-row"><span>Missing runtime payload</span><strong>{missingRuntimeNodes}</strong></div>
                      </div>
                    </article>
                    {alertItems.length === 0 ? (
                      <EmptyState
                        title="No active alerts"
                        detail="Plane is currently stable from the controller point of view."
                      />
                    ) : (
                      alertItems.map((alert, index) => {
                        const severityClass = alertSeverityClass(alert.severity);
                        return (
                          <article
                            className={`list-card alert-card ${severityClass}`}
                            key={`${alert.kind}-${alert.node_name || "global"}-${alert.assignment_id || alert.event_id || index}`}
                          >
                            <div className="card-row">
                              <strong>{alert.title}</strong>
                              <span className={`tag ${severityClass}`}>
                                {statusDot(severityClass)}
                                <span>{alert.severity}</span>
                              </span>
                            </div>
                            <div className="list-detail">
                              <div>{alert.detail || "n/a"}</div>
                              {alert.node_name ? <div>node {alert.node_name}</div> : null}
                              {alert.worker_name ? <div>worker {alert.worker_name}</div> : null}
                              <div>{alert.kind}</div>
                            </div>
                          </article>
                        );
                      })
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Instances</h3>
                    <span className="subpanel-meta">Desired configuration</span>
                  </div>
                  <div className="instance-grid">
                    {instances.length === 0 ? (
                      <EmptyState title="No instances" />
                    ) : (
                      displayedInstances.map((instance) => (
                        <article className="instance-card" key={instance.name}>
                          <div className="card-row">
                            <strong>{instance.name}</strong>
                            <span className="tag">{instanceRole(instance)}</span>
                          </div>
                          <div className="metric-grid">
                            <div className="metric-row"><span>Node</span><strong>{instance.node_name || "auto"}</strong></div>
                            <div className="metric-row"><span>GPU</span><strong>{instance.gpu_device || "auto"}</strong></div>
                            <div className="metric-row"><span>Share</span><strong>{instance.share_mode || "n/a"}</strong></div>
                            <div className="metric-row"><span>Fraction</span><strong>{instance.gpu_fraction ?? "n/a"}</strong></div>
                            <div className="metric-row"><span>Placement</span><strong>{instance.placement_mode || "n/a"}</strong></div>
                            <div className="metric-row"><span>Memory cap</span><strong>{instance.memory_cap_mb ? `${instance.memory_cap_mb} MB` : "n/a"}</strong></div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Nodes</h3>
                    <span className="subpanel-meta">Availability and runtime posture</span>
                  </div>
                  <div className="node-grid">
                    {nodeItems.length === 0 ? (
                      <EmptyState title="No nodes in current plane" />
                    ) : (
                      nodeItems.map((node) => (
                        (() => {
                          const derivedRuntime = observedRuntime.nodeRuntime.get(node.node_name);
                          const effectiveRuntimeLaunchReady =
                            node.runtime_launch_ready !== null && node.runtime_launch_ready !== undefined
                              ? node.runtime_launch_ready
                              : derivedRuntime?.runtimeLaunchReady;
                          const effectiveRuntimePhase =
                            node.runtime_phase || derivedRuntime?.runtimePhase || "pending";
                          const effectiveIndicatorClass = runtimeIndicatorClass(
                            effectiveRuntimeLaunchReady,
                            node.health,
                          );
                          return (
                            <article className="node-card" key={node.node_name}>
                              <div className="card-row">
                                <strong>{node.node_name}</strong>
                                <div className={`pill ${effectiveIndicatorClass}`}>
                                  {statusDot(effectiveIndicatorClass)}
                                  <span>{nodeStatusLabel(effectiveRuntimeLaunchReady, effectiveRuntimePhase, node.health)}</span>
                                </div>
                              </div>
                              <div className="metric-grid">
                                <div className="metric-row"><span>Availability</span><strong>{node.availability || "active"}</strong></div>
                                <div className="metric-row"><span>Status</span><strong>{node.status || "n/a"}</strong></div>
                                <div className="metric-row"><span>Runtime</span><strong>{effectiveRuntimePhase}</strong></div>
                                <div className="metric-row"><span>Launch ready</span><strong>{yesNo(effectiveRuntimeLaunchReady)}</strong></div>
                                <div className="metric-row"><span>GPU count</span><strong>{node.gpu_count ?? "n/a"}</strong></div>
                                <div className="metric-row"><span>Telemetry degraded</span><strong>{yesNo(node.telemetry_degraded)}</strong></div>
                              </div>
                            </article>
                          );
                        })()
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Assignments</h3>
                    <span className="subpanel-meta">Latest per node</span>
                  </div>
                  <div className="assignment-grid">
                    {assignmentItems.length === 0 ? (
                      <EmptyState title="No assignment activity" />
                    ) : (
                      assignmentItems.map((item) => (
                        <article className="assignment-card" key={item.node_name}>
                          <div className="card-row">
                            <strong>{item.node_name}</strong>
                            <span className="tag">{item.latest_status}</span>
                          </div>
                          <div className="metric-grid">
                            <div className="metric-row"><span>Assignment</span><strong>#{item.latest_assignment_id}</strong></div>
                            <div className="metric-row"><span>Pending</span><strong>{item.pending}</strong></div>
                            <div className="metric-row"><span>Claimed</span><strong>{item.claimed}</strong></div>
                            <div className="metric-row"><span>Failed</span><strong>{item.failed}</strong></div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>System telemetry</h3>
                    <span className="subpanel-meta">CPU, GPU, network, and disk summaries per node</span>
                  </div>
                  <div className="list-column">
                    {observationItems.length === 0 ? (
                      <EmptyState title="No host observations" detail="Report observed state to populate telemetry." />
                    ) : (
                      observationItems.map((observation) => {
                        const cpu = observation.cpu_telemetry?.summary || {};
                        const gpu = observation.gpu_telemetry?.summary || {};
                        const network = observation.network_telemetry?.summary || {};
                        const disk = observation.disk_telemetry?.summary || {};
                        return (
                          <article className="list-card" key={`telemetry-${observation.node_name}`}>
                            <div className="card-row">
                              <strong>{observation.node_name}</strong>
                              <span className="tag">{observation.status}</span>
                            </div>
                            <div className="metric-grid">
                              <div className="metric-row"><span>CPU</span><strong>{cpu.utilization_pct !== undefined ? `${Math.round(cpu.utilization_pct)}%` : "n/a"}</strong></div>
                              <div className="metric-row"><span>Load avg</span><strong>{cpu.loadavg_1m !== undefined ? Number(cpu.loadavg_1m).toFixed(2) : "n/a"}</strong></div>
                              <div className="metric-row"><span>Memory</span><strong>{compactBytes(cpu.used_memory_bytes)} / {compactBytes(cpu.total_memory_bytes)}</strong></div>
                              <div className="metric-row"><span>GPU VRAM</span><strong>{gpu.used_vram_mb !== undefined ? `${gpu.used_vram_mb}/${gpu.total_vram_mb} MB` : "n/a"}</strong></div>
                              <div className="metric-row"><span>GPU util</span><strong>{gpu.device_count ? `${gpu.device_count} dev / ${gpu.owned_process_count ?? 0} proc` : "n/a"}</strong></div>
                              <div className="metric-row"><span>Network</span><strong>{compactBytes(network.rx_bytes)} rx / {compactBytes(network.tx_bytes)} tx</strong></div>
                              <div className="metric-row"><span>Interfaces</span><strong>{network.up_count ?? 0}/{network.interface_count ?? 0} up</strong></div>
                              <div className="metric-row"><span>Disk used</span><strong>{compactBytes(disk.used_bytes)} / {compactBytes(disk.total_bytes)}</strong></div>
                            </div>
                          </article>
                        );
                      })
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Rollout actions</h3>
                    <span className="subpanel-meta">Deferred scheduler workflow</span>
                  </div>
                  <div className="list-column">
                    {rolloutItems.length === 0 ? (
                      <EmptyState title="No rollout actions" />
                    ) : (
                      rolloutItems.map((action) => (
                        <article className="list-card" key={action.id}>
                          <div className="card-row">
                            <strong>{action.worker_name}</strong>
                            <span className="tag">{action.status}</span>
                          </div>
                          <div className="list-detail">
                            <div>step {action.step}</div>
                            <div>{action.action}</div>
                            <div>
                              {action.target_node_name || "n/a"}:{action.target_gpu_device || "n/a"}
                            </div>
                            <div>{action.reason || "n/a"}</div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Rebalance plan</h3>
                    <span className="subpanel-meta">Current scheduler proposals</span>
                  </div>
                  <div className="list-column">
                    {rebalanceItems.length === 0 ? (
                      <EmptyState title="No rebalance entries" />
                    ) : (
                      rebalanceItems.map((item) => (
                        <article className="list-card" key={item.worker_name}>
                          <div className="card-row">
                            <strong>{item.worker_name}</strong>
                            <span className="tag">{item.decision}</span>
                          </div>
                          <div className="list-detail">
                            <div>{item.state}</div>
                            <div>{item.action || "n/a"}</div>
                            <div>
                              {(item.current_node_name || "n/a")}:{item.current_gpu_device || "n/a"} →{" "}
                              {(item.target_node_name || "n/a")}:{item.target_gpu_device || "n/a"}
                            </div>
                            <div>{item.gate_reason || `score ${item.score}`}</div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel">
                  <div className="subpanel-header">
                    <h3>Disk state</h3>
                    <span className="subpanel-meta">Desired vs realized storage</span>
                  </div>
                  <div className="list-column">
                    {diskItems.length === 0 ? (
                      <EmptyState title="No disk state" />
                    ) : (
                      diskItems.map((item) => (
                        <article className="list-card" key={`${item.disk_name}@${item.node_name}`}>
                          <div className="card-row">
                            <strong>{item.disk_name}</strong>
                            <span className="tag">{item.realized_state || "unknown"}</span>
                          </div>
                          <div className="list-detail">
                            <div>{item.node_name}</div>
                            <div>{item.kind || item.desired_state || "disk"}</div>
                            <div>
                              used {compactBytes(item.usage_bytes?.used_bytes)} / total{" "}
                              {compactBytes(item.usage_bytes?.total_bytes)}
                            </div>
                            <div>
                              io {compactBytes(item.io_bytes?.read_bytes)} read /{" "}
                              {compactBytes(item.io_bytes?.write_bytes)} write
                            </div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                </section>

                <section className="subpanel event-panel">
                  <div className="subpanel-header">
                    <h3>Recent events</h3>
                    <span className="subpanel-meta">Plane-scoped persisted event log</span>
                  </div>
                  <div className="event-list">
                    {filteredEvents.length === 0 ? (
                      <EmptyState title="No operator-facing events" detail="Routine host observation noise is hidden from this view." />
                    ) : (
                      filteredEvents.map((event) => (
                        <article className={`event-card ${eventSeverityClass(event.severity)}`} key={event.id}>
                          <div className="card-row">
                            <strong>
                              {event.category}.{event.event_type}
                            </strong>
                            <span className="tag">{event.severity}</span>
                          </div>
                          <div className="event-meta">
                            <span>{formatTimestamp(event.created_at)}</span>
                            {event.node_name ? <span>{event.node_name}</span> : null}
                            {event.worker_name ? <span>{event.worker_name}</span> : null}
                          </div>
                          <p className="event-message">{event.message || "n/a"}</p>
                        </article>
                      ))
                    )}
                  </div>
                </section>
              </div>
                </>
              ) : (
                <div
                  className="interaction-layout"
                  ref={interactionSplitRef}
                  style={{ gridTemplateColumns: `${interactionPaneWidth}% 14px minmax(0, 1fr)` }}
                >
                  <section className="subpanel interaction-panel">
                    <div className="subpanel-header">
                      <h3>Interaction</h3>
                      <span className="subpanel-meta">Controller-proxied chat with the active model</span>
                    </div>
                    <div className="list-card interaction-status-card">
                      <div className="metric-grid compact-metric-grid">
                        <div className="metric-row"><span>Plane mode</span><strong>{planeMode}</strong></div>
                        <div className="metric-row"><span>Plane state</span><strong>{interactionStatus?.plane_state || planeRecord.state || "n/a"}</strong></div>
                        <div className="metric-row"><span>Ready</span><strong>{interactionReady ? "yes" : "no"}</strong></div>
                        <div className="metric-row"><span>Model</span><strong>{interactionStatus?.served_model_name || interactionStatus?.active_model_id || "n/a"}</strong></div>
                        <div className="metric-row"><span>Default language</span><strong>{desiredState?.interaction?.default_response_language || interactionStatus?.default_response_language || "n/a"}</strong></div>
                        <div className="metric-row"><span>Follow user language</span><strong>{yesNo(desiredState?.interaction?.follow_user_language ?? interactionStatus?.follow_user_language)}</strong></div>
                      </div>
                      <div className="metric-row"><span>System prompt</span><strong>{desiredState?.interaction?.system_prompt ? "configured" : "not configured"}</strong></div>
                      {!interactionReady ? (
                        <EmptyState
                          title="Interaction unavailable"
                          detail={interactionReasonMessage(interactionStatus)}
                        />
                      ) : null}
                    </div>
                  </section>

                  <div
                    className={`interaction-divider ${draggingDivider ? "is-dragging" : ""}`}
                    role="separator"
                    aria-orientation="vertical"
                    onMouseDown={() => setDraggingDivider(true)}
                  >
                    <span className="interaction-divider-handle" />
                  </div>

                  <section className="subpanel chat-panel">
                    <div className="subpanel-header">
                      <h3>Chat</h3>
                      <span className="subpanel-meta">Session-only transcript</span>
                    </div>
                    <div className="chat-transcript" ref={chatTranscriptRef}>
                      {chatMessages.length === 0 ? (
                        <EmptyState
                          title="No chat turns yet"
                          detail="Send a prompt to test the running model through the controller proxy."
                        />
                      ) : (
                        chatMessages.map((message) => (
                          <article
                            className={`chat-message ${message.role === "assistant" ? "is-assistant" : "is-user"}`}
                            key={message.id}
                          >
                            <div className="card-row">
                              <strong>{message.role === "assistant" ? "Assistant" : "User"}</strong>
                              {message.metrics ? (
                                <span className="tag">
                                  {Math.round(message.metrics.latencyMs)} ms /{" "}
                                  {message.metrics.completionTokens} tok /{" "}
                                  {message.metrics.tokensPerSecond.toFixed(1)} tok/s
                                </span>
                              ) : null}
                            </div>
                            <p className="chat-message-text">{message.content || (message.role === "assistant" && chatBusy ? "Streaming..." : "")}</p>
                            {message.error ? (
                              <div className="chat-error-line">{message.error}</div>
                            ) : null}
                            {message.session ? (
                              <div className="chat-metrics-line">
                                session {message.session.status || "in_progress"}
                                {message.session.segmentCount
                                  ? ` / ${message.session.segmentCount} segment${
                                      message.session.segmentCount === 1 ? "" : "s"
                                    }`
                                  : ""}
                                {message.session.continuationCount
                                  ? ` / ${message.session.continuationCount} continuation${
                                      message.session.continuationCount === 1 ? "" : "s"
                                    }`
                                  : ""}
                                {message.session.stopReason
                                  ? ` / ${message.session.stopReason}`
                                  : ""}
                              </div>
                            ) : null}
                            {message.metrics ? (
                              <div className="chat-metrics-line">
                                total {message.metrics.totalTokens} tokens
                              </div>
                            ) : null}
                          </article>
                        ))
                      )}
                      <div ref={chatTranscriptEndRef} aria-hidden="true" />
                    </div>
                    <div className="chat-composer">
                      <div className="chat-toolbar">
                        <label className="field-label chat-language-field">
                          <span>Language</span>
                          <select
                            className="chat-language-select"
                            value={chatLanguage}
                            onChange={(event) => setChatLanguage(event.target.value)}
                            disabled={chatBusy}
                          >
                            {chatLanguageOptions.map((option) => (
                              <option key={option.value} value={option.value}>
                                {option.label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <span className="composer-hint">Enter to send, Shift+Enter for a new line</span>
                      </div>
                      <textarea
                        className="editor-textarea chat-input"
                        value={chatInput}
                        onChange={(event) => setChatInput(event.target.value)}
                        onKeyDown={handleChatInputKeyDown}
                        placeholder="Ask the running model a question"
                        disabled={!interactionReady || chatBusy}
                      />
                      <div className="toolbar">
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={sendChatPrompt}
                          disabled={!interactionReady || chatBusy || !chatInput.trim()}
                        >
                          Send
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={stopChatStream}
                          disabled={!chatBusy}
                        >
                          Stop
                        </button>
                      </div>
                      {chatError ? <div className="error-banner">{chatError}</div> : null}
                    </div>
                  </section>
                </div>
              )}
            </>
          )}
          </section>
        )}
      </main>
      <PlaneEditorDialog
        dialog={planeDialog}
        setDialog={setPlaneDialog}
        onClose={() =>
          setPlaneDialog({
            open: false,
            mode: "new",
            planeName: "",
            text: "",
            busy: false,
            error: "",
          })
        }
        onSave={savePlaneDialog}
      />
    </div>
  );
}

export default App;
