import { useEffect, useRef, useState, startTransition } from "react";
import { startAuthentication, startRegistration } from "@simplewebauthn/browser";
import {
  buildDesiredStateV2FromForm,
  buildNewPlaneFormState,
  buildPlaneFormStateFromDesiredStateV2,
  isDesiredStateV2,
  PlaneV2FormBuilder,
  validatePlaneV2Form,
} from "./planeV2Form.jsx";

const REFRESH_DEBOUNCE_MS = 350;
const AUTO_REFRESH_MS = 5000;
const MODEL_LIBRARY_ACTIVE_POLL_MS = 1000;
const EVENT_LIMIT = 24;
const MODEL_LIBRARY_PAGE_SIZE = 24;
const MODEL_LIBRARY_JOB_PAGE_SIZE = 8;
const CHAT_LANGUAGE_OPTIONS = [
  { value: "en", label: "English" },
  { value: "de", label: "Deutsch" },
  { value: "uk", label: "Українська" },
  { value: "ru", label: "Русский" },
];

class HttpError extends Error {
  constructor(status, message, payload) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.payload = payload;
  }
}

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
      throw new HttpError(response.status, message, payload);
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
  if (kind === "close") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path
          d="M6 6l12 12M18 6 6 18"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    );
  }
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

function modelLibraryJobStatusClass(status) {
  switch (String(status || "").toLowerCase()) {
    case "completed":
      return "is-healthy";
    case "failed":
      return "is-critical";
    case "stopped":
    case "stopping":
      return "is-warning";
    case "running":
    case "queued":
      return "is-booting";
    default:
      return "is-booting";
  }
}

function modelLibraryJobProgress(job) {
  const bytesDone = Number(job?.bytes_done ?? 0);
  const bytesTotal = Number(job?.bytes_total ?? NaN);
  if (
    !Number.isFinite(bytesDone) ||
    !Number.isFinite(bytesTotal) ||
    bytesTotal <= 0 ||
    bytesTotal < bytesDone
  ) {
    return null;
  }
  return Math.max(0, Math.min(100, (bytesDone / bytesTotal) * 100));
}

function modelLibraryJobProgressLabel(job) {
  const progress = modelLibraryJobProgress(job);
  return progress === null ? "Downloading" : `${Math.round(progress)}% complete`;
}

function modelLibraryJobByteSummary(job) {
  const bytesDone = Number(job?.bytes_done ?? 0);
  const bytesTotal = Number(job?.bytes_total ?? NaN);
  if (
    Number.isFinite(bytesDone) &&
    Number.isFinite(bytesTotal) &&
    bytesTotal > 0 &&
    bytesTotal >= bytesDone
  ) {
    return `${compactBytes(bytesDone)} / ${compactBytes(bytesTotal)}`;
  }
  return `${compactBytes(bytesDone)} downloaded`;
}

function formatTemperature(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Math.round(Number(value))} C`;
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

function instanceRole(instance) {
  const subrole =
    instance?.labels?.["comet.subrole"] ||
    instance?.environment?.COMET_INSTANCE_SUBROLE ||
    "";
  if (subrole === "aggregator") {
    return "aggregator";
  }
  const upstreams = instance?.environment?.COMET_REPLICA_UPSTREAMS || "";
  if ((instance?.kind === "infer" || instance?.role === "infer") && upstreams) {
    return "aggregator";
  }
  return instance?.kind || instance?.role || "instance";
}

function instanceSortWeight(instance) {
  const role = instanceRole(instance);
  if (role === "app") {
    return 0;
  }
  if (role === "aggregator") {
    return 1;
  }
  if (role === "infer") {
    return 2;
  }
  if (role === "worker") {
    return 3;
  }
  return 4;
}

function sortInstances(instances) {
  return [...(instances || [])].sort((left, right) => {
    const weightDiff = instanceSortWeight(left) - instanceSortWeight(right);
    if (weightDiff !== 0) {
      return weightDiff;
    }
    return String(left?.name || "").localeCompare(String(right?.name || ""));
  });
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
    observedInstances: sortInstances(observedInstances),
    readyNodes,
    notReadyNodes,
    nodeRuntime,
  };
}

function hostObservationStatusClass(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "failed" || normalized === "error") {
    return "is-critical";
  }
  if (normalized === "stale" || normalized === "degraded") {
    return "is-warning";
  }
  if (normalized === "ok" || normalized === "healthy" || normalized === "ready") {
    return "is-healthy";
  }
  return "is-booting";
}

function formatInstanceRoleSummary(instances) {
  const roleCounts = new Map();
  for (const instance of instances || []) {
    const role = instanceRole(instance);
    roleCounts.set(role, (roleCounts.get(role) || 0) + 1);
  }

  const preferredOrder = ["app", "aggregator", "infer", "worker"];
  const parts = [];

  for (const role of preferredOrder) {
    const count = roleCounts.get(role) || 0;
    if (count > 0) {
      parts.push(`${count} ${role}`);
      roleCounts.delete(role);
    }
  }

  for (const [role, count] of roleCounts.entries()) {
    if (count > 0) {
      parts.push(`${count} ${role}`);
    }
  }

  return parts.length > 0 ? parts.join(" / ") : "0 instances";
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
        The platform is running. Create your first plane, paste a compact desired-state v2
        template, then stage and start it from the operator workflow.
      </p>
      <div className="onboarding-steps">
        <div>1. Open the plane editor.</div>
        <div>2. Paste or adjust the desired-state v2 JSON.</div>
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

function PlaneEditorDialog({ dialog, setDialog, onClose, onSave, modelLibraryItems }) {
  if (!dialog.open) {
    return null;
  }

  const readOnly = dialog.mode === "view";
  const showFormBuilder = !readOnly && Boolean(dialog.form);
  const formValidation = showFormBuilder
    ? validatePlaneV2Form(dialog.form || buildNewPlaneFormState())
    : { errors: [], warnings: [] };
  const desiredStateLabel = showFormBuilder
    ? "Generated desired state v2 JSON"
    : "Desired state JSON";
  const editorCopy = readOnly
    ? "Read-only desired state JSON for the selected plane."
    : showFormBuilder
      ? "Build or adjust a compact desired-state v2 plane in the form below. The generated JSON preview is staged in the controller when you save."
      : "Edit desired state JSON and stage it in the controller. Runtime changes are applied only after Start plane.";
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
          <button
            className="ghost-button compact-button icon-button"
            type="button"
            aria-label="Close dialog"
            title="Close dialog"
            onClick={onClose}
          >
            <ActionIcon kind="close" />
          </button>
        </div>
        <p className="editor-copy">{editorCopy}</p>
        {dialog.error ? <div className="error-banner">{dialog.error}</div> : null}
        {formValidation.errors.length > 0 ? (
          <div className="error-banner">
            <strong>Form validation</strong>
            <ul className="banner-list">
              {formValidation.errors.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {formValidation.warnings.length > 0 ? (
          <div className="warning-banner">
            <strong>Warnings</strong>
            <ul className="banner-list">
              {formValidation.warnings.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {showFormBuilder ? (
          <PlaneV2FormBuilder
            dialog={dialog}
            setDialog={setDialog}
            languageOptions={CHAT_LANGUAGE_OPTIONS}
            modelLibraryItems={modelLibraryItems || []}
          />
        ) : null}
        <label className="field-label" htmlFor="plane-editor-json">
          {desiredStateLabel}
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
          readOnly={readOnly || showFormBuilder}
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
            <>
              <button className="ghost-button" type="button" onClick={onClose} disabled={dialog.busy}>
                Cancel
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={onSave}
                disabled={dialog.busy || formValidation.errors.length > 0}
              >
                {dialog.mode === "new" ? "Create plane" : "Save staged changes"}
              </button>
            </>
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
  const initialInviteToken = window.location.pathname.startsWith("/register/")
    ? decodeURIComponent(window.location.pathname.slice("/register/".length))
    : "";
  const [planes, setPlanes] = useState([]);
  const [selectedPlane, setSelectedPlane] = useState(initialPlane);
  const [selectedPage, setSelectedPage] = useState(
    ["dashboard", "planes", "models", "access"].includes(initialPage) ? initialPage : "dashboard",
  );
  const [authState, setAuthState] = useState({
    loading: true,
    authenticated: false,
    setupRequired: false,
    user: null,
  });
  const [authMode, setAuthMode] = useState(initialInviteToken ? "register" : "login");
  const [authBusy, setAuthBusy] = useState(false);
  const [authError, setAuthError] = useState("");
  const [authForm, setAuthForm] = useState({
    username: "",
    password: "",
    inviteToken: initialInviteToken,
  });
  const [adminInvites, setAdminInvites] = useState([]);
  const [sshKeys, setSshKeys] = useState([]);
  const [accessBusy, setAccessBusy] = useState("");
  const [accessError, setAccessError] = useState("");
  const [sshKeyForm, setSshKeyForm] = useState({
    label: "",
    publicKey: "",
  });
  const [planeDetail, setPlaneDetail] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [hostObservations, setHostObservations] = useState(null);
  const [globalHostObservations, setGlobalHostObservations] = useState(null);
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
  const [visibleModelCount, setVisibleModelCount] = useState(MODEL_LIBRARY_PAGE_SIZE);
  const [modelJobPage, setModelJobPage] = useState(0);
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
    form: null,
    busy: false,
    error: "",
  });

  const refreshTimerRef = useRef(null);
  const eventSourceRef = useRef(null);
  const chatAbortRef = useRef(null);
  const interactionSplitRef = useRef(null);
  const chatTranscriptRef = useRef(null);
  const chatTranscriptEndRef = useRef(null);
  const modelLibraryListRef = useRef(null);
  const modelLibraryLoadMoreRef = useRef(null);

  function resetProtectedData() {
    setPlanes([]);
    setPlaneDetail(null);
    setDashboard(null);
    setHostObservations(null);
    setGlobalHostObservations(null);
    setDiskState(null);
    setRolloutState(null);
    setRebalancePlan(null);
    setEvents([]);
    setInteractionStatus(null);
    setModelLibrary({ items: [], roots: [], jobs: [] });
    setChatMessages([]);
    setChatError("");
    setApiHealthy(false);
    setStreamHealthy(false);
  }

  function handleUnauthorized() {
    resetProtectedData();
    setAuthState((current) => ({
      ...current,
      loading: false,
      authenticated: false,
      user: null,
    }));
    setAdminInvites([]);
    setSshKeys([]);
    setSelectedPage("dashboard");
    setAuthError("");
  }

  async function refreshAuthState() {
    const payload = await fetchJson("/api/v1/auth/state");
    const nextState = {
      loading: false,
      authenticated: payload.authenticated === true,
      setupRequired: payload.setup_required === true,
      user: payload.user || null,
    };
    setAuthState(nextState);
    setAuthMode(
      nextState.setupRequired
        ? "bootstrap"
        : authForm.inviteToken
          ? "register"
          : nextState.authenticated
            ? authMode
            : "login",
    );
    return nextState;
  }

  async function refreshAccessData() {
    if (!authState.authenticated) {
      return;
    }
    try {
      const [sshKeysPayload, invitesPayload] = await Promise.all([
        fetchJson("/api/v1/auth/ssh-keys"),
        authState.user?.role === "admin"
          ? fetchJson("/api/v1/auth/invites")
          : Promise.resolve({ items: [] }),
      ]);
      setSshKeys(Array.isArray(sshKeysPayload.items) ? sshKeysPayload.items : []);
      setAdminInvites(Array.isArray(invitesPayload.items) ? invitesPayload.items : []);
      setAccessError("");
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setAccessError(error.message || String(error));
    }
  }

  async function loadPlanes(preferredPlane = selectedPlane) {
    const payload = await fetchJson("/api/v1/planes");
    const items = Array.isArray(payload.items) ? payload.items : [];
    setPlanes(items);
    const planeExists =
      preferredPlane && items.some((item) => item.name === preferredPlane);
    const nextPlane = planeExists ? preferredPlane : "";
    if (nextPlane !== selectedPlane) {
      startTransition(() => {
        setSelectedPlane(nextPlane);
      });
    }
    return nextPlane;
  }

  async function refreshModelLibrary() {
    try {
      const payload = await fetchJson(modelLibraryPath());
      const nextJobs = Array.isArray(payload.jobs) ? payload.jobs : [];
      setModelLibrary({
        items: Array.isArray(payload.items) ? payload.items : [],
        roots: Array.isArray(payload.roots) ? payload.roots : [],
        jobs: nextJobs,
      });
      setModelJobPage((current) => {
        const nextPageCount = Math.max(
          1,
          Math.ceil(nextJobs.length / MODEL_LIBRARY_JOB_PAGE_SIZE),
        );
        return Math.min(current, nextPageCount - 1);
      });
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      throw error;
    }
  }

  async function refreshAll(planeOverride) {
    setLoading(true);
    setApiError("");
    try {
      const planeName = await loadPlanes(planeOverride ?? selectedPlane);
      const globalHostObservationsRequest = fetchJson(
        queryPath("/api/v1/host-observations", {
          stale_after: 30,
        }),
      );
      if (!planeName) {
        const globalHostObservationsPayload = await globalHostObservationsRequest;
        setPlaneDetail(null);
        setDashboard(null);
        setHostObservations(null);
        setGlobalHostObservations(globalHostObservationsPayload);
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
        globalHostObservationsPayload,
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
        globalHostObservationsRequest,
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
      setGlobalHostObservations(globalHostObservationsPayload);
      setDiskState(diskPayload);
      setRolloutState(rolloutPayload);
      setRebalancePlan(rebalancePayload);
      setEvents(Array.isArray(eventsPayload.events) ? eventsPayload.events : []);
      setInteractionStatus(interactionPayload);
      setApiHealthy(true);
      setLastRefreshAt(new Date().toISOString());
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setApiHealthy(false);
      setApiError(error.message || String(error));
    } finally {
      setLoading(false);
    }
  }

  async function completeAuthentication(method, beginPayload) {
    setAuthBusy(true);
    setAuthError("");
    try {
      const begin = await fetchJson(`/api/v1/auth/${method}/begin`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(beginPayload),
      });
      const response =
        method === "login"
          ? await startAuthentication({ optionsJSON: begin.options })
          : await startRegistration({ optionsJSON: begin.options });
      await fetchJson(`/api/v1/auth/${method}/finish`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          flow_id: begin.flow_id,
          response,
        }),
      });
      if (method === "register" && window.location.pathname.startsWith("/register/")) {
        window.history.replaceState({}, "", "/");
      }
      const nextAuth = await refreshAuthState();
      if (nextAuth.authenticated) {
        await Promise.all([refreshAccessData(), refreshAll(selectedPlane), refreshModelLibrary()]);
      }
    } catch (error) {
      setAuthError(error.message || String(error));
    } finally {
      setAuthBusy(false);
    }
  }

  async function handleBootstrapSubmit(event) {
    event.preventDefault();
    await completeAuthentication("bootstrap", {
      username: authForm.username.trim(),
      password: authForm.password,
    });
  }

  async function handleLoginSubmit(event) {
    event.preventDefault();
    await completeAuthentication("login", {
      username: authForm.username.trim(),
    });
  }

  async function handleRegisterSubmit(event) {
    event.preventDefault();
    await completeAuthentication("register", {
      invite_token: authForm.inviteToken.trim(),
      username: authForm.username.trim(),
      password: authForm.password,
    });
  }

  async function handleLogout() {
    setAccessBusy("logout");
    try {
      await fetchJson("/api/v1/auth/logout", { method: "POST" });
    } catch (_) {
      // The server clears the cookie even when the session is already gone.
    } finally {
      setAccessBusy("");
      handleUnauthorized();
    }
  }

  async function handleCreateInvite() {
    setAccessBusy("create-invite");
    try {
      await fetchJson("/api/v1/auth/invites", { method: "POST" });
      await refreshAccessData();
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setAccessError(error.message || String(error));
    } finally {
      setAccessBusy("");
    }
  }

  async function handleRevokeInvite(inviteId) {
    setAccessBusy(`revoke-invite:${inviteId}`);
    try {
      await fetchJson(`/api/v1/auth/invites/${inviteId}`, { method: "DELETE" });
      await refreshAccessData();
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setAccessError(error.message || String(error));
    } finally {
      setAccessBusy("");
    }
  }

  async function handleAddSshKey(event) {
    event.preventDefault();
    setAccessBusy("create-ssh-key");
    setAccessError("");
    try {
      await fetchJson("/api/v1/auth/ssh-keys", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          label: sshKeyForm.label.trim(),
          public_key: sshKeyForm.publicKey.trim(),
        }),
      });
      setSshKeyForm({ label: "", publicKey: "" });
      await refreshAccessData();
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setAccessError(error.message || String(error));
    } finally {
      setAccessBusy("");
    }
  }

  async function handleDeleteSshKey(keyId) {
    setAccessBusy(`delete-ssh-key:${keyId}`);
    try {
      await fetchJson(`/api/v1/auth/ssh-keys/${keyId}`, { method: "DELETE" });
      await refreshAccessData();
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setAccessError(error.message || String(error));
    } finally {
      setAccessBusy("");
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

  async function stopModelLibraryJob(job) {
    if (!job?.id) {
      return;
    }
    setModelLibraryBusy(`stop:${job.id}`);
    try {
      await fetchJson(modelLibraryPath("jobs/stop"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ job_id: job.id }),
      });
      await refreshModelLibrary();
    } finally {
      setModelLibraryBusy("");
    }
  }

  async function resumeModelLibraryJob(job) {
    if (!job?.id) {
      return;
    }
    setModelLibraryBusy(`resume:${job.id}`);
    try {
      await fetchJson(modelLibraryPath("jobs/resume"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ job_id: job.id }),
      });
      await refreshModelLibrary();
    } finally {
      setModelLibraryBusy("");
    }
  }

  async function hideModelLibraryJob(job) {
    if (!job?.id) {
      return;
    }
    setModelLibraryBusy(`hide:${job.id}`);
    try {
      await fetchJson(modelLibraryPath("jobs/hide"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ job_id: job.id }),
      });
      await refreshModelLibrary();
    } finally {
      setModelLibraryBusy("");
    }
  }

  async function deleteModelLibraryJob(job) {
    if (!job?.id) {
      return;
    }
    const confirmed = window.confirm(
      `Delete download job ${job.model_id || job.id}? This removes the queue record and deletes downloaded files from disk.`,
    );
    if (!confirmed) {
      return;
    }
    setModelLibraryBusy(`job-delete:${job.id}`);
    try {
      await fetchJson(modelLibraryPath("jobs"), {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ job_id: job.id }),
      });
      await refreshModelLibrary();
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
        const form = buildNewPlaneFormState();
        setPlaneDialog({
          open: true,
          mode,
          planeName: "",
          text: JSON.stringify(buildDesiredStateV2FromForm(form), null, 2),
          form,
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
        text: JSON.stringify(payload.desired_state_v2 || payload.desired_state || {}, null, 2),
        form: payload.desired_state_v2
          ? buildPlaneFormStateFromDesiredStateV2(payload.desired_state_v2)
          : null,
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
      if (planeDialog.form) {
        const validation = validatePlaneV2Form(planeDialog.form);
        if (validation.errors.length > 0) {
          throw new Error(validation.errors[0]);
        }
      }
      const desiredState = JSON.parse(planeDialog.text);
      if (!desiredState?.plane_name) {
        throw new Error("plane_name is required.");
      }
      if (planeDialog.mode === "edit" && desiredState.plane_name !== planeDialog.planeName) {
        throw new Error("Plane rename is not supported in edit mode.");
      }
      const requestBody = isDesiredStateV2(desiredState)
        ? { desired_state_v2: desiredState }
        : { desired_state: desiredState };
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
        body: JSON.stringify(requestBody),
      });
      setPlaneDialog({
        open: false,
        mode: "new",
        planeName: "",
        text: "",
        form: null,
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
    refreshAuthState()
      .then((nextAuth) => {
        if (!nextAuth.authenticated) {
          return;
        }
        return Promise.all([
          refreshAll(initialPlane),
          refreshModelLibrary().catch(() => {}),
          refreshAccessData(),
        ]);
      })
      .catch((error) => {
        setAuthState((current) => ({ ...current, loading: false }));
        setAuthError(error.message || String(error));
      });
    return () => {
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (!authState.authenticated) {
      return undefined;
    }
    const timer = setInterval(() => {
      refreshAll(selectedPlane);
    }, AUTO_REFRESH_MS);
    return () => clearInterval(timer);
  }, [authState.authenticated, selectedPlane]);

  useEffect(() => {
    if (!authState.authenticated) {
      return undefined;
    }
    const hasActiveModelJobs = Array.isArray(modelLibrary.jobs)
      ? modelLibrary.jobs.some((job) => {
          const status = String(job?.status || "").toLowerCase();
          return status === "queued" || status === "running" || status === "stopping";
        })
      : false;
    if (!hasActiveModelJobs) {
      return undefined;
    }
    const timer = setInterval(() => {
      refreshModelLibrary().catch(() => {});
    }, MODEL_LIBRARY_ACTIVE_POLL_MS);
    return () => clearInterval(timer);
  }, [authState.authenticated, modelLibrary.jobs]);

  useEffect(() => {
    if (!authState.authenticated || !selectedPlane) {
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
  }, [authState.authenticated, selectedPlane]);

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
  const globalObservationItems = globalHostObservations?.observations || [];
  const assignmentItems = dashboard?.assignments?.by_node || [];
  const rolloutItems = rolloutState?.actions || [];
  const rebalanceItems = rebalancePlan?.rebalance_plan || [];
  const diskItems = diskState?.items || [];
  const instances = sortInstances(desiredState?.instances || []);
  const observedRuntime = deriveObservedRuntime(observationItems, nodeItems);
  const observedInstances = observedRuntime.observedInstances;
  const displayedInstances = observedInstances.length > 0 ? observedInstances : instances;
  const instanceRoleSummary = formatInstanceRoleSummary(displayedInstances);
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
  const globalObservationSummary = globalObservationItems.reduce(
    (summary, observation) => {
      const cpu = observation?.cpu_telemetry?.summary || {};
      const gpu = observation?.gpu_telemetry?.summary || {};
      const network = observation?.network_telemetry?.summary || {};
      const disk = observation?.disk_telemetry?.summary || {};
      const status = String(observation?.status || "").toLowerCase();
      if (status !== "stale" && status !== "failed" && status !== "error") {
        summary.healthyNodes += 1;
      }
      if (
        observation?.runtime_status?.available !== true &&
        observation?.instance_runtimes?.available !== true
      ) {
        summary.missingRuntime += 1;
      }
      summary.totalMemoryBytes += Number(cpu.total_memory_bytes || 0);
      summary.usedMemoryBytes += Number(cpu.used_memory_bytes || 0);
      summary.totalGpuVramMb += Number(gpu.total_vram_mb || 0);
      summary.usedGpuVramMb += Number(gpu.used_vram_mb || 0);
      summary.gpuDeviceCount += Number(gpu.device_count || 0);
      if (cpu.temperature_available) {
        summary.cpuTemperatureHostCount += 1;
        summary.maxCpuTemperatureC = Math.max(
          summary.maxCpuTemperatureC,
          Number(cpu.max_temperature_c || cpu.temperature_c || 0),
        );
      }
      if (Number(gpu.temperature_device_count || 0) > 0) {
        summary.gpuTemperatureDeviceCount += Number(gpu.temperature_device_count || 0);
        summary.maxGpuTemperatureC = Math.max(
          summary.maxGpuTemperatureC,
          Number(gpu.hottest_temperature_c || 0),
        );
      }
      summary.networkRxBytes += Number(network.rx_bytes || 0);
      summary.networkTxBytes += Number(network.tx_bytes || 0);
      summary.diskUsedBytes += Number(disk.used_bytes || 0);
      summary.diskTotalBytes += Number(disk.total_bytes || 0);
      return summary;
    },
    {
      healthyNodes: 0,
      missingRuntime: 0,
      totalMemoryBytes: 0,
      usedMemoryBytes: 0,
      totalGpuVramMb: 0,
      usedGpuVramMb: 0,
      gpuDeviceCount: 0,
      cpuTemperatureHostCount: 0,
      maxCpuTemperatureC: 0,
      gpuTemperatureDeviceCount: 0,
      maxGpuTemperatureC: 0,
      networkRxBytes: 0,
      networkTxBytes: 0,
      diskUsedBytes: 0,
      diskTotalBytes: 0,
    },
  );
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
  const runningPlanes = planes.filter((plane) => plane?.state === "running");
  const activePlanes = planes.filter((plane) => {
    const state = String(plane?.state || "").toLowerCase();
    return state !== "" && state !== "stopped" && state !== "deleting";
  });
  const runningPlaneCount = runningPlanes.length;
  const usedModelCount = new Set(
    runningPlanes
      .map((plane) => String(plane?.model_id || "").trim())
      .filter(Boolean),
  ).size;
  const activeModelCount = Array.isArray(modelLibrary.items) ? modelLibrary.items.length : 0;
  const visibleModelItems = (modelLibrary.items || []).slice(0, visibleModelCount);
  const hasMoreModelItems = activeModelCount > visibleModelCount;
  const modelLibraryJobs = Array.isArray(modelLibrary.jobs) ? modelLibrary.jobs : [];
  const modelJobPageCount = Math.max(
    1,
    Math.ceil(modelLibraryJobs.length / MODEL_LIBRARY_JOB_PAGE_SIZE),
  );
  const currentModelJobPage = Math.min(modelJobPage, modelJobPageCount - 1);
  const visibleModelJobs = modelLibraryJobs.slice(
    currentModelJobPage * MODEL_LIBRARY_JOB_PAGE_SIZE,
    (currentModelJobPage + 1) * MODEL_LIBRARY_JOB_PAGE_SIZE,
  );
  const activeModelJobs = Array.isArray(modelLibrary.jobs)
    ? modelLibrary.jobs.filter((job) => {
        const status = String(job?.status || "").toLowerCase();
        return status !== "" &&
          status !== "completed" &&
          status !== "complete" &&
          status !== "failed" &&
          status !== "stopped";
      }).length
    : 0;
  const modelsNavClass = apiError
    ? "is-critical"
    : activeModelJobs > 0
      ? "is-warning"
      : usedModelCount > 0
        ? "is-healthy"
        : "is-booting";
  const modelsNavLabel = `${usedModelCount} used`;
  const planesNavLabel = `${runningPlaneCount} running`;
  const planesNavMeta = selectedPlane || `${planes.length} registered`;
  const modelsNavMeta = activeModelJobs > 0
    ? `${activeModelJobs} download job${activeModelJobs === 1 ? "" : "s"}`
    : `${activeModelCount} discovered model${activeModelCount === 1 ? "" : "s"}`;
  function handleModelLibraryScroll(event) {
    const node = event.currentTarget;
    if (!hasMoreModelItems) {
      return;
    }
    const remaining = node.scrollHeight - node.scrollTop - node.clientHeight;
    if (remaining <= 72) {
      setVisibleModelCount((current) =>
        Math.min(activeModelCount, current + MODEL_LIBRARY_PAGE_SIZE),
      );
    }
  }

  useEffect(() => {
    const root = modelLibraryListRef.current;
    const target = modelLibraryLoadMoreRef.current;
    if (!root || !target || !hasMoreModelItems) {
      return undefined;
    }
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (!entry.isIntersecting) {
            continue;
          }
          setVisibleModelCount((current) =>
            Math.min(activeModelCount, current + MODEL_LIBRARY_PAGE_SIZE),
          );
        }
      },
      {
        root,
        rootMargin: "0px 0px 120px 0px",
        threshold: 0.1,
      },
    );
    observer.observe(target);
    return () => observer.disconnect();
  }, [activeModelCount, hasMoreModelItems, visibleModelItems.length]);

  function openPlaneDashboard(planeName) {
    setSelectedPlane(planeName);
    setSelectedPage("dashboard");
    setSelectedTab("status");
    refreshAll(planeName);
  }

  function closePlaneDashboard() {
    setSelectedPlane("");
    setSelectedTab("status");
  }

  function renderPlaneCards(planeItems, deleteTargetPage = "planes") {
    return (
      <div className="plane-list">
        {planeItems.map((plane) => {
          const selected = plane.name === selectedPlane;
          const displayState = planeDisplayState(plane);
          const displayStateClass = planeDisplayStateClass(plane);
          const planeDeleting = plane.state === "deleting";
          const planeStartDisabled =
            actionBusy !== "" ||
            planeDeleting ||
            plane.state === "running" ||
            plane.state === "starting";
          const planeStopDisabled =
            actionBusy !== "" ||
            planeDeleting ||
            plane.state === "stopped" ||
            plane.state === "stopping";
          const planeModeLabel = plane.plane_mode || "compute";
          return (
            <article
              key={plane.name}
              className={`plane-card ${selected ? "is-selected" : ""}`}
            >
              <button
                className="plane-card-main"
                type="button"
                aria-label={`plane ${plane.name} ${displayState} generation ${plane.generation ?? "n/a"}`}
                onClick={() => openPlaneDashboard(plane.name)}
              >
                <div className="plane-card-top">
                  <div className="plane-name">{plane.name}</div>
                  <div className={`pill ${displayStateClass}`}>
                    {statusDot(displayStateClass)}
                    <span>{displayState}</span>
                  </div>
                </div>
                <div className="plane-card-meta">
                  <span>{planeModeLabel}</span>
                  <span>gen {plane.generation ?? "n/a"}</span>
                  <span>applied {plane.applied_generation ?? 0}</span>
                  <span>{plane.instance_count ?? 0} instances</span>
                  <span>{plane.node_count ?? 0} nodes</span>
                  {plane.failed_assignments ? (
                    <span>{plane.failed_assignments} failed assignments</span>
                  ) : null}
                </div>
              </button>
              <div className="plane-card-actions">
                <button
                  className="ghost-button compact-button plane-action-button"
                  type="button"
                  disabled={planeStartDisabled}
                  onClick={() => executePlaneAction("start", plane.name)}
                >
                  Start
                </button>
                <button
                  className="ghost-button compact-button plane-action-button"
                  type="button"
                  disabled={planeStopDisabled}
                  onClick={() => executePlaneAction("stop", plane.name)}
                >
                  Stop
                </button>
                <button
                  className="ghost-button compact-button icon-button"
                  type="button"
                  aria-label={`View plane ${plane.name}`}
                  title={`View plane ${plane.name}`}
                  onClick={() => openPlaneDashboard(plane.name)}
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
                      setSelectedPage(deleteTargetPage);
                    });
                    await executePlaneAction("delete", plane.name);
                  }}
                >
                  <ActionIcon kind="delete" />
                </button>
              </div>
            </article>
          );
        })}
      </div>
    );
  }

  function renderPlaneListPanel({
    sectionLabel,
    title,
    copy,
    planeItems,
    deleteTargetPage = "planes",
    showCreateButton = false,
    emptyContent = null,
  }) {
    return (
      <section className="panel page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">{sectionLabel}</div>
            <h2>{title}</h2>
          </div>
          {showCreateButton ? (
            <button
              className="ghost-button"
              type="button"
              onClick={() => openPlaneDialog("new")}
            >
              New plane
            </button>
          ) : null}
        </div>
        {copy ? <div className="page-copy">{copy}</div> : null}
        {planeItems.length === 0
          ? emptyContent
          : renderPlaneCards(planeItems, deleteTargetPage)}
      </section>
    );
  }

  function renderDashboardPlaneDetailModal() {
    if (!selectedPlane || !planeRecord || !dashboard) {
      return null;
    }

    return (
      <div className="modal-backdrop" role="presentation" onClick={closePlaneDashboard}>
        <section
          className={`modal-card plane-detail-modal ${selectedTab === "interaction" ? "is-interaction" : ""}`}
          role="dialog"
          aria-modal="true"
          aria-labelledby="plane-detail-title"
          onClick={(event) => event.stopPropagation()}
        >
          <div className="panel-header">
            <div>
              <div className="section-label">Plane detail</div>
              <h2 id="plane-detail-title">{selectedPlane}</h2>
            </div>
            <div className="toolbar">
              <button
                className="ghost-button"
                type="button"
                onClick={() => executePlaneAction("start")}
                disabled={
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
                  actionBusy !== "" ||
                  planeRecord?.state === "stopped" ||
                  selectedPlaneDeleting
                }
              >
                Stop plane
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => openPlaneDialog("edit", selectedPlane)}
                disabled={actionBusy !== ""}
              >
                Edit plane
              </button>
              <button
                className="ghost-button compact-button icon-button"
                type="button"
                aria-label="Close plane details"
                title="Close plane details"
                onClick={closePlaneDashboard}
              >
                <ActionIcon kind="close" />
              </button>
            </div>
          </div>

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

          {apiError ? <div className="error-banner">{apiError}</div> : null}

          {selectedTab === "status" ? (
            <div className="dashboard-plane-detail-scroll">
              <div className="summary-grid">
                <SummaryCard
                  label="Nodes"
                  value={dashboard.plane?.node_count ?? 0}
                  meta={`${readyNodes} ready / ${notReadyNodes} not ready`}
                />
                <SummaryCard
                  label="Instances"
                  value={displayedInstanceCount}
                  meta={instanceRoleSummary}
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
                      nodeItems.map((node) => {
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
                      })
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
                      <EmptyState
                        title="No operator-facing events"
                        detail="Routine host observation noise is hidden from this view."
                      />
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
            </div>
          ) : (
            <div className="dashboard-plane-detail-scroll">
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
                                {Math.round(message.metrics.latencyMs)} ms / {message.metrics.completionTokens} tok /{" "}
                                {message.metrics.tokensPerSecond.toFixed(1)} tok/s
                              </span>
                            ) : null}
                          </div>
                          <p className="chat-message-text">
                            {message.content || (message.role === "assistant" && chatBusy ? "Streaming..." : "")}
                          </p>
                          {message.error ? <div className="chat-error-line">{message.error}</div> : null}
                          {message.session ? (
                            <div className="chat-metrics-line">
                              session {message.session.status || "in_progress"}
                              {message.session.segmentCount
                                ? ` / ${message.session.segmentCount} segment${message.session.segmentCount === 1 ? "" : "s"}`
                                : ""}
                              {message.session.continuationCount
                                ? ` / ${message.session.continuationCount} continuation${message.session.continuationCount === 1 ? "" : "s"}`
                                : ""}
                              {message.session.stopReason ? ` / ${message.session.stopReason}` : ""}
                            </div>
                          ) : null}
                          {message.metrics ? (
                            <div className="chat-metrics-line">total {message.metrics.totalTokens} tokens</div>
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
                            <option value={option.value} key={option.value}>
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
                      <button className="ghost-button" type="button" onClick={stopChatStream} disabled={!chatBusy}>
                        Stop
                      </button>
                    </div>
                    {chatError ? <div className="error-banner">{chatError}</div> : null}
                  </div>
                </section>
              </div>
            </div>
          )}
        </section>
      </div>
    );
  }

  function renderPlanesRegistry() {
    return renderPlaneListPanel({
      sectionLabel: "Planes",
      title: "Plane registry",
      copy:
        "Select a plane to inspect it on Dashboard, or manage its lifecycle directly from the editor and action controls.",
      planeItems: planes,
      deleteTargetPage: "planes",
      showCreateButton: true,
      emptyContent: <OnboardingCard onCreatePlane={() => openPlaneDialog("new")} />,
    });
  }

  function renderModelsLibrary() {
    return (
      <section className="panel page-panel models-page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Models</div>
            <h2>Model Library</h2>
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
        <div className="models-page-stack">
          <div className="subpanel models-catalog-panel">
            <div className="subpanel-header">
              <div>
                <div className="section-label">Catalog</div>
                <h3>Models</h3>
              </div>
              <div className={`tag ${modelsNavClass}`}>
                {statusDot(modelsNavClass)}
                <span>{modelsNavLabel}</span>
              </div>
            </div>
            <div className="models-catalog-meta">
              <span>Loaded {visibleModelItems.length} of {activeModelCount}</span>
              <span>Scroll to load the next page of tracked artifacts.</span>
            </div>
            <div
              className={`list-column model-library-list model-library-list-expanded ${
                (modelLibrary.items || []).length === 0 ? "model-library-list-empty" : ""
              }`}
              ref={modelLibraryListRef}
              onScroll={handleModelLibraryScroll}
            >
              {(modelLibrary.items || []).length === 0 ? (
                <EmptyState
                  title="No discovered models"
                  detail="Add a model URL below or use local_path to seed the library."
                />
              ) : (
                visibleModelItems.map((item) => (
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
              {hasMoreModelItems ? (
                <div
                  ref={modelLibraryLoadMoreRef}
                  className="model-library-scroll-sentinel"
                  aria-hidden="true"
                />
              ) : null}
            </div>
            {hasMoreModelItems ? (
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() =>
                    setVisibleModelCount((current) =>
                      Math.min(activeModelCount, current + MODEL_LIBRARY_PAGE_SIZE),
                    )
                  }
                >
                  Load more
                </button>
              </div>
            ) : null}
          </div>
          <div className="models-page-grid">
            <div className="subpanel models-jobs-panel">
              <div className="subpanel-header">
                <div>
                  <div className="section-label">Queue</div>
                  <h3>Download jobs</h3>
                </div>
              </div>
              {modelLibraryJobs.length > 0 ? (
                <>
                  <div className="models-catalog-meta model-job-pagination-meta">
                    <span>
                      Showing {visibleModelJobs.length} of {modelLibraryJobs.length}
                    </span>
                    <span>
                      Page {currentModelJobPage + 1} of {modelJobPageCount}
                    </span>
                  </div>
                  <div className="list-column model-library-jobs model-library-jobs-expanded">
                  {visibleModelJobs.map((job) => (
                    <article className="list-card" key={job.id}>
                      <div className="card-row">
                        <strong>{job.model_id || job.id}</strong>
                        <span className={`tag ${modelLibraryJobStatusClass(job.status)}`}>{job.status}</span>
                      </div>
                      <div className="list-detail">
                        <div>{job.target_root}{job.target_subdir ? `/${job.target_subdir}` : ""}</div>
                        <div>{modelLibraryJobByteSummary(job)}</div>
                        {job.current_item ? <div>{job.current_item}</div> : null}
                        {job.error_message ? <div>{job.error_message}</div> : null}
                      </div>
                      <div className="model-job-progress">
                        <div className="model-job-progress-meta">
                          <span>{modelLibraryJobProgressLabel(job)}</span>
                          {modelLibraryJobProgress(job) !== null ? (
                            <span>{Math.round(modelLibraryJobProgress(job))}%</span>
                          ) : (
                            <span>{job.status}</span>
                          )}
                        </div>
                        <progress
                          className="model-job-progress-bar"
                          value={modelLibraryJobProgress(job) === null ? undefined : modelLibraryJobProgress(job)}
                          max="100"
                          aria-label={modelLibraryJobProgressLabel(job)}
                        />
                      </div>
                      <div className="toolbar model-job-toolbar">
                        <button
                          className="ghost-button"
                          type="button"
                          disabled={modelLibraryBusy !== "" || job.can_stop !== true}
                          onClick={() => stopModelLibraryJob(job)}
                        >
                          Stop
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          disabled={modelLibraryBusy !== "" || job.can_resume !== true}
                          onClick={() => resumeModelLibraryJob(job)}
                        >
                          Resume
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          disabled={modelLibraryBusy !== "" || job.can_hide !== true}
                          onClick={() => hideModelLibraryJob(job)}
                        >
                          Hide
                        </button>
                        <button
                          className="ghost-button danger-button"
                          type="button"
                          disabled={modelLibraryBusy !== "" || job.can_delete !== true}
                          onClick={() => deleteModelLibraryJob(job)}
                        >
                          Delete
                        </button>
                      </div>
                    </article>
                  ))}
                  </div>
                  {modelJobPageCount > 1 ? (
                    <div className="toolbar model-job-pagination">
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={currentModelJobPage <= 0}
                        onClick={() => setModelJobPage((current) => Math.max(0, current - 1))}
                      >
                        Previous
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={currentModelJobPage >= modelJobPageCount - 1}
                        onClick={() =>
                          setModelJobPage((current) =>
                            Math.min(modelJobPageCount - 1, current + 1),
                          )
                        }
                      >
                        Next
                      </button>
                    </div>
                  ) : null}
                </>
              ) : (
                <EmptyState
                  title="No queued downloads"
                  detail="Visible download jobs will appear here while they are running or after they finish."
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

  function renderAccessPage() {
    return (
      <section className="panel page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Access</div>
            <h2>User profile</h2>
          </div>
          <div className="toolbar">
            <button
              className="ghost-button"
              type="button"
              disabled={accessBusy !== ""}
              onClick={() => refreshAccessData()}
            >
              Refresh access
            </button>
            <button
              className="ghost-button danger-button"
              type="button"
              disabled={accessBusy !== ""}
              onClick={handleLogout}
            >
              Log out
            </button>
          </div>
        </div>
        {accessError ? <div className="error-banner">{accessError}</div> : null}
        <div className="panel-grid">
          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Identity</h3>
              <span className="subpanel-meta">Controller-owned WebAuthn session</span>
            </div>
            <div className="list-card">
              <div className="metric-grid compact-metric-grid">
                <div className="metric-row"><span>User</span><strong>{authState.user?.username || "n/a"}</strong></div>
                <div className="metric-row"><span>Role</span><strong>{authState.user?.role || "n/a"}</strong></div>
                <div className="metric-row"><span>Last login</span><strong>{formatTimestamp(authState.user?.last_login_at)}</strong></div>
              </div>
            </div>
          </section>

          {authState.user?.role === "admin" ? (
            <section className="subpanel">
              <div className="subpanel-header">
                <h3>Registration invites</h3>
                <span className="subpanel-meta">One-time links valid for one hour</span>
              </div>
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={accessBusy !== ""}
                  onClick={handleCreateInvite}
                >
                  Create invite
                </button>
              </div>
              <div className="list-column">
                {adminInvites.length === 0 ? (
                  <EmptyState title="No active invites" detail="Create a one-time link for a new user." />
                ) : (
                  adminInvites.map((invite) => (
                    <article className="list-card" key={invite.id}>
                      <div className="card-row">
                        <strong>Invite #{invite.id}</strong>
                        <span className="tag">{formatTimestamp(invite.expires_at)}</span>
                      </div>
                      <div className="list-detail">
                        <div>{invite.registration_url}</div>
                        <div>expires {formatTimestamp(invite.expires_at)}</div>
                      </div>
                      <div className="toolbar">
                        <button
                          className="ghost-button compact-button"
                          type="button"
                          onClick={() => navigator.clipboard?.writeText(invite.registration_url || "")}
                        >
                          Copy
                        </button>
                        <button
                          className="ghost-button compact-button danger-button"
                          type="button"
                          disabled={accessBusy !== ""}
                          onClick={() => handleRevokeInvite(invite.id)}
                        >
                          Revoke
                        </button>
                      </div>
                    </article>
                  ))
                )}
              </div>
            </section>
          ) : null}

          <section className="subpanel">
            <div className="subpanel-header">
              <h3>SSH public keys</h3>
              <span className="subpanel-meta">Used for protected plane API access</span>
            </div>
            <form className="list-card access-form-card" onSubmit={handleAddSshKey}>
              <label className="field-label" htmlFor="ssh-key-label">Label</label>
              <input
                id="ssh-key-label"
                className="text-input"
                type="text"
                value={sshKeyForm.label}
                onChange={(event) =>
                  setSshKeyForm((current) => ({ ...current, label: event.target.value }))
                }
                placeholder="Laptop"
              />
              <label className="field-label" htmlFor="ssh-key-public">Public key</label>
              <textarea
                id="ssh-key-public"
                className="editor-textarea"
                value={sshKeyForm.publicKey}
                onChange={(event) =>
                  setSshKeyForm((current) => ({ ...current, publicKey: event.target.value }))
                }
                placeholder="ssh-ed25519 AAAA..."
              />
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="submit"
                  disabled={accessBusy !== "" || !sshKeyForm.publicKey.trim()}
                >
                  Add SSH key
                </button>
              </div>
            </form>
            <div className="list-column">
              {sshKeys.length === 0 ? (
                <EmptyState title="No SSH keys" detail="Attach a public key to enable SSH API authorization." />
              ) : (
                sshKeys.map((key) => (
                  <article className="list-card" key={key.id}>
                    <div className="card-row">
                      <strong>{key.label || "SSH key"}</strong>
                      <span className="tag">{key.fingerprint}</span>
                    </div>
                    <div className="list-detail">
                      <div>{key.public_key}</div>
                      <div>created {formatTimestamp(key.created_at)}</div>
                    </div>
                    <div className="toolbar">
                      <button
                        className="ghost-button compact-button danger-button"
                        type="button"
                        disabled={accessBusy !== ""}
                        onClick={() => handleDeleteSshKey(key.id)}
                      >
                        Remove
                      </button>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>
        </div>
      </section>
    );
  }

  function renderAuthShell() {
    const isBootstrap = authMode === "bootstrap";
    const isRegister = authMode === "register";
    const title = isBootstrap
      ? "Create the first admin"
      : isRegister
        ? "Complete invite registration"
        : "Sign in with a passkey";
    const detail = isBootstrap
      ? "The controller has no users yet. Create the initial admin and register a WebAuthn credential."
      : isRegister
        ? "Finish account activation from a one-time invite link. Password is stored only for enrollment; future login uses WebAuthn."
        : "Enter your username and continue with WebAuthn authentication.";
    return (
      <div className="app-shell auth-shell">
        <div className="starfield" aria-hidden="true" />
        <main className="auth-layout">
          <section className="panel auth-panel">
            <div className="panel-header">
              <div>
                <div className="section-label">Protected Access</div>
                <h2>{title}</h2>
              </div>
            </div>
            <div className="page-copy">{detail}</div>
            {authError ? <div className="error-banner">{authError}</div> : null}
            <form
              className="auth-form"
              onSubmit={
                isBootstrap
                  ? handleBootstrapSubmit
                  : isRegister
                    ? handleRegisterSubmit
                    : handleLoginSubmit
              }
            >
              {isRegister ? (
                <>
                  <label className="field-label" htmlFor="invite-token">Invite token</label>
                  <input
                    id="invite-token"
                    className="text-input"
                    type="text"
                    value={authForm.inviteToken}
                    onChange={(event) =>
                      setAuthForm((current) => ({ ...current, inviteToken: event.target.value }))
                    }
                    spellCheck="false"
                  />
                </>
              ) : null}
              <label className="field-label" htmlFor="auth-username">Username</label>
              <input
                id="auth-username"
                className="text-input"
                type="text"
                value={authForm.username}
                onChange={(event) =>
                  setAuthForm((current) => ({ ...current, username: event.target.value }))
                }
                autoComplete="username webauthn"
                spellCheck="false"
              />
              {isBootstrap || isRegister ? (
                <>
                  <label className="field-label" htmlFor="auth-password">Password</label>
                  <input
                    id="auth-password"
                    className="text-input"
                    type="password"
                    value={authForm.password}
                    onChange={(event) =>
                      setAuthForm((current) => ({ ...current, password: event.target.value }))
                    }
                    autoComplete="new-password"
                  />
                </>
              ) : null}
              <div className="toolbar">
                <button className="ghost-button" type="submit" disabled={authBusy}>
                  {authBusy ? "Working..." : isBootstrap ? "Create admin" : isRegister ? "Register user" : "Continue"}
                </button>
                {!isBootstrap ? (
                  <button
                    className="ghost-button"
                    type="button"
                    disabled={authBusy}
                    onClick={() => setAuthMode(initialInviteToken ? "register" : "login")}
                  >
                    Reset
                  </button>
                ) : null}
              </div>
            </form>
          </section>
        </main>
      </div>
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
    if (!authState.authenticated) {
      setAdminInvites([]);
      setSshKeys([]);
      return;
    }
    refreshAccessData();
  }, [authState.authenticated, authState.user?.role]);

  useEffect(() => {
    setVisibleModelCount((current) => {
      const nextMinimum = Math.min(
        Math.max(activeModelCount, 0),
        Math.max(MODEL_LIBRARY_PAGE_SIZE, current),
      );
      return nextMinimum === current ? current : nextMinimum;
    });
  }, [activeModelCount]);

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

  if (authState.loading) {
    return (
      <div className="app-shell auth-shell">
        <div className="starfield" aria-hidden="true" />
        <main className="auth-layout">
          <section className="panel auth-panel">
            <div className="panel-header">
              <div>
                <div className="section-label">Protected Access</div>
                <h2>Loading access policy</h2>
              </div>
            </div>
          </section>
        </main>
      </div>
    );
  }

  if (!authState.authenticated) {
    return renderAuthShell();
  }

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
          <div className="meta-card">
            <span className="meta-label">Signed in as</span>
            <span className="meta-value">
              {authState.user?.username || "n/a"} ({authState.user?.role || "n/a"})
            </span>
          </div>
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
              onClick={() => {
                setSelectedPage("dashboard");
                setSelectedTab("status");
              }}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Dashboard</span>
                <span className="side-menu-meta">{selectedPlane || "Plane detail and live status"}</span>
              </div>
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
              <span className={`tag ${runningPlaneCount > 0 ? "is-healthy" : "is-booting"}`}>
                {statusDot(runningPlaneCount > 0 ? "is-healthy" : "is-booting")}
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
            <button
              className={`side-menu-item ${selectedPage === "access" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("access")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Access</span>
                <span className="side-menu-meta">Invites, SSH keys, and logout</span>
              </div>
              <span className="tag">
                <span>{authState.user?.role || "user"}</span>
              </span>
            </button>
          </div>
        </aside>

        {selectedPage === "planes" ? (
          renderPlanesRegistry()
        ) : selectedPage === "models" ? (
          renderModelsLibrary()
        ) : selectedPage === "access" ? (
          renderAccessPage()
        ) : (
          <section className="panel plane-overview">
            <div className="panel-header">
              <div>
                <div className="section-label">Dashboard</div>
                <h2>Fleet overview</h2>
              </div>
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => openPlaneDialog("new")}
                >
                  New plane
                </button>
              </div>
            </div>

          {apiError ? <div className="error-banner">{apiError}</div> : null}

          <div className="dashboard-stack">
              <section className="subpanel server-telemetry-panel">
                <div className="subpanel-header">
                  <h3>Server telemetry</h3>
                  <span className="subpanel-meta">
                    Host-level CPU, GPU, temperature, network, disk, and runtime posture across all observed nodes
                  </span>
                </div>
                <div className="summary-grid server-summary-grid">
                  <SummaryCard
                    label="Hosts"
                    value={globalObservationItems.length}
                    meta={`${globalObservationSummary.healthyNodes} healthy / ${Math.max(0, globalObservationItems.length - globalObservationSummary.healthyNodes)} degraded`}
                  />
                  <SummaryCard
                    label="Physical GPUs"
                    value={globalObservationSummary.gpuDeviceCount}
                    meta={
                      globalObservationSummary.totalGpuVramMb > 0
                        ? `${globalObservationSummary.usedGpuVramMb}/${globalObservationSummary.totalGpuVramMb} MB VRAM`
                        : "no GPU telemetry"
                    }
                  />
                  <SummaryCard
                    label="Memory total"
                    value={compactBytes(globalObservationSummary.totalMemoryBytes)}
                    meta={`used ${compactBytes(globalObservationSummary.usedMemoryBytes)}`}
                  />
                  <SummaryCard
                    label="CPU temp"
                    value={
                      globalObservationSummary.cpuTemperatureHostCount > 0
                        ? formatTemperature(globalObservationSummary.maxCpuTemperatureC)
                        : "n/a"
                    }
                    meta={`${globalObservationSummary.cpuTemperatureHostCount} hosts reporting`}
                  />
                  <SummaryCard
                    label="GPU temp"
                    value={
                      globalObservationSummary.gpuTemperatureDeviceCount > 0
                        ? formatTemperature(globalObservationSummary.maxGpuTemperatureC)
                        : "n/a"
                    }
                    meta={`${globalObservationSummary.gpuTemperatureDeviceCount} GPU devices reporting`}
                  />
                  <SummaryCard
                    label="Runtime gaps"
                    value={globalObservationSummary.missingRuntime}
                    meta="hosts missing runtime payload"
                  />
                </div>
              </section>

              <section className="subpanel dashboard-planes-panel">
                <div className="subpanel-header">
                  <h3>Active planes</h3>
                  <span className="subpanel-meta">
                    Running and transitional planes with quick controls. Click a plane to open its detailed dashboard.
                  </span>
                </div>
                {activePlanes.length === 0 ? (
                  planes.length === 0 ? (
                    <OnboardingCard onCreatePlane={() => openPlaneDialog("new")} />
                  ) : (
                    <EmptyState
                      title="No active planes"
                      detail="Stopped planes remain available on the Planes page. Start one to bring it back into the active fleet view."
                    />
                  )
                ) : (
                  renderPlaneCards(activePlanes, "dashboard")
                )}
              </section>
            </div>
          </section>
        )}
      </main>
      {renderDashboardPlaneDetailModal()}
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
        modelLibraryItems={modelLibrary.items || []}
      />
    </div>
  );
}

export default App;
