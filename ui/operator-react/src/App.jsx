import React, { useEffect, useRef, useState, startTransition } from "react";
import { startAuthentication, startRegistration } from "@simplewebauthn/browser";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  buildDesiredStateV2FromForm,
  buildNewPlaneFormState,
  buildPlaneFormStateFromDesiredStateV2,
  isDesiredStateV2,
  PlaneV2FormBuilder,
  validatePlaneV2Form,
} from "./planeV2Form.jsx";
import { formatPlaneDashboardSkillsSummary } from "./planeSkills.js";
import {
  detectModelSourceFormat,
  MODEL_LIBRARY_FORMAT_OPTIONS,
  MODEL_LIBRARY_GGUF_QUANTIZATIONS,
  MODEL_LIBRARY_QUANTIZATION_FILTERS,
  formatModelLibraryDisplayName,
  modelLibraryJobProgress,
  normalizeModelDownloadSourceUrls,
  normalizeModelLibraryItemQuantization,
  normalizeModelLibraryJobKind,
} from "./modelLibrary.js";
import {
  buildSkillsFactoryGroupTree,
  collectSkillsFactoryTreePaths,
  filterSkillsFactoryItems,
  formatSkillGroupPath,
  isBuiltinSkillsFactoryGroupPath,
  sortSkillsFactoryItems,
} from "./skillsFactory.js";
import { KnowledgeCubeGraph } from "./KnowledgeCubeGraph.jsx";
import {
  areKnowledgeGraphsEqual,
  buildPlaneKnowledgeGraphRequest,
  buildKnowledgeGraphRequest,
  KNOWLEDGE_GRAPH_LIMIT,
  normalizeKnowledgeResults,
  summarizeKnowledgeGraph,
} from "./knowledgeVault.js";

const REFRESH_DEBOUNCE_MS = 350;
const AUTO_REFRESH_MS = 5000;
const FLEET_REFRESH_MS = 2000;
const MODEL_LIBRARY_ACTIVE_POLL_MS = 1000;
const MODEL_LIBRARY_BACKGROUND_POLL_MS = 10000;
const EVENT_LIMIT = 24;
const REFRESH_EVENT_NAMES = [
  "host-observation.reported",
  "host-registry.provisioned",
  "host-registry.registered",
  "host-registry.revoked",
  "host-registry.rotated-key",
  "host-registry.reset-onboarding",
  "host-registry.session-opened",
  "host-registry.storage-role-updated",
  "host-assignment.applied",
  "host-assignment.failed",
  "model-library.downloaded",
];
const MODEL_LIBRARY_PAGE_SIZE = 24;
const MODEL_LIBRARY_JOB_PAGE_SIZE = 8;
const METRIC_HISTORY_MAX_POINTS = 72;
const CHAT_LANGUAGE_OPTIONS = [
  { value: "en", label: "English" },
  { value: "es", label: "Español" },
  { value: "pt", label: "Português" },
  { value: "zh", label: "中文" },
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

function delay(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
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

function modelLibraryJobsPath() {
  return "/api/v1/model-library/jobs";
}

function knowledgeVaultPath(suffix = "") {
  return suffix ? `/api/v1/knowledge-vault/${suffix}` : "/api/v1/knowledge-vault/status";
}

function planeKnowledgeVaultPath(planeName, suffix = "") {
  return planePath(planeName, `knowledge-vault/${suffix}`);
}

function hostdHostsPath(nodeName = "") {
  return queryPath("/api/v1/hostd/hosts", { node: nodeName });
}

function skillsFactoryPath(suffix = "") {
  return suffix ? `/api/v1/skills-factory/${suffix}` : "/api/v1/skills-factory";
}

function protocolsPath(suffix = "") {
  return suffix ? `/api/v1/protocols/${suffix}` : "/api/v1/protocols";
}

function interactionPath(planeName, suffix) {
  return planePath(planeName, `interaction/${suffix}`);
}

function skillsPath(planeName, suffix = "") {
  return suffix ? planePath(planeName, `skills/${suffix}`) : planePath(planeName, "skills");
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
  if (kind === "worker") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path
          d="M8 9.4c0-2.2 1.8-4 4-4s4 1.8 4 4"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M7.2 10.2h9.6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M9.2 10.2v1.3M14.8 10.2v1.3"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
        />
        <circle
          cx="12"
          cy="13.6"
          r="2.1"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
        />
        <path
          d="M7.8 20c.7-2.5 2.4-3.8 4.2-3.8s3.5 1.3 4.2 3.8"
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

function formatTimeOfDay(value) {
  if (!value) {
    return "--:--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (part) => String(part).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
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

function parseLineSeparatedValues(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function renderLineSeparatedValues(values) {
  return Array.isArray(values) ? values.join("\n") : "";
}

function buildEmptySkillForm() {
  return {
    id: "",
    name: "",
    groupPath: "",
    description: "",
    content: "",
    matchTermsText: "",
    internal: false,
    enabled: true,
    sessionIdsText: "",
    naimLinksText: "",
  };
}

function formatDashboardBytesMbGb(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  const oneGb = 1024 ** 3;
  if (numeric >= oneGb) {
    return `${(numeric / oneGb).toFixed(1)} GB`;
  }
  return `${Math.round(numeric / (1024 ** 2))} MB`;
}

function formatDashboardMegabytesMbGb(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  if (numeric >= 1024) {
    return `${(numeric / 1024).toFixed(1)} GB`;
  }
  return `${Math.round(numeric)} MB`;
}

function formatChartGigabytesFromBytes(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return `${(numeric / (1024 ** 3)).toFixed(2)} GB`;
}

function formatChartGigabytesFromMegabytes(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return `${(numeric / 1024).toFixed(2)} GB`;
}

function formatBytesPerSecond(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return "n/a";
  }
  return `${compactBytes(numeric)}/s`;
}

function latestRatePerSecond(history) {
  if (!Array.isArray(history) || history.length < 2) {
    return null;
  }
  const lastPoint = history[history.length - 1];
  const previousPoint = history[history.length - 2];
  const deltaSeconds = (Number(lastPoint?.ts) - Number(previousPoint?.ts)) / 1000;
  const deltaValue = Number(lastPoint?.value) - Number(previousPoint?.value);
  if (!Number.isFinite(deltaSeconds) || deltaSeconds <= 0) {
    return null;
  }
  if (!Number.isFinite(deltaValue) || deltaValue < 0) {
    return null;
  }
  return deltaValue / deltaSeconds;
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

function modelLibraryJobProgressLabel(job) {
  const phase = String(job?.phase || job?.status || "").toLowerCase();
  const progress = modelLibraryJobProgress(job);
  if (progress !== null) {
    return `${Math.round(progress)}% complete`;
  }
  if (phase === "converting") {
    return "Converting to GGUF";
  }
  if (phase === "quantizing") {
    return "Quantizing GGUF";
  }
  if (phase === "cleaning") {
    return "Finalizing outputs";
  }
  if (phase === "completed") {
    return "Completed";
  }
  if (phase === "failed") {
    return "Failed";
  }
  if (phase === "stopped") {
    return "Stopped";
  }
  return "Downloading";
}

function modelLibraryJobByteSummary(job) {
  const phase = String(job?.phase || job?.status || "").toLowerCase();
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
  if (phase === "converting" || phase === "quantizing" || phase === "cleaning") {
    return "Waiting on conversion pipeline";
  }
  return `${compactBytes(bytesDone)} downloaded`;
}

function formatTemperature(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Math.round(Number(value))} C`;
}

function summarizeGlobalObservations(items) {
  return (items || []).reduce(
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
      summary.cpuLoadHostCount += 1;
      summary.totalCpuLoad1m += Number(cpu.loadavg_1m || 0);
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
      summary.diskReadBytes += Number(disk.read_bytes || 0);
      summary.diskWriteBytes += Number(disk.write_bytes || 0);
      summary.diskUsedBytes += Number(disk.used_bytes || 0);
      summary.diskTotalBytes += Number(disk.total_bytes || 0);
      return summary;
    },
    {
      healthyNodes: 0,
      missingRuntime: 0,
      cpuLoadHostCount: 0,
      totalCpuLoad1m: 0,
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
      diskReadBytes: 0,
      diskWriteBytes: 0,
      diskUsedBytes: 0,
      diskTotalBytes: 0,
    },
  );
}

function appendMetricHistory(previousHistory, samples, timestamp = Date.now()) {
  const nextHistory = { ...previousHistory };
  for (const [key, sample] of Object.entries(samples || {})) {
    if (!sample || !Number.isFinite(sample.value)) {
      continue;
    }
    const currentSeries = Array.isArray(previousHistory[key]) ? previousHistory[key] : [];
    const nextPoint = {
      ts: timestamp,
      value: Number(sample.value),
    };
    const lastPoint = currentSeries[currentSeries.length - 1];
    if (
      lastPoint &&
      lastPoint.value === nextPoint.value &&
      timestamp - lastPoint.ts < AUTO_REFRESH_MS - 250
    ) {
      nextHistory[key] = currentSeries;
      continue;
    }
    nextHistory[key] = [...currentSeries, nextPoint].slice(-METRIC_HISTORY_MAX_POINTS);
  }
  return nextHistory;
}

function buildServerMetricSamples(summary, observedNodeCount) {
  return {
    "server.healthy_hosts": { value: Number(summary?.healthyNodes || 0) },
    "server.cpu_load_1m": { value: Number(summary?.totalCpuLoad1m || 0) },
    "server.used_gpu_vram_mb": { value: Number(summary?.usedGpuVramMb || 0) },
    "server.used_memory_bytes": { value: Number(summary?.usedMemoryBytes || 0) },
    "server.max_cpu_temp_c": { value: Number(summary?.maxCpuTemperatureC || 0) },
    "server.max_gpu_temp_c": { value: Number(summary?.maxGpuTemperatureC || 0) },
    "server.network_tx_bytes": { value: Number(summary?.networkTxBytes || 0) },
    "server.disk_write_bytes": { value: Number(summary?.diskWriteBytes || 0) },
    "server.missing_runtime": { value: Number(summary?.missingRuntime || 0) },
    "server.observed_nodes": { value: Number(observedNodeCount || 0) },
  };
}

function buildPlaneMetricSamples({
  planeName,
  readyNodes,
  observedInstanceCount,
  rolloutActions,
  alertCount,
  missingRuntimeNodes,
}) {
  if (!planeName) {
    return {};
  }
  const prefix = `plane.${planeName}`;
  return {
    [`${prefix}.ready_nodes`]: { value: Number(readyNodes || 0) },
    [`${prefix}.observed_instances`]: { value: Number(observedInstanceCount || 0) },
    [`${prefix}.rollout_actions`]: { value: Number(rolloutActions || 0) },
    [`${prefix}.alerts`]: { value: Number(alertCount || 0) },
    [`${prefix}.missing_runtime_nodes`]: { value: Number(missingRuntimeNodes || 0) },
  };
}

function formatMetricHistoryTimestamp(ts) {
  const date = new Date(ts);
  const pad = (value) => String(value).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function formatMetricChartValue(value, formatter) {
  if (typeof formatter === "function") {
    return formatter(value);
  }
  return String(value);
}

function buildMetricChartDomain(history) {
  const values = (Array.isArray(history) ? history : [])
    .map((entry) => Number(entry?.value))
    .filter((value) => Number.isFinite(value));
  if (values.length === 0) {
    return [0, 1];
  }

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  if (Math.abs(maxValue - minValue) < 1e-9) {
    const padding = Math.max(Math.abs(maxValue) * 0.05, 1);
    const lowerBound = minValue >= 0 ? Math.max(0, minValue - padding) : minValue - padding;
    return [lowerBound, maxValue + padding];
  }

  const spread = maxValue - minValue;
  const magnitude = Math.max(Math.abs(minValue), Math.abs(maxValue), 1);
  const padding = Math.max(
    spread * 0.12,
    Math.min(magnitude * 0.0025, spread),
    0.05,
  );
  const lowerBound = minValue >= 0 ? Math.max(0, minValue - padding) : minValue - padding;
  const upperBound = maxValue + padding;
  if (Math.abs(upperBound - lowerBound) < 1e-9) {
    return [lowerBound, lowerBound + 1];
  }
  return [lowerBound, upperBound];
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
    return `${desiredState?.inference?.model_cache_dir || desiredState?.inference?.gguf_cache_dir || "/naim/shared/models/cache"}/multipart`;
  }
  const targetFilename =
    bootstrapModel.target_filename ||
    bootstrapModel.local_path?.split("/").pop() ||
    bootstrapModel.source_url?.split("/").pop() ||
    "model.gguf";
  return `${desiredState?.inference?.gguf_cache_dir || "/naim/shared/models/gguf"}/${targetFilename}`;
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
  if (reason === "turboquant_unsupported") {
    return failureDetail || "TurboQuant is enabled but the configured llama.cpp runtime does not support the requested cache types.";
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

function browsingIndicatorCompact(browsing) {
  return browsing?.indicator?.compact || "";
}

function browsingTraceCompact(browsing) {
  if (!Array.isArray(browsing?.trace) || browsing.trace.length === 0) {
    return "";
  }
  return browsing.trace
    .map((item) => item?.compact || "")
    .filter(Boolean)
    .join(" · ");
}

function selectedKnowledgeIdsFromDesiredState(desiredState) {
  return Array.isArray(desiredState?.knowledge?.selected_knowledge_ids)
    ? desiredState.knowledge.selected_knowledge_ids.filter((item) => typeof item === "string" && item.trim())
    : [];
}

function knowledgeContextPolicyFromDesiredState(desiredState) {
  return desiredState?.knowledge?.context_policy || {};
}

function compactSkillList(items) {
  const skills = Array.isArray(items) ? items : [];
  return skills
    .map((item) => item?.name || item?.id || item?.skill_id || "")
    .filter(Boolean)
    .slice(0, 4)
    .join(", ");
}

function buildChatEnrichmentMeta(payload = {}, session = {}, previous = null) {
  const appliedSkills = payload.applied_skills || session.applied_skills || previous?.appliedSkills || [];
  const autoAppliedSkills =
    payload.auto_applied_skills || session.auto_applied_skills || previous?.autoAppliedSkills || [];
  const knowledge =
    payload.knowledge ||
    payload.knowledge_context ||
    session.knowledge ||
    session.knowledge_context ||
    previous?.knowledge ||
    null;
  const compression =
    payload.compression ||
    payload.compression_state ||
    session.compression ||
    session.compression_state ||
    previous?.compression ||
    null;
  const hasPayload =
    (Array.isArray(appliedSkills) && appliedSkills.length > 0) ||
    (Array.isArray(autoAppliedSkills) && autoAppliedSkills.length > 0) ||
    Boolean(knowledge) ||
    Boolean(compression);
  return hasPayload
    ? {
        appliedSkills,
        autoAppliedSkills,
        knowledge,
        compression,
      }
    : previous;
}

function observedStateForObservation(observation) {
  return observation?.observed_state || {};
}

function observedInstancesForObservation(observation) {
  const state = observedStateForObservation(observation);
  return Array.isArray(state.instances) ? state.instances : [];
}

function hostObservationItemsFromPayload(payload) {
  if (Array.isArray(payload?.observations)) {
    return payload.observations.map((item) => ({
      ...item,
      observed_at: item?.observed_at || item?.heartbeat_at || null,
    }));
  }
  if (Array.isArray(payload?.items)) {
    return payload.items.map((item) => ({
      ...item,
      observed_at: item?.observed_at || item?.heartbeat_at || null,
    }));
  }
  return [];
}

function instanceRole(instance) {
  const subrole =
    instance?.labels?.["naim.subrole"] ||
    instance?.environment?.NAIM_INSTANCE_SUBROLE ||
    "";
  if (subrole === "aggregator") {
    return "aggregator";
  }
  const upstreams = instance?.environment?.NAIM_REPLICA_UPSTREAMS || "";
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

function selfServiceStatusClass(health) {
  const normalized = String(health || "").toLowerCase();
  if (normalized === "critical") {
    return "is-critical";
  }
  if (normalized === "warning") {
    return "is-warning";
  }
  if (normalized === "healthy") {
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

export function PlaneEditorDialog({
  dialog,
  setDialog,
  onClose,
  onSave,
  modelLibraryItems,
  hostdHosts,
  peerLinks,
  skillsFactoryItems,
  skillsFactoryGroups = [],
  onResetLtCypherDeployment,
}) {
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
      ? dialog.mode === "edit" && dialog.planeState === "running"
        ? "Build or adjust a compact desired-state v2 plane in the form below. Saving updates the controller state first, and the UI can restart the running plane right after save so the new runtime config takes effect."
        : "Build or adjust a compact desired-state v2 plane in the form below. The generated JSON preview is staged in the controller when you save."
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
        <div className="plane-editor-scroll">
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
              hostdHosts={hostdHosts || []}
              peerLinks={peerLinks || null}
              skillsFactoryItems={skillsFactoryItems || []}
              skillsFactoryGroups={skillsFactoryGroups || []}
              onResetLtCypherDeployment={onResetLtCypherDeployment}
            />
          ) : null}
          {showFormBuilder ? (
            <details className="plane-advanced-section plane-generated-json-section">
              <summary className="plane-advanced-summary">Generated JSON</summary>
              <div className="plane-advanced-body">
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
                  readOnly
                  spellCheck="false"
                />
              </div>
            </details>
          ) : (
            <>
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
            </>
          )}
        </div>
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
                {dialog.mode === "new"
                  ? "Create plane"
                  : dialog.planeState === "running"
                    ? "Save changes"
                    : "Save staged changes"}
              </button>
            </>
          )}
        </div>
      </section>
    </div>
  );
}

function MetricSparklineButton({ label, history, onOpen }) {
  const series = Array.isArray(history) ? history : [];
  if (series.length < 2) {
    return null;
  }
  const domain = buildMetricChartDomain(series);
  return (
    <button
      className="metric-sparkline-button"
      type="button"
      aria-label={`Open ${label} chart`}
      title={`Open ${label} chart`}
      onClick={onOpen}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={series}>
          <YAxis type="number" domain={domain} hide />
          <Area
            type="monotone"
            dataKey="value"
            stroke="rgba(255, 213, 94, 0.96)"
            fill="rgba(120, 190, 255, 0.18)"
            strokeWidth={1.8}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </button>
  );
}

function SkillsDialog({
  dialog,
  onClose,
  onRefresh,
  onStartCreate,
  onEdit,
  onFormChange,
  onSave,
  onToggle,
  onDelete,
}) {
  if (!dialog?.open) {
    return null;
  }

  const items = Array.isArray(dialog.items) ? dialog.items : [];
  const form = dialog.form || buildEmptySkillForm();
  const editing = dialog.mode === "edit";

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-card plane-detail-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="skills-dialog-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-header">
          <div>
            <div className="section-label">Plane skills</div>
            <h2 id="skills-dialog-title">{dialog.planeName}</h2>
          </div>
          <div className="toolbar">
            <button className="ghost-button" type="button" onClick={onRefresh} disabled={dialog.busy}>
              Refresh
            </button>
            <button className="ghost-button" type="button" onClick={onStartCreate} disabled={dialog.busy}>
              New skill
            </button>
            <button
              className="ghost-button compact-button icon-button"
              type="button"
              aria-label="Close skills dialog"
              title="Close skills dialog"
              onClick={onClose}
            >
              <ActionIcon kind="close" />
            </button>
          </div>
        </div>
        {dialog.error ? <div className="error-banner">{dialog.error}</div> : null}
        <div className="panel-grid">
          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Stored skills</h3>
              <span className="subpanel-meta">{items.length} item(s)</span>
            </div>
            <div className="list-column">
              {items.length === 0 ? (
                <EmptyState
                  title="No skills yet"
                  detail="Create the first skill for this plane and bind it to sessions or explicit request ids later."
                />
              ) : (
                items.map((item) => (
                  <article className="list-card" key={item.id}>
                    <div className="card-row">
                      <div>
                        <strong>{item.name || item.id}</strong>
                        <div className="summary-meta">{item.description || "No description"}</div>
                      </div>
                      <span className={`tag ${item.enabled ? "is-healthy" : "is-warning"}`}>
                        {statusDot(item.enabled ? "is-healthy" : "is-warning")}
                        <span>{item.enabled ? "enabled" : "disabled"}</span>
                      </span>
                    </div>
                    <div className="metric-grid compact-metric-grid">
                      <div className="metric-row"><span>Sessions</span><strong>{Array.isArray(item.session_ids) ? item.session_ids.length : 0}</strong></div>
                      <div className="metric-row"><span>Links</span><strong>{Array.isArray(item.naim_links) ? item.naim_links.length : 0}</strong></div>
                      <div className="metric-row"><span>Updated</span><strong>{formatTimestamp(item.updated_at)}</strong></div>
                    </div>
                    <div className="toolbar">
                      <button className="ghost-button" type="button" onClick={() => onEdit(item)} disabled={dialog.busy}>
                        Edit
                      </button>
                      <button className="ghost-button" type="button" onClick={() => onToggle(item)} disabled={dialog.busy}>
                        {item.enabled ? "Disable" : "Enable"}
                      </button>
                      <button className="ghost-button" type="button" onClick={() => onDelete(item)} disabled={dialog.busy}>
                        Delete
                      </button>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>

          <section className="subpanel">
            <div className="subpanel-header">
              <h3>{editing ? "Edit skill" : "Create skill"}</h3>
            </div>
            <div className="plane-form-grid">
              <label className="field-label">
                <span className="field-label-title">Name</span>
                <input
                  className="text-input"
                  value={form.name}
                  onChange={(event) => onFormChange("name", event.target.value)}
                />
              </label>
              <label className="field-label plane-checkbox">
                <input
                  type="checkbox"
                  checked={Boolean(form.enabled)}
                  onChange={(event) => onFormChange("enabled", event.target.checked)}
                />
                <span className="field-label-inline">Enabled</span>
              </label>
            </div>
            <label className="field-label">
              <span className="field-label-title">Description</span>
              <textarea
                className="editor-textarea"
                value={form.description}
                onChange={(event) => onFormChange("description", event.target.value)}
                rows={3}
              />
            </label>
            <label className="field-label">
              <span className="field-label-title">Content</span>
              <textarea
                className="editor-textarea"
                value={form.content}
                onChange={(event) => onFormChange("content", event.target.value)}
                rows={10}
              />
            </label>
            <div className="plane-form-grid">
              <label className="field-label">
                <span className="field-label-title">Session ids</span>
                <textarea
                  className="editor-textarea"
                  value={form.sessionIdsText}
                  onChange={(event) => onFormChange("sessionIdsText", event.target.value)}
                  rows={6}
                />
              </label>
              <label className="field-label">
                <span className="field-label-title">Naim links</span>
                <textarea
                  className="editor-textarea"
                  value={form.naimLinksText}
                  onChange={(event) => onFormChange("naimLinksText", event.target.value)}
                  rows={6}
                />
              </label>
            </div>
            <div className="toolbar">
              <button className="ghost-button" type="button" onClick={onSave} disabled={dialog.busy}>
                {editing ? "Save skill" : "Create skill"}
              </button>
            </div>
          </section>
        </div>
      </section>
    </div>
  );
}

function SummaryCard({ label, value, meta, history, onOpenTrend }) {
  return (
    <article className="summary-card">
      <div className="summary-label">{label}</div>
      <div className="summary-value-row">
        <div className="summary-value">{value}</div>
        <MetricSparklineButton label={label} history={history} onOpen={onOpenTrend} />
      </div>
      <div className="summary-meta">{meta}</div>
    </article>
  );
}

function TelemetryChartDialog({ chart, onClose }) {
  if (!chart?.open) {
    return null;
  }
  const series = Array.isArray(chart.data) ? chart.data : [];
  const domain = buildMetricChartDomain(series);
  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-card telemetry-chart-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="telemetry-chart-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-header">
          <div>
            <div className="section-label">Telemetry history</div>
            <h2 id="telemetry-chart-title">{chart.title}</h2>
          </div>
          <button
            className="ghost-button compact-button icon-button"
            type="button"
            aria-label="Close chart"
            title="Close chart"
            onClick={onClose}
          >
            <ActionIcon kind="close" />
          </button>
        </div>
        <p className="editor-copy">{chart.meta || "Live metric history from controller polling."}</p>
        <div className="telemetry-chart-shell">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={series}>
              <CartesianGrid stroke="rgba(120, 190, 255, 0.12)" vertical={false} />
              <XAxis
                dataKey="ts"
                tickFormatter={formatMetricHistoryTimestamp}
                stroke="rgba(174, 194, 224, 0.56)"
                minTickGap={28}
              />
              <YAxis
                stroke="rgba(174, 194, 224, 0.56)"
                domain={domain}
                allowDataOverflow
                tickFormatter={(value) => formatMetricChartValue(value, chart.valueFormatter)}
                width={72}
              />
              <Tooltip
                formatter={(value) => formatMetricChartValue(value, chart.valueFormatter)}
                labelFormatter={formatMetricHistoryTimestamp}
                contentStyle={{
                  borderRadius: 14,
                  border: "1px solid rgba(120, 190, 255, 0.18)",
                  background: "rgba(6, 18, 39, 0.96)",
                }}
              />
              <Line
                type="monotone"
                dataKey="value"
                stroke="rgba(255, 213, 94, 0.96)"
                strokeWidth={2.4}
                dot={false}
                activeDot={{ r: 4 }}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>
    </div>
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

function nodeConnectivityStatus(host) {
  const registry = host?.registry || host || {};
  const runtimeStatus = host?.runtime_status || null;
  const runtimeAvailable = runtimeStatus?.available === true;
  const runtimePhase = runtimeStatus?.phase || "";
  const health = String(host?.status || "").toLowerCase();
  const sessionState = String(registry?.session_state || "").toLowerCase();
  const registrationState = String(registry?.registration_state || "").toLowerCase();
  const onboardingState = String(registry?.onboarding_state || "").toLowerCase();

  if (runtimeAvailable) {
    return { indicatorClass: "is-healthy", label: "ready" };
  }
  if (runtimePhase === "starting" || runtimePhase === "stopping" || runtimePhase === "pending") {
    return {
      indicatorClass: runtimeIndicatorClass(false, health),
      label: runtimePhase,
    };
  }
  if (health === "stale" || health === "failed" || health === "error") {
    return { indicatorClass: "is-critical", label: health };
  }
  if (sessionState === "connected") {
    return { indicatorClass: "is-healthy", label: "connected" };
  }
  if (sessionState === "disconnected") {
    return { indicatorClass: "is-warning", label: "disconnected" };
  }
  if (onboardingState === "pending" || registrationState === "provisioned") {
    return { indicatorClass: "is-booting", label: "onboarding" };
  }
  return {
    indicatorClass: hostObservationStatusClass(health),
    label: health || sessionState || registrationState || "unknown",
  };
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
    ["dashboard", "planes", "models", "knowledge-vault", "protocols", "skills-factory", "access"].includes(initialPage)
      ? initialPage
      : "dashboard",
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
  const [hostdHosts, setHostdHosts] = useState([]);
  const [protocolRegistry, setProtocolRegistry] = useState({ items: [], summary: {} });
  const [selectedHostNodeName, setSelectedHostNodeName] = useState("");
  const [diskState, setDiskState] = useState(null);
  const [rolloutState, setRolloutState] = useState(null);
  const [rebalancePlan, setRebalancePlan] = useState(null);
  const [events, setEvents] = useState([]);
  const [interactionStatus, setInteractionStatus] = useState(null);
  const [knowledgeVaultStatus, setKnowledgeVaultStatus] = useState(null);
  const [knowledgeVaultGraph, setKnowledgeVaultGraph] = useState({
    nodes: [],
    edges: [],
    warnings: [],
  });
  const [knowledgeVaultGraphBusy, setKnowledgeVaultGraphBusy] = useState(false);
  const [knowledgeVaultGraphError, setKnowledgeVaultGraphError] = useState("");
  const [planeKnowledge, setPlaneKnowledge] = useState({
    results: [],
    graph: { nodes: [], edges: [], warnings: [] },
    busy: false,
    error: "",
    resolverPrompt: "",
    resolverResult: null,
    resolverBusy: false,
    resolverError: "",
  });
  const [modelLibrary, setModelLibrary] = useState({ items: [], roots: [], jobs: [], nodes: [] });
  const [skillsFactory, setSkillsFactory] = useState({
    items: [],
    groups: [],
    busy: false,
    error: "",
    mode: "create",
    form: buildEmptySkillForm(),
    search: "",
    sort: "name",
    selectedGroupPath: "",
    expandedGroupPaths: [""],
  });
  const [selectedTab, setSelectedTab] = useState("status");
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLanguage, setChatLanguage] = useState("en");
  const [chatBusy, setChatBusy] = useState(false);
  const [chatError, setChatError] = useState("");
  const [metricHistory, setMetricHistory] = useState({});
  const [telemetryChart, setTelemetryChart] = useState({
    open: false,
    title: "",
    meta: "",
    historyKey: "",
    data: [],
    valueFormatter: null,
  });
  const [interactionPaneWidth, setInteractionPaneWidth] = useState(34);
  const [draggingDivider, setDraggingDivider] = useState(false);
  const [loading, setLoading] = useState(true);
  const [actionBusy, setActionBusy] = useState("");
  const [modelLibraryBusy, setModelLibraryBusy] = useState("");
  const [modelsTab, setModelsTab] = useState("library");
  const [visibleModelCount, setVisibleModelCount] = useState(MODEL_LIBRARY_PAGE_SIZE);
  const [modelJobPage, setModelJobPage] = useState(0);
  const [modelDownloadForm, setModelDownloadForm] = useState({
    modelId: "",
    targetRoot: "",
    targetNodeName: "",
    targetSubdir: "",
    sourceUrls: "",
    format: "unknown",
    formatLocked: false,
  });
  const [quantizationSearch, setQuantizationSearch] = useState("");
  const [quantizationFilter, setQuantizationFilter] = useState("all");
  const [quantizationDialog, setQuantizationDialog] = useState({
    open: false,
    item: null,
    quantization: MODEL_LIBRARY_GGUF_QUANTIZATIONS[0],
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
    planeState: "",
    text: "",
    form: null,
    originalSkillsEnabled: false,
    busy: false,
    error: "",
  });
  const [skillsDialog, setSkillsDialog] = useState({
    open: false,
    planeName: "",
    items: [],
    mode: "create",
    form: buildEmptySkillForm(),
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
    setHostdHosts([]);
    setDiskState(null);
    setRolloutState(null);
    setRebalancePlan(null);
    setEvents([]);
    setInteractionStatus(null);
    setKnowledgeVaultStatus(null);
    setKnowledgeVaultGraph({ nodes: [], edges: [], warnings: [] });
    setKnowledgeVaultGraphBusy(false);
    setKnowledgeVaultGraphError("");
    setPlaneKnowledge({
      results: [],
      graph: { nodes: [], edges: [], warnings: [] },
      busy: false,
      error: "",
      resolverPrompt: "",
      resolverResult: null,
      resolverBusy: false,
      resolverError: "",
    });
    setModelLibrary({ items: [], roots: [], jobs: [], nodes: [] });
    setModelsTab("library");
    setSkillsFactory({
      items: [],
      busy: false,
      error: "",
      mode: "create",
      form: buildEmptySkillForm(),
      search: "",
      sort: "name",
      selectedGroupPath: "",
      expandedGroupPaths: [""],
    });
    setChatMessages([]);
    setChatError("");
    setApiHealthy(false);
    setStreamHealthy(false);
    setQuantizationSearch("");
    setQuantizationFilter("all");
    setQuantizationDialog({
      open: false,
      item: null,
      quantization: MODEL_LIBRARY_GGUF_QUANTIZATIONS[0],
    });
    setSkillsDialog({
      open: false,
      planeName: "",
      items: [],
      mode: "create",
      form: buildEmptySkillForm(),
      busy: false,
      error: "",
    });
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
      startTransition(() => {
        setModelLibrary({
          items: Array.isArray(payload.items) ? payload.items : [],
          roots: Array.isArray(payload.roots) ? payload.roots : [],
          jobs: nextJobs,
          nodes: Array.isArray(payload.nodes) ? payload.nodes : [],
        });
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

  async function refreshModelLibraryJobs() {
    try {
      const payload = await fetchJson(modelLibraryJobsPath());
      const nextJobs = Array.isArray(payload.jobs) ? payload.jobs : [];
      startTransition(() => {
        setModelLibrary((current) => ({
          ...current,
          jobs: nextJobs,
        }));
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

  async function refreshKnowledgeVaultGraph() {
    setKnowledgeVaultGraphBusy(true);
    setKnowledgeVaultGraphError("");
    try {
      const searchPayload = await fetchJson(knowledgeVaultPath("search"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: "",
          list_all: true,
          limit: KNOWLEDGE_GRAPH_LIMIT,
        }),
      });
      const results = normalizeKnowledgeResults(searchPayload?.results);
      const graphRequest = buildKnowledgeGraphRequest(results, [], KNOWLEDGE_GRAPH_LIMIT);
      if (graphRequest.knowledge_ids.length === 0) {
        const emptyGraph = { nodes: [], edges: [], warnings: [] };
        setKnowledgeVaultGraph((current) =>
          areKnowledgeGraphsEqual(current, emptyGraph) ? current : emptyGraph,
        );
        return;
      }
      const graphPayload = await fetchJson(knowledgeVaultPath("graph-neighborhood"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(graphRequest),
      });
      const nextGraph = {
        nodes: Array.isArray(graphPayload?.nodes) ? graphPayload.nodes : [],
        edges: Array.isArray(graphPayload?.edges) ? graphPayload.edges : [],
        warnings: Array.isArray(graphPayload?.warnings) ? graphPayload.warnings : [],
      };
      setKnowledgeVaultGraph((current) =>
        areKnowledgeGraphsEqual(current, nextGraph) ? current : nextGraph,
      );
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setKnowledgeVaultGraphError(error.message || String(error));
      const emptyGraph = { nodes: [], edges: [], warnings: [] };
      setKnowledgeVaultGraph((current) =>
        areKnowledgeGraphsEqual(current, emptyGraph) ? current : emptyGraph,
      );
    } finally {
      setKnowledgeVaultGraphBusy(false);
    }
  }

  async function refreshPlaneKnowledge() {
    if (!selectedPlane) {
      return;
    }
    setPlaneKnowledge((current) => ({ ...current, busy: true, error: "" }));
    try {
      const searchPayload = await fetchJson(planeKnowledgeVaultPath(selectedPlane, "search"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: "",
          list_all: true,
          limit: KNOWLEDGE_GRAPH_LIMIT,
        }),
      });
      const results = normalizeKnowledgeResults(searchPayload?.results);
      const selectedKnowledgeIds = selectedKnowledgeIdsFromDesiredState(desiredStateV2 || desiredState);
      const graphRequest = buildPlaneKnowledgeGraphRequest(
        results,
        selectedKnowledgeIds,
        KNOWLEDGE_GRAPH_LIMIT,
      );
      let graph = { nodes: [], edges: [], warnings: [] };
      if (graphRequest.knowledge_ids.length > 0) {
        const graphPayload = await fetchJson(
          planeKnowledgeVaultPath(selectedPlane, "graph-neighborhood"),
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(graphRequest),
          },
        );
        graph = {
          nodes: Array.isArray(graphPayload?.nodes) ? graphPayload.nodes : [],
          edges: Array.isArray(graphPayload?.edges) ? graphPayload.edges : [],
          warnings: Array.isArray(graphPayload?.warnings) ? graphPayload.warnings : [],
        };
      }
      setPlaneKnowledge((current) => ({
        ...current,
        results,
        graph,
        busy: false,
        error: "",
      }));
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setPlaneKnowledge((current) => ({
        ...current,
        results: [],
        graph: { nodes: [], edges: [], warnings: [] },
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function resolvePlaneEnrichment() {
    if (!selectedPlane || !planeKnowledge.resolverPrompt.trim()) {
      return;
    }
    setPlaneKnowledge((current) => ({
      ...current,
      resolverBusy: true,
      resolverError: "",
      resolverResult: null,
    }));
    try {
      const payload = await fetchJson(skillsPath(selectedPlane, "resolve-context"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messages: [
            {
              role: "user",
              content: planeKnowledge.resolverPrompt.trim(),
            },
          ],
        }),
      });
      setPlaneKnowledge((current) => ({
        ...current,
        resolverBusy: false,
        resolverResult: payload,
        resolverError: "",
      }));
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setPlaneKnowledge((current) => ({
        ...current,
        resolverBusy: false,
        resolverError: error.message || String(error),
      }));
    }
  }

  async function refreshSkillsFactory() {
    try {
      const payload = await fetchJson(skillsFactoryPath());
      const nextItems = Array.isArray(payload.skills) ? payload.skills : [];
      const nextGroups = Array.isArray(payload.groups) ? payload.groups : [];
      const nextTree = buildSkillsFactoryGroupTree(nextItems, nextGroups);
      const nextPaths = new Set(collectSkillsFactoryTreePaths(nextTree));
      setSkillsFactory((current) => ({
        ...current,
        items: nextItems,
        groups: nextGroups,
        selectedGroupPath: nextPaths.has(current.selectedGroupPath)
          ? current.selectedGroupPath
          : "",
        expandedGroupPaths: Array.isArray(current.expandedGroupPaths)
          ? current.expandedGroupPaths.filter((path) => nextPaths.has(path))
          : [""],
        error: "",
      }));
    } catch (error) {
      if (error?.status === 401) {
        handleUnauthorized();
        return;
      }
      setSkillsFactory((current) => ({
        ...current,
        error: error.message || String(error),
      }));
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
      const hostdHostsRequest = fetchJson(hostdHostsPath());
      const knowledgeVaultStatusRequest = fetchJson(knowledgeVaultPath());
      const protocolRegistryRequest = fetchJson(protocolsPath()).catch(() => ({
        items: [],
        summary: {},
      }));
      if (!planeName) {
        const [
          dashboardPayload,
          globalHostObservationsPayload,
          hostdHostsPayload,
          knowledgeVaultStatusPayload,
          protocolRegistryPayload,
        ] = await Promise.all([
          fetchJson(queryPath("/api/v1/dashboard", { stale_after: 30 })),
          globalHostObservationsRequest,
          hostdHostsRequest,
          knowledgeVaultStatusRequest,
          protocolRegistryRequest,
        ]);
        const globalObservationItems = hostObservationItemsFromPayload(
          globalHostObservationsPayload,
        );
        const serverSummary = summarizeGlobalObservations(globalObservationItems);
        setPlaneDetail(null);
        setDashboard(dashboardPayload);
        setHostObservations(null);
        setGlobalHostObservations(globalHostObservationsPayload);
        setHostdHosts(Array.isArray(hostdHostsPayload.items) ? hostdHostsPayload.items : []);
        setKnowledgeVaultStatus(knowledgeVaultStatusPayload);
        setProtocolRegistry(protocolRegistryPayload);
        setMetricHistory((current) =>
          appendMetricHistory(
            current,
            buildServerMetricSamples(
              serverSummary,
              globalObservationItems.length,
            ),
            Date.now(),
          ),
        );
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
        hostdHostsPayload,
        knowledgeVaultStatusPayload,
        protocolRegistryPayload,
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
        hostdHostsRequest,
        knowledgeVaultStatusRequest,
        protocolRegistryRequest,
      ]);

      setPlaneDetail(planePayload);
      setDashboard(dashboardPayload);
      setHostObservations(hostObservationsPayload);
      setGlobalHostObservations(globalHostObservationsPayload);
      setHostdHosts(Array.isArray(hostdHostsPayload.items) ? hostdHostsPayload.items : []);
      setProtocolRegistry(protocolRegistryPayload);
      setDiskState(diskPayload);
      setRolloutState(rolloutPayload);
      setRebalancePlan(rebalancePayload);
      setEvents(Array.isArray(eventsPayload.events) ? eventsPayload.events : []);
      setInteractionStatus(interactionPayload);
      setKnowledgeVaultStatus(knowledgeVaultStatusPayload);
      const globalObservationItems = hostObservationItemsFromPayload(
        globalHostObservationsPayload,
      );
      const serverSummary = summarizeGlobalObservations(globalObservationItems);
      const dashboardRuntime = dashboardPayload?.runtime || {};
      const observationItems = hostObservationItemsFromPayload(hostObservationsPayload);
      const observedInstancesCount = observationItems.reduce((count, observation) => {
        const instances = observedInstancesForObservation(observation);
        return count + instances.length;
      }, 0);
      const readyNodeCount =
        Number(dashboardRuntime.ready_nodes ?? 0) > 0 || observationItems.length === 0
          ? Number(dashboardRuntime.ready_nodes ?? 0)
          : observationItems.filter((item) => {
              const status = String(item?.status || "").toLowerCase();
              return status !== "stale" && status !== "failed" && status !== "error";
            }).length;
      const missingRuntimeCount = observationItems.filter(
        (observation) =>
          observation?.runtime_status?.available !== true &&
          observation?.instance_runtimes?.available !== true,
      ).length;
      setMetricHistory((current) =>
        appendMetricHistory(
          current,
          {
            ...buildServerMetricSamples(
              serverSummary,
              globalObservationItems.length,
            ),
            ...buildPlaneMetricSamples({
              planeName,
              readyNodes: readyNodeCount,
              observedInstanceCount:
                observedInstancesCount > 0
                  ? observedInstancesCount
                  : Number(dashboardPayload?.plane?.instance_count ?? 0),
              rolloutActions: Number(dashboardPayload?.rollout?.total_actions ?? 0),
              alertCount: Number((dashboardPayload?.alerts || {}).total ?? 0),
              missingRuntimeNodes: missingRuntimeCount,
            }),
          },
          Date.now(),
        ),
      );
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

  function openTelemetryChart(title, history, meta, valueFormatter = null, historyKey = "") {
    setTelemetryChart({
      open: true,
      title,
      meta,
      historyKey,
      data: Array.isArray(history) ? history : [],
      valueFormatter,
    });
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
        await Promise.all([
          refreshAccessData(),
          refreshAll(selectedPlane),
          refreshModelLibrary(),
          refreshSkillsFactory(),
        ]);
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
    const sourceUrls = normalizeModelDownloadSourceUrls(modelDownloadForm.sourceUrls);
    if (
      (!modelDownloadForm.targetRoot.trim() && !modelDownloadForm.targetNodeName.trim()) ||
      sourceUrls.length === 0
    ) {
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
          target_root: modelDownloadForm.targetRoot.trim() || undefined,
          target_node_name: modelDownloadForm.targetNodeName.trim() || undefined,
          target_subdir: modelDownloadForm.targetSubdir.trim() || undefined,
          source_urls: sourceUrls,
          format: modelDownloadForm.format,
        }),
      });
      setModelDownloadForm((current) => ({
        ...current,
        sourceUrls: "",
        format: "unknown",
        formatLocked: false,
      }));
      await refreshModelLibrary();
      await refreshAll(selectedPlane);
    } finally {
      setModelLibraryBusy("");
    }
  }

  async function setHostStorageRole(host, enabled) {
    const nodeName = host?.node_name;
    if (!nodeName) {
      return;
    }
    setActionBusy(`storage-role:${nodeName}`);
    try {
      await fetchJson(`/api/v1/hostd/hosts/${encodeURIComponent(nodeName)}/storage-role`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          enabled,
          message: enabled
            ? "storage role enabled from web ui"
            : "storage role disabled from web ui",
        }),
      });
      await Promise.all([refreshAll(selectedPlane), refreshModelLibrary()]);
    } finally {
      setActionBusy("");
    }
  }

  async function setHostKnowledgeVault(host, enabled) {
    const nodeName = host?.node_name;
    if (!nodeName) {
      return;
    }
    const confirmation = enabled
      ? `Start Knowledge Vault on ${nodeName}?`
      : `Stop Knowledge Vault on ${nodeName}?`;
    if (!window.confirm(confirmation)) {
      return;
    }
    setActionBusy(`knowledge-vault:${nodeName}`);
    try {
      await fetchJson(knowledgeVaultPath(enabled ? "apply" : "stop"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          service_id: "kv_default",
          node_name: nodeName,
        }),
      });
      const [statusPayload] = await Promise.all([
        fetchJson(knowledgeVaultPath()),
        refreshAll(selectedPlane),
      ]);
      setKnowledgeVaultStatus(statusPayload);
    } finally {
      setActionBusy("");
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

  function openQuantizationDialog(item) {
    setQuantizationDialog({
      open: true,
      item,
      quantization: MODEL_LIBRARY_GGUF_QUANTIZATIONS[0],
    });
  }

  function closeQuantizationDialog() {
    setQuantizationDialog({
      open: false,
      item: null,
      quantization: MODEL_LIBRARY_GGUF_QUANTIZATIONS[0],
    });
  }

  async function enqueueModelQuantization() {
    if (!quantizationDialog.item?.path) {
      return;
    }
    setModelLibraryBusy(`quantize:${quantizationDialog.item.path}`);
    try {
      await fetchJson(modelLibraryPath("quantize"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          source_path: quantizationDialog.item.path,
          quantization: quantizationDialog.quantization,
          replace_existing: true,
        }),
      });
      closeQuantizationDialog();
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
      if (action === "delete") {
        await fetchJson(planePath(planeName), { method: "DELETE" });
      } else if (action === "restart") {
        await fetchJson(planePath(planeName, "stop"), { method: "POST" });
        await fetchJson(planePath(planeName, "start"), { method: "POST" });
      } else {
        await fetchJson(planePath(planeName, action), { method: "POST" });
      }
      setPendingOperation({
        kind: action === "restart" ? "start" : action,
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

  async function resetLtCypherDeployment() {
    const planeName = "lt-cypher-ai";
    const confirmed = window.confirm(
      "Reset failed lt-cypher-ai deployment? This deletes only the plane/runtime state and waits for controller cleanup. Model-library artifacts and prepared caches are preserved.",
    );
    if (!confirmed) {
      return;
    }
    setActionBusy("reset-lt-cypher-ai");
    setApiError("");
    try {
      startTransition(() => {
        setSelectedPlane(planeName);
        setSelectedPage("dashboard");
      });
      for (let attempt = 0; attempt < 8; attempt += 1) {
        try {
          await fetchJson(planePath(planeName), { method: "DELETE" });
        } catch (error) {
          if (error?.status === 404) {
            break;
          }
          throw error;
        }
        await delay(1500);
        const payload = await fetchJson("/api/v1/planes");
        const items = Array.isArray(payload.items) ? payload.items : [];
        if (!items.some((item) => item.name === planeName)) {
          break;
        }
      }
      setPendingOperation({
        kind: "delete",
        planeName,
        startedAt: new Date().toISOString(),
      });
      await refreshAll("");
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
          planeState: "",
          text: JSON.stringify(buildDesiredStateV2FromForm(form), null, 2),
          form,
          originalSkillsEnabled: false,
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
        planeState: payload?.state || "",
        text: JSON.stringify(payload.desired_state_v2 || payload.desired_state || {}, null, 2),
        form: payload.desired_state_v2
          ? buildPlaneFormStateFromDesiredStateV2(payload.desired_state_v2)
          : null,
        originalSkillsEnabled: Boolean(payload?.desired_state_v2?.skills?.enabled),
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
      if (
        planeDialog.mode === "edit" &&
        planeDialog.originalSkillsEnabled &&
        !Boolean(desiredState?.skills?.enabled)
      ) {
        const confirmed = window.confirm(
          "Disable Skills for this plane? The dedicated skills service and its stored data will be removed on rollout.",
        );
        if (!confirmed) {
          setPlaneDialog((current) => ({ ...current, busy: false }));
          return;
        }
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
      await refreshSkillsFactory();
      const skillsEnabledNow = Boolean(desiredState?.skills?.enabled);
      const skillsJustEnabled =
        planeDialog.mode === "edit" &&
        !planeDialog.originalSkillsEnabled &&
        skillsEnabledNow;
      const shouldAutoRestartForSkills =
        planeDialog.mode === "edit" &&
        planeDialog.planeState === "running" &&
        skillsJustEnabled;
      const shouldOfferRestart =
        planeDialog.mode === "edit" &&
        planeDialog.planeState === "running" &&
        !shouldAutoRestartForSkills;
      let restartAccepted = shouldAutoRestartForSkills;
      if (shouldOfferRestart) {
        restartAccepted = window.confirm(
          "This plane is currently running. The updated desired state is staged in the controller and requires a restart to take effect.\n\nRestart the plane now?",
        );
      }
      setPlaneDialog({
        open: false,
        mode: "new",
        planeName: "",
        planeState: "",
        text: "",
        form: null,
        originalSkillsEnabled: false,
        busy: false,
        error: "",
      });
      startTransition(() => {
        setSelectedPlane(desiredState.plane_name);
      });
      if (restartAccepted) {
        await executePlaneAction("restart", desiredState.plane_name);
        return;
      }
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
        browsing: null,
        enrichment: null,
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
        if (event === "session_started") {
          setChatMessages((current) =>
            current.map((item) =>
              item.id === assistantId
                ? {
                    ...item,
                    browsing: payload.browsing || item.browsing || null,
                    session: {
                      ...(item.session || {}),
                      status: "in_progress",
                    },
                  }
                : item,
            ),
          );
          return;
        }
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
                    browsing: payload.browsing || session.browsing || item.browsing || null,
                    enrichment: buildChatEnrichmentMeta(payload, session, item.enrichment),
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
                    browsing: payload.browsing || payload.session?.browsing || item.browsing || null,
                    enrichment: buildChatEnrichmentMeta(payload, payload.session || {}, item.enrichment),
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
          refreshSkillsFactory().catch(() => {}),
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
    const refreshDelay = selectedPage === "dashboard" ? FLEET_REFRESH_MS : AUTO_REFRESH_MS;
    const timer = setInterval(() => {
      refreshAll(selectedPlane);
    }, refreshDelay);
    return () => clearInterval(timer);
  }, [authState.authenticated, selectedPlane, selectedPage]);

  useEffect(() => {
    if (!authState.authenticated || selectedPage !== "knowledge-vault") {
      return undefined;
    }
    refreshKnowledgeVaultGraph();
    const timer = setInterval(() => {
      refreshKnowledgeVaultGraph();
    }, AUTO_REFRESH_MS);
    return () => clearInterval(timer);
  }, [authState.authenticated, selectedPage]);

  useEffect(() => {
    if (!authState.authenticated || !selectedPlane || selectedTab !== "knowledge") {
      return;
    }
    refreshPlaneKnowledge();
  }, [authState.authenticated, selectedPlane, selectedTab, planeDetail]);

  const hasActiveModelJobsForPolling = Array.isArray(modelLibrary.jobs)
    ? modelLibrary.jobs.some((job) => {
        const status = String(job?.status || "").toLowerCase();
        return status === "queued" || status === "running" || status === "stopping";
      })
    : false;

  useEffect(() => {
    if (!authState.authenticated) {
      return undefined;
    }
    if (!hasActiveModelJobsForPolling) {
      return undefined;
    }
    const timer = setInterval(() => {
      if (selectedPage === "models") {
        refreshModelLibraryJobs().catch(() => {});
        return;
      }
      refreshModelLibrary().catch(() => {});
    }, selectedPage === "models" ? MODEL_LIBRARY_ACTIVE_POLL_MS : MODEL_LIBRARY_BACKGROUND_POLL_MS);
    return () => clearInterval(timer);
  }, [authState.authenticated, hasActiveModelJobsForPolling, selectedPage]);

  useEffect(() => {
    if (!authState.authenticated) {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      setStreamHealthy(false);
      return;
    }

    const source = new EventSource(
      queryPath("/api/v1/events/stream", {
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
    const handleRefreshEvent = (event) => {
      setLastEventName(event.type || "message");
      scheduleRefresh(selectedPlane);
    };
    source.onmessage = handleRefreshEvent;
    for (const eventName of REFRESH_EVENT_NAMES) {
      source.addEventListener(eventName, handleRefreshEvent);
    }

    return () => {
      for (const eventName of REFRESH_EVENT_NAMES) {
        source.removeEventListener(eventName, handleRefreshEvent);
      }
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
    setPlaneKnowledge((current) => ({
      ...current,
      results: [],
      graph: { nodes: [], edges: [], warnings: [] },
      error: "",
      resolverResult: null,
      resolverError: "",
    }));
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
  const desiredStateV2 = planeDetail?.desired_state_v2 || null;
  const planeRecord =
    planeDetail?.planes?.find((item) => item.name === selectedPlane) || null;
  const planeMode =
    dashboard?.plane?.plane_mode ||
    desiredState?.plane_mode ||
    planeRecord?.plane_mode ||
    "compute";
  const llmPlane = planeMode === "llm";
  const skillsEnabled =
    Boolean(desiredStateV2?.skills?.enabled) || Boolean(desiredState?.skills?.enabled);
  const browsingEnabled =
    Boolean(desiredStateV2?.browsing?.enabled) || Boolean(desiredState?.browsing?.enabled);
  const knowledgeEnabled =
    Boolean(desiredStateV2?.knowledge?.enabled) || Boolean(desiredState?.knowledge?.enabled);
  const selectedKnowledgeIds = selectedKnowledgeIdsFromDesiredState(desiredStateV2 || desiredState);
  const knowledgeContextPolicy = knowledgeContextPolicyFromDesiredState(desiredStateV2 || desiredState);
  const dashboardSkillsSummary = dashboard?.skills || {
    enabled: skillsEnabled,
    enabled_count: 0,
    total_count: 0,
  };
  const dashboardBrowsingSummary = dashboard?.browsing || {
    browsing_enabled: browsingEnabled,
    browsing_ready: false,
    browser_session_enabled: false,
    reason: browsingEnabled ? "pending" : "browsing_disabled",
  };
  const formattedSkillsSummary =
    formatPlaneDashboardSkillsSummary(dashboardSkillsSummary);
  const browsingSummaryValue = dashboardBrowsingSummary?.browsing_ready
    ? "ready"
    : browsingEnabled
      ? "enabled"
      : "disabled";
  const browsingSummaryMeta = browsingEnabled
    ? `${dashboardBrowsingSummary?.browser_session_enabled ? "browser on" : "browser off"} / ${
        dashboardBrowsingSummary?.reason || "pending"
      }`
    : "disabled";
  const peerLinkSummary = dashboard?.peer_links?.summary || {
    total: 0,
    direct: 0,
    partial: 0,
    stale: 0,
  };
  const peerLinkItems = Array.isArray(dashboard?.peer_links?.items)
    ? dashboard.peer_links.items
    : [];
  const chatLanguageOptions = supportedChatLanguageOptions(desiredState, interactionStatus);
  const interactionReady = interactionStatus?.ready === true;
  const nodeItems = dashboard?.nodes || [];
  const observationItems = hostObservationItemsFromPayload(hostObservations);
  const globalObservationItems = hostObservationItemsFromPayload(globalHostObservations);
  const dashboardHostItems = buildFleetHostItems(globalObservationItems);
  const selectedHostOverview =
    dashboardHostItems.find((host) => host?.node_name === selectedHostNodeName) || null;
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
  const globalObservationSummary = summarizeGlobalObservations(globalObservationItems);
  const serverHealthyHostsHistory = metricHistory["server.healthy_hosts"] || [];
  const serverCpuLoadHistory = metricHistory["server.cpu_load_1m"] || [];
  const serverGpuVramHistory = metricHistory["server.used_gpu_vram_mb"] || [];
  const serverMemoryHistory = metricHistory["server.used_memory_bytes"] || [];
  const serverCpuTempHistory = metricHistory["server.max_cpu_temp_c"] || [];
  const serverGpuTempHistory = metricHistory["server.max_gpu_temp_c"] || [];
  const serverNetworkTxHistory = metricHistory["server.network_tx_bytes"] || [];
  const serverDiskWriteHistory = metricHistory["server.disk_write_bytes"] || [];
  const planeMetricPrefix = selectedPlane ? `plane.${selectedPlane}` : "";
  const planeReadyNodesHistory = planeMetricPrefix
    ? metricHistory[`${planeMetricPrefix}.ready_nodes`] || []
    : [];
  const planeObservedInstancesHistory = planeMetricPrefix
    ? metricHistory[`${planeMetricPrefix}.observed_instances`] || []
    : [];
  const planeRolloutHistory = planeMetricPrefix
    ? metricHistory[`${planeMetricPrefix}.rollout_actions`] || []
    : [];
  const planeAlertHistory = planeMetricPrefix
    ? metricHistory[`${planeMetricPrefix}.alerts`] || []
    : [];
  const resolvedTelemetryChartData = telemetryChart.historyKey
    ? metricHistory[telemetryChart.historyKey] || []
    : telemetryChart.data;
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
  const dashboardSelfServiceSummary = dashboard?.self_services?.summary || {
    total: 0,
    healthy: 0,
    warning: 0,
    critical: 0,
    unknown: 0,
  };
  const dashboardSelfServices = Array.isArray(dashboard?.self_services?.items)
    ? dashboard.self_services.items
    : [];
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
  const modelLibraryNodes = Array.isArray(modelLibrary.nodes) ? modelLibrary.nodes : [];
  const storageModelNodes = modelLibraryNodes.filter(
    (node) =>
      node?.role_eligible === true &&
      node?.registration_state === "registered" &&
      node?.session_state === "connected" &&
      node?.storage_root,
  );
  const modelLibraryJobs = Array.isArray(modelLibrary.jobs) ? modelLibrary.jobs : [];
  const downloadModelJobs = modelLibraryJobs.filter(
    (job) => normalizeModelLibraryJobKind(job?.job_kind) === "download",
  );
  const quantizationModelJobs = modelLibraryJobs.filter(
    (job) => normalizeModelLibraryJobKind(job?.job_kind) === "quantization",
  );
  const modelJobPageCount = Math.max(
    1,
    Math.ceil(downloadModelJobs.length / MODEL_LIBRARY_JOB_PAGE_SIZE),
  );
  const currentModelJobPage = Math.min(modelJobPage, modelJobPageCount - 1);
  const visibleModelJobs = downloadModelJobs.slice(
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
  const quantizationItems = (modelLibrary.items || [])
    .filter((item) => item?.format === "gguf" && item?.kind === "file")
    .filter((item) => {
      if (!quantizationSearch.trim()) {
        return true;
      }
      const haystack = [
        item?.name,
        item?.path,
        item?.quantization,
        formatModelLibraryDisplayName(item),
      ]
        .join("\n")
        .toLowerCase();
      return haystack.includes(quantizationSearch.trim().toLowerCase());
    })
    .filter((item) => {
      const normalized = normalizeModelLibraryItemQuantization(item?.quantization);
      return quantizationFilter === "all" ? true : normalized === quantizationFilter;
    });
  const activeQuantizationJobs = quantizationModelJobs.filter((job) => {
    const status = String(job?.status || "").toLowerCase();
    return status === "queued" || status === "running" || status === "stopping";
  });
  const planesNavLabel = `${runningPlaneCount} running`;
  const planesNavMeta = selectedPlane || `${planes.length} registered`;
  const modelsNavMeta = activeModelJobs > 0
    ? `${activeModelJobs} download job${activeModelJobs === 1 ? "" : "s"}`
    : `${activeModelCount} discovered model${activeModelCount === 1 ? "" : "s"}`;
  const knowledgeGraphSummary = summarizeKnowledgeGraph(knowledgeVaultGraph);
  const knowledgeVaultStatusText = String(knowledgeVaultStatus?.status || "inactive");
  const knowledgeVaultStatusLower = knowledgeVaultStatusText.toLowerCase();
  const knowledgeVaultNavClass = knowledgeVaultGraphError
    ? "is-critical"
    : knowledgeVaultGraphBusy
      ? "is-warning"
      : ["active", "ready", "running", "healthy"].includes(knowledgeVaultStatusLower)
        ? "is-healthy"
        : "is-booting";
  const knowledgeVaultNavMeta = knowledgeVaultStatus?.node_name
    ? `node ${knowledgeVaultStatus.node_name}`
    : `${knowledgeGraphSummary.nodeCount} record${knowledgeGraphSummary.nodeCount === 1 ? "" : "s"}`;
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

  async function refreshSkillsDialog(planeName = skillsDialog.planeName || selectedPlane) {
    if (!planeName) {
      return;
    }
    setSkillsDialog((current) => ({ ...current, busy: true, error: "" }));
    try {
      const payload = await fetchJson(skillsPath(planeName));
      setSkillsDialog((current) => ({
        ...current,
        open: true,
        planeName,
        items: Array.isArray(payload.skills) ? payload.skills : [],
        busy: false,
        error: "",
      }));
    } catch (error) {
      setSkillsDialog((current) => ({
        ...current,
        open: true,
        planeName,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function openSkillsDialog(planeName = selectedPlane) {
    setSkillsDialog({
      open: true,
      planeName,
      items: [],
      mode: "create",
      form: buildEmptySkillForm(),
      busy: true,
      error: "",
    });
    await refreshSkillsDialog(planeName);
  }

  function closeSkillsDialog() {
    setSkillsDialog({
      open: false,
      planeName: "",
      items: [],
      mode: "create",
      form: buildEmptySkillForm(),
      busy: false,
      error: "",
    });
  }

  function startCreateSkill() {
    setSkillsDialog((current) => ({
      ...current,
      mode: "create",
      form: buildEmptySkillForm(),
      error: "",
    }));
  }

  function startEditSkill(skill) {
    setSkillsDialog((current) => ({
      ...current,
      mode: "edit",
      form: {
        id: skill.id || "",
        name: skill.name || "",
        description: skill.description || "",
        content: skill.content || "",
        enabled: skill.enabled !== false,
        sessionIdsText: renderLineSeparatedValues(skill.session_ids),
        naimLinksText: renderLineSeparatedValues(skill.naim_links),
      },
      error: "",
    }));
  }

  function updateSkillFormField(key, value) {
    setSkillsDialog((current) => ({
      ...current,
      form: {
        ...(current.form || buildEmptySkillForm()),
        [key]: value,
      },
    }));
  }

  async function saveSkillForm() {
    const planeName = skillsDialog.planeName || selectedPlane;
    if (!planeName) {
      return;
    }
    const payload = {
      name: String(skillsDialog.form?.name || "").trim(),
      description: String(skillsDialog.form?.description || "").trim(),
      content: String(skillsDialog.form?.content || "").trim(),
      enabled: Boolean(skillsDialog.form?.enabled),
      session_ids: parseLineSeparatedValues(skillsDialog.form?.sessionIdsText),
      naim_links: parseLineSeparatedValues(skillsDialog.form?.naimLinksText),
    };
    if (!payload.name || !payload.description || !payload.content) {
      setSkillsDialog((current) => ({
        ...current,
        error: "Name, description, and content are required.",
      }));
      return;
    }
    setSkillsDialog((current) => ({ ...current, busy: true, error: "" }));
    try {
      const editing = skillsDialog.mode === "edit" && skillsDialog.form?.id;
      await fetchJson(
        editing ? skillsPath(planeName, encodeURIComponent(skillsDialog.form.id)) : skillsPath(planeName),
        {
          method: editing ? "PUT" : "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      );
      await refreshSkillsDialog(planeName);
      await refreshSkillsFactory();
      setSkillsDialog((current) => ({
        ...current,
        mode: "create",
        form: buildEmptySkillForm(),
      }));
    } catch (error) {
      setSkillsDialog((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function toggleSkill(skill) {
    const planeName = skillsDialog.planeName || selectedPlane;
    if (!planeName || !skill?.id) {
      return;
    }
    setSkillsDialog((current) => ({ ...current, busy: true, error: "" }));
    try {
      await fetchJson(skillsPath(planeName, encodeURIComponent(skill.id)), {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ enabled: !skill.enabled }),
      });
      await refreshSkillsDialog(planeName);
      await refreshSkillsFactory();
    } catch (error) {
      setSkillsDialog((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function deleteSkill(skill) {
    const planeName = skillsDialog.planeName || selectedPlane;
    if (!planeName || !skill?.id) {
      return;
    }
    const confirmed = window.confirm(`Delete skill ${skill.name || skill.id}?`);
    if (!confirmed) {
      return;
    }
    setSkillsDialog((current) => ({ ...current, busy: true, error: "" }));
    try {
      await fetchJson(skillsPath(planeName, encodeURIComponent(skill.id)), {
        method: "DELETE",
      });
      await refreshSkillsDialog(planeName);
      await refreshSkillsFactory();
      if (skillsDialog.form?.id === skill.id) {
        setSkillsDialog((current) => ({
          ...current,
          mode: "create",
          form: buildEmptySkillForm(),
        }));
      }
    } catch (error) {
      setSkillsDialog((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  function startCreateFactorySkill() {
    setSkillsFactory((current) => ({
      ...current,
      mode: "create",
      form: {
        ...buildEmptySkillForm(),
        groupPath:
          current.selectedGroupPath && !isBuiltinSkillsFactoryGroupPath(current.selectedGroupPath)
            ? current.selectedGroupPath
            : "",
      },
      error: "",
    }));
  }

  function startEditFactorySkill(skill) {
    setSkillsFactory((current) => ({
      ...current,
      mode: "edit",
      form: {
        id: skill.id || "",
        name: skill.name || "",
        groupPath: skill.group_path || "",
        description: skill.description || "",
        content: skill.content || "",
        matchTermsText: renderLineSeparatedValues(skill.match_terms),
        internal: skill.internal === true,
        enabled: true,
        sessionIdsText: "",
        naimLinksText: "",
      },
      selectedGroupPath: skill.group_path || current.selectedGroupPath,
      error: "",
    }));
  }

  function updateFactorySkillFormField(key, value) {
    setSkillsFactory((current) => ({
      ...current,
      form: {
        ...(current.form || buildEmptySkillForm()),
        [key]: value,
      },
    }));
  }

  async function createFactoryGroup() {
    const seed =
      skillsFactory.selectedGroupPath && !isBuiltinSkillsFactoryGroupPath(skillsFactory.selectedGroupPath)
        ? `${skillsFactory.selectedGroupPath}/`
        : "";
    const path = window.prompt("New group path", seed);
    if (path === null) {
      return;
    }
    setSkillsFactory((current) => ({ ...current, busy: true, error: "" }));
    try {
      const created = await fetchJson(skillsFactoryPath("groups"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path }),
      });
      await refreshSkillsFactory();
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        selectedGroupPath: created.path || current.selectedGroupPath,
      }));
    } catch (error) {
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function renameFactoryGroup() {
    const fromPath = String(skillsFactory.selectedGroupPath || "");
    if (!fromPath || isBuiltinSkillsFactoryGroupPath(fromPath)) {
      return;
    }
    const toPath = window.prompt("Rename group to", fromPath);
    if (toPath === null) {
      return;
    }
    setSkillsFactory((current) => ({ ...current, busy: true, error: "" }));
    try {
      const renamed = await fetchJson(skillsFactoryPath("groups/rename"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ from_path: fromPath, to_path: toPath }),
      });
      await refreshSkillsFactory();
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        selectedGroupPath: renamed.to_path || "",
      }));
    } catch (error) {
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function deleteFactoryGroup() {
    const path = String(skillsFactory.selectedGroupPath || "");
    if (!path || isBuiltinSkillsFactoryGroupPath(path)) {
      return;
    }
    const confirmed = window.confirm(
      `Delete group ${formatSkillGroupPath(path)}? Skills in this group will move to No group.`,
    );
    if (!confirmed) {
      return;
    }
    setSkillsFactory((current) => ({ ...current, busy: true, error: "" }));
    try {
      await fetchJson(skillsFactoryPath("groups/delete"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path }),
      });
      await refreshSkillsFactory();
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        selectedGroupPath: "",
      }));
    } catch (error) {
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function saveFactorySkillForm() {
    const payload = {
      name: String(skillsFactory.form?.name || "").trim(),
      group_path: String(skillsFactory.form?.groupPath || "").trim(),
      description: String(skillsFactory.form?.description || "").trim(),
      content: String(skillsFactory.form?.content || "").trim(),
      match_terms: parseLineSeparatedValues(skillsFactory.form?.matchTermsText),
      internal: Boolean(skillsFactory.form?.internal),
    };
    if (!payload.name || !payload.description || !payload.content) {
      setSkillsFactory((current) => ({
        ...current,
        error: "Name, description, and content are required.",
      }));
      return;
    }
    setSkillsFactory((current) => ({ ...current, busy: true, error: "" }));
    try {
      const editing = skillsFactory.mode === "edit" && skillsFactory.form?.id;
      await fetchJson(
        editing
          ? skillsFactoryPath(encodeURIComponent(skillsFactory.form.id))
          : skillsFactoryPath(),
        {
          method: editing ? "PUT" : "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      );
      await refreshSkillsFactory();
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        mode: "create",
        form: buildEmptySkillForm(),
      }));
      if (selectedPlane) {
        await refreshAll(selectedPlane);
      }
    } catch (error) {
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function deleteFactorySkill(skill) {
    if (!skill?.id) {
      return;
    }
    const confirmed = window.confirm(
      `Delete factory skill ${skill.name || skill.id}? This detaches it from every plane.`,
    );
    if (!confirmed) {
      return;
    }
    setSkillsFactory((current) => ({ ...current, busy: true, error: "" }));
    try {
      await fetchJson(skillsFactoryPath(encodeURIComponent(skill.id)), {
        method: "DELETE",
      });
      await refreshSkillsFactory();
      if (skillsDialog.open && skillsDialog.planeName) {
        await refreshSkillsDialog(skillsDialog.planeName);
      }
      if (selectedPlane) {
        await refreshAll(selectedPlane);
      }
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        mode: current.form?.id === skill.id ? "create" : current.mode,
        form: current.form?.id === skill.id ? buildEmptySkillForm() : current.form,
      }));
    } catch (error) {
      setSkillsFactory((current) => ({
        ...current,
        busy: false,
        error: error.message || String(error),
      }));
    }
  }

  async function setSkillsFactoryWorker(item) {
    if (!item?.path) {
      return;
    }
    setModelLibraryBusy(`worker:${item.path}`);
    try {
      await fetchJson(modelLibraryPath("skills-factory-worker"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: item.path }),
      });
      await refreshModelLibrary();
    } finally {
      setModelLibraryBusy("");
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

  function buildFleetHostItems(hostItems) {
    const registeredByNode = new Map(
      (hostdHosts || [])
        .filter((host) => host?.node_name)
        .map((host) => [host.node_name, host]),
    );
    const mergedByNode = new Map();
    for (const host of hostItems || []) {
      if (host?.node_name) {
        mergedByNode.set(host.node_name, {
          ...host,
          registry: registeredByNode.get(host.node_name) || null,
        });
      }
    }
    for (const host of hostdHosts || []) {
      if (host?.node_name && !mergedByNode.has(host.node_name)) {
        mergedByNode.set(host.node_name, {
          node_name: host.node_name,
          status: host.session_state || host.registration_state || "registered",
          heartbeat_at: host.last_heartbeat_at || host.last_session_at,
          registry: host,
        });
      }
    }
    const items = [...mergedByNode.values()].sort((left, right) => {
      const leftReady = left?.runtime_status?.available === true ? 0 : 1;
      const rightReady = right?.runtime_status?.available === true ? 0 : 1;
      if (leftReady !== rightReady) {
        return leftReady - rightReady;
      }
      return String(left?.node_name || "").localeCompare(String(right?.node_name || ""));
    });
    return items;
  }

  function normalizedHostRole(host) {
    const registry = host?.registry || host || {};
    return String(registry?.derived_role || "").toLowerCase();
  }

  function isAutoStorageRole(host) {
    return normalizedHostRole(host) === "storage";
  }

  function isManualStorageRole(host) {
    const registry = host?.registry || host || {};
    return registry?.storage_role_enabled === true;
  }

  function isStorageRoleSelected(host) {
    return isAutoStorageRole(host) || isManualStorageRole(host);
  }

  function isStorageRoleEligible(host) {
    const registry = host?.registry || host || {};
    const capacity = registry?.capacity_summary || {};
    return (
      registry?.storage_role_eligible === true ||
      isStorageRoleSelected(host) ||
      Boolean(capacity.storage_root || registry?.storage_root)
    );
  }

  function isKnowledgeVaultServiceActive() {
    const status = String(knowledgeVaultStatus?.status || "").toLowerCase();
    return status !== "" && status !== "stopped" && status !== "deleted";
  }

  function isKnowledgeVaultSelectedForHost(host) {
    const registry = host?.registry || host || {};
    return (
      isKnowledgeVaultServiceActive() &&
      knowledgeVaultStatus?.node_name === registry?.node_name
    );
  }

  function knowledgeVaultServiceLabel(host) {
    if (isKnowledgeVaultSelectedForHost(host)) {
      return `active: ${knowledgeVaultStatus?.status || "unknown"}`;
    }
    if (isKnowledgeVaultServiceActive()) {
      return `assigned to ${knowledgeVaultStatus?.node_name || "another node"}`;
    }
    return "not assigned";
  }

  function nodeRoleLabels(host) {
    const registry = host?.registry || host || {};
    const capacity = registry?.capacity_summary || {};
    const roles = [];
    if (String(registry?.derived_role || "").toLowerCase() === "worker") {
      roles.push("worker");
    }
    if (isStorageRoleSelected(host)) {
      roles.push("storage");
    }
    if (roles.length === 0 && Number(capacity.gpu_count || 0) > 0) {
      roles.push("gpu-capable");
    }
    if (roles.length === 0 && registry?.registration_state === "registered") {
      roles.push("registered");
    }
    return roles.length > 0 ? roles : ["unclassified"];
  }

  function rolePillClass(role) {
    if (role === "worker") {
      return "is-healthy";
    }
    if (role === "storage") {
      return "is-warning";
    }
    if (role === "unclassified") {
      return "is-muted";
    }
    return "is-booting";
  }

  function renderNodeRoleBadges(host) {
    return (
      <div className="role-badges">
        {nodeRoleLabels(host).map((role) => (
          <span className={`tag ${rolePillClass(role)}`} key={`${host?.node_name}:${role}`}>
            {role}
          </span>
        ))}
      </div>
    );
  }

  function openHostOverview(nodeName) {
    if (nodeName) {
      setSelectedHostNodeName(nodeName);
    }
  }

  function closeHostOverview() {
    setSelectedHostNodeName("");
  }

  function renderHostServiceControls(host) {
    const registry = host?.registry || host || {};
    if (!registry?.node_name) {
      return null;
    }
    const nodeName = registry.node_name;
    const autoStorage = isAutoStorageRole(host);
    const manualStorage = isManualStorageRole(host);
    const storageSelected = isStorageRoleSelected(host);
    const storageEligible = isStorageRoleEligible(host);
    const storageBusy = actionBusy === `storage-role:${nodeName}`;
    const knowledgeBusy = actionBusy === `knowledge-vault:${nodeName}`;
    const canManageRoles = authState.user?.role === "admin";
    const knowledgeSelected = isKnowledgeVaultSelectedForHost(host);
    const knowledgeOwnedByOtherNode =
      isKnowledgeVaultServiceActive() &&
      knowledgeVaultStatus?.node_name &&
      knowledgeVaultStatus.node_name !== nodeName;
    const storageDetail = autoStorage
      ? "auto-assigned"
      : manualStorage
        ? "manual override"
        : storageEligible
          ? "eligible"
          : "not eligible";
    const storageDisabled =
      autoStorage ||
      storageBusy ||
      !canManageRoles ||
      (!manualStorage && !storageEligible);
    const knowledgeDisabled =
      knowledgeBusy ||
      !canManageRoles ||
      !storageSelected ||
      knowledgeOwnedByOtherNode;

    return (
      <div className="node-service-controls">
        <label
          className={`node-service-checkbox ${storageSelected ? "is-active" : ""}`}
          title={
            autoStorage
              ? "Storage role is derived automatically from node inventory"
              : !canManageRoles
                ? "Admin role required"
                : storageEligible
                  ? "Select this node for storage-capable services"
                  : "Host has not reported a usable storage root yet"
          }
        >
          <input
            type="checkbox"
            checked={storageSelected}
            disabled={storageDisabled}
            onChange={(event) => setHostStorageRole(registry, event.target.checked)}
          />
          <span>
            <strong>Storage</strong>
            <small>{storageBusy ? "updating" : storageDetail}</small>
          </span>
        </label>
        <label
          className={`node-service-checkbox ${knowledgeSelected ? "is-active" : ""}`}
          title={
            !canManageRoles
              ? "Admin role required"
              : !storageSelected
                ? "Requires selected or auto-assigned storage role"
                : knowledgeOwnedByOtherNode
                  ? "Knowledge Vault is assigned to another node"
                  : "Run canonical Knowledge Vault as a separate service container on this storage node"
          }
        >
          <input
            type="checkbox"
            checked={knowledgeSelected}
            disabled={knowledgeDisabled}
            onChange={(event) => setHostKnowledgeVault(registry, event.target.checked)}
          />
          <span>
            <strong>Knowledge Vault</strong>
            <small>{knowledgeBusy ? "updating" : knowledgeVaultServiceLabel(host)}</small>
          </span>
        </label>
      </div>
    );
  }

  function renderHostCards(items) {
    return (
      <div className="plane-list">
        {items.map((host) => {
          const cpu = host?.cpu_telemetry?.summary || {};
          const gpu = host?.gpu_telemetry?.summary || {};
          const connectivity = nodeConnectivityStatus(host);
          const registry = host?.registry || null;
          const storageCapacity = registry?.capacity_summary || {};
          const usedMemoryBytes = Number(cpu.used_memory_bytes || 0);
          const totalMemoryBytes = Number(cpu.total_memory_bytes || storageCapacity.total_memory_bytes || 0);
          const gpuDeviceCount = Number(gpu.device_count || storageCapacity.gpu_count || 0);
          const lanPeers = Array.isArray(registry?.lan_peers) ? registry.lan_peers : [];
          const directLanPeers = lanPeers.filter((peer) => peer?.tcp_reachable === true);
          const storageTotalBytes = Number(storageCapacity.storage_total_bytes || 0);
          const storageFreeBytes = Number(storageCapacity.storage_free_bytes || 0);
          return (
            <article className="node-card" key={host?.node_name || host?.observed_at}>
              <button
                className="node-card-main"
                type="button"
                onClick={() => openHostOverview(host?.node_name)}
                aria-label={`open node overview for ${host?.node_name || "unknown host"}`}
              >
                <div className="card-row">
                  <strong>{host?.node_name || "unknown-host"}</strong>
                  <div className={`pill ${connectivity.indicatorClass}`}>
                    {statusDot(connectivity.indicatorClass)}
                    <span>{connectivity.label}</span>
                  </div>
                </div>
                {renderNodeRoleBadges(host)}
              </button>
              <div className="metric-grid">
                <div className="metric-row"><span>GPU count</span><strong>{gpuDeviceCount || "n/a"}</strong></div>
                <div className="metric-row"><span>Memory</span><strong>{totalMemoryBytes > 0 ? `${compactBytes(usedMemoryBytes)} / ${compactBytes(totalMemoryBytes)}` : "n/a"}</strong></div>
                <div className="metric-row"><span>Storage</span><strong>{storageTotalBytes > 0 ? `${compactBytes(storageFreeBytes)} free` : "n/a"}</strong></div>
                <div className="metric-row"><span>LAN peers</span><strong>{lanPeers.length > 0 ? `${directLanPeers.length}/${lanPeers.length} direct` : "n/a"}</strong></div>
                <div className="metric-row"><span>Heartbeat</span><strong>{formatTimestamp(host?.observed_at || host?.heartbeat_at)}</strong></div>
              </div>
              {registry ? renderHostServiceControls(host) : null}
            </article>
          );
        })}
      </div>
    );
  }

  function renderHostOverviewModal(host) {
    if (!host) {
      return null;
    }
    const registry = host?.registry || host || {};
    const cpu = host?.cpu_telemetry?.summary || {};
    const gpu = host?.gpu_telemetry?.summary || {};
    const capacity = registry?.capacity_summary || {};
    const connectivity = nodeConnectivityStatus(host);
    const storageSelected = isStorageRoleSelected(host);
    const storageEligible = isStorageRoleEligible(host);
    const lanPeers = Array.isArray(registry?.lan_peers) ? registry.lan_peers : [];
    const gpuDeviceCount = Number(gpu.device_count || capacity.gpu_count || 0);
    const totalMemoryBytes = Number(cpu.total_memory_bytes || capacity.total_memory_bytes || 0);
    const usedMemoryBytes = Number(cpu.used_memory_bytes || 0);
    const storageTotalBytes = Number(capacity.storage_total_bytes || 0);
    const storageFreeBytes = Number(capacity.storage_free_bytes || 0);
    const showStorageDetails = storageSelected || storageEligible || storageTotalBytes > 0;
    const transportCapabilities = registry?.transport_capabilities || {};
    const supportedControlTransports = Array.isArray(
      transportCapabilities.supported_control_transports,
    )
      ? transportCapabilities.supported_control_transports.join(", ")
      : "http-poll";

    return (
      <div className="modal-backdrop" role="presentation" onClick={closeHostOverview}>
        <section
          className="modal-card node-overview-modal"
          role="dialog"
          aria-modal="true"
          aria-label={`node overview ${host?.node_name || "unknown-host"}`}
          onClick={(event) => event.stopPropagation()}
        >
          <div className="card-row">
            <div>
              <h2>{host?.node_name || "unknown-host"}</h2>
              {renderNodeRoleBadges(host)}
            </div>
            <div className={`pill ${connectivity.indicatorClass}`}>
              {statusDot(connectivity.indicatorClass)}
              <span>{connectivity.label}</span>
            </div>
          </div>
          <div className="metric-grid">
            <div className="metric-row"><span>Registration</span><strong>{registry?.registration_state || "n/a"}</strong></div>
            <div className="metric-row"><span>Session</span><strong>{registry?.session_state || "n/a"}</strong></div>
            <div className="metric-row"><span>Runtime</span><strong>{runtimePhase}</strong></div>
            <div className="metric-row"><span>Launch ready</span><strong>{yesNo(runtimeAvailable)}</strong></div>
            <div className="metric-row"><span>Derived role</span><strong>{registry?.derived_role || "n/a"}</strong></div>
            <div className="metric-row"><span>Role reason</span><strong>{registry?.role_reason || "n/a"}</strong></div>
            <div className="metric-row"><span>Execution</span><strong>{registry?.execution_mode || "n/a"}</strong></div>
            <div className="metric-row"><span>Transport</span><strong>{registry?.transport_mode || "n/a"}</strong></div>
            <div className="metric-row"><span>Control</span><strong>{transportCapabilities.preferred_control_transport || "http-poll"}</strong></div>
            <div className="metric-row"><span>Supported control</span><strong>{supportedControlTransports}</strong></div>
            <div className="metric-row"><span>Resumable bulk</span><strong>{yesNo(transportCapabilities.supports_resumable_transfer)}</strong></div>
            <div className="metric-row"><span>Advertised</span><strong>{registry?.advertised_address || "n/a"}</strong></div>
            <div className="metric-row"><span>Heartbeat</span><strong>{formatTimestamp(host?.observed_at || host?.heartbeat_at || registry?.last_heartbeat_at)}</strong></div>
          </div>
          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Capacity</h3>
              <span className="subpanel-meta">Host inventory and live telemetry for this node.</span>
            </div>
            <div className="metric-grid">
              <div className="metric-row"><span>GPU count</span><strong>{gpuDeviceCount || "n/a"}</strong></div>
              {gpuDeviceCount > 0 ? (
                <>
                  <div className="metric-row"><span>GPU temp</span><strong>{Number(gpu.temperature_device_count || 0) > 0 ? formatTemperature(gpu.hottest_temperature_c) : "n/a"}</strong></div>
                  <div className="metric-row"><span>GPU VRAM</span><strong>{Number(gpu.total_vram_mb || 0) > 0 ? `${formatDashboardMegabytesMbGb(gpu.used_vram_mb)} / ${formatDashboardMegabytesMbGb(gpu.total_vram_mb)}` : "n/a"}</strong></div>
                </>
              ) : null}
              <div className="metric-row"><span>Memory</span><strong>{totalMemoryBytes > 0 ? `${compactBytes(usedMemoryBytes)} / ${compactBytes(totalMemoryBytes)}` : "n/a"}</strong></div>
              <div className="metric-row"><span>CPU temp</span><strong>{cpu.temperature_available ? formatTemperature(cpu.max_temperature_c ?? cpu.temperature_c) : "n/a"}</strong></div>
              {showStorageDetails ? (
                <>
                  <div className="metric-row"><span>Storage role</span><strong>{storageSelected ? "selected" : "available"}</strong></div>
                  <div className="metric-row"><span>Storage root</span><strong>{capacity.storage_root || "n/a"}</strong></div>
                  <div className="metric-row"><span>Storage total</span><strong>{storageTotalBytes > 0 ? compactBytes(storageTotalBytes) : "n/a"}</strong></div>
                  <div className="metric-row"><span>Storage free</span><strong>{storageFreeBytes > 0 ? compactBytes(storageFreeBytes) : "n/a"}</strong></div>
                  <div className="metric-row"><span>Knowledge Vault</span><strong>{knowledgeVaultServiceLabel(host)}</strong></div>
                </>
              ) : null}
            </div>
          </section>
          {lanPeers.length > 0 ? (
            <section className="subpanel">
              <div className="subpanel-header">
                <h3>LAN peers</h3>
                <span className="subpanel-meta">Direct transfer visibility reported by this node.</span>
              </div>
              <div className="metric-grid">
                {lanPeers.map((peer) => (
                  <div className="metric-row" key={`${host?.node_name}:${peer?.peer_node_name}`}>
                    <span>{peer?.peer_node_name || "peer"}</span>
                    <strong>
                      {peer?.tcp_reachable ? "direct" : peer?.seen_udp ? "partial" : "stale"}
                      {peer?.peer_endpoint ? ` / ${peer.peer_endpoint}` : ""}
                    </strong>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
          <div className="toolbar">
            {registry?.node_name ? renderHostServiceControls(host) : null}
            <button className="ghost-button" type="button" onClick={closeHostOverview}>
              Close
            </button>
          </div>
        </section>
      </div>
    );
  }

  function renderSelfServiceCards(serviceItems) {
    return (
      <div className="plane-list">
        {serviceItems.map((service) => {
          const indicatorClass = selfServiceStatusClass(service?.health);
          const targets = Array.isArray(service?.targets) ? service.targets : [];
          return (
            <article className="node-card" key={service?.id || service?.label}>
              <div className="card-row">
                <strong>{service?.label || "unknown-service"}</strong>
                <div className={`pill ${indicatorClass}`}>
                  {statusDot(indicatorClass)}
                  <span>{service?.health || "unknown"}</span>
                </div>
              </div>
              <div className="metric-grid">
                <div className="metric-row"><span>Kind</span><strong>{service?.kind || "n/a"}</strong></div>
                <div className="metric-row"><span>State</span><strong>{service?.state || "unknown"}</strong></div>
                <div className="metric-row"><span>Detail</span><strong>{service?.detail || "n/a"}</strong></div>
                <div className="metric-row"><span>Updated</span><strong>{formatTimestamp(service?.updated_at)}</strong></div>
                {targets.length === 0 ? (
                  <div className="metric-row"><span>Targets</span><strong>n/a</strong></div>
                ) : (
                  targets.map((target, index) => {
                    const reachable =
                      target?.reachable === true
                        ? "reachable"
                        : target?.reachable === false
                          ? "unreachable"
                          : "unchecked";
                    return (
                      <div className="metric-row" key={`${service?.id || "service"}-${index}`}>
                        <span>{target?.label || "target"}</span>
                        <strong>{`${target?.value || "n/a"} (${reachable})`}</strong>
                      </div>
                    );
                  })
                )}
              </div>
            </article>
          );
        })}
      </div>
    );
  }

  function renderPlaneCards(planeItems, deleteTargetPage = "planes") {
    return (
      <div className="plane-list">
        {planeItems.map((plane) => {
          const selected = plane.name === selectedPlane;
          const displayState = planeDisplayState(plane);
          const displayStateClass = planeDisplayStateClass(plane);
          const planeDeleting = plane.state === "deleting";
          const planeCanRestart = plane.state === "running" && plane.staged_update;
          const planeStartDisabled =
            actionBusy !== "" ||
            planeDeleting ||
            (plane.state === "running" && !planeCanRestart) ||
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
                  onClick={() => executePlaneAction(planeCanRestart ? "restart" : "start", plane.name)}
                >
                  {planeCanRestart ? "Restart" : "Start"}
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

  function renderPlaneKnowledgeTab() {
    const graphSummary = summarizeKnowledgeGraph(planeKnowledge.graph);
    const resolverSelectedSkills = Array.isArray(planeKnowledge.resolverResult?.selected_skills)
      ? planeKnowledge.resolverResult.selected_skills
      : [];
    return (
      <div className="dashboard-plane-detail-scroll">
        <div className="summary-grid">
          <SummaryCard
            label="Knowledge"
            value={knowledgeEnabled ? "enabled" : "disabled"}
            meta={`${selectedKnowledgeIds.length} selected`}
          />
          <SummaryCard
            label="Graph nodes"
            value={graphSummary.nodeCount}
            meta={`${graphSummary.edgeCount} relations`}
          />
          <SummaryCard
            label="Context budget"
            value={knowledgeContextPolicy.token_budget ?? "default"}
            meta={`graph ${knowledgeContextPolicy.include_graph === false ? "off" : "on"} / depth ${knowledgeContextPolicy.max_graph_depth ?? 1}`}
          />
        </div>

        {planeKnowledge.error ? <div className="error-banner">{planeKnowledge.error}</div> : null}
        {!knowledgeEnabled ? (
          <EmptyState
            title="Knowledge Base disabled"
            detail="Enable Knowledge Base in the plane editor to attach canonical records to interaction context."
          />
        ) : null}

        <div className="panel-grid plane-knowledge-grid">
          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Selected records</h3>
              <span className="subpanel-meta">{selectedKnowledgeIds.length} id(s)</span>
            </div>
            <div className="toolbar">
              <button
                className="ghost-button"
                type="button"
                onClick={refreshPlaneKnowledge}
                disabled={planeKnowledge.busy}
              >
                Refresh knowledge
              </button>
            </div>
            <div className="list-column">
              {selectedKnowledgeIds.length === 0 ? (
                <EmptyState
                  title="No selected records"
                  detail="The graph falls back to plane-scoped search results."
                />
              ) : (
                selectedKnowledgeIds.map((knowledgeId) => (
                  <article className="list-card" key={knowledgeId}>
                    <div className="factory-skill-table-id">{knowledgeId}</div>
                  </article>
                ))
              )}
            </div>
          </section>

          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Plane Knowledge Graph</h3>
              <span className="subpanel-meta">
                {planeKnowledge.busy ? "loading" : `${graphSummary.nodeCount} node(s)`}
              </span>
            </div>
            <div className="knowledge-vault-graph-panel">
              <KnowledgeCubeGraph graph={planeKnowledge.graph} variant="lattice" />
            </div>
          </section>

          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Search results</h3>
              <span className="subpanel-meta">{planeKnowledge.results.length} item(s)</span>
            </div>
            <div className="list-column">
              {planeKnowledge.results.length === 0 ? (
                <EmptyState title="No plane knowledge records" />
              ) : (
                planeKnowledge.results.slice(0, 12).map((item) => (
                  <article className="list-card" key={`${item.knowledge_id || item.id}:${item.block_id || ""}`}>
                    <div className="card-row">
                      <strong>{item.title || item.knowledge_id || item.block_id}</strong>
                      <span className="tag">{item.type || "knowledge"}</span>
                    </div>
                    <div className="list-detail">
                      <div className="factory-skill-table-id">{item.knowledge_id || item.id || item.block_id}</div>
                      <div>{item.summary || "No summary"}</div>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>

          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Enrichment resolver</h3>
              <span className="subpanel-meta">Plane-owned SkillsFactory replica</span>
            </div>
            <textarea
              className="editor-textarea plane-form-textarea"
              value={planeKnowledge.resolverPrompt}
              onChange={(event) =>
                setPlaneKnowledge((current) => ({
                  ...current,
                  resolverPrompt: event.target.value,
                }))
              }
              placeholder="Type a prompt to see which plane skills would be selected"
            />
            <div className="toolbar">
              <button
                className="ghost-button"
                type="button"
                disabled={planeKnowledge.resolverBusy || !planeKnowledge.resolverPrompt.trim()}
                onClick={resolvePlaneEnrichment}
              >
                Resolve enrichment
              </button>
            </div>
            {planeKnowledge.resolverError ? (
              <div className="error-banner">{planeKnowledge.resolverError}</div>
            ) : null}
            {planeKnowledge.resolverResult ? (
              <div className="list-card">
                <div className="metric-grid compact-metric-grid">
                  <div className="metric-row"><span>Mode</span><strong>{planeKnowledge.resolverResult.skill_resolution_mode || "none"}</strong></div>
                  <div className="metric-row"><span>Candidates</span><strong>{planeKnowledge.resolverResult.candidate_count ?? 0}</strong></div>
                  <div className="metric-row"><span>Selected</span><strong>{resolverSelectedSkills.length}</strong></div>
                </div>
                <div className="list-column">
                  {resolverSelectedSkills.length === 0 ? (
                    <EmptyState title="No skills matched" detail="This prompt should not trigger Skills enrichment." />
                  ) : (
                    resolverSelectedSkills.map((skill) => (
                      <article className="list-card" key={skill.id}>
                        <div className="card-row">
                          <strong>{skill.name || skill.id}</strong>
                          <span className="tag">{skill.score ?? 0}</span>
                        </div>
                        <div className="list-detail">
                          <div className="factory-skill-table-id">{skill.id}</div>
                          <div>{skill.rationale || "No rationale"}</div>
                        </div>
                      </article>
                    ))
                  )}
                </div>
              </div>
            ) : null}
          </section>
        </div>
      </div>
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
              {(() => {
                const canRestart = planeRecord?.state === "running" && planeRecord?.staged_update;
                return (
              <button
                className="ghost-button"
                type="button"
                onClick={() => executePlaneAction(canRestart ? "restart" : "start")}
                disabled={
                  actionBusy !== "" ||
                  (planeRecord?.state === "running" && !canRestart) ||
                  selectedPlaneDeleting
                }
              >
                {canRestart ? "Restart plane" : "Start plane"}
              </button>
                );
              })()}
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
              {skillsEnabled ? (
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => openSkillsDialog(selectedPlane)}
                  disabled={actionBusy !== ""}
                >
                  Skills
                </button>
              ) : null}
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
            {llmPlane ? (
              <button
                className={`tab-button ${selectedTab === "knowledge" ? "is-active" : ""}`}
                type="button"
                role="tab"
                aria-selected={selectedTab === "knowledge"}
                onClick={() => setSelectedTab("knowledge")}
              >
                <span className="tab-button-title">Knowledge</span>
                <span className="tab-button-meta">Vault graph and enrichment</span>
              </button>
            ) : null}
          </div>

          {apiError ? <div className="error-banner">{apiError}</div> : null}

          {selectedTab === "status" ? (
            <div className="dashboard-plane-detail-scroll">
              <div className="summary-grid">
                <SummaryCard
                  label="Ready nodes"
                  value={readyNodes}
                  meta={`${dashboard.plane?.node_count ?? 0} total / ${notReadyNodes} not ready`}
                  history={planeReadyNodesHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Plane ready nodes",
                        planeReadyNodesHistory,
                        `Plane ${selectedPlane || "n/a"} readiness over time.`,
                        null,
                        `${planeMetricPrefix}.ready_nodes`,
                      )
                    }
                  />
                <SummaryCard
                  label="Observed instances"
                  value={displayedInstanceCount}
                  meta={instanceRoleSummary}
                  history={planeObservedInstancesHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Plane observed instances",
                        planeObservedInstancesHistory,
                        `Plane ${selectedPlane || "n/a"} observed instance count over time.`,
                        null,
                        `${planeMetricPrefix}.observed_instances`,
                      )
                    }
                  />
                <SummaryCard
                  label="Skills"
                  value={formattedSkillsSummary.value}
                  meta={formattedSkillsSummary.meta}
                />
                <SummaryCard
                  label="Browsing"
                  value={browsingSummaryValue}
                  meta={browsingSummaryMeta}
                />
                <SummaryCard
                  label="Rollout actions"
                  value={dashboard.rollout?.total_actions ?? 0}
                  meta={`${dashboard.rollout?.loop_status ?? "n/a"} / ${dashboard.rollout?.loop_reason ?? "n/a"}`}
                  history={planeRolloutHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Plane rollout actions",
                        planeRolloutHistory,
                        `Plane ${selectedPlane || "n/a"} rollout queue size over time.`,
                        null,
                        `${planeMetricPrefix}.rollout_actions`,
                      )
                    }
                  />
                <SummaryCard
                  label="Alerts"
                  value={alertSummary.total ?? 0}
                  meta={`${alertSummary.critical ?? 0} critical / ${alertSummary.warning ?? 0} warning / ${alertSummary.booting ?? 0} booting`}
                  history={planeAlertHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Plane alerts",
                        planeAlertHistory,
                        `Plane ${selectedPlane || "n/a"} alert count over time.`,
                        null,
                        `${planeMetricPrefix}.alerts`,
                      )
                    }
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
                        <div className="metric-row"><span>KV cache</span><strong>{formatDashboardBytesMbGb(runtimeSummary.kv_cache_bytes)}</strong></div>
                        <div className="metric-row"><span>TurboQuant</span><strong>{runtimeSummary.turboquant_enabled ? `${runtimeSummary.active_cache_type_k || "?"}/${runtimeSummary.active_cache_type_v || "?"}` : "off"}</strong></div>
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
                    <h3>Isolated browsing</h3>
                    <span className="subpanel-meta">Plane-scoped broker status and browser-session posture</span>
                  </div>
                  <div className="list-card">
                    <div className="metric-grid compact-metric-grid">
                      <div className="metric-row"><span>Enabled</span><strong>{yesNo(dashboardBrowsingSummary?.browsing_enabled)}</strong></div>
                      <div className="metric-row"><span>Ready</span><strong>{yesNo(dashboardBrowsingSummary?.browsing_ready)}</strong></div>
                      <div className="metric-row"><span>Browser sessions</span><strong>{yesNo(dashboardBrowsingSummary?.browser_session_enabled)}</strong></div>
                      <div className="metric-row"><span>Container</span><strong>{dashboardBrowsingSummary?.browsing_container_name || "n/a"}</strong></div>
                      <div className="metric-row"><span>Reason</span><strong>{dashboardBrowsingSummary?.reason || "n/a"}</strong></div>
                      <div className="metric-row"><span>Target</span><strong>{dashboardBrowsingSummary?.browsing_target || "n/a"}</strong></div>
                    </div>
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
          ) : selectedTab === "knowledge" ? (
            renderPlaneKnowledgeTab()
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
                      <div className="metric-row"><span>KV Cache</span><strong>{formatDashboardBytesMbGb(interactionStatus?.kv_cache_bytes ?? runtimeSummary.kv_cache_bytes)}</strong></div>
                      <div className="metric-row"><span>TurboQuant</span><strong>{interactionStatus?.turboquant_enabled ? `${interactionStatus?.active_cache_type_k || "?"}/${interactionStatus?.active_cache_type_v || "?"}` : "off"}</strong></div>
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
                          {message.role === "assistant" && message.browsing ? (
                            <div className="chat-browsing-line">
                              {browsingIndicatorCompact(message.browsing) ? (
                                <span
                                  className={`tag chat-browsing-tag chat-browsing-${(message.browsing.lookup_state || "disabled").replace(/_/g, "-")}`}
                                  title={message.browsing?.indicator?.label || "Web execution state"}
                                >
                                  {browsingIndicatorCompact(message.browsing)}
                                </span>
                              ) : null}
                              {browsingTraceCompact(message.browsing) ? (
                                <span className="chat-browsing-trace">{browsingTraceCompact(message.browsing)}</span>
                              ) : null}
                            </div>
                          ) : null}
                          {message.role === "assistant" && message.enrichment ? (
                            <div className="chat-enrichment-line">
                              {compactSkillList(message.enrichment.appliedSkills) ? (
                                <span>skills {compactSkillList(message.enrichment.appliedSkills)}</span>
                              ) : null}
                              {compactSkillList(message.enrichment.autoAppliedSkills) ? (
                                <span>auto {compactSkillList(message.enrichment.autoAppliedSkills)}</span>
                              ) : null}
                              {message.enrichment.knowledge ? <span>knowledge context attached</span> : null}
                              {message.enrichment.compression ? <span>compression active</span> : null}
                            </div>
                          ) : null}
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
                    <div className="toolbar skills-factory-card-actions">
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

  function renderKnowledgeVaultPage() {
    return (
      <section className="panel page-panel knowledge-vault-page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Knowledge Vault</div>
            <h2>Canonical Knowledge Vault</h2>
          </div>
          <div className="toolbar">
            <span className={`tag ${knowledgeVaultNavClass}`}>
              {statusDot(knowledgeVaultNavClass)}
              <span>{knowledgeVaultStatusText}</span>
            </span>
            <button
              className="ghost-button"
              type="button"
              onClick={refreshKnowledgeVaultGraph}
              disabled={knowledgeVaultGraphBusy}
            >
              Refresh
            </button>
          </div>
        </div>

        {knowledgeVaultGraphError ? (
          <div className="error-banner">{knowledgeVaultGraphError}</div>
        ) : null}

        <div className="knowledge-vault-overview-grid">
          <article className="summary-card">
            <span>Active molecules</span>
            <strong>{knowledgeGraphSummary.nodeCount}</strong>
          </article>
          <article className="summary-card">
            <span>Relations</span>
            <strong>{knowledgeGraphSummary.edgeCount}</strong>
          </article>
          <article className="summary-card">
            <span>Service</span>
            <strong>{knowledgeVaultStatusText}</strong>
          </article>
          <article className="summary-card">
            <span>Node</span>
            <strong>{knowledgeVaultStatus?.node_name || "unassigned"}</strong>
          </article>
        </div>

        <div className="knowledge-vault-graph-panel">
          <KnowledgeCubeGraph graph={knowledgeVaultGraph} variant="lattice" />
        </div>
      </section>
    );
  }

  function renderModelsLibrary() {
    const detectedSourceFormat = detectModelSourceFormat(modelDownloadForm.sourceUrls);
    const renderLibraryCatalog = () => (
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
                    className={`ghost-button compact-button icon-button ${
                      item.skills_factory_worker ? "warning-button" : ""
                    }`}
                    type="button"
                    disabled={modelLibraryBusy !== ""}
                    onClick={() => setSkillsFactoryWorker(item)}
                    title={
                      item.skills_factory_worker
                        ? "Current Skills Factory Worker"
                        : "Set as Skills Factory Worker"
                    }
                  >
                    <ActionIcon kind="worker" />
                  </button>
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
    );

    const renderDownloadJobs = () => (
      <div className="subpanel models-jobs-panel">
        <div className="subpanel-header">
          <div>
            <div className="section-label">Queue</div>
            <h3>Download jobs</h3>
          </div>
        </div>
        {downloadModelJobs.length > 0 ? (
          <>
            <div className="models-catalog-meta model-job-pagination-meta">
              <span>
                Showing {visibleModelJobs.length} of {downloadModelJobs.length}
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
                    {job.detected_source_format || job.desired_output_format ? (
                      <div>
                        {job.detected_source_format || "unknown"} -&gt;{" "}
                        {job.desired_output_format || "unknown"}
                      </div>
                    ) : null}
                    <div>{modelLibraryJobByteSummary(job)}</div>
                    {job.phase && job.phase !== job.status ? <div>phase {job.phase}</div> : null}
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
                      value={
                        modelLibraryJobProgress(job) === null ? undefined : modelLibraryJobProgress(job)
                      }
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
    );

    const renderDownloadForm = () => (
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
          disabled={Boolean(modelDownloadForm.targetNodeName)}
          spellCheck="false"
        />
        <label className="field-label" htmlFor="model-target-node">
          Storage node
        </label>
        <select
          id="model-target-node"
          className="text-input"
          value={modelDownloadForm.targetNodeName}
          onChange={(event) =>
            setModelDownloadForm((current) => ({
              ...current,
              targetNodeName: event.target.value,
              targetRoot: event.target.value ? "" : current.targetRoot,
            }))
          }
        >
          <option value="">Manual target root</option>
          {storageModelNodes.map((node) => (
            <option key={node.node_name} value={node.node_name}>
              {node.node_name} - {node.storage_root}
            </option>
          ))}
        </select>
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
        <div className="plane-form-section-meta">
          Detected source format: {detectedSourceFormat}
        </div>
        <label className="field-label" htmlFor="model-format-select">
          Format
        </label>
        <select
          id="model-format-select"
          className="text-input"
          value={modelDownloadForm.format}
          onChange={(event) =>
            setModelDownloadForm((current) => ({
              ...current,
              format: event.target.value,
              formatLocked: true,
            }))
          }
        >
          {modelDownloadForm.format === "unknown" ? (
            <option value="unknown">Auto detect</option>
          ) : null}
          {MODEL_LIBRARY_FORMAT_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <div className="plane-form-section-meta">
          Source files keeps safetensors bundles unchanged. GGUF conversion retains only the
          base GGUF. Use the Quantization tab for post-download quantization variants.
        </div>
        <div className="toolbar">
          <button
            className="ghost-button"
            type="button"
            disabled={
              modelLibraryBusy !== "" ||
              (!modelDownloadForm.targetRoot.trim() &&
                !modelDownloadForm.targetNodeName.trim()) ||
              !modelDownloadForm.sourceUrls.trim()
            }
            onClick={enqueueModelLibraryDownload}
          >
            Download model
          </button>
        </div>
      </div>
    );

    const renderQuantizationTab = () => (
      <div className="models-page-stack">
        <div className="subpanel">
          <div className="subpanel-header">
            <div>
              <div className="section-label">Quantization</div>
              <h3>GGUF variants</h3>
            </div>
          </div>
          <div className="models-quantization-toolbar">
            <input
              className="text-input"
              type="text"
              value={quantizationSearch}
              onChange={(event) => setQuantizationSearch(event.target.value)}
              placeholder="Search GGUF models"
              spellCheck="false"
            />
            <div className="models-quantization-filters" role="toolbar" aria-label="Quantization filters">
              <button
                className={`ghost-button compact-button ${quantizationFilter === "all" ? "warning-button" : ""}`}
                type="button"
                onClick={() => setQuantizationFilter("all")}
              >
                All
              </button>
              {MODEL_LIBRARY_QUANTIZATION_FILTERS.map((option) => (
                <button
                  className={`ghost-button compact-button ${
                    quantizationFilter === option ? "warning-button" : ""
                  }`}
                  type="button"
                  key={option}
                  onClick={() => setQuantizationFilter(option)}
                >
                  {option}
                </button>
              ))}
            </div>
          </div>
          {activeQuantizationJobs.length > 0 ? (
            <div className="models-quantization-progress">
              {activeQuantizationJobs.map((job) => (
                <article className="list-card" key={job.id}>
                  <div className="card-row">
                    <strong>{job.model_id || job.id}</strong>
                    <span className={`tag ${modelLibraryJobStatusClass(job.status)}`}>{job.phase || job.status}</span>
                  </div>
                  <div className="list-detail">
                    <div>{job.current_item || "quantizing"}</div>
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
                </article>
              ))}
            </div>
          ) : null}
          <div className={`list-column model-library-list model-library-list-expanded ${
            quantizationItems.length === 0 ? "model-library-list-empty" : ""
          }`}>
            {quantizationItems.length === 0 ? (
              <EmptyState
                title="No GGUF models match"
                detail="Download or convert a base GGUF model first, then launch quantization from this tab."
              />
            ) : (
              quantizationItems.map((item) => {
                const itemQuantization = normalizeModelLibraryItemQuantization(item.quantization);
                const canQuantize = itemQuantization === "base";
                return (
                  <article className="list-card" key={item.path}>
                    <div className="card-row">
                      <strong>{formatModelLibraryDisplayName(item)}</strong>
                      <div className="models-quantization-badges">
                        <span className="tag">{itemQuantization}</span>
                        <span className="tag">{modelLibraryItemSummary(item)}</span>
                      </div>
                    </div>
                    <div className="list-detail">
                      <div>{item.path}</div>
                      {item.quantized_from_path ? <div>derived from {item.quantized_from_path}</div> : null}
                      {Array.isArray(item.referenced_by) && item.referenced_by.length > 0 ? (
                        <div>used by {item.referenced_by.join(", ")}</div>
                      ) : (
                        <div>not referenced by any plane</div>
                      )}
                    </div>
                    <div className="toolbar">
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={modelLibraryBusy !== "" || !canQuantize}
                        onClick={() => openQuantizationDialog(item)}
                        title={
                          canQuantize
                            ? "Create a new GGUF quantization variant"
                            : "Only base GGUF models can be quantized"
                        }
                      >
                        {canQuantize ? "Quantize" : "Base only"}
                      </button>
                    </div>
                  </article>
                );
              })
            )}
          </div>
        </div>
      </div>
    );

    return (
      <>
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
            Manage discovered model artifacts, queue downloads, and create GGUF
            quantization variants.
          </div>
          <div className="tab-strip" role="tablist" aria-label="Models tabs">
            {[
              ["library", "Library", "Catalog and model actions"],
              ["quantization", "Quantization", "GGUF post-processing workflow"],
              ["downloads", "Downloads", "Queue and Add Model"],
            ].map(([value, title, meta]) => (
              <button
                key={value}
                className={`tab-button ${modelsTab === value ? "is-active" : ""}`}
                type="button"
                role="tab"
                aria-selected={modelsTab === value}
                onClick={() => setModelsTab(value)}
              >
                <span className="tab-button-title">{title}</span>
                <span className="tab-button-meta">{meta}</span>
              </button>
            ))}
          </div>
          {modelsTab === "library" ? (
            <div className="models-page-stack">{renderLibraryCatalog()}</div>
          ) : null}
          {modelsTab === "downloads" ? (
            <div className="models-page-grid">
              {renderDownloadJobs()}
              {renderDownloadForm()}
            </div>
          ) : null}
          {modelsTab === "quantization" ? renderQuantizationTab() : null}
        </section>
        {quantizationDialog.open ? (
          <div className="modal-backdrop" role="presentation" onClick={closeQuantizationDialog}>
            <div
              className="modal-card quantization-modal"
              role="dialog"
              aria-modal="true"
              aria-labelledby="quantization-dialog-title"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="panel-header">
                <div>
                  <div className="section-label">Quantization</div>
                  <h2 id="quantization-dialog-title">
                    {formatModelLibraryDisplayName(quantizationDialog.item)}
                  </h2>
                </div>
                <button
                  className="ghost-button compact-button icon-button"
                  type="button"
                  onClick={closeQuantizationDialog}
                  title="Close quantization dialog"
                  aria-label="Close quantization dialog"
                >
                  <ActionIcon kind="close" />
                </button>
              </div>
              <div className="list-detail">
                <div>{quantizationDialog.item?.path}</div>
                <div>Select the GGUF quantization type to generate.</div>
              </div>
              <label className="field-label" htmlFor="quantization-type-select">
                Quantization type
              </label>
              <select
                id="quantization-type-select"
                className="text-input"
                value={quantizationDialog.quantization}
                onChange={(event) =>
                  setQuantizationDialog((current) => ({
                    ...current,
                    quantization: event.target.value,
                  }))
                }
              >
                {MODEL_LIBRARY_GGUF_QUANTIZATIONS.map((quantization) => (
                  <option key={quantization} value={quantization}>
                    {quantization}
                  </option>
                ))}
              </select>
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={modelLibraryBusy !== ""}
                  onClick={enqueueModelQuantization}
                >
                  Apply
                </button>
                <button
                  className="ghost-button danger-button"
                  type="button"
                  disabled={modelLibraryBusy !== ""}
                  onClick={closeQuantizationDialog}
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        ) : null}
      </>
    );
  }

  function renderSkillsFactoryPage() {
    const groupTree = buildSkillsFactoryGroupTree(skillsFactory.items || [], skillsFactory.groups || []);
    const availableTreePaths = collectSkillsFactoryTreePaths(groupTree);
    const expandedGroupPaths = Array.isArray(skillsFactory.expandedGroupPaths)
      ? skillsFactory.expandedGroupPaths
      : [""];
    const expandedGroupPathSet = new Set(expandedGroupPaths);

    function setExpandedGroupPaths(nextPaths) {
      setSkillsFactory((current) => ({
        ...current,
        expandedGroupPaths: nextPaths,
      }));
    }

    function toggleGroupExpansion(path) {
      const normalizedPath = typeof path === "string" ? path : "";
      setSkillsFactory((current) => {
        const currentPaths = new Set(
          Array.isArray(current.expandedGroupPaths) ? current.expandedGroupPaths : [""],
        );
        if (currentPaths.has(normalizedPath)) {
          currentPaths.delete(normalizedPath);
        } else {
          currentPaths.add(normalizedPath);
        }
        return {
          ...current,
          expandedGroupPaths: [...currentPaths],
        };
      });
    }

    function renderGroupNode(node) {
      const isExpanded = expandedGroupPathSet.has(node.path || "");
      const hasChildren = node.children.length > 0;
      return (
        <div className="skills-factory-group-node" key={node.path || "__root__"}>
          <div className="skills-factory-group-row">
            <button
              className={`ghost-button compact-button skills-factory-group-toggle ${isExpanded ? "is-expanded" : ""}`}
              type="button"
              disabled={!hasChildren}
              onClick={() => toggleGroupExpansion(node.path || "")}
              aria-label={isExpanded ? `Collapse ${node.label}` : `Expand ${node.label}`}
            >
              <span className="skills-factory-group-toggle-glyph">{hasChildren ? "▾" : "•"}</span>
            </button>
            <button
              className={`ghost-button skills-factory-group-button ${skillsFactory.selectedGroupPath === node.path ? "is-active" : ""}`}
              type="button"
              onClick={() =>
                setSkillsFactory((current) => ({ ...current, selectedGroupPath: node.path }))
              }
            >
              <span className="skills-factory-group-copy">
                <span>{node.label}</span>
                {node.direct_skill_count > 0 ? (
                  <span className="skills-factory-group-meta">
                    {node.direct_skill_count} direct
                  </span>
                ) : null}
              </span>
              <span className="tag">{node.total_skill_count}</span>
            </button>
          </div>
          {hasChildren && isExpanded ? (
            <div className="skills-factory-group-children">
              {node.children.map((child) => renderGroupNode(child))}
            </div>
          ) : null}
        </div>
      );
    }

    const filteredItems = sortSkillsFactoryItems(
      filterSkillsFactoryItems(
        skillsFactory.items || [],
        skillsFactory.search,
        skillsFactory.selectedGroupPath,
      ),
      skillsFactory.sort,
    );
    const editing = skillsFactory.mode === "edit";
    const form = skillsFactory.form || buildEmptySkillForm();
    const totalItems = Array.isArray(skillsFactory.items) ? skillsFactory.items.length : 0;
    const selectedBuiltinGroup = isBuiltinSkillsFactoryGroupPath(skillsFactory.selectedGroupPath);
    const attachedPlaneCount = new Set(
      (skillsFactory.items || []).flatMap((item) =>
        Array.isArray(item.plane_names) ? item.plane_names : [],
      ),
    ).size;
    return (
      <section className="panel page-panel models-page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Skills Factory</div>
            <h2>Global skill catalog</h2>
          </div>
          <div className="toolbar">
            <button
              className="ghost-button"
              type="button"
              disabled={skillsFactory.busy}
              onClick={() => refreshSkillsFactory()}
            >
              Refresh factory
            </button>
            <button
              className="ghost-button"
              type="button"
              disabled={skillsFactory.busy}
              onClick={startCreateFactorySkill}
            >
              New skill
            </button>
          </div>
        </div>
        <div className="page-copy">
          Canonical skill content lives here and can be attached to multiple planes.
        </div>
        {skillsFactory.error ? <div className="error-banner">{skillsFactory.error}</div> : null}
        <div className="skills-factory-overview-grid">
          <article className="summary-card skills-factory-overview-card">
            <span className="summary-label">Catalog size</span>
            <strong className="summary-value">{totalItems}</strong>
            <span className="summary-meta">Canonical skills in the factory</span>
          </article>
          <article className="summary-card skills-factory-overview-card">
            <span className="summary-label">Visible now</span>
            <strong className="summary-value">{filteredItems.length}</strong>
            <span className="summary-meta">
              {skillsFactory.selectedGroupPath
                ? formatSkillGroupPath(skillsFactory.selectedGroupPath)
                : "All groups"}
            </span>
          </article>
          <article className="summary-card skills-factory-overview-card">
            <span className="summary-label">Planes using skills</span>
            <strong className="summary-value">{attachedPlaneCount}</strong>
            <span className="summary-meta">Unique attached planes across the catalog</span>
          </article>
        </div>
        <div className="panel-grid skills-factory-page-grid">
          <div className="skills-factory-side-stack">
            <section className="subpanel">
              <div className="subpanel-header">
                <div>
                  <div className="section-label">Groups</div>
                  <h3>Browse tree</h3>
                </div>
              </div>
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={skillsFactory.busy}
                  onClick={createFactoryGroup}
                >
                  New group
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  disabled={skillsFactory.busy || !skillsFactory.selectedGroupPath || selectedBuiltinGroup}
                  onClick={renameFactoryGroup}
                >
                  Rename group
                </button>
                <button
                  className="ghost-button danger-button"
                  type="button"
                  disabled={skillsFactory.busy || !skillsFactory.selectedGroupPath || selectedBuiltinGroup}
                  onClick={deleteFactoryGroup}
                >
                  Delete group
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  disabled={!skillsFactory.selectedGroupPath}
                  onClick={() =>
                    setSkillsFactory((current) => ({ ...current, selectedGroupPath: "" }))
                  }
                >
                  Show all skills
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => setExpandedGroupPaths(availableTreePaths)}
                >
                  Expand all
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => setExpandedGroupPaths([""])}
                >
                  Collapse tree
                </button>
              </div>
              <div className="skills-factory-group-tree">
                {renderGroupNode(groupTree)}
              </div>
            </section>
            <section className="subpanel skills-factory-editor-panel">
              <div className="subpanel-header">
                <h3>{editing ? "Edit / rename factory skill" : "Create factory skill"}</h3>
              </div>
              <div className="plane-form-grid">
                <label className="field-label">
                  <span className="field-label-title">Name</span>
                  <input
                    className="text-input"
                    value={form.name}
                    onChange={(event) => updateFactorySkillFormField("name", event.target.value)}
                  />
                </label>
                <label className="field-label">
                  <span className="field-label-title">Id</span>
                  <input className="text-input" value={form.id} readOnly />
                </label>
                <label className="field-label">
                  <span className="field-label-title">Group path</span>
                  <input
                    className="text-input"
                    value={form.groupPath}
                    onChange={(event) => updateFactorySkillFormField("groupPath", event.target.value)}
                    placeholder="code-agent/debugging"
                  />
                </label>
              </div>
              <div className="plane-form-section-meta">
                Use slash-separated groups. Skill ids stay stable; rename and regroup through this
                form.
              </div>
              <label className="field-label plane-checkbox">
                <input
                  type="checkbox"
                  checked={Boolean(form.internal)}
                  onChange={(event) => updateFactorySkillFormField("internal", event.target.checked)}
                />
                <span>
                  Mark as internal support-layer skill
                </span>
              </label>
              <label className="field-label">
                <span className="field-label-title">Description</span>
                <textarea
                  className="editor-textarea"
                  rows={4}
                  value={form.description}
                  onChange={(event) => updateFactorySkillFormField("description", event.target.value)}
                />
              </label>
              <label className="field-label">
                <span className="field-label-title">Content</span>
                <textarea
                  className="editor-textarea"
                  rows={12}
                  value={form.content}
                  onChange={(event) => updateFactorySkillFormField("content", event.target.value)}
                />
              </label>
              <label className="field-label">
                <span className="field-label-title">Match terms</span>
                <textarea
                  className="editor-textarea plane-form-textarea match-terms-textarea"
                  rows={6}
                  value={form.matchTermsText}
                  onChange={(event) => updateFactorySkillFormField("matchTermsText", event.target.value)}
                  placeholder={"One term per line\nlogin\nlogout\naccess cookie"}
                />
              </label>
              <div className="plane-form-section-meta">
                These terms drive contextual skill matching. One term per line.
              </div>
              <div className="toolbar">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={skillsFactory.busy}
                  onClick={saveFactorySkillForm}
                >
                  {editing ? "Save skill" : "Create skill"}
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  disabled={skillsFactory.busy}
                  onClick={startCreateFactorySkill}
                >
                  {editing ? "Cancel edit" : "Reset form"}
                </button>
              </div>
            </section>
          </div>
          <section className="subpanel">
            <div className="subpanel-header">
              <div>
                <div className="section-label">Catalog</div>
                <h3>Skills</h3>
              </div>
              <span className="subpanel-meta">{filteredItems.length} item(s)</span>
            </div>
            <div className="skills-factory-catalog-toolbar">
              <label className="field-label">
                <span className="field-label-title">Search</span>
                <input
                  className="text-input"
                  value={skillsFactory.search}
                  onChange={(event) =>
                    setSkillsFactory((current) => ({ ...current, search: event.target.value }))
                  }
                  placeholder="Filter by id, name, description, content, or plane usage"
                />
              </label>
              <label className="field-label">
                <span className="field-label-title">Sort</span>
                <select
                  className="text-input"
                  value={skillsFactory.sort}
                  onChange={(event) =>
                    setSkillsFactory((current) => ({ ...current, sort: event.target.value }))
                  }
                >
                  <option value="name">Name</option>
                  <option value="plane_count">Plane count</option>
                </select>
              </label>
              <div className="skills-factory-active-filter">
                <span className="field-label-title">Scope</span>
                <div className="skills-factory-active-filter-value">
                  {skillsFactory.selectedGroupPath
                    ? formatSkillGroupPath(skillsFactory.selectedGroupPath)
                    : "All groups"}
                </div>
              </div>
            </div>
            <div className="list-column model-library-list model-library-list-expanded">
              {filteredItems.length === 0 ? (
                <EmptyState
                  title="No factory skills"
                  detail="Create the first canonical skill or adjust the current filter."
                />
              ) : (
                filteredItems.map((item) => (
                  <article className="list-card" key={item.id}>
                    <div className="card-row">
                      <div className="skills-factory-item-head">
                        <strong>{item.name || item.id}</strong>
                        <span className="skills-factory-item-group">
                          {formatSkillGroupPath(item.group_path)}
                        </span>
                      </div>
                      <div className="toolbar">
                        {item.internal ? <span className="tag is-warning">Internal</span> : null}
                        <span className="tag">{item.plane_count || 0} plane(s)</span>
                      </div>
                    </div>
                    <div className="list-detail">
                      <div className="factory-skill-table-id">{item.id}</div>
                      <div>{item.description || "No description"}</div>
                      <div className="skills-factory-match-terms">
                        <span className="skills-factory-match-terms-label">Match terms</span>
                        <span>
                          {Array.isArray(item.match_terms) && item.match_terms.length > 0
                            ? item.match_terms.join(", ")
                            : "none"}
                        </span>
                      </div>
                      <div>{Array.isArray(item.plane_names) && item.plane_names.length > 0 ? item.plane_names.join(", ") : "not attached to any plane"}</div>
                    </div>
                    <div className="toolbar">
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={skillsFactory.busy}
                        onClick={() => startEditFactorySkill(item)}
                      >
                        Edit
                      </button>
                      <button
                        className="ghost-button danger-button"
                        type="button"
                        disabled={skillsFactory.busy}
                        onClick={() => deleteFactorySkill(item)}
                      >
                        Delete
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

  function renderProtocolsPage() {
    const items = Array.isArray(protocolRegistry.items) ? protocolRegistry.items : [];
    const summary = protocolRegistry.summary || {};
    const selectedTransport = interactionStatus?.naim?.transport || interactionStatus?.transport || null;

    return (
      <section className="panel page-panel protocol-registry-page-panel">
        <div className="panel-header">
          <div>
            <div className="section-label">Network</div>
            <h2>Protocol Registry</h2>
          </div>
          <div className="toolbar">
            <span className="tag">{summary.total ?? items.length} protocols</span>
            <span className="tag">Direct {summary.direct ?? 0}</span>
            <span className="tag">Optional {summary.optional ?? 0}</span>
          </div>
        </div>

        {selectedTransport ? (
          <section className="subpanel">
            <div className="subpanel-header">
              <h3>Selected plane transport</h3>
              <span className="subpanel-meta">{selectedPlane || "fleet"}</span>
            </div>
            <div className="transport-badge-row">
              <span className={`tag ${selectedTransport.degraded ? "is-warning" : "is-healthy"}`}>
                {selectedTransport.mode || "unknown"}
              </span>
              <span className="tag">{selectedTransport.protocol_id || "protocol n/a"}</span>
              {selectedTransport.supports_sse ? <span className="tag">SSE</span> : null}
              {selectedTransport.supports_websocket ? <span className="tag">WebSocket</span> : null}
              {selectedTransport.supports_rpc ? <span className="tag">RPC</span> : null}
              {selectedTransport.requires_hostd_relay ? <span className="tag is-warning">Relay</span> : null}
            </div>
          </section>
        ) : null}

        <div className="protocol-registry-grid">
          {items.length === 0 ? (
            <EmptyState title="No protocol registry data" />
          ) : (
            items.map((item) => {
              const capabilities = item.capabilities || {};
              const capabilityLabels = Object.entries(capabilities)
                .filter(([, value]) => value === true)
                .map(([key]) => key.replace(/^supports_/, "").replaceAll("_", " "));
              return (
                <article className="list-card protocol-card" key={item.protocol_id}>
                  <div className="card-row">
                    <strong>{item.protocol_id}</strong>
                    <span className={`tag ${item.status === "active" ? "is-healthy" : "is-booting"}`}>
                      {item.status || "unknown"}
                    </span>
                  </div>
                  <div className="metric-grid">
                    <div className="metric-row"><span>Owner</span><strong>{item.owner || "n/a"}</strong></div>
                    <div className="metric-row"><span>Transport</span><strong>{item.transport || "n/a"}</strong></div>
                    <div className="metric-row"><span>Latency</span><strong>{item.latency_class || "n/a"}</strong></div>
                    <div className="metric-row"><span>Timeout</span><strong>{item.timeout || "n/a"}</strong></div>
                  </div>
                  <div className="list-detail">
                    <div><strong>Fallback:</strong> {item.fallback || "n/a"}</div>
                    <div><strong>SLO:</strong> {item.slo || "n/a"}</div>
                  </div>
                  {capabilityLabels.length > 0 ? (
                    <div className="transport-badge-row">
                      {capabilityLabels.map((label) => (
                        <span className="tag" key={`${item.protocol_id}:${label}`}>{label}</span>
                      ))}
                    </div>
                  ) : null}
                </article>
              );
            })
          )}
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
      "en";
    const options = supportedChatLanguageOptions(desiredState, interactionStatus);
    const allowed = options.some((option) => option.value === nextDefault)
      ? nextDefault
      : options[0]?.value || "en";
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
    const detectedSourceFormat = detectModelSourceFormat(modelDownloadForm.sourceUrls);
    if (modelDownloadForm.formatLocked) {
      return;
    }
    if (modelDownloadForm.format === detectedSourceFormat) {
      return;
    }
    setModelDownloadForm((current) => ({
      ...current,
      format: detectedSourceFormat,
    }));
  }, [modelDownloadForm.sourceUrls, modelDownloadForm.format, modelDownloadForm.formatLocked]);

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
    if (!llmPlane && (selectedTab === "interaction" || selectedTab === "knowledge")) {
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

  const streamLabel = streamHealthy ? "Events live" : "Events reconnecting";
  const streamTitle = selectedPlane
    ? `Global controller event stream; refreshing fleet and selected plane ${selectedPlane}.`
    : "Global controller event stream; refreshing fleet data.";
  const lastRefreshTitle = `Last refresh: ${formatTimestamp(lastRefreshAt)}`;
  const lastEventTitle = `Last event: ${lastEventName || "none"}`;

  return (
    <div className="app-shell">
      <div className="starfield" aria-hidden="true" />
      <header className="hero">
        <div className="hero-copy">
          <div className="eyebrow">Naim Operator Interface</div>
          <h1>Constellation Control</h1>
          <p className="hero-text">
            Multi-plane control surface for lifecycle, rollout pressure, runtime
            readiness, and live controller telemetry.
          </p>
        </div>
        <div className="hero-meta">
          <div className="meta-card hero-status-card">
            <span className="hero-status-user" title="Signed in as">
              {authState.user?.username || "n/a"}
              <span>{authState.user?.role || "n/a"}</span>
            </span>
            <span className="status-chip" title={apiError || "Controller API status"}>
              {statusDot(apiHealthy ? "is-healthy" : apiError ? "is-critical" : "is-booting")}
              <span>{apiHealthy ? "API" : apiError ? "API error" : "API..."}</span>
            </span>
            <span className="status-chip" title={streamTitle}>
              {statusDot(streamHealthy ? "is-healthy" : selectedPlane ? "is-warning" : "is-booting")}
              <span>{streamLabel}</span>
            </span>
            <span className="hero-status-time" title={lastRefreshTitle}>
              Refresh {formatTimeOfDay(lastRefreshAt)}
            </span>
            <span className="hero-status-time" title={lastEventTitle}>
              Event {lastEventName || "none"}
            </span>
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
              className={`side-menu-item ${selectedPage === "knowledge-vault" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("knowledge-vault")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Knowledge Vault</span>
                <span className="side-menu-meta">{knowledgeVaultNavMeta}</span>
              </div>
              <span className={`tag ${knowledgeVaultNavClass}`}>
                {statusDot(knowledgeVaultNavClass)}
                <span>{knowledgeGraphSummary.nodeCount} items</span>
              </span>
            </button>
            <button
              className={`side-menu-item ${selectedPage === "protocols" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("protocols")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Protocols</span>
                <span className="side-menu-meta">Network registry and plane transport</span>
              </div>
              <span className="tag">
                <span>{protocolRegistry.summary?.total ?? protocolRegistry.items?.length ?? 0} items</span>
              </span>
            </button>
            <button
              className={`side-menu-item ${selectedPage === "skills-factory" ? "is-active" : ""}`}
              type="button"
              onClick={() => setSelectedPage("skills-factory")}
            >
              <div className="side-menu-copy">
                <span className="side-menu-title">Skills Factory</span>
                <span className="side-menu-meta">
                  {`${skillsFactory.items.length} canonical skill${skillsFactory.items.length === 1 ? "" : "s"}`}
                </span>
              </div>
              <span className={`tag ${skillsFactory.items.length > 0 ? "is-healthy" : "is-booting"}`}>
                {statusDot(skillsFactory.items.length > 0 ? "is-healthy" : "is-booting")}
                <span>{skillsFactory.items.length} items</span>
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
        ) : selectedPage === "knowledge-vault" ? (
          renderKnowledgeVaultPage()
        ) : selectedPage === "protocols" ? (
          renderProtocolsPage()
        ) : selectedPage === "skills-factory" ? (
          renderSkillsFactoryPage()
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
                    label="Healthy hosts"
                    value={globalObservationSummary.healthyNodes}
                    meta={`${globalObservationItems.length} observed / ${Math.max(0, globalObservationItems.length - globalObservationSummary.healthyNodes)} degraded`}
                    history={serverHealthyHostsHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Healthy hosts",
                        serverHealthyHostsHistory,
                        "Observed healthy hosts over time.",
                        null,
                        "server.healthy_hosts",
                      )
                    }
                  />
                  <SummaryCard
                    label="CPU load"
                    value={
                      globalObservationSummary.cpuLoadHostCount > 0
                        ? globalObservationSummary.totalCpuLoad1m.toFixed(2)
                        : "n/a"
                    }
                    meta={`${globalObservationSummary.cpuLoadHostCount} hosts reporting / 1m load average`}
                    history={serverCpuLoadHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "CPU load",
                        serverCpuLoadHistory,
                        "Aggregate 1-minute CPU load across observed hosts.",
                        (value) => Number(value).toFixed(2),
                        "server.cpu_load_1m",
                      )
                    }
                  />
                  <SummaryCard
                    label="Memory used"
                    value={formatDashboardBytesMbGb(globalObservationSummary.usedMemoryBytes)}
                    meta={`of ${formatDashboardBytesMbGb(globalObservationSummary.totalMemoryBytes)} total`}
                    history={serverMemoryHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Memory used",
                        serverMemoryHistory,
                        "Aggregate used host memory over time.",
                        (value) => formatChartGigabytesFromBytes(value),
                        "server.used_memory_bytes",
                      )
                    }
                  />
                  <SummaryCard
                    label="GPU VRAM used"
                    value={formatDashboardMegabytesMbGb(globalObservationSummary.usedGpuVramMb)}
                    meta={
                      globalObservationSummary.totalGpuVramMb > 0
                        ? `${formatDashboardMegabytesMbGb(globalObservationSummary.totalGpuVramMb)} total across ${globalObservationSummary.gpuDeviceCount} GPUs`
                        : "no GPU telemetry"
                    }
                    history={serverGpuVramHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "GPU VRAM used",
                        serverGpuVramHistory,
                        "Aggregate used GPU VRAM across observed hosts.",
                        (value) => formatChartGigabytesFromMegabytes(value),
                        "server.used_gpu_vram_mb",
                      )
                    }
                  />
                  <SummaryCard
                    label="CPU temp"
                    value={
                      globalObservationSummary.cpuTemperatureHostCount > 0
                        ? formatTemperature(globalObservationSummary.maxCpuTemperatureC)
                        : "n/a"
                    }
                    meta={`${globalObservationSummary.cpuTemperatureHostCount} hosts reporting`}
                    history={serverCpuTempHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "CPU temperature",
                        serverCpuTempHistory,
                        "Highest reported CPU temperature across observed hosts.",
                        (value) => formatTemperature(value),
                        "server.max_cpu_temp_c",
                      )
                    }
                  />
                  <SummaryCard
                    label="GPU temp"
                    value={
                      globalObservationSummary.gpuTemperatureDeviceCount > 0
                        ? formatTemperature(globalObservationSummary.maxGpuTemperatureC)
                        : "n/a"
                    }
                    meta={`${globalObservationSummary.gpuTemperatureDeviceCount} GPU devices reporting`}
                    history={serverGpuTempHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "GPU temperature",
                        serverGpuTempHistory,
                        "Highest reported GPU temperature across observed devices.",
                        (value) => formatTemperature(value),
                        "server.max_gpu_temp_c",
                      )
                    }
                  />
                  <SummaryCard
                    label="Network tx"
                    value={formatBytesPerSecond(latestRatePerSecond(serverNetworkTxHistory))}
                    meta={`${compactBytes(globalObservationSummary.networkTxBytes)} total transmitted`}
                    history={serverNetworkTxHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Network transmitted bytes",
                        serverNetworkTxHistory,
                        "Aggregate transmitted network bytes across observed hosts.",
                        (value) => compactBytes(value),
                        "server.network_tx_bytes",
                      )
                    }
                  />
                  <SummaryCard
                    label="Disk writes"
                    value={formatBytesPerSecond(latestRatePerSecond(serverDiskWriteHistory))}
                    meta={`${compactBytes(globalObservationSummary.diskWriteBytes)} total written`}
                    history={serverDiskWriteHistory}
                    onOpenTrend={() =>
                      openTelemetryChart(
                        "Disk written bytes",
                        serverDiskWriteHistory,
                        "Aggregate written disk bytes across observed hosts.",
                        (value) => compactBytes(value),
                        "server.disk_write_bytes",
                      )
                    }
                  />
                </div>
              </section>

              <section className="subpanel dashboard-services-panel">
                <div className="subpanel-header">
                  <h3>LAN peer links</h3>
                  <span className="subpanel-meta">
                    {`${peerLinkSummary.direct || 0} direct / ${peerLinkSummary.partial || 0} partial / ${peerLinkSummary.stale || 0} stale`}
                  </span>
                </div>
                {peerLinkItems.length === 0 ? (
                  <EmptyState
                    title="No LAN peers"
                    detail="Waiting for hostd peer discovery telemetry."
                  />
                ) : (
                  <div className="plane-list">
                    {peerLinkItems.slice(0, 8).map((link) => (
                      <article
                        className="node-card"
                        key={`${link?.observer_node_name}:${link?.peer_node_name}`}
                      >
                        <div className="card-row">
                          <strong>{link?.observer_node_name || "node"} to {link?.peer_node_name || "peer"}</strong>
                          <div className={`pill ${link?.state === "direct" ? "is-healthy" : link?.state === "partial" ? "is-warning" : "is-muted"}`}>
                            {statusDot(link?.state === "direct" ? "is-healthy" : link?.state === "partial" ? "is-warning" : "is-muted")}
                            <span>{link?.state || "unknown"}</span>
                          </div>
                        </div>
                        <div className="metric-grid">
                          <div className="metric-row"><span>Endpoint</span><strong>{link?.peer_endpoint || "n/a"}</strong></div>
                          <div className="metric-row"><span>Remote</span><strong>{link?.remote_address || "n/a"}</strong></div>
                          <div className="metric-row"><span>Interface</span><strong>{link?.local_interface || "n/a"}</strong></div>
                          <div className="metric-row"><span>RTT</span><strong>{link?.rtt_ms ? `${link.rtt_ms} ms` : "n/a"}</strong></div>
                        </div>
                      </article>
                    ))}
                  </div>
                )}
              </section>

              <section className="subpanel dashboard-hosts-panel">
                <div className="subpanel-header">
                  <h3>Naim nodes</h3>
                  <span className="subpanel-meta">
                    Registered and observed nodes with roles, capacity, LAN peers, and hostd heartbeat.
                  </span>
                </div>
                {dashboardHostItems.length === 0 ? (
                  <EmptyState title="No connected hosts" detail="Waiting for hostd registration or telemetry." />
                ) : (
                  renderHostCards(dashboardHostItems)
                )}
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
      {renderHostOverviewModal(selectedHostOverview)}
      <TelemetryChartDialog
        chart={{
          ...telemetryChart,
          data: resolvedTelemetryChartData,
        }}
        onClose={() =>
          setTelemetryChart({
            open: false,
            title: "",
            meta: "",
            historyKey: "",
            data: [],
            valueFormatter: null,
          })
        }
      />
      <PlaneEditorDialog
        dialog={planeDialog}
        setDialog={setPlaneDialog}
        onClose={() =>
          setPlaneDialog({
            open: false,
            mode: "new",
            planeName: "",
            text: "",
            form: null,
            originalSkillsEnabled: false,
            busy: false,
            error: "",
          })
        }
        onSave={savePlaneDialog}
        modelLibraryItems={modelLibrary.items || []}
        hostdHosts={hostdHosts || []}
        peerLinks={dashboard?.peer_links || null}
        skillsFactoryItems={skillsFactory.items || []}
        skillsFactoryGroups={skillsFactory.groups || []}
        onResetLtCypherDeployment={resetLtCypherDeployment}
      />
      <SkillsDialog
        dialog={skillsDialog}
        onClose={closeSkillsDialog}
        onRefresh={() => refreshSkillsDialog()}
        onStartCreate={startCreateSkill}
        onEdit={startEditSkill}
        onFormChange={updateSkillFormField}
        onSave={saveSkillForm}
        onToggle={toggleSkill}
        onDelete={deleteSkill}
      />
    </div>
  );
}

export default App;
