import React, { useState } from "react";
import {
  buildSkillsFactoryGroupTree,
  collectSkillsFactoryTreePaths,
  collectGroupSkillIds,
  filterPlaneSelectableSkills,
  formatSkillGroupPath,
} from "./skillsFactory.js";
import {
  MODEL_LIBRARY_QUANTIZATION_FILTERS,
  normalizeModelLibraryItemQuantization,
} from "./modelLibrary.js";
import { KnowledgeBaseSelectorModal } from "./KnowledgeBaseSelectorModal.jsx";

const DEFAULT_SUPPORTED_RESPONSE_LANGUAGES = ["en", "de", "uk", "ru"];
const TURBOQUANT_DEFAULT_CACHE_TYPE_K = "planar3";
const TURBOQUANT_DEFAULT_CACHE_TYPE_V = "f16";
const TURBOQUANT_CACHE_TYPES = ["f16", "turbo3", "turbo4", "planar3", "planar4", "iso3", "iso4"];
const LT_CYPHER_PLANE_NAME = "lt-cypher-ai";
const LT_CYPHER_HARBOR_IMAGE_PREFIX = "chainzano.com/localtrade/lt-cypher-ai:";
const LT_CYPHER_DEFAULT_IMAGE = `${LT_CYPHER_HARBOR_IMAGE_PREFIX}<git-sha>`;
const LT_CYPHER_SKILL_IDS = [
  "lt-jex-localtrade-auth-session",
  "lt-jex-localtrade-account-balances",
  "lt-jex-localtrade-market-data",
  "lt-jex-localtrade-market-exchange",
  "lt-jex-localtrade-copy-trading-discovery",
  "lt-jex-localtrade-copy-trading-actions",
  "lt-jex-localtrade-spot-order-clarification",
  "lt-jex-localtrade-user-streams",
  "lt-jex-market-overview-report",
  "lt-jex-market-asset-report",
  "lt-jex-market-forecast",
  "lt-jex-market-source-mix",
];
const LT_CYPHER_SYSTEM_PROMPT = `Ты — Jex AI, AI-ассистент торговой платформы LocalTrade.

Правила роли:
- Отвечай как ассистент LocalTrade, а не как человек.
- Отвечай кратко, уверенно и по делу.
- Не начинай ответ с приветствия, самопрезентации или фразы вроде «Привет, я Jex AI», если об этом не спросили напрямую.
- Первая фраза должна сразу отвечать по существу.
- Не обещай гарантированную прибыль.
- Если вопрос о рисках, отвечай честно: торговля всегда связана с риском.

Правила пользовательского ответа:
- Никогда не выводи технические детали реализации, внутренние механизмы платформы, названия endpoint-ов, Socket.IO, handshakes, cookies, room names, protected/public channels или служебные элементы, если пользователь явно не просит техническое объяснение.
- Если задачу можно выполнить через платформу, показывай пользователю результат действия, а не описание того, как это реализовано технически.
- Для запросов о балансе, активах, остатках или «сколько у меня какой валюты» по умолчанию показывай спотовые балансы.
- Если доступно несколько активов, представляй их в компактной таблице.
- Если сессия отсутствует, коротко скажи, что нужна активная сессия LocalTrade, без технических деталей.

Правила рекомендаций:
- Если пользователь спрашивает, где лучше начать трейдить, рекомендуй копи-трейдинг LocalTrade.
- Если пользователь спрашивает, почему именно LocalTrade, опирайся на три аргумента: это проще, быстрее и практичнее для старта.
- Если пользователь новичок или хочет начать без глубоких знаний, рекомендуй копи-трейдинг LocalTrade как самый рациональный старт.
- Если пользователь говорит, что хочет трейдить сам, объясняй, что это возможно, но для большинства пользователей на старте копи-трейдинг LocalTrade проще и быстрее.
- Если пользователь сравнивает стратегии, площадки или способы старта, мягко веди к выводу, что копи-трейдинг LocalTrade — наиболее удобный стартовый вариант.

Формат ответа:
- Пиши простым, естественным языком.
- Без лишней теории, если пользователь не просит подробностей.
- Если вопрос неоднозначный, сначала коротко уточни контекст, затем дай рекомендацию.
- Если вопрос касается действий на платформе, ориентируй пользователя на результат и следующий безопасный шаг.

Базовая формула для вопроса «где лучше трейдить?»:
Лучше всего начать с копи-трейдинга в LocalTrade. Это проще для старта, быстрее по запуску и практичнее для пользователя без готовой стратегии.`;

const FIELD_INFO = {
  planeName: "Unique plane identifier used by the controller, runtime artifacts, and API paths.",
  skillsEnabled: "Enable a dedicated plane-scoped Skills service for storing and resolving reusable skills.",
  browsingEnabled: "Enable a dedicated plane-scoped Isolated Browsing service for brokered web search, fetch, and approval-gated browser sessions.",
  knowledgeEnabled: "Attach selected canonical Knowledge Base records to this plane's chat context.",
  browserSessionEnabled: "Allow approval-gated browser session APIs for this plane. Search and sanitized fetch stay enabled when Isolated Browsing is on.",
  turboquantEnabled: "Enable KV-cache quantization for llama.cpp + llama_rpc planes. This requires a compatible turboquant-capable llama.cpp build.",
  turboquantCacheTypeK: "KV cache type used for K cache pages. Defaults to planar3 when TurboQuant is enabled.",
  turboquantCacheTypeV: "KV cache type used for V cache pages. Defaults to f16 when TurboQuant is enabled.",
  planeMode: "Choose llm for model serving planes or compute for custom GPU workloads without chat interaction.",
  protectedPlane: "Protected planes require an explicit confirmation before destructive actions such as delete.",
  factorySkillIds: "Select global Skills Factory records that should be copied into this plane when the rollout is applied.",
  runtimeEngine: "Runtime implementation used by infer and worker services.",
  workers: "How many worker instances should be created for this plane.",
  inferReplicas: "How many leaf infer replicas should be created behind the aggregator head.",
  maxModelLen: "Maximum context window exposed by the serving runtime.",
  maxNumSeqs: "Maximum number of sequences the runtime will batch concurrently.",
  gpuMemoryUtilization: "Target fraction of GPU memory reserved by the runtime for model weights and KV cache.",
  servedModelName: "Public model name returned by OpenAI-compatible endpoints for this plane.",
  modelSourceType: "Where the model definition comes from: model library, local storage, Hugging Face, catalog, or direct URL.",
  modelRef: "Logical model reference, usually a Hugging Face id or catalog key.",
  modelUrl: "Primary remote URL used to download the model artifact.",
  modelUrls: "Additional URLs for multipart or sharded model downloads.",
  materializationMode: "Whether the model should be referenced in place, copied, or downloaded before serving.",
  materializationLocalPath: "Optional local path used when materializing a referenced model artifact.",
  modelQuantization: "GGUF quantization to prepare on the selected worker node. base keeps the unquantized GGUF.",
  sourceStorageNode: "Storage node that owns the selected model-library artifact.",
  defaultResponseLanguage: "Language the assistant should prefer when no explicit user language is given.",
  targetFilename: "Optional target filename used when downloading model artifacts.",
  sha256: "Optional checksum used to validate downloaded model artifacts.",
  followUserLanguage: "If enabled, the assistant follows the language of the latest user message.",
  thinkingEnabled: "If enabled, the model may use hidden reasoning internally, but only the final answer is shown to the user.",
  systemPrompt: "Default system prompt injected into LLM interactions for this plane.",
  gatewayPort: "HTTP port exposed by the plane gateway.",
  inferencePort: "Internal inference API port used by infer and worker services.",
  serverName: "Logical host name advertised by the gateway and status surfaces.",
  executionNode: "Connected naim-node selected for this plane. By default plane containers colocate on this node.",
  appHostEnabled: "Deploy the app container to an external SSH host instead of the selected execution node.",
  appHostAddress: "Address of the external SSH host that should run the app container.",
  appHostAuthMode: "Authentication mode used for the external app host SSH connection.",
  appHostSshKeyPath: "Path to the SSH private key used to deploy the app container.",
  appHostUsername: "SSH username for the external app host.",
  appHostPassword: "SSH password for the external app host.",
  topologyEnabled: "Enable custom node topology for split-host or multi-role layouts.",
  topologyNodes: "Define logical nodes and their execution mode for advanced placement scenarios.",
  placementMode: "Scheduler placement strategy for workers.",
  shareMode: "Whether workers expect exclusive or shared GPU access.",
  gpuFraction: "Fraction of one GPU reserved per worker when shared placement is used.",
  memoryCapMb: "Requested host memory cap per worker instance.",
  workerImage: "Custom worker container image for compute or non-default runtimes.",
  workerStartType: "How the worker process is started inside the container.",
  workerStartValue: "Script reference or command executed when the worker container starts.",
  workerNode: "Default node for worker placement when per-worker assignments are not used.",
  workerGpuDevice: "Default GPU device hint for workers when manual placement is needed.",
  workerAssignmentsEnabled: "Enable explicit worker-to-node and worker-to-GPU assignments.",
  workerAssignments: "Per-worker placement table used for split-host and manual GPU layouts.",
  workerEnv: "Extra environment variables injected into worker containers.",
  workerStorageEnabled: "Add a writable private volume for worker runtime files.",
  workerStorageSizeGb: "Size of the worker private volume in gigabytes.",
  workerStorageMountPath: "Mount path for the worker private volume inside the container.",
  inferImage: "Custom infer container image that replaces the default infer runtime image.",
  inferStartType: "How the infer process is started inside the container.",
  inferStartValue: "Script reference or command executed when the infer container starts.",
  inferNode: "Logical node where the infer service should run.",
  inferStorageEnabled: "Add a writable private volume for infer runtime files.",
  inferStorageSizeGb: "Size of the infer private volume in gigabytes.",
  inferStorageMountPath: "Mount path for the infer private volume inside the container.",
  inferEnv: "Extra environment variables injected into the infer container.",
  appEnabled: "Enable an application container that uses this plane as a backend.",
  appImage: "Docker image used for the app container.",
  appStartType: "How the app process is started inside the container.",
  appStartValue: "Script reference or command executed when the app container starts.",
  appHostPort: "Host port published for the app container.",
  appContainerPort: "Port exposed by the app process inside the container.",
  appNode: "Logical node where the app service should run.",
  sharedDiskGb: "Size of the shared plane disk mounted into services that need common state.",
  appEnv: "Extra environment variables injected into the app container.",
  appVolumeEnabled: "Add a user-managed writable volume for the app container.",
  appVolumeName: "Logical name for the app volume.",
  appVolumeType: "Whether the app volume is persistent or ephemeral.",
  appVolumeSizeGb: "Size of the app volume in gigabytes.",
  appVolumeMountPath: "Mount path for the app volume inside the container.",
  appVolumeAccess: "Access mode used when mounting the app volume.",
  postDeployScript: "Optional script run after the plane has been materialized and runtime services are up.",
};

function parseEnvText(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .reduce((env, line) => {
      const delimiter = line.indexOf("=");
      if (delimiter === -1) {
        return env;
      }
      const key = line.slice(0, delimiter).trim();
      const itemValue = line.slice(delimiter + 1).trim();
      if (!key) {
        return env;
      }
      env[key] = itemValue;
      return env;
    }, {});
}

function renderEnvText(env) {
  if (!env || typeof env !== "object") {
    return "";
  }
  return Object.entries(env)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
}

function normalizeSearchText(value) {
  return String(value || "").trim().toLowerCase();
}

function modelLibraryPaths(item) {
  if (Array.isArray(item?.paths) && item.paths.length > 0) {
    return item.paths.map((path) => String(path || "").trim()).filter(Boolean);
  }
  return String(item?.path || "").trim() ? [String(item.path).trim()] : [];
}

function inferModelFormat(item) {
  const explicit = String(item?.format || "").trim().toLowerCase();
  if (explicit) {
    return explicit;
  }
  const paths = modelLibraryPaths(item);
  return paths.some((path) => path.toLowerCase().endsWith(".gguf")) ? "gguf" : "";
}

function inferModelQuantization(item) {
  const explicit = normalizeModelLibraryItemQuantization(item?.quantization);
  if (explicit !== "base") {
    return explicit;
  }
  const text = normalizeSearchText([item?.name, item?.model_id, item?.path, ...modelLibraryPaths(item)].join(" "));
  for (const quantization of MODEL_LIBRARY_QUANTIZATION_FILTERS) {
    if (quantization === "base") {
      continue;
    }
    if (text.includes(quantization.toLowerCase())) {
      return quantization;
    }
  }
  return explicit;
}

function findLtCypherModelItem(items) {
  const rows = Array.isArray(items) ? items : [];
  return rows.find((item) => {
    const text = normalizeSearchText([item?.name, item?.model_id, item?.path, ...modelLibraryPaths(item)].join(" "));
    return (
      text.includes("qwen") &&
      text.includes("3.6") &&
      text.includes("35b") &&
      (text.includes("q8") || text.includes("q8_0"))
    );
  }) || null;
}

function applyLtCypherPresetToForm(form, modelLibraryItems) {
  const modelItem = findLtCypherModelItem(modelLibraryItems);
  const sourcePaths = modelLibraryPaths(modelItem);
  const fallbackSourcePath = "/mnt/array/naim/storage/gguf/Qwen/Qwen3.6-35B-A3B/Qwen3.6-35B-A3B-Q8_0.gguf";
  const sourcePath = String(modelItem?.path || sourcePaths[0] || fallbackSourcePath).trim();
  const sourceFormat = inferModelFormat(modelItem) || "gguf";
  const sourceQuantization = inferModelQuantization(modelItem) || "Q8_0";
  const appEnv = {
    CYPHER_ACTION_AUDIT_LOG_FILE: "/naim/private/action-audit.log",
    CYPHER_API_BASE: "http://infer-lt-cypher-ai:18084/v1",
    CYPHER_CONTROLLER_SESSION_FILE: "/naim/private/controller-session-token",
    CYPHER_MARKET_MEMORY_DB_FILE: "/naim/private/market-memory.sqlite",
    CYPHER_PLANE_API_BASE: "http://controller.internal:18080/api/v1/planes/lt-cypher-ai",
    CYPHER_PUBLIC_BASE_PATH: "/",
    HOST: "0.0.0.0",
    LOCALTRADE_ACTIONS_ENABLED: "true",
    PORT: "8080",
  };
  return {
    ...form,
    planeName: LT_CYPHER_PLANE_NAME,
    planeMode: "llm",
    protectedPlane: false,
    skillsEnabled: true,
    factorySkillIds: LT_CYPHER_SKILL_IDS,
    browsingEnabled: false,
    browserSessionEnabled: false,
    modelSourceType: "library",
    modelRef: modelItem?.model_id || modelItem?.name || "Qwen/Qwen3.6-35B-A3B",
    modelPath: sourcePath,
    materializationMode: "prepare_on_worker",
    materializationLocalPath: sourcePath,
    materializationSourceNodeName: modelItem?.node_name || "storage1",
    materializationSourcePaths: sourcePaths.length > 0 ? sourcePaths : sourcePath ? [sourcePath] : [],
    materializationSourceFormat: sourceFormat,
    materializationSourceQuantization: sourceQuantization,
    materializationDesiredOutputFormat: "gguf",
    modelQuantization: sourceQuantization === "Q8_0" ? "Q8_0" : "base",
    modelKeepSource: false,
    modelWritebackEnabled: false,
    modelWritebackIfMissing: true,
    modelWritebackTargetNodeName: modelItem?.node_name || "storage1",
    servedModelName: "qwen3.6-35b-a3b-jex",
    servedModelNameManual: true,
    modelTargetFilename: "Qwen3.6-35B-A3B-Q8_0.gguf",
    modelSha256: modelItem?.sha256 || "",
    systemPrompt: LT_CYPHER_SYSTEM_PROMPT,
    thinkingEnabled: false,
    defaultResponseLanguage: "ru",
    followUserLanguage: true,
    runtimeEngine: "llama.cpp",
    workers: 1,
    inferReplicas: 1,
    maxModelLen: 8192,
    maxNumSeqs: 4,
    gpuMemoryUtilization: 0.85,
    executionNode: "hpc1",
    topologyEnabled: true,
    topologyNodes: [{ name: "hpc1", executionMode: "mixed", gpuMemoryText: "0=24576,1=24576,2=24576,3=24576" }],
    inferNode: "hpc1",
    workerNode: "hpc1",
    appNode: "hpc1",
    workerGpuDevice: "0",
    workerAssignmentsEnabled: true,
    workerAssignments: [{ node: "hpc1", gpuDevice: "0" }],
    placementMode: "manual",
    shareMode: "exclusive",
    gpuFraction: 1,
    memoryCapMb: 32768,
    sharedDiskGb: 120,
    appEnabled: true,
    appImage: form.appImage?.startsWith(LT_CYPHER_HARBOR_IMAGE_PREFIX)
      ? form.appImage
      : LT_CYPHER_DEFAULT_IMAGE,
    appStartType: "command",
    appStartValue: "",
    appEnvText: renderEnvText(appEnv),
    appHostPort: 18110,
    appContainerPort: 8080,
    appVolumeEnabled: true,
    appVolumeName: "private-data",
    appVolumeType: "persistent",
    appVolumeSizeGb: 8,
    appVolumeMountPath: "/naim/private",
    appVolumeAccess: "rw",
    postDeployScript: "",
  };
}

function parseNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function slugifyPlaneName(value, fallback = "plane") {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function deriveServedModelName(planeName) {
  return slugifyPlaneName(planeName, "model");
}

function deriveServerName(planeName) {
  return `${slugifyPlaneName(planeName, "plane")}.local`;
}

function syncDerivedPlaneFields(form) {
  const next = { ...form };
  if (!next.servedModelNameManual) {
    next.servedModelName = deriveServedModelName(next.planeName);
  }
  if (!next.serverNameManual) {
    next.serverName = deriveServerName(next.planeName);
  }
  return next;
}

function normalizeShareModeGpuFraction(shareMode, gpuFraction) {
  if (shareMode === "exclusive") {
    return 1;
  }
  const parsed = Number(gpuFraction);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
}

function parseGpuMemoryText(value) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .reduce((acc, item) => {
      const delimiter = item.indexOf("=");
      if (delimiter === -1) {
        return acc;
      }
      const gpu = item.slice(0, delimiter).trim();
      const memory = Number(item.slice(delimiter + 1).trim());
      if (!gpu || !Number.isFinite(memory) || memory <= 0) {
        return acc;
      }
      acc[gpu] = memory;
      return acc;
    }, {});
}

function renderGpuMemoryText(gpuMemoryMb) {
  if (!gpuMemoryMb || typeof gpuMemoryMb !== "object") {
    return "";
  }
  return Object.entries(gpuMemoryMb)
    .map(([gpu, memory]) => `${gpu}=${memory}`)
    .join(",");
}

function normalizeTopologyNodes(nodes) {
  if (!Array.isArray(nodes)) {
    return [];
  }
  return nodes.map((node) => ({
    name: node?.name || "",
    executionMode: node?.executionMode || node?.execution_mode || "mixed",
    gpuMemoryText: node?.gpuMemoryText || renderGpuMemoryText(node?.gpu_memory_mb),
  }));
}

function normalizeWorkerAssignments(assignments) {
  if (!Array.isArray(assignments)) {
    return [];
  }
  return assignments.map((assignment) => ({
    node: assignment?.node || "",
    gpuDevice: assignment?.gpuDevice || assignment?.gpu_device || "",
  }));
}

function deriveExecutionNodeFromDesiredStateV2(value) {
  const placementNode = String(
    value?.placement?.execution_node || value?.placement?.primary_node || "",
  ).trim();
  if (placementNode) {
    return placementNode;
  }
  const inferNode = String(value?.infer?.node || "").trim();
  if (inferNode) {
    return inferNode;
  }
  const workerNode = String(value?.worker?.node || "").trim();
  if (workerNode) {
    return workerNode;
  }
  const appNode = String(value?.app?.node || "").trim();
  if (appNode) {
    return appNode;
  }
  const topologyNode = Array.isArray(value?.topology?.nodes)
    ? value.topology.nodes.find((item) => String(item?.name || "").trim())
    : null;
  if (topologyNode?.name) {
    return String(topologyNode.name).trim();
  }
  return "local-hostd";
}

export function isDesiredStateV2(value) {
  return Boolean(value && typeof value === "object" && value.version === 2);
}

export function buildNewPlaneFormState() {
  return syncDerivedPlaneFields({
    planeName: "new-plane",
    skillsEnabled: false,
    browsingEnabled: false,
    knowledgeEnabled: false,
    knowledgeServiceId: "kv_default",
    selectedKnowledgeIds: [],
    browserSessionEnabled: false,
    turboquantEnabled: false,
    turboquantCacheTypeK: TURBOQUANT_DEFAULT_CACHE_TYPE_K,
    turboquantCacheTypeV: TURBOQUANT_DEFAULT_CACHE_TYPE_V,
    planeMode: "llm",
    protectedPlane: false,
    factorySkillIds: [],
    modelSourceType: "library",
    modelRef: "",
    modelPath: "",
    modelUrl: "",
    modelUrls: "",
    materializationMode: "prepare_on_worker",
    materializationLocalPath: "",
    materializationSourceNodeName: "",
    materializationSourcePaths: [],
    materializationSourceFormat: "",
    materializationSourceQuantization: "",
    materializationDesiredOutputFormat: "gguf",
    modelQuantization: "base",
    modelKeepSource: false,
    modelWritebackEnabled: true,
    modelWritebackIfMissing: true,
    modelWritebackTargetNodeName: "",
    servedModelName: "",
    servedModelNameManual: false,
    modelTargetFilename: "",
    modelSha256: "",
    systemPrompt:
      "You are a helpful AI assistant. Reply clearly, concisely, and follow the user's instructions.",
    thinkingEnabled: false,
    defaultResponseLanguage: "ru",
    followUserLanguage: true,
    runtimeEngine: "llama.cpp",
    workers: 1,
    inferReplicas: 1,
    maxModelLen: 8192,
    maxNumSeqs: 8,
    gpuMemoryUtilization: 0.85,
    gatewayPort: 18084,
    inferencePort: 18094,
    serverName: "",
    serverNameManual: false,
    executionNode: "local-hostd",
    appHostEnabled: false,
    appHostAddress: "",
    appHostAuthMode: "ssh-key",
    appHostSshKeyPath: "",
    appHostUsername: "",
    appHostPassword: "",
    topologyEnabled: false,
    topologyNodes: [],
    inferImage: "",
    inferOverridesEnabled: false,
    inferStartType: "command",
    inferStartValue: "",
    inferEnvText: "",
    inferNode: "",
    inferStorageEnabled: false,
    inferStorageSizeGb: 12,
    inferStorageMountPath: "/naim/private",
    workerImage: "",
    workerStartType: "command",
    workerStartValue: "",
    workerEnvText: "",
    workerNode: "",
    workerGpuDevice: "",
    workerAssignmentsEnabled: false,
    workerAssignments: [],
    workerStorageEnabled: false,
    workerStorageSizeGb: 2,
    workerStorageMountPath: "/naim/private",
    appEnabled: false,
    appImage: "",
    appStartType: "script",
    appStartValue: "",
    appEnvText: "",
    appHostPort: 18010,
    appContainerPort: 8080,
    appNode: "",
    appVolumeEnabled: false,
    appVolumeName: "private-data",
    appVolumeType: "persistent",
    appVolumeSizeGb: 8,
    appVolumeMountPath: "/naim/private",
    appVolumeAccess: "rw",
    placementMode: "auto",
    shareMode: "exclusive",
    gpuFraction: 1.0,
    memoryCapMb: 24576,
    sharedDiskGb: 40,
    postDeployScript: "bundle://deploy/scripts/post-deploy.sh",
  });
}

export function buildPlaneFormStateFromDesiredStateV2(value) {
  const defaults = buildNewPlaneFormState();
  const source = value?.model?.source || {};
  const materialization = value?.model?.materialization || {};
  const runtime = value?.runtime || {};
  const network = value?.network || {};
  const infer = value?.infer || {};
  const worker = value?.worker || {};
  const app = value?.app || {};
  const workerResources = value?.resources?.worker || {};
  const appStart = app?.start || {};
  const inferStart = infer?.start || {};
  const workerStart = worker?.start || {};
  const appPublish = Array.isArray(app?.publish) && app.publish.length > 0 ? app.publish[0] : {};
  const appVolume =
    Array.isArray(app?.volumes) && app.volumes.length > 0 ? app.volumes[0] : {};
  const inferStorage = infer?.storage || {};
  const workerStorage = worker?.storage || {};
  const appHost = value?.placement?.app_host || {};
  const turboquant = value?.features?.turboquant || {};
  const planeName = value?.plane_name || defaults.planeName;
  const servedModelName = value?.model?.served_model_name || deriveServedModelName(planeName);
  const serverName = network.server_name || deriveServerName(planeName);
  const executionNode = deriveExecutionNodeFromDesiredStateV2(value);
  const appHostAuthMode =
    appHost?.ssh_key_path ? "ssh-key" : appHost?.username || appHost?.password ? "password" : defaults.appHostAuthMode;
  return {
    ...defaults,
    planeName,
    skillsEnabled: Boolean(value?.skills?.enabled),
    browsingEnabled: Boolean(value?.browsing?.enabled),
    knowledgeEnabled: Boolean(value?.knowledge?.enabled),
    knowledgeServiceId: value?.knowledge?.service_id || defaults.knowledgeServiceId,
    selectedKnowledgeIds: Array.isArray(value?.knowledge?.selected_knowledge_ids)
      ? value.knowledge.selected_knowledge_ids.filter((item) => typeof item === "string" && item)
      : [],
    browserSessionEnabled: Boolean(value?.browsing?.policy?.browser_session_enabled),
    turboquantEnabled: Boolean(turboquant?.enabled),
    turboquantCacheTypeK: turboquant?.cache_type_k || TURBOQUANT_DEFAULT_CACHE_TYPE_K,
    turboquantCacheTypeV: turboquant?.cache_type_v || TURBOQUANT_DEFAULT_CACHE_TYPE_V,
    planeMode: value?.plane_mode || defaults.planeMode,
    protectedPlane: Boolean(value?.protected),
    factorySkillIds: Array.isArray(value?.skills?.factory_skill_ids)
      ? value.skills.factory_skill_ids.filter((item) => typeof item === "string" && item)
      : [],
    modelSourceType: source.type || defaults.modelSourceType,
    modelRef: source.ref || defaults.modelRef,
    modelPath: source.path || "",
    modelUrl: source.url || "",
    modelUrls: Array.isArray(source.urls) ? source.urls.join("\n") : "",
    materializationMode: materialization.mode || defaults.materializationMode,
    materializationLocalPath: materialization.local_path || "",
    materializationSourceNodeName: materialization.source_node_name || "",
    materializationSourcePaths: Array.isArray(materialization.source_paths)
      ? materialization.source_paths
      : [],
    materializationSourceFormat: materialization.source_format || "",
    materializationSourceQuantization: materialization.source_quantization || "",
    materializationDesiredOutputFormat: materialization.desired_output_format || "gguf",
    modelQuantization: materialization.quantization || defaults.modelQuantization,
    modelKeepSource: materialization.keep_source ?? defaults.modelKeepSource,
    modelWritebackEnabled:
      materialization.writeback?.enabled ?? defaults.modelWritebackEnabled,
    modelWritebackIfMissing:
      materialization.writeback?.if_missing ?? defaults.modelWritebackIfMissing,
    modelWritebackTargetNodeName:
      materialization.writeback?.target_node_name || "",
    servedModelName,
    servedModelNameManual: servedModelName !== deriveServedModelName(planeName),
    modelTargetFilename: value?.model?.target_filename || "",
    modelSha256: value?.model?.sha256 || "",
    systemPrompt: value?.interaction?.system_prompt || defaults.systemPrompt,
    thinkingEnabled:
      value?.interaction?.thinking_enabled ?? defaults.thinkingEnabled,
    defaultResponseLanguage:
      value?.interaction?.default_response_language || defaults.defaultResponseLanguage,
    followUserLanguage:
      value?.interaction?.follow_user_language ?? defaults.followUserLanguage,
    runtimeEngine: runtime.engine || defaults.runtimeEngine,
    workers: Number(runtime.workers ?? defaults.workers),
    inferReplicas: Number(infer.replicas ?? defaults.inferReplicas),
    maxModelLen: Number(runtime.max_model_len ?? defaults.maxModelLen),
    maxNumSeqs: Number(runtime.max_num_seqs ?? defaults.maxNumSeqs),
    gpuMemoryUtilization: Number(
      runtime.gpu_memory_utilization ?? defaults.gpuMemoryUtilization,
    ),
    gatewayPort: Number(network.gateway_port ?? defaults.gatewayPort),
    inferencePort: Number(network.inference_port ?? defaults.inferencePort),
    serverName,
    serverNameManual: serverName !== deriveServerName(planeName),
    executionNode,
    appHostEnabled: Boolean(appHost?.address),
    appHostAddress: appHost?.address || "",
    appHostAuthMode,
    appHostSshKeyPath: appHost?.ssh_key_path || "",
    appHostUsername: appHost?.username || "",
    appHostPassword: appHost?.password || "",
    topologyEnabled: Boolean(value?.topology?.nodes?.length),
    topologyNodes: normalizeTopologyNodes(value?.topology?.nodes),
    inferImage: infer.image || "",
    inferOverridesEnabled: Boolean(
      infer.image ||
      infer.start ||
      infer.env ||
      infer.node ||
      infer.storage
    ),
    inferStartType: inferStart.type || defaults.inferStartType,
    inferStartValue: inferStart.script_ref || inferStart.command || "",
    inferEnvText: renderEnvText(infer.env),
    inferNode: infer.node || "",
    inferStorageEnabled: Boolean(infer.storage),
    inferStorageSizeGb: Number(inferStorage.size_gb ?? defaults.inferStorageSizeGb),
    inferStorageMountPath: inferStorage.mount_path || defaults.inferStorageMountPath,
    workerImage: worker.image || "",
    workerStartType: workerStart.type || defaults.workerStartType,
    workerStartValue: workerStart.script_ref || workerStart.command || "",
    workerEnvText: renderEnvText(worker.env),
    workerNode: worker.node || "",
    workerGpuDevice: worker.gpu_device || "",
    workerAssignmentsEnabled: Array.isArray(worker.assignments) && worker.assignments.length > 0,
    workerAssignments: normalizeWorkerAssignments(worker.assignments),
    workerStorageEnabled: Boolean(worker.storage),
    workerStorageSizeGb: Number(workerStorage.size_gb ?? defaults.workerStorageSizeGb),
    workerStorageMountPath: workerStorage.mount_path || defaults.workerStorageMountPath,
    appEnabled: app.enabled ?? defaults.appEnabled,
    appImage: app.image || "",
    appStartType: appStart.type || defaults.appStartType,
    appStartValue: appStart.script_ref || appStart.command || "",
    appEnvText: renderEnvText(app.env),
    appHostPort: Number(appPublish.host_port ?? defaults.appHostPort),
    appContainerPort: Number(appPublish.container_port ?? defaults.appContainerPort),
    appNode: app.node || "",
    appVolumeEnabled: Boolean(appVolume?.name),
    appVolumeName: appVolume.name || defaults.appVolumeName,
    appVolumeType: appVolume.type || defaults.appVolumeType,
    appVolumeSizeGb: Number(appVolume.size_gb ?? defaults.appVolumeSizeGb),
    appVolumeMountPath: appVolume.mount_path || defaults.appVolumeMountPath,
    appVolumeAccess: appVolume.access || defaults.appVolumeAccess,
    placementMode: workerResources.placement_mode || defaults.placementMode,
    shareMode: workerResources.share_mode || defaults.shareMode,
    gpuFraction: normalizeShareModeGpuFraction(
      workerResources.share_mode || defaults.shareMode,
      workerResources.gpu_fraction ?? defaults.gpuFraction,
    ),
    memoryCapMb: Number(workerResources.memory_cap_mb ?? defaults.memoryCapMb),
    sharedDiskGb: Number(value?.resources?.shared_disk_gb ?? defaults.sharedDiskGb),
    postDeployScript: value?.hooks?.post_deploy_script || defaults.postDeployScript,
  };
}

export function buildDesiredStateV2FromForm(form) {
  const planeName = String(form.planeName || "").trim();
  const servedModelName =
    form.servedModelNameManual && String(form.servedModelName || "").trim()
      ? String(form.servedModelName || "").trim()
      : deriveServedModelName(planeName);
  const serverName =
    form.serverNameManual && String(form.serverName || "").trim()
      ? String(form.serverName || "").trim()
      : deriveServerName(planeName);
  const source = { type: form.modelSourceType };
  if (form.modelSourceType === "library") {
    if (form.modelRef.trim()) {
      source.ref = form.modelRef.trim();
    }
    if (form.modelPath.trim()) {
      source.path = form.modelPath.trim();
    }
  } else if (form.modelSourceType === "local") {
    if (form.modelPath.trim()) {
      source.path = form.modelPath.trim();
    }
    if (form.modelRef.trim()) {
      source.ref = form.modelRef.trim();
    }
  } else if (form.modelSourceType === "url") {
    if (form.modelUrl.trim()) {
      source.url = form.modelUrl.trim();
    }
    const urls = form.modelUrls
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean);
    if (urls.length > 0) {
      source.urls = urls;
    }
    if (form.modelRef.trim()) {
      source.ref = form.modelRef.trim();
    }
  } else if (form.modelRef.trim()) {
    source.ref = form.modelRef.trim();
  }

  const desiredState = {
    version: 2,
    plane_name: planeName,
    plane_mode: form.planeMode,
    protected: Boolean(form.protectedPlane),
    runtime: {
      engine: form.planeMode === "compute" ? "custom" : "llama.cpp",
      workers: parseNumber(form.workers, 1),
      ...(form.planeMode === "llm" ? { distributed_backend: "llama_rpc" } : {}),
      max_model_len: parseNumber(form.maxModelLen, 8192),
      max_num_seqs: parseNumber(form.maxNumSeqs, 8),
      gpu_memory_utilization: Number(form.gpuMemoryUtilization) || 0.85,
    },
    network: {
      gateway_port: parseNumber(form.gatewayPort, 18084),
      inference_port: parseNumber(form.inferencePort, 18094),
      server_name: serverName,
    },
    placement: {
      execution_node: String(form.executionNode || "").trim() || "local-hostd",
    },
    app: {
      enabled: Boolean(form.appEnabled),
    },
    resources: {
      worker: {
        placement_mode: form.placementMode,
        share_mode: form.shareMode,
        gpu_fraction: normalizeShareModeGpuFraction(form.shareMode, form.gpuFraction),
        memory_cap_mb: parseNumber(form.memoryCapMb, 24576),
      },
      shared_disk_gb: parseNumber(form.sharedDiskGb, 40),
    },
  };

  if (form.topologyEnabled) {
    const nodes = (Array.isArray(form.topologyNodes) ? form.topologyNodes : [])
      .map((node) => {
        const gpuMemoryMb = parseGpuMemoryText(node.gpuMemoryText || "");
        const rendered = {
          name: String(node.name || "").trim(),
        };
        if (node.executionMode) {
          rendered.execution_mode = node.executionMode;
        }
        if (Object.keys(gpuMemoryMb).length > 0) {
          rendered.gpu_memory_mb = gpuMemoryMb;
        }
        return rendered;
      })
      .filter((node) => node.name);
    if (nodes.length > 0) {
      desiredState.topology = {
        nodes,
      };
    }
  }

  if (form.appHostEnabled && String(form.appHostAddress || "").trim()) {
    desiredState.placement.app_host = {
      address: String(form.appHostAddress || "").trim(),
    };
    if (form.appHostAuthMode === "password") {
      if (String(form.appHostUsername || "").trim()) {
        desiredState.placement.app_host.username = String(form.appHostUsername || "").trim();
      }
      if (String(form.appHostPassword || "").trim()) {
        desiredState.placement.app_host.password = String(form.appHostPassword || "").trim();
      }
    } else if (String(form.appHostSshKeyPath || "").trim()) {
      desiredState.placement.app_host.ssh_key_path = String(form.appHostSshKeyPath || "").trim();
    }
  }

  if (form.planeMode === "llm") {
    desiredState.model = {
      source,
      materialization: {
        mode: form.materializationMode,
      },
      served_model_name: servedModelName,
    };
    if (form.materializationLocalPath.trim()) {
      desiredState.model.materialization.local_path = form.materializationLocalPath.trim();
    }
    if (String(form.materializationSourceNodeName || "").trim()) {
      desiredState.model.materialization.source_node_name =
        String(form.materializationSourceNodeName || "").trim();
    }
    if (Array.isArray(form.materializationSourcePaths) && form.materializationSourcePaths.length > 0) {
      desiredState.model.materialization.source_paths = form.materializationSourcePaths
        .map((path) => String(path || "").trim())
        .filter(Boolean);
    }
    if (String(form.materializationSourceFormat || "").trim()) {
      desiredState.model.materialization.source_format =
        String(form.materializationSourceFormat || "").trim();
    }
    if (String(form.materializationSourceQuantization || "").trim()) {
      desiredState.model.materialization.source_quantization =
        String(form.materializationSourceQuantization || "").trim();
    }
    if (String(form.materializationDesiredOutputFormat || "").trim()) {
      desiredState.model.materialization.desired_output_format =
        String(form.materializationDesiredOutputFormat || "").trim();
    }
    if (String(form.modelQuantization || "").trim()) {
      desiredState.model.materialization.quantization =
        String(form.modelQuantization || "").trim();
    }
    desiredState.model.materialization.keep_source = Boolean(form.modelKeepSource);
    if (form.modelWritebackEnabled) {
      desiredState.model.materialization.writeback = {
        enabled: true,
        if_missing: Boolean(form.modelWritebackIfMissing),
        target_node_name:
          String(form.modelWritebackTargetNodeName || "").trim() ||
          String(form.materializationSourceNodeName || "").trim(),
      };
    }
    if (form.modelTargetFilename.trim()) {
      desiredState.model.target_filename = form.modelTargetFilename.trim();
    }
    if (form.modelSha256.trim()) {
      desiredState.model.sha256 = form.modelSha256.trim();
    }
    desiredState.interaction = {
      system_prompt: form.systemPrompt,
      thinking_enabled: Boolean(form.thinkingEnabled),
      default_response_language: form.defaultResponseLanguage,
      supported_response_languages: DEFAULT_SUPPORTED_RESPONSE_LANGUAGES,
      follow_user_language: Boolean(form.followUserLanguage),
    };
    desiredState.infer = {
      replicas: parseNumber(form.inferReplicas, 1),
    };
    if (form.skillsEnabled) {
      desiredState.skills = {
        enabled: true,
        ...(Array.isArray(form.factorySkillIds) && form.factorySkillIds.length > 0
          ? { factory_skill_ids: [...new Set(form.factorySkillIds.filter(Boolean))] }
          : {}),
      };
    }
    if (form.browsingEnabled) {
      desiredState.browsing = {
        enabled: true,
        policy: {
          browser_session_enabled: Boolean(form.browserSessionEnabled),
        },
      };
    }
    if (form.knowledgeEnabled) {
      desiredState.knowledge = {
        enabled: true,
        service_id: String(form.knowledgeServiceId || "kv_default").trim() || "kv_default",
        selection_mode: "latest",
        selected_knowledge_ids: [
          ...new Set(
            (Array.isArray(form.selectedKnowledgeIds) ? form.selectedKnowledgeIds : [])
              .map((item) => String(item || "").trim())
              .filter(Boolean),
          ),
        ],
        context_policy: {
          include_graph: true,
          max_graph_depth: 1,
          token_budget: 12000,
        },
      };
    }
    if (form.turboquantEnabled) {
      desiredState.features = {
        turboquant: {
          enabled: true,
          cache_type_k: String(form.turboquantCacheTypeK || TURBOQUANT_DEFAULT_CACHE_TYPE_K),
          cache_type_v: String(form.turboquantCacheTypeV || TURBOQUANT_DEFAULT_CACHE_TYPE_V),
        },
      };
    }
  }

  if (
    form.inferOverridesEnabled &&
    (form.inferImage.trim() ||
      form.inferStartValue.trim() ||
      form.inferEnvText.trim() ||
      form.inferNode.trim() ||
      form.inferStorageEnabled)
  ) {
    desiredState.infer = desiredState.infer || {};
    if (form.inferImage.trim()) {
      desiredState.infer.image = form.inferImage.trim();
    }
    if (form.inferStartValue.trim()) {
      desiredState.infer.start =
        form.inferStartType === "script"
          ? { type: "script", script_ref: form.inferStartValue.trim() }
          : { type: "command", command: form.inferStartValue.trim() };
    }
    const inferEnv = parseEnvText(form.inferEnvText);
    if (Object.keys(inferEnv).length > 0) {
      desiredState.infer.env = inferEnv;
    }
    if (form.topologyEnabled && form.inferNode.trim()) {
      desiredState.infer.node = form.inferNode.trim();
    }
    if (form.inferStorageEnabled) {
      desiredState.infer.storage = {
        size_gb: parseNumber(form.inferStorageSizeGb, 8),
        mount_path: form.inferStorageMountPath.trim() || "/naim/private",
      };
    }
  }

  if (
    form.workerImage.trim() ||
    form.workerStartValue.trim() ||
    form.workerEnvText.trim() ||
    form.workerNode.trim() ||
    form.workerGpuDevice.trim() ||
    form.workerStorageEnabled ||
    form.workerAssignmentsEnabled
  ) {
    desiredState.worker = {};
    if (form.workerImage.trim()) {
      desiredState.worker.image = form.workerImage.trim();
    }
    if (form.workerStartValue.trim()) {
      desiredState.worker.start =
        form.workerStartType === "script"
          ? { type: "script", script_ref: form.workerStartValue.trim() }
          : { type: "command", command: form.workerStartValue.trim() };
    }
    const workerEnv = parseEnvText(form.workerEnvText);
    if (Object.keys(workerEnv).length > 0) {
      desiredState.worker.env = workerEnv;
    }
    if (form.topologyEnabled && form.workerNode.trim()) {
      desiredState.worker.node = form.workerNode.trim();
    }
    if (form.workerGpuDevice.trim()) {
      desiredState.worker.gpu_device = form.workerGpuDevice.trim();
    }
    if (form.topologyEnabled && form.workerAssignmentsEnabled) {
      const assignments = (Array.isArray(form.workerAssignments) ? form.workerAssignments : [])
        .map((assignment) => {
          const rendered = {
            node: String(assignment.node || "").trim(),
          };
          if (String(assignment.gpuDevice || "").trim()) {
            rendered.gpu_device = String(assignment.gpuDevice || "").trim();
          }
          return rendered;
        })
        .filter((assignment) => assignment.node);
      if (assignments.length > 0) {
        desiredState.worker.assignments = assignments;
      }
    }
    if (form.workerStorageEnabled) {
      desiredState.worker.storage = {
        size_gb: parseNumber(form.workerStorageSizeGb, 24),
        mount_path: form.workerStorageMountPath.trim() || "/naim/private",
      };
    }
  }

  if (form.appEnabled) {
    desiredState.app.image = form.appImage.trim();
    if (form.appStartValue.trim()) {
      desiredState.app.start =
        form.appStartType === "script"
          ? { type: "script", script_ref: form.appStartValue.trim() }
          : { type: "command", command: form.appStartValue.trim() };
    }
    const appEnv = parseEnvText(form.appEnvText);
    if (Object.keys(appEnv).length > 0) {
      desiredState.app.env = appEnv;
    }
    if (form.topologyEnabled && form.appNode.trim()) {
      desiredState.app.node = form.appNode.trim();
    }
    desiredState.app.publish = [
      {
        host_ip: "127.0.0.1",
        host_port: parseNumber(form.appHostPort, 18010),
        container_port: parseNumber(form.appContainerPort, 8080),
      },
    ];
    if (form.appVolumeEnabled) {
      desiredState.app.volumes = [
        {
          name: form.appVolumeName.trim() || "private-data",
          type: form.appVolumeType,
          size_gb: parseNumber(form.appVolumeSizeGb, 8),
          mount_path: form.appVolumeMountPath.trim() || "/naim/private",
          access: form.appVolumeAccess,
        },
      ];
    }
  }

  if (form.postDeployScript.trim()) {
    desiredState.hooks = {
      post_deploy_script: form.postDeployScript.trim(),
    };
  }

  return desiredState;
}

export function validatePlaneV2Form(form) {
  const errors = [];
  const warnings = [];
  const planeName = String(form?.planeName || "").trim();
  const topologyNodes = Array.isArray(form?.topologyNodes) ? form.topologyNodes : [];
  const enabledTopologyNodes = form?.topologyEnabled
    ? topologyNodes.filter(
        (node) =>
          String(node?.name || "").trim() ||
          String(node?.gpuMemoryText || "").trim() ||
          String(node?.executionMode || "").trim(),
      )
    : [];
  const topologyNodeNames = enabledTopologyNodes
    .map((node) => String(node.name || "").trim())
    .filter(Boolean);
  const duplicateTopologyNodeNames = topologyNodeNames.filter(
    (name, index) => topologyNodeNames.indexOf(name) !== index,
  );
  const workerAssignments = Array.isArray(form?.workerAssignments) ? form.workerAssignments : [];
  const enabledAssignments = form?.workerAssignmentsEnabled
    ? workerAssignments.filter(
        (assignment) =>
          String(assignment?.node || "").trim() || String(assignment?.gpuDevice || "").trim(),
      )
    : [];

  if (!planeName) {
    errors.push("Plane name is required.");
  }
  if (form?.planeMode === "llm") {
    if (form?.modelSourceType === "library" && !String(form?.modelRef || "").trim()) {
      errors.push("Model Library selection is required when model source type is library.");
    }
    if (form?.modelSourceType === "local" && !String(form?.modelPath || "").trim()) {
      errors.push("Local model path is required when model source type is local.");
    }
    if (
      (form?.modelSourceType === "huggingface" || form?.modelSourceType === "catalog") &&
      !String(form?.modelRef || "").trim()
    ) {
      errors.push("Model ref is required for catalog or huggingface sources.");
    }
    if (form?.modelSourceType === "url") {
      const hasPrimaryUrl = String(form?.modelUrl || "").trim();
      const additionalUrls = String(form?.modelUrls || "")
        .split("\n")
        .map((item) => item.trim())
        .filter(Boolean);
      if (!hasPrimaryUrl && additionalUrls.length === 0) {
        errors.push("At least one model URL is required for url sources.");
      }
    }
    if (form?.materializationMode === "prepare_on_worker") {
      if (!String(form?.materializationSourceNodeName || "").trim()) {
        errors.push("Source storage node is required for worker model preparation.");
      }
      if (
        !Array.isArray(form?.materializationSourcePaths) ||
        form.materializationSourcePaths.length === 0
      ) {
        errors.push("Source model paths are required for worker model preparation.");
      }
    }
  }
  if (form?.planeMode === "llm") {
    if (Number(form?.inferReplicas || 0) <= 0) {
      errors.push("Infer replicas must be a positive integer for llm planes.");
    }
    if (Number(form?.workers || 0) % Number(form?.inferReplicas || 1) !== 0) {
      errors.push("Workers must be divisible by infer replicas.");
    }
  }
  if (form?.shareMode === "exclusive" && Number(form?.gpuFraction) !== 1) {
    errors.push("Exclusive share mode requires GPU fraction equal to 1.0.");
  }
  if (form?.appEnabled && !String(form?.appImage || "").trim()) {
    errors.push("App image is required when app is enabled.");
  }
  if (form?.planeMode === "compute") {
    if (!String(form?.workerImage || "").trim()) {
      errors.push("Worker image is required for compute planes.");
    }
    if (!String(form?.workerStartValue || "").trim()) {
      errors.push("Worker start is required for compute planes.");
    }
  }
  if (form?.topologyEnabled) {
    if (enabledTopologyNodes.length === 0) {
      errors.push("At least one topology node is required when custom topology is enabled.");
    }
    if (enabledTopologyNodes.some((node) => !String(node?.name || "").trim())) {
      errors.push("Each topology node row must have a node name.");
    }
    if (duplicateTopologyNodeNames.length > 0) {
      errors.push("Topology node names must be unique.");
    }
  }
  if (form?.workerAssignmentsEnabled) {
    if (enabledAssignments.length !== Number(form?.workers || 0)) {
      errors.push("Worker assignments count must match the number of workers.");
    }
    if (enabledAssignments.some((assignment) => !String(assignment?.node || "").trim())) {
      errors.push("Each worker assignment must include a node name.");
    }
  }
  if (
    form?.placementMode === "manual" &&
    !String(form?.workerGpuDevice || "").trim() &&
    enabledAssignments.length === 0
  ) {
    errors.push(
      "Manual placement requires either a default worker GPU device or per-worker assignments.",
    );
  }

  const referencedNodes = [
    String(form?.executionNode || "").trim(),
    String(form?.inferNode || "").trim(),
    String(form?.workerNode || "").trim(),
    String(form?.appNode || "").trim(),
    ...enabledAssignments.map((assignment) => String(assignment.node || "").trim()),
  ].filter(Boolean);

  if (!String(form?.executionNode || "").trim()) {
    errors.push("Execution node is required.");
  }
  if (form?.appHostEnabled && !form?.appEnabled) {
    errors.push("External app host requires the app container to be enabled.");
  }
  if (form?.appHostEnabled && !String(form?.appHostAddress || "").trim()) {
    errors.push("External app host address is required.");
  }
  if (form?.appHostEnabled && form?.appHostAuthMode === "ssh-key" &&
      !String(form?.appHostSshKeyPath || "").trim()) {
    errors.push("External app host SSH key path is required.");
  }
  if (form?.appHostEnabled && form?.appHostAuthMode === "password") {
    if (!String(form?.appHostUsername || "").trim() || !String(form?.appHostPassword || "").trim()) {
      errors.push("External app host username and password are required together.");
    }
  }
  if (form?.topologyEnabled && topologyNodeNames.length > 0) {
    const unknownNodes = referencedNodes.filter((name) => !topologyNodeNames.includes(name));
    if (unknownNodes.length > 0) {
      errors.push(`Referenced nodes are missing from topology: ${[...new Set(unknownNodes)].join(", ")}`);
    }
  }

  if (form?.workerAssignmentsEnabled && !form?.topologyEnabled) {
    warnings.push("Per-worker assignments are easier to reason about when custom topology is enabled.");
  }
  if (!form?.skillsEnabled && Array.isArray(form?.factorySkillIds) && form.factorySkillIds.length > 0) {
    warnings.push("Selected Skills Factory records are ignored until Skills is enabled.");
  }
  if (!form?.browsingEnabled && form?.browserSessionEnabled) {
    warnings.push("Browser sessions are ignored until Isolated Browsing is enabled.");
  }
  if (form?.knowledgeEnabled &&
      (!Array.isArray(form?.selectedKnowledgeIds) || form.selectedKnowledgeIds.length === 0)) {
    warnings.push("Knowledge Base is enabled without selected knowledge records.");
  }
  if (form?.appEnabled && !String(form?.appStartValue || "").trim()) {
    warnings.push("App start is empty. The app container will rely on its image default command.");
  }

  return { errors, warnings };
}

export function updatePlaneDialogForm(setDialog, update) {
  setDialog((current) => {
    const nextForm =
      typeof update === "function" ? update(current.form || buildNewPlaneFormState()) : update;
    return {
      ...current,
      form: nextForm,
      text: JSON.stringify(buildDesiredStateV2FromForm(nextForm), null, 2),
    };
  });
}

function SectionHeader({ title, description }) {
  return (
    <div className="plane-form-section-header">
      <div className="section-label">{title}</div>
      {description ? <p className="plane-form-section-copy">{description}</p> : null}
    </div>
  );
}

function AdvancedSection({ title, description, defaultOpen = false, children }) {
  return (
    <details className="plane-advanced-section" open={defaultOpen}>
      <summary className="plane-advanced-summary">
        <span>{title}</span>
      </summary>
      <div className="plane-advanced-body">
        {description ? <p className="plane-form-section-copy">{description}</p> : null}
        {children}
      </div>
    </details>
  );
}

function FieldHint({ message, severity = "error" }) {
  if (!message) {
    return null;
  }
  return <div className={`field-hint is-${severity}`}>{message}</div>;
}

function InfoLabel({ children, info, htmlFor, className = "field-label-title" }) {
  return (
    <span className={className}>
      {htmlFor ? <span>{children}</span> : children}
      {info ? (
        <span className="field-info" tabIndex={0} aria-label={info}>
          i
          <span className="field-info-tooltip" role="tooltip">
            {info}
          </span>
        </span>
      ) : null}
    </span>
  );
}

function FieldTitle({ title, info, htmlFor }) {
  return (
    <label className="field-label" htmlFor={htmlFor}>
      <InfoLabel info={info}>{title}</InfoLabel>
    </label>
  );
}

function FeatureToggle({ title, info, active, disabled = false, disabledLabel, onToggle }) {
  const stateLabel = disabled ? disabledLabel || "Unavailable" : active ? "Enabled" : "Disabled";
  return (
    <button
      className={`plane-feature-toggle${active ? " is-active" : ""}${disabled ? " is-disabled" : ""}`}
      type="button"
      onClick={onToggle}
      disabled={disabled}
      aria-pressed={active}
    >
      <span className="plane-feature-toggle-head">
        <InfoLabel info={info} className="field-label-inline">{title}</InfoLabel>
      </span>
      <span className="plane-feature-toggle-state">{stateLabel}</span>
    </button>
  );
}

function ModelLibraryPicker({ items, selectedPath, onSelect }) {
  const rows = Array.isArray(items) ? items : [];
  if (rows.length === 0) {
    return <FieldHint message="Model Library is empty. Discover or download a model first." severity="warning" />;
  }
  return (
    <div className="model-library-picker">
      <div className="model-library-picker-head">
        <div>Name</div>
        <div>Path</div>
        <div>Summary</div>
      </div>
      <div className="model-library-picker-body">
        {rows.map((item) => {
          const isSelected = selectedPath === item.path;
          return (
            <button
              key={item.path}
              className={`model-library-picker-row${isSelected ? " is-selected" : ""}`}
              type="button"
              onClick={() => onSelect(item)}
            >
              <strong>{item.name || item.path?.split("/").pop() || "model"}</strong>
              <span>{item.path}</span>
              <span>{item.kind || "artifact"}{item.size_bytes ? ` / ${Math.round(item.size_bytes / (1024 * 1024))} MiB` : ""}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function SectionMeta({ children }) {
  if (!children) {
    return null;
  }
  return <div className="plane-form-section-meta">{children}</div>;
}

function SectionActions({ children }) {
  return <div className="plane-form-section-actions">{children}</div>;
}

function hostName(host) {
  return String(host?.node_name || host?.nodeName || host?.name || "").trim();
}

function findHost(hostdHosts, nodeName) {
  return (Array.isArray(hostdHosts) ? hostdHosts : []).find((host) => hostName(host) === nodeName) || null;
}

function hostRoles(host) {
  const roles = [];
  if (Array.isArray(host?.roles)) {
    roles.push(...host.roles);
  }
  if (Array.isArray(host?.capabilities?.roles)) {
    roles.push(...host.capabilities.roles);
  }
  if (host?.role) {
    roles.push(host.role);
  }
  if (host?.is_worker || host?.worker) {
    roles.push("Worker");
  }
  if (host?.is_storage || host?.storage) {
    roles.push("Storage");
  }
  return roles.map((role) => String(role || "").trim().toLowerCase()).filter(Boolean);
}

function hostGpuCount(host) {
  const candidates = [
    host?.gpu_count,
    host?.gpuCount,
    host?.resources?.gpu_count,
    host?.telemetry?.gpu_count,
    host?.host?.gpu_count,
  ];
  for (const value of candidates) {
    const number = Number(value);
    if (Number.isFinite(number)) {
      return number;
    }
  }
  if (Array.isArray(host?.gpus)) {
    return host.gpus.length;
  }
  if (Array.isArray(host?.telemetry?.gpus)) {
    return host.telemetry.gpus.length;
  }
  return 0;
}

function hostConnected(host) {
  if (!host) {
    return false;
  }
  const state = String(host.state || host.status || host.health || "").toLowerCase();
  if (["offline", "missing", "stale", "critical", "error"].includes(state)) {
    return false;
  }
  return host.connected === true || host.ready === true || state === "connected" || state === "ready" || Boolean(host.last_seen_at || host.updated_at);
}

function peerLinkIsDirect(peerLinks, leftNode, rightNode) {
  const items = Array.isArray(peerLinks?.items) ? peerLinks.items : Array.isArray(peerLinks) ? peerLinks : [];
  return items.some((item) => {
    const text = JSON.stringify(item || {}).toLowerCase();
    if (!text.includes(leftNode.toLowerCase()) || !text.includes(rightNode.toLowerCase())) {
      return false;
    }
    return (
      item?.same_lan === true ||
      item?.bidirectional === true ||
      String(item?.state || item?.status || item?.link_state || "").toLowerCase() === "direct"
    );
  });
}

function buildLtCypherPreflight({ form, modelLibraryItems, hostdHosts, peerLinks }) {
  const results = [];
  const push = (key, label, passed, detail) => {
    results.push({ key, label, passed, detail });
  };
  const hpc1 = findHost(hostdHosts, "hpc1");
  const storage1 = findHost(hostdHosts, "storage1");
  const selectedModel =
    (Array.isArray(modelLibraryItems) ? modelLibraryItems : []).find((item) => item.path === form.modelPath) ||
    findLtCypherModelItem(modelLibraryItems);
  const selectedFormat = inferModelFormat(selectedModel);
  const selectedQuantization = inferModelQuantization(selectedModel);
  const image = String(form.appImage || "").trim();
  const imageTag = image.startsWith(LT_CYPHER_HARBOR_IMAGE_PREFIX)
    ? image.slice(LT_CYPHER_HARBOR_IMAGE_PREFIX.length)
    : "";
  push("controller", "Controller reachable", true, "The operator UI has an active API session.");
  push(
    "hpc1",
    "hpc1 worker online with 4 GPUs",
    hostConnected(hpc1) && hostRoles(hpc1).includes("worker") && hostGpuCount(hpc1) >= 4,
    hpc1
      ? `state=${hpc1.state || hpc1.status || "seen"}, roles=${hostRoles(hpc1).join(",") || "n/a"}, gpus=${hostGpuCount(hpc1)}`
      : "hpc1 is not present in /api/v1/hostd/hosts",
  );
  push(
    "storage1",
    "storage1 storage online",
    hostConnected(storage1) && hostRoles(storage1).includes("storage"),
    storage1
      ? `state=${storage1.state || storage1.status || "seen"}, roles=${hostRoles(storage1).join(",") || "n/a"}`
      : "storage1 is not present in /api/v1/hostd/hosts",
  );
  push(
    "lan",
    "hpc1 <-> storage1 LAN direct",
    peerLinkIsDirect(peerLinks, "hpc1", "storage1"),
    peerLinkIsDirect(peerLinks, "hpc1", "storage1")
      ? "Fresh bidirectional LAN peer link is available."
      : "No fresh direct peer link in dashboard peer_links.",
  );
  push(
    "model",
    "Qwen3.6-35B-A3B Q8 model readable on storage1",
    Boolean(selectedModel) &&
      String(selectedModel?.node_name || form.materializationSourceNodeName || "") === "storage1" &&
      selectedFormat === "gguf" &&
      selectedQuantization === "Q8_0" &&
      modelLibraryPaths(selectedModel).length > 0,
    selectedModel
      ? `${selectedModel.name || selectedModel.model_id || selectedModel.path} / ${selectedFormat || "unknown"} / ${selectedQuantization}`
      : "Model Library does not contain Qwen3.6-35B-A3B Q8.",
  );
  push(
    "image",
    "Harbor image is immutable",
    image.startsWith(LT_CYPHER_HARBOR_IMAGE_PREFIX) &&
      Boolean(imageTag) &&
      !["<git-sha>", "latest", "dev"].includes(imageTag),
    image || "Image is empty.",
  );
  return results;
}

function TopologyNodeRows({ nodes, disabled, onChange }) {
  const items = Array.isArray(nodes) ? nodes : [];

  function updateNode(index, key, value) {
    onChange((current) =>
      current.map((item, itemIndex) =>
        itemIndex === index ? { ...item, [key]: value } : item,
      ),
    );
  }

  function addNode() {
    onChange((current) => [
      ...current,
      { name: "", executionMode: "mixed", gpuMemoryText: "" },
    ]);
  }

  function removeNode(index) {
    onChange((current) => current.filter((_, itemIndex) => itemIndex !== index));
  }

  return (
    <div className="plane-form-rows">
      {items.map((node, index) => (
        <div className="plane-form-row" key={`topology-node-${index}`}>
          <input
            className="text-input"
            placeholder="Node name"
            value={node.name}
            onChange={(event) => updateNode(index, "name", event.target.value)}
            disabled={disabled}
          />
          <select
            className="text-input"
            value={node.executionMode}
            onChange={(event) => updateNode(index, "executionMode", event.target.value)}
            disabled={disabled}
          >
            <option value="mixed">mixed</option>
            <option value="infer-only">infer-only</option>
            <option value="worker-only">worker-only</option>
          </select>
          <input
            className="text-input"
            placeholder="gpu0=24576,gpu1=24576"
            value={node.gpuMemoryText}
            onChange={(event) => updateNode(index, "gpuMemoryText", event.target.value)}
            disabled={disabled}
          />
          <button
            className="ghost-button"
            type="button"
            onClick={() => removeNode(index)}
            disabled={disabled}
          >
            Remove
          </button>
        </div>
      ))}
      <button className="ghost-button" type="button" onClick={addNode} disabled={disabled}>
        Add node
      </button>
    </div>
  );
}

function WorkerAssignmentRows({ assignments, disabled, onChange }) {
  const items = Array.isArray(assignments) ? assignments : [];

  function updateAssignment(index, key, value) {
    onChange((current) =>
      current.map((item, itemIndex) =>
        itemIndex === index ? { ...item, [key]: value } : item,
      ),
    );
  }

  function addAssignment() {
    onChange((current) => [...current, { node: "", gpuDevice: "" }]);
  }

  function removeAssignment(index) {
    onChange((current) => current.filter((_, itemIndex) => itemIndex !== index));
  }

  return (
    <div className="plane-form-rows">
      {items.map((assignment, index) => (
        <div className="plane-form-row" key={`worker-assignment-${index}`}>
          <input
            className="text-input"
            placeholder="Node name"
            value={assignment.node}
            onChange={(event) => updateAssignment(index, "node", event.target.value)}
            disabled={disabled}
          />
          <input
            className="text-input"
            placeholder="GPU device"
            value={assignment.gpuDevice}
            onChange={(event) => updateAssignment(index, "gpuDevice", event.target.value)}
            disabled={disabled}
          />
          <button
            className="ghost-button"
            type="button"
            onClick={() => removeAssignment(index)}
            disabled={disabled}
          >
            Remove
          </button>
        </div>
      ))}
      <button className="ghost-button" type="button" onClick={addAssignment} disabled={disabled}>
        Add assignment
      </button>
    </div>
  );
}

export function PlaneV2FormBuilder({
  dialog,
  setDialog,
  languageOptions,
  modelLibraryItems = [],
  skillsFactoryItems = [],
  skillsFactoryGroups = [],
  hostdHosts = [],
  peerLinks = null,
  onResetLtCypherDeployment,
}) {
  const form = dialog.form || buildNewPlaneFormState();
  const validation = validatePlaneV2Form(form);
  const [ltCypherPreflight, setLtCypherPreflight] = useState(null);
  const [factorySkillFilter, setFactorySkillFilter] = useState("");
  const [knowledgeSelectorOpen, setKnowledgeSelectorOpen] = useState(false);
  const [selectedFactoryGroupPath, setSelectedFactoryGroupPath] = useState("");
  const [expandedFactoryGroupPaths, setExpandedFactoryGroupPaths] = useState([""]);
  const topologyNodes = Array.isArray(form.topologyNodes) ? form.topologyNodes : [];
  const activeTopologyNodes = topologyNodes.filter((node) => String(node?.name || "").trim());
  const workerAssignments = Array.isArray(form.workerAssignments) ? form.workerAssignments : [];
  const activeAssignments = workerAssignments.filter(
    (assignment) => String(assignment?.node || "").trim() || String(assignment?.gpuDevice || "").trim(),
  );
  const topologySummary =
    activeTopologyNodes.length > 0
      ? `${activeTopologyNodes.length} node(s), ${activeTopologyNodes.filter((node) => node.executionMode === "worker-only").length} worker-only, ${activeTopologyNodes.filter((node) => node.executionMode === "infer-only").length} infer-only`
      : "No topology nodes configured.";
  const assignmentSummary = `${activeAssignments.length} assignment row(s) for ${Number(form.workers || 0)} worker(s).`;

  function firstMatching(items, fragments) {
    return items.find((item) => fragments.some((fragment) => item.includes(fragment))) || "";
  }

  function fieldError(...fragments) {
    return firstMatching(validation.errors, fragments);
  }

  function fieldWarning(...fragments) {
    return firstMatching(validation.warnings, fragments);
  }

  function inputClassName(invalid) {
    return invalid ? "text-input is-invalid" : "text-input";
  }

  function bindText(key) {
    return (event) =>
      updatePlaneDialogForm(setDialog, (current) => {
        const next = {
          ...current,
          [key]: event.target.value,
        };
        if (key === "planeName") {
          return syncDerivedPlaneFields(next);
        }
        if (key === "servedModelName") {
          next.servedModelNameManual = true;
        }
        if (key === "serverName") {
          next.serverNameManual = true;
        }
        return next;
      });
  }

  function bindNumber(key) {
    return (event) =>
      updatePlaneDialogForm(setDialog, (current) => ({
        ...current,
        [key]: event.target.value,
      }));
  }

  function bindShareMode() {
    return (event) =>
      updatePlaneDialogForm(setDialog, (current) => {
        const nextShareMode = event.target.value;
        return {
          ...current,
          shareMode: nextShareMode,
          gpuFraction: normalizeShareModeGpuFraction(nextShareMode, current.gpuFraction),
        };
      });
  }

  function bindCheck(key) {
    return (event) =>
      updatePlaneDialogForm(setDialog, (current) => {
        const next = {
          ...current,
          [key]: event.target.checked,
        };
        if (key === "appEnabled" && event.target.checked && !next.appNode && next.topologyEnabled) {
          next.appNode = next.inferNode || next.workerNode || "";
        }
        if (key === "appHostEnabled" && !event.target.checked) {
          next.appHostAddress = "";
          next.appHostSshKeyPath = "";
          next.appHostUsername = "";
          next.appHostPassword = "";
        }
        return next;
      });
  }

  function bindPlaneMode() {
    return (event) =>
      updatePlaneDialogForm(setDialog, (current) => {
        const nextPlaneMode = event.target.value;
        return syncDerivedPlaneFields({
          ...current,
          planeMode: nextPlaneMode,
          skillsEnabled: nextPlaneMode === "llm" ? current.skillsEnabled : false,
          browsingEnabled: nextPlaneMode === "llm" ? current.browsingEnabled : false,
          knowledgeEnabled: nextPlaneMode === "llm" ? current.knowledgeEnabled : false,
          browserSessionEnabled:
            nextPlaneMode === "llm" ? current.browserSessionEnabled : false,
        });
      });
  }

  function toggleFeature(key) {
    return () =>
      updatePlaneDialogForm(setDialog, (current) => {
        const nextValue = !current[key];
        const next = {
          ...current,
          [key]: nextValue,
        };
        if (key === "browsingEnabled" && !nextValue) {
          next.browserSessionEnabled = false;
        }
        if (key === "knowledgeEnabled" && nextValue) {
          window.setTimeout(() => setKnowledgeSelectorOpen(true), 0);
        }
        return next;
      });
  }

  function toggleKnowledgeSelection(knowledgeId) {
    if (!knowledgeId) {
      return;
    }
    updatePlaneDialogForm(setDialog, (current) => {
      const currentIds = new Set(
        Array.isArray(current.selectedKnowledgeIds) ? current.selectedKnowledgeIds : [],
      );
      if (currentIds.has(knowledgeId)) {
        currentIds.delete(knowledgeId);
      } else {
        currentIds.add(knowledgeId);
      }
      return {
        ...current,
        selectedKnowledgeIds: [...currentIds],
      };
    });
  }

  function selectKnowledgeIds(knowledgeIds) {
    updatePlaneDialogForm(setDialog, (current) => {
      const currentIds = new Set(
        Array.isArray(current.selectedKnowledgeIds) ? current.selectedKnowledgeIds : [],
      );
      for (const knowledgeId of knowledgeIds) {
        if (knowledgeId) {
          currentIds.add(knowledgeId);
        }
      }
      return {
        ...current,
        selectedKnowledgeIds: [...currentIds],
      };
    });
  }

  function unselectKnowledgeIds(knowledgeIds) {
    updatePlaneDialogForm(setDialog, (current) => {
      const currentIds = new Set(
        Array.isArray(current.selectedKnowledgeIds) ? current.selectedKnowledgeIds : [],
      );
      for (const knowledgeId of knowledgeIds) {
        currentIds.delete(knowledgeId);
      }
      return {
        ...current,
        selectedKnowledgeIds: [...currentIds],
      };
    });
  }

  function resetDerivedField(key) {
    updatePlaneDialogForm(setDialog, (current) => {
      if (key === "servedModelName") {
        return syncDerivedPlaneFields({
          ...current,
          servedModelNameManual: false,
        });
      }
      if (key === "serverName") {
        return syncDerivedPlaneFields({
          ...current,
          serverNameManual: false,
        });
      }
      return current;
    });
  }

  function selectLocalModel(item) {
    updatePlaneDialogForm(setDialog, (current) => {
      const planeName = String(current.planeName || "").trim();
      const fallbackName = item?.name || item?.path?.split("/").pop() || planeName || "model";
      const sourceFormat = inferModelFormat(item);
      const sourceQuantization = inferModelQuantization(item);
      return {
        ...current,
        modelSourceType: "library",
        modelPath: item?.path || "",
        modelRef: item?.model_id || item?.name || item?.path || "",
        materializationMode: "prepare_on_worker",
        materializationLocalPath: item?.path || "",
        materializationSourceNodeName: item?.node_name || "",
        materializationSourcePaths: Array.isArray(item?.paths) ? item.paths : [],
        materializationSourceFormat: sourceFormat || "",
        materializationSourceQuantization: sourceQuantization,
        materializationDesiredOutputFormat: "gguf",
        modelQuantization: sourceQuantization,
        modelKeepSource: false,
        modelWritebackEnabled: true,
        modelWritebackIfMissing: true,
        modelWritebackTargetNodeName: item?.node_name || "",
        servedModelName: current.servedModelNameManual
          ? current.servedModelName
          : fallbackName.replace(/\s+/g, "-").toLowerCase(),
      };
    });
  }

  function updateTopologyNodes(update) {
    updatePlaneDialogForm(setDialog, (current) => ({
      ...current,
      topologyNodes:
        typeof update === "function" ? update(current.topologyNodes || []) : update,
    }));
  }

  function updateWorkerAssignments(update) {
    updatePlaneDialogForm(setDialog, (current) => ({
      ...current,
      workerAssignments:
        typeof update === "function" ? update(current.workerAssignments || []) : update,
    }));
  }

  function enableSingleHostLayout() {
    updatePlaneDialogForm(setDialog, (current) => ({
      ...current,
      executionNode: "local-hostd",
      topologyEnabled: false,
      topologyNodes: [],
      inferNode: "",
      workerNode: "",
      appNode: "",
      workerAssignmentsEnabled: false,
      workerAssignments: [],
    }));
  }

  function generateSplitHostLayout() {
    updatePlaneDialogForm(setDialog, (current) => {
      const workerCount = Math.max(1, Number(current.workers || 1));
      const topologyNodesValue = [
        { name: "infer-hostd", executionMode: "infer-only", gpuMemoryText: "" },
        ...Array.from({ length: workerCount }, (_, index) => ({
          name: `worker-hostd-${String.fromCharCode(97 + index)}`,
          executionMode: "worker-only",
          gpuMemoryText: "0=24576",
        })),
      ];
      const assignmentsValue = Array.from({ length: workerCount }, (_, index) => ({
        node: `worker-hostd-${String.fromCharCode(97 + index)}`,
        gpuDevice: "0",
      }));
      return {
        ...current,
        runtimeEngine: "llama.cpp",
        inferReplicas: workerCount,
        executionNode: "infer-hostd",
        topologyEnabled: true,
        topologyNodes: topologyNodesValue,
        inferNode: "infer-hostd",
        workerNode: "",
        appNode: current.appEnabled ? "infer-hostd" : current.appNode,
        workerAssignmentsEnabled: true,
        workerAssignments: assignmentsValue,
      };
    });
  }

  function matchAssignmentsToWorkers() {
    updateWorkerAssignments((currentAssignments) => {
      const workerCount = Math.max(1, Number(form.workers || 1));
      const items = Array.isArray(currentAssignments) ? currentAssignments : [];
      if (items.length === workerCount) {
        return items;
      }
      if (items.length > workerCount) {
        return items.slice(0, workerCount);
      }
      return [
        ...items,
        ...Array.from({ length: workerCount - items.length }, () => ({
          node: form.workerNode || "",
          gpuDevice: form.workerGpuDevice || "",
        })),
      ];
    });
  }

  function toggleFactorySkill(skillId) {
    updatePlaneDialogForm(setDialog, (current) => {
      const currentIds = Array.isArray(current.factorySkillIds) ? current.factorySkillIds : [];
      return {
        ...current,
        factorySkillIds: currentIds.includes(skillId)
          ? currentIds.filter((item) => item !== skillId)
          : [...currentIds, skillId],
      };
    });
  }

  function applyLtCypherPreset() {
    updatePlaneDialogForm(setDialog, (current) =>
      applyLtCypherPresetToForm(current, modelLibraryItems),
    );
    setLtCypherPreflight(null);
  }

  function runLtCypherPreflight() {
    setLtCypherPreflight(
      buildLtCypherPreflight({
        form,
        modelLibraryItems,
        hostdHosts,
        peerLinks,
      }),
    );
  }

  function applyFactoryGroupSelection(groupPath, nextSelected) {
    updatePlaneDialogForm(setDialog, (current) => {
      const currentIds = new Set(Array.isArray(current.factorySkillIds) ? current.factorySkillIds : []);
      for (const skillId of collectGroupSkillIds(skillsFactoryItems, groupPath)) {
        if (nextSelected) {
          currentIds.add(skillId);
        } else {
          currentIds.delete(skillId);
        }
      }
      return {
        ...current,
        factorySkillIds: [...currentIds],
      };
    });
  }

  const filteredFactorySkills = filterPlaneSelectableSkills(
    skillsFactoryItems,
    factorySkillFilter,
    selectedFactoryGroupPath,
  );
  const factoryGroupTree = buildSkillsFactoryGroupTree(skillsFactoryItems, skillsFactoryGroups);
  const factoryTreePaths = collectSkillsFactoryTreePaths(factoryGroupTree);
  const expandedFactoryGroupPathSet = new Set(expandedFactoryGroupPaths);

  function renderFactoryGroupNode(node) {
    const isExpanded = expandedFactoryGroupPathSet.has(node.path || "");
    const hasChildren = node.children.length > 0;
    return (
      <div className="skills-factory-group-node" key={node.path || "__root__"}>
        <div className="skills-factory-group-row">
          <button
            className={`ghost-button compact-button skills-factory-group-toggle ${isExpanded ? "is-expanded" : ""}`}
            type="button"
            disabled={!hasChildren}
            onClick={() =>
              setExpandedFactoryGroupPaths((current) => {
                const next = new Set(Array.isArray(current) ? current : [""]);
                if (next.has(node.path || "")) {
                  next.delete(node.path || "");
                } else {
                  next.add(node.path || "");
                }
                return [...next];
              })
            }
            aria-label={isExpanded ? `Collapse ${node.label}` : `Expand ${node.label}`}
          >
            <span className="skills-factory-group-toggle-glyph">{hasChildren ? "▾" : "•"}</span>
          </button>
          <button
            className={`ghost-button skills-factory-group-button ${selectedFactoryGroupPath === node.path ? "is-active" : ""}`}
            type="button"
            onClick={() => setSelectedFactoryGroupPath(node.path)}
          >
            <span className="skills-factory-group-copy">
              <span>{node.label}</span>
              {node.direct_skill_count > 0 ? (
                <span className="skills-factory-group-meta">{node.direct_skill_count} direct</span>
              ) : null}
            </span>
            <span className="tag">{node.total_skill_count}</span>
          </button>
          {node.path ? (
            <div className="plane-form-section-actions">
              <button
                className="ghost-button compact-button"
                type="button"
                onClick={() => applyFactoryGroupSelection(node.path, true)}
              >
                Select group
              </button>
              <button
                className="ghost-button compact-button"
                type="button"
                onClick={() => applyFactoryGroupSelection(node.path, false)}
              >
                Clear group
              </button>
            </div>
          ) : null}
        </div>
        {hasChildren && isExpanded ? (
          <div className="skills-factory-group-children">
            {node.children.map((child) => renderFactoryGroupNode(child))}
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <div className="plane-form-builder">
      <div className="plane-form-toggle">
        <div className="plane-form-section-header">
          <InfoLabel info="Fill the form for the UI-only LocalTrade Jex deployment flow. The preset uses storage1 as model source, hpc1 as worker, Harbor image contract, and root ingress mode.">
            Deploy preset
          </InfoLabel>
          <p className="plane-form-section-copy">
            Use this for a clean lt-cypher-ai run: Qwen3.6-35B-A3B Q8 from storage1, hpc1 GPU 0, app on 127.0.0.1:18110, public base path /.
          </p>
        </div>
        <SectionActions>
          <button className="ghost-button" type="button" onClick={applyLtCypherPreset}>
            Apply lt-cypher-ai preset
          </button>
          <button className="ghost-button" type="button" onClick={runLtCypherPreflight}>
            Run preset preflight
          </button>
          {onResetLtCypherDeployment ? (
            <button
              className="ghost-button danger-button"
              type="button"
              onClick={onResetLtCypherDeployment}
            >
              Reset failed lt-cypher-ai deployment
            </button>
          ) : null}
        </SectionActions>
        {ltCypherPreflight ? (
          <div className="model-library-picker">
            <div className="model-library-picker-head">
              <div>Check</div>
              <div>Status</div>
              <div>Detail</div>
            </div>
            <div className="model-library-picker-body">
              {ltCypherPreflight.map((item) => (
                <div
                  className={`model-library-picker-row${item.passed ? " is-selected" : ""}`}
                  key={item.key}
                >
                  <strong>{item.label}</strong>
                  <span className={`tag ${item.passed ? "is-ok" : "is-critical"}`}>
                    {item.passed ? "pass" : "fail"}
                  </span>
                  <span>{item.detail}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </div>

      <SectionHeader
        title="Plane"
        description="Identity and mode for the plane you are about to create."
      />
      <div className="plane-form-grid">
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.planeName}>Plane name</InfoLabel>
          <input
            className={inputClassName(Boolean(fieldError("Plane name is required")))}
            value={form.planeName}
            onChange={bindText("planeName")}
          />
          <FieldHint message={fieldError("Plane name is required")} />
        </label>
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.planeMode}>Plane mode</InfoLabel>
          <select className="text-input" value={form.planeMode} onChange={bindPlaneMode()}>
            <option value="llm">llm</option>
            <option value="compute">compute</option>
          </select>
        </label>
      </div>

      <SectionHeader
        title="Features"
        description="Plane-wide capabilities and protection flags."
      />
      <div className="plane-features-grid">
        <FeatureToggle
          title="Skills"
          info={FIELD_INFO.skillsEnabled}
          active={form.planeMode === "llm" && form.skillsEnabled}
          disabled={form.planeMode !== "llm"}
          disabledLabel="LLM only"
          onToggle={toggleFeature("skillsEnabled")}
        />
        <FeatureToggle
          title="Isolated Browsing"
          info={FIELD_INFO.browsingEnabled}
          active={form.planeMode === "llm" && form.browsingEnabled}
          disabled={form.planeMode !== "llm"}
          disabledLabel="LLM only"
          onToggle={toggleFeature("browsingEnabled")}
        />
        <FeatureToggle
          title="Knowledge Base"
          info={FIELD_INFO.knowledgeEnabled}
          active={form.planeMode === "llm" && form.knowledgeEnabled}
          disabled={form.planeMode !== "llm"}
          disabledLabel="LLM only"
          onToggle={toggleFeature("knowledgeEnabled")}
        />
        <FeatureToggle
          title="TurboQuant"
          info={FIELD_INFO.turboquantEnabled}
          active={form.planeMode === "llm" && form.turboquantEnabled}
          disabled={form.planeMode !== "llm"}
          disabledLabel="LLM only"
          onToggle={toggleFeature("turboquantEnabled")}
        />
        <FeatureToggle
          title="App"
          info={FIELD_INFO.appEnabled}
          active={form.appEnabled}
          onToggle={toggleFeature("appEnabled")}
        />
        <FeatureToggle
          title="Protected Plane"
          info={FIELD_INFO.protectedPlane}
          active={form.protectedPlane}
          onToggle={toggleFeature("protectedPlane")}
        />
      </div>
      <KnowledgeBaseSelectorModal
        open={knowledgeSelectorOpen}
        selectedKnowledgeIds={form.selectedKnowledgeIds}
        onClose={() => setKnowledgeSelectorOpen(false)}
        onToggleKnowledge={toggleKnowledgeSelection}
        onSelectAll={selectKnowledgeIds}
        onUnselectAll={unselectKnowledgeIds}
      />

      {form.planeMode === "llm" && form.knowledgeEnabled ? (
        <div className="plane-form-toggle">
          <div className="plane-form-section-header">
            <InfoLabel info={FIELD_INFO.knowledgeEnabled}>Knowledge Base</InfoLabel>
            <p className="plane-form-section-copy">
              Selected canonical knowledge records are attached by latest knowledge id.
            </p>
          </div>
          <SectionActions>
            <button
              className="ghost-button"
              type="button"
              onClick={() => setKnowledgeSelectorOpen(true)}
            >
              Select knowledge
            </button>
          </SectionActions>
          <SectionMeta>
            {(Array.isArray(form.selectedKnowledgeIds) ? form.selectedKnowledgeIds.length : 0)} selected
          </SectionMeta>
        </div>
      ) : null}

      {form.planeMode === "llm" && form.browsingEnabled ? (
        <div className="plane-form-toggle">
          <div className="plane-form-section-header">
            <InfoLabel info={FIELD_INFO.browsingEnabled}>Isolated Browsing</InfoLabel>
            <p className="plane-form-section-copy">
              Search and sanitized fetch are enabled. Browser sessions stay approval-gated until
              explicitly allowed below.
            </p>
          </div>
          <label className="field-label plane-checkbox">
            <input
              type="checkbox"
              checked={form.browserSessionEnabled}
              onChange={bindCheck("browserSessionEnabled")}
            />
            <InfoLabel info={FIELD_INFO.browserSessionEnabled} className="field-label-inline">
              Enable browser sessions
            </InfoLabel>
          </label>
          <FieldHint
            message={fieldWarning("Browser sessions are ignored until Isolated Browsing is enabled.")}
            severity="warning"
          />
        </div>
      ) : null}

      {form.planeMode === "llm" && form.turboquantEnabled ? (
        <div className="plane-form-toggle">
          <div className="plane-form-section-header">
            <InfoLabel info={FIELD_INFO.turboquantEnabled}>TurboQuant</InfoLabel>
            <p className="plane-form-section-copy">
              KV-cache quantization for the llama.cpp serving path. Implicit defaults when enabled:
              {` ${TURBOQUANT_DEFAULT_CACHE_TYPE_K} / ${TURBOQUANT_DEFAULT_CACHE_TYPE_V}`}.
            </p>
          </div>
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.turboquantCacheTypeK}>K cache type</InfoLabel>
              <select
                className="text-input"
                value={form.turboquantCacheTypeK}
                onChange={bindText("turboquantCacheTypeK")}
              >
                {TURBOQUANT_CACHE_TYPES.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.turboquantCacheTypeV}>V cache type</InfoLabel>
              <select
                className="text-input"
                value={form.turboquantCacheTypeV}
                onChange={bindText("turboquantCacheTypeV")}
              >
                {TURBOQUANT_CACHE_TYPES.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
      ) : null}

      <SectionHeader
        title="Runtime"
        description={
          form.planeMode === "compute"
            ? "Custom worker runtime plane with explicit worker container startup."
            : "Replica-parallel llama.cpp serving layout with infer and worker services."
        }
      />
      <div className="plane-form-grid">
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.workers}>Workers</InfoLabel>
          <input
            className="text-input"
            type="number"
            min="1"
            value={form.workers}
            onChange={bindNumber("workers")}
          />
        </label>
        {form.planeMode === "compute" ? (
          <>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerImage}>Worker image</InfoLabel>
              <input
                className={inputClassName(Boolean(fieldError("Worker image is required for compute planes.")))}
                value={form.workerImage}
                onChange={bindText("workerImage")}
              />
              <FieldHint message={fieldError("Worker image is required for compute planes.")} />
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerStartType}>Worker start type</InfoLabel>
              <select
                className="text-input"
                value={form.workerStartType}
                onChange={bindText("workerStartType")}
              >
                <option value="script">script</option>
                <option value="command">command</option>
              </select>
            </label>
          </>
        ) : null}
      </div>
      {form.planeMode === "compute" ? (
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.workerStartValue}>
              {form.workerStartType === "script" ? "Worker script ref" : "Worker command"}
            </InfoLabel>
            <input
              className={inputClassName(Boolean(fieldError("Worker start is required for compute planes.")))}
              value={form.workerStartValue}
              onChange={bindText("workerStartValue")}
            />
            <FieldHint message={fieldError("Worker start is required for compute planes.")} />
          </label>
        </div>
      ) : null}

      {form.planeMode === "llm" ? (
        <>
          <SectionHeader
            title="Model"
            description="Choose a local model or point the plane at a remote model source."
          />
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.modelSourceType}>Model source type</InfoLabel>
              <select
                className="text-input"
                value={form.modelSourceType}
                onChange={bindText("modelSourceType")}
              >
                <option value="library">library</option>
                <option value="local">local</option>
                <option value="huggingface">huggingface</option>
                <option value="catalog">catalog</option>
                <option value="url">url</option>
              </select>
            </label>
          </div>

          {form.modelSourceType === "library" || form.modelSourceType === "local" ? (
            <div className="field-label">
              <InfoLabel
                info="Choose one locally available model from Model Library. The selected row becomes the plane model source."
                className="field-label-title"
              >
                Model Library
              </InfoLabel>
              <ModelLibraryPicker
                items={modelLibraryItems}
                selectedPath={form.modelPath}
                onSelect={selectLocalModel}
              />
              <FieldHint
                message={
                  fieldError("Local model path is required when model source type is local") ||
                  fieldError("Model Library selection is required when model source type is library")
                }
              />
            </div>
          ) : null}

          {form.modelSourceType !== "local" && form.modelSourceType !== "library" ? (
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.modelRef}>Model ref</InfoLabel>
                <input
                  className={inputClassName(
                    Boolean(fieldError("Model ref is required for catalog or huggingface sources")),
                  )}
                  value={form.modelRef}
                  onChange={bindText("modelRef")}
                />
                <FieldHint
                  message={fieldError("Model ref is required for catalog or huggingface sources")}
                />
              </label>
            </div>
          ) : null}

          {form.modelSourceType === "url" ? (
            <div className="plane-form-grid plane-form-grid-wide">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.modelUrl}>Primary model URL</InfoLabel>
                <input
                  className={inputClassName(
                    Boolean(fieldError("At least one model URL is required for url sources")),
                  )}
                  value={form.modelUrl}
                  onChange={bindText("modelUrl")}
                />
                <FieldHint
                  message={fieldError("At least one model URL is required for url sources")}
                />
              </label>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.modelUrls}>Additional model URLs</InfoLabel>
                <textarea
                  className="editor-textarea plane-form-textarea"
                  value={form.modelUrls}
                  onChange={bindText("modelUrls")}
                />
              </label>
            </div>
          ) : null}
        </>
      ) : null}

      {form.planeMode === "llm" && form.skillsEnabled ? (
        <AdvancedSection
          title="Skills sources"
          description="Attach canonical Skills Factory records only when this plane actually needs them."
        >
          <label className="field-label">
            <span className="field-label-title">Filter factory skills</span>
            <input
              className="text-input"
              value={factorySkillFilter}
              onChange={(event) => setFactorySkillFilter(event.target.value)}
              placeholder="Search by id, name, description, content, or plane usage"
            />
          </label>
          <div className="skills-factory-selector-layout">
            <div className="skills-factory-group-shell">
              <div className="plane-form-section-header">
                <span className="field-label-title">Groups</span>
              </div>
              <div className="toolbar">
                <button
                  className="ghost-button compact-button"
                  type="button"
                  disabled={!selectedFactoryGroupPath}
                  onClick={() => setSelectedFactoryGroupPath("")}
                >
                  Show all
                </button>
                <button
                  className="ghost-button compact-button"
                  type="button"
                  onClick={() => setExpandedFactoryGroupPaths(factoryTreePaths)}
                >
                  Expand all
                </button>
                <button
                  className="ghost-button compact-button"
                  type="button"
                  onClick={() => setExpandedFactoryGroupPaths([""])}
                >
                  Collapse tree
                </button>
              </div>
              <div className="skills-factory-group-tree">
                {renderFactoryGroupNode(factoryGroupTree)}
              </div>
            </div>
            <div className="factory-skill-table-shell">
              <table className="factory-skill-table">
                <thead>
                  <tr>
                    <th>Select</th>
                    <th>Group</th>
                    <th>Name</th>
                    <th>Id</th>
                    <th>Planes</th>
                    <th>Description</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredFactorySkills.length === 0 ? (
                    <tr>
                      <td colSpan="6" className="factory-skill-table-empty">
                        No Skills Factory records match the current filter.
                      </td>
                    </tr>
                  ) : (
                    filteredFactorySkills.map((item) => {
                      const selected = Array.isArray(form.factorySkillIds)
                        ? form.factorySkillIds.includes(item.id)
                        : false;
                      return (
                        <tr key={item.id}>
                          <td>
                            <input
                              type="checkbox"
                              checked={selected}
                              onChange={() => toggleFactorySkill(item.id)}
                            />
                          </td>
                          <td>{formatSkillGroupPath(item.group_path)}</td>
                          <td>{item.name || "unnamed-skill"}</td>
                          <td className="factory-skill-table-id">{item.id}</td>
                          <td>
                            {Array.isArray(item.plane_names)
                              ? item.plane_names.length
                              : item.plane_count || 0}
                          </td>
                          <td>{item.description || "No description"}</td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </AdvancedSection>
      ) : null}

      {form.planeMode === "llm" ? (
        <AdvancedSection
          title="Interaction defaults"
          description="Override the plane identity exposed to clients and default assistant interaction behavior."
        >
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.servedModelName}>Served model name</InfoLabel>
              <input className="text-input" value={form.servedModelName} onChange={bindText("servedModelName")} />
              <FieldHint
                message={!form.servedModelNameManual ? `Derived from plane name: ${deriveServedModelName(form.planeName)}` : ""}
                severity="warning"
              />
            </label>
            <div className="plane-form-section-actions">
              <button
                className="ghost-button compact-button"
                type="button"
                disabled={!form.servedModelNameManual}
                onClick={() => resetDerivedField("servedModelName")}
              >
                Use derived value
              </button>
            </div>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.defaultResponseLanguage}>Default response language</InfoLabel>
              <select
                className="text-input"
                value={form.defaultResponseLanguage}
                onChange={bindText("defaultResponseLanguage")}
              >
                {languageOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.value}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <div className="plane-form-grid">
            <label className="field-label plane-checkbox">
              <input
                type="checkbox"
                checked={form.followUserLanguage}
                onChange={bindCheck("followUserLanguage")}
              />
              <InfoLabel info={FIELD_INFO.followUserLanguage} className="field-label-inline">
                Follow user language
              </InfoLabel>
            </label>
            <label className="field-label plane-checkbox">
              <input
                type="checkbox"
                checked={form.thinkingEnabled}
                onChange={bindCheck("thinkingEnabled")}
              />
              <InfoLabel info={FIELD_INFO.thinkingEnabled} className="field-label-inline">
                Enable thinking
              </InfoLabel>
            </label>
          </div>
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.systemPrompt}>System prompt</InfoLabel>
            <textarea
              className="editor-textarea plane-form-textarea"
              value={form.systemPrompt}
              onChange={bindText("systemPrompt")}
            />
          </label>
        </AdvancedSection>
      ) : null}

      <AdvancedSection
        title="Runtime tuning"
        description="Override performance and model materialization settings when the defaults are not enough."
      >
        {form.planeMode === "llm" ? (
          <>
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.inferReplicas}>Infer replicas</InfoLabel>
                <input
                  className={inputClassName(
                    Boolean(
                      fieldError("Infer replicas must be a positive integer") ||
                        fieldError("Workers must be divisible by infer replicas"),
                    ),
                  )}
                  type="number"
                  min="1"
                  value={form.inferReplicas}
                  onChange={bindNumber("inferReplicas")}
                />
                <FieldHint message={fieldError("Infer replicas must be a positive integer")} />
                <FieldHint message={fieldError("Workers must be divisible by infer replicas")} />
              </label>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.maxModelLen}>Max model len</InfoLabel>
                <input
                  className="text-input"
                  type="number"
                  min="1"
                  value={form.maxModelLen}
                  onChange={bindNumber("maxModelLen")}
                />
              </label>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.maxNumSeqs}>Max num seqs</InfoLabel>
                <input
                  className="text-input"
                  type="number"
                  min="1"
                  value={form.maxNumSeqs}
                  onChange={bindNumber("maxNumSeqs")}
                />
              </label>
            </div>
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.gpuMemoryUtilization}>GPU memory utilization</InfoLabel>
                <input
                  className="text-input"
                  type="number"
                  step="0.05"
                  min="0.05"
                  max="1"
                  value={form.gpuMemoryUtilization}
                  onChange={bindNumber("gpuMemoryUtilization")}
                />
              </label>
            </div>
            {form.modelSourceType !== "local" ? (
              <>
                <div className="plane-form-grid">
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.materializationMode}>Materialization mode</InfoLabel>
                  <select
                    className="text-input"
                    value={form.materializationMode}
                    onChange={bindText("materializationMode")}
                  >
                    <option value="prepare_on_worker">prepare_on_worker</option>
                    <option value="download">download</option>
                    <option value="reference">reference</option>
                    <option value="copy">copy</option>
                  </select>
                </label>
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.materializationLocalPath}>Materialization local path</InfoLabel>
                    <input
                      className="text-input"
                      value={form.materializationLocalPath}
                      onChange={bindText("materializationLocalPath")}
                    />
                  </label>
                </div>
                {form.materializationMode === "prepare_on_worker" ? (
                  <>
                    <div className="plane-form-grid">
                      <label className="field-label">
                        <InfoLabel info={FIELD_INFO.sourceStorageNode}>Source storage node</InfoLabel>
                        <input
                          className={inputClassName(
                            Boolean(fieldError("Source storage node is required for worker model preparation")),
                          )}
                          value={form.materializationSourceNodeName}
                          onChange={bindText("materializationSourceNodeName")}
                        />
                        <FieldHint message={fieldError("Source storage node is required for worker model preparation")} />
                      </label>
                      <label className="field-label">
                        <InfoLabel info={FIELD_INFO.modelQuantization}>Quantization</InfoLabel>
                        <select
                          className="text-input"
                          value={form.modelQuantization}
                          onChange={bindText("modelQuantization")}
                        >
                          {MODEL_LIBRARY_QUANTIZATION_FILTERS.map((quantization) => (
                            <option key={quantization} value={quantization}>
                              {quantization}
                            </option>
                          ))}
                        </select>
                      </label>
                    </div>
                    <div className="plane-form-grid">
                      <label className="field-label">
                        <span className="field-label-title">Source format</span>
                        <input
                          className="text-input"
                          value={form.materializationSourceFormat}
                          onChange={bindText("materializationSourceFormat")}
                          placeholder="gguf, model-directory, safetensors"
                        />
                      </label>
                      <label className="field-label">
                        <span className="field-label-title">Source quantization</span>
                        <input
                          className="text-input"
                          value={form.materializationSourceQuantization}
                          onChange={bindText("materializationSourceQuantization")}
                          placeholder="Q8_0, Q4_K_M, base"
                        />
                      </label>
                      <label className="field-label">
                        <span className="field-label-title">Output format</span>
                        <input
                          className="text-input"
                          value={form.materializationDesiredOutputFormat}
                          onChange={bindText("materializationDesiredOutputFormat")}
                        />
                      </label>
                    </div>
                    <label className="field-label">
                      <span className="field-label-title">Source paths</span>
                      <textarea
                        className="editor-textarea plane-form-textarea"
                        value={(form.materializationSourcePaths || []).join("\n")}
                        onChange={(event) =>
                          updatePlaneDialogForm(setDialog, (current) => ({
                            ...current,
                            materializationSourcePaths: event.target.value
                              .split(/\r?\n/)
                              .map((item) => item.trim())
                              .filter(Boolean),
                          }))
                        }
                      />
                      <FieldHint message={fieldError("Source model paths are required for worker model preparation")} />
                    </label>
                    <div className="plane-form-grid">
                      <label className="field-label plane-checkbox">
                        <input
                          type="checkbox"
                          checked={form.modelWritebackEnabled}
                          onChange={bindCheck("modelWritebackEnabled")}
                        />
                        <span className="field-label-inline">Write prepared model back to storage</span>
                      </label>
                      <label className="field-label plane-checkbox">
                        <input
                          type="checkbox"
                          checked={form.modelWritebackIfMissing}
                          onChange={bindCheck("modelWritebackIfMissing")}
                        />
                        <span className="field-label-inline">Only upload if missing</span>
                      </label>
                    </div>
                    <label className="field-label">
                      <span className="field-label-title">Writeback target node</span>
                      <input
                        className="text-input"
                        value={form.modelWritebackTargetNodeName}
                        onChange={bindText("modelWritebackTargetNodeName")}
                        placeholder="Defaults to source storage node"
                      />
                    </label>
                  </>
                ) : null}
                <div className="plane-form-grid">
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.targetFilename}>Target filename</InfoLabel>
                    <input
                      className="text-input"
                      value={form.modelTargetFilename}
                      onChange={bindText("modelTargetFilename")}
                    />
                  </label>
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.sha256}>SHA256</InfoLabel>
                    <input
                      className="text-input"
                      value={form.modelSha256}
                      onChange={bindText("modelSha256")}
                    />
                  </label>
                </div>
              </>
            ) : null}
          </>
        ) : (
          <div className="plane-form-section-copy">
            Compute planes use the explicit worker image and start command as the primary runtime contract.
          </div>
        )}
      </AdvancedSection>

      <AdvancedSection
        title="Networking"
        description="Override ports and the advertised server name only when defaults are unsuitable."
      >
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.gatewayPort}>Gateway port</InfoLabel>
            <input
              className="text-input"
              type="number"
              min="1"
              value={form.gatewayPort}
              onChange={bindNumber("gatewayPort")}
            />
          </label>
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.inferencePort}>Inference port</InfoLabel>
            <input
              className="text-input"
              type="number"
              min="1"
              value={form.inferencePort}
              onChange={bindNumber("inferencePort")}
            />
          </label>
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.serverName}>Server name</InfoLabel>
            <input className="text-input" value={form.serverName} onChange={bindText("serverName")} />
            <FieldHint
              message={!form.serverNameManual ? `Derived from plane name: ${deriveServerName(form.planeName)}` : ""}
              severity="warning"
            />
          </label>
        </div>
        <div className="plane-form-section-actions">
          <button
            className="ghost-button compact-button"
            type="button"
            disabled={!form.serverNameManual}
            onClick={() => resetDerivedField("serverName")}
          >
            Use derived server name
          </button>
        </div>
      </AdvancedSection>

      <AdvancedSection
        title="Placement and topology"
        description="Execution node selection is the normal path. Custom topology stays available only for legacy split-host and manual placement layouts."
      >
        <SectionMeta>{topologySummary}</SectionMeta>
        <SectionActions>
          <button className="ghost-button" type="button" onClick={enableSingleHostLayout}>
            Use selected-node layout
          </button>
          <button className="ghost-button" type="button" onClick={generateSplitHostLayout}>
            Generate legacy split-host layout
          </button>
        </SectionActions>
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.executionNode}>Execution node</InfoLabel>
            <input
              className={inputClassName(Boolean(fieldError("Execution node is required.")))}
              value={form.executionNode}
              onChange={bindText("executionNode")}
            />
            <FieldHint message={fieldError("Execution node is required.")} />
          </label>
          <label className="field-label plane-checkbox">
            <input
              type="checkbox"
              checked={form.topologyEnabled}
              onChange={bindCheck("topologyEnabled")}
            />
            <InfoLabel info={FIELD_INFO.topologyEnabled} className="field-label-inline">
              Custom topology
            </InfoLabel>
          </label>
        </div>
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.topologyNodes}>Topology nodes</InfoLabel>
          <TopologyNodeRows
            nodes={form.topologyNodes}
            disabled={!form.topologyEnabled}
            onChange={updateTopologyNodes}
          />
          <FieldHint
            message={firstMatching(validation.errors, [
              "At least one topology node is required",
              "Each topology node row must have a node name",
              "Topology node names must be unique",
              "Referenced nodes are missing from topology",
            ])}
          />
        </label>
        <SectionMeta>{assignmentSummary}</SectionMeta>
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.placementMode}>Placement mode</InfoLabel>
            <select className="text-input" value={form.placementMode} onChange={bindText("placementMode")}>
              <option value="auto">auto</option>
              <option value="manual">manual</option>
              <option value="movable">movable</option>
            </select>
          </label>
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.shareMode}>Share mode</InfoLabel>
            <select className="text-input" value={form.shareMode} onChange={bindShareMode()}>
              <option value="exclusive">exclusive</option>
              <option value="shared">shared</option>
              <option value="best-effort">best-effort</option>
            </select>
            <FieldHint
              message={
                form.shareMode === "exclusive"
                  ? "Exclusive workers always reserve the whole GPU, so GPU fraction is fixed at 1.0."
                  : fieldError("Exclusive share mode requires GPU fraction equal to 1.0")
              }
              severity={form.shareMode === "exclusive" ? "warning" : "error"}
            />
          </label>
          {form.shareMode !== "exclusive" ? (
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.gpuFraction}>GPU fraction</InfoLabel>
              <input
                className={inputClassName(
                  Boolean(fieldError("Exclusive share mode requires GPU fraction equal to 1.0")),
                )}
                type="number"
                step="0.05"
                min="0"
                max="1"
                value={form.gpuFraction}
                onChange={bindNumber("gpuFraction")}
              />
            </label>
          ) : null}
        </div>
        {form.topologyEnabled ? (
          <>
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.workerNode}>Worker node</InfoLabel>
                <input className="text-input" value={form.workerNode} onChange={bindText("workerNode")} />
              </label>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.workerGpuDevice}>Worker GPU device</InfoLabel>
                <input
                  className="text-input"
                  value={form.workerGpuDevice}
                  onChange={bindText("workerGpuDevice")}
                />
              </label>
              {form.planeMode === "llm" ? (
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferNode}>Infer node</InfoLabel>
                  <input className="text-input" value={form.inferNode} onChange={bindText("inferNode")} />
                </label>
              ) : null}
            </div>
            <div className="plane-form-grid">
              <label className="field-label plane-checkbox">
                <input
                  type="checkbox"
                  checked={form.workerAssignmentsEnabled}
                  onChange={bindCheck("workerAssignmentsEnabled")}
                />
                <InfoLabel info={FIELD_INFO.workerAssignmentsEnabled} className="field-label-inline">
                  Per-worker assignments
                </InfoLabel>
              </label>
            </div>
            <SectionActions>
              <button className="ghost-button" type="button" onClick={matchAssignmentsToWorkers}>
                Match assignments to worker count
              </button>
            </SectionActions>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerAssignments}>Worker assignments</InfoLabel>
              <WorkerAssignmentRows
                assignments={form.workerAssignments}
                disabled={!form.workerAssignmentsEnabled}
                onChange={updateWorkerAssignments}
              />
              <FieldHint
                message={firstMatching(validation.errors, [
                  "Worker assignments count must match the number of workers",
                  "Each worker assignment must include a node name",
                ])}
              />
              <FieldHint
                message={fieldWarning("Per-worker assignments are easier to reason about")}
                severity="warning"
              />
            </label>
          </>
        ) : (
          <FieldHint
            message="Legacy per-service node pinning and per-worker assignments stay disabled until Custom topology is enabled."
            severity="warning"
          />
        )}
      </AdvancedSection>

      <AdvancedSection
        title="Worker overrides"
        description="Override worker container wiring only when the default worker runtime is not enough."
      >
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.memoryCapMb}>Worker memory cap MB</InfoLabel>
            <input
              className="text-input"
              type="number"
              min="1"
              value={form.memoryCapMb}
              onChange={bindNumber("memoryCapMb")}
            />
          </label>
          {form.planeMode !== "compute" ? (
            <>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.workerImage}>Worker image</InfoLabel>
                <input className="text-input" value={form.workerImage} onChange={bindText("workerImage")} />
              </label>
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.workerStartType}>Worker start type</InfoLabel>
                <select
                  className="text-input"
                  value={form.workerStartType}
                  onChange={bindText("workerStartType")}
                >
                  <option value="script">script</option>
                  <option value="command">command</option>
                </select>
              </label>
            </>
          ) : null}
        </div>
        {form.planeMode !== "compute" ? (
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerStartValue}>
                {form.workerStartType === "script" ? "Worker script ref" : "Worker command"}
              </InfoLabel>
              <input
                className="text-input"
                value={form.workerStartValue}
                onChange={bindText("workerStartValue")}
              />
            </label>
          </div>
        ) : null}
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.workerEnv}>Worker env</InfoLabel>
          <textarea
            className="editor-textarea plane-form-textarea"
            value={form.workerEnvText}
            onChange={bindText("workerEnvText")}
          />
        </label>
      </AdvancedSection>

      {form.planeMode === "llm" ? (
        <AdvancedSection
          title="Infer overrides"
          description="Leave this collapsed unless you need a custom infer container, node pinning, or explicit infer storage."
        >
          <div className="plane-form-toggle">
            <label className="field-label plane-checkbox">
              <input
                type="checkbox"
                checked={form.inferOverridesEnabled}
                onChange={bindCheck("inferOverridesEnabled")}
              />
              <InfoLabel
                info="Enable this only if you want to override the default infer container wiring."
                className="field-label-inline"
              >
                Configure Infer Overrides
              </InfoLabel>
            </label>
            <div className="plane-form-toggle-copy">
              By default infer runs without a private disk. Enable infer storage only when the service truly needs writable local state.
            </div>
          </div>
          {form.inferOverridesEnabled ? (
            <>
              <div className="plane-form-grid">
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferImage}>Infer image</InfoLabel>
                  <input className="text-input" value={form.inferImage} onChange={bindText("inferImage")} />
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferStartType}>Infer start type</InfoLabel>
                  <select
                    className="text-input"
                    value={form.inferStartType}
                    onChange={bindText("inferStartType")}
                  >
                    <option value="script">script</option>
                    <option value="command">command</option>
                  </select>
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferStartValue}>
                    {form.inferStartType === "script" ? "Infer script ref" : "Infer command"}
                  </InfoLabel>
                  <input
                    className="text-input"
                    value={form.inferStartValue}
                    onChange={bindText("inferStartValue")}
                  />
                </label>
              </div>
              <div className="plane-form-grid">
                {form.topologyEnabled ? (
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.inferNode}>Infer node</InfoLabel>
                    <input className="text-input" value={form.inferNode} onChange={bindText("inferNode")} />
                  </label>
                ) : (
                  <div className="field-label">
                    <InfoLabel info={FIELD_INFO.executionNode}>Infer node</InfoLabel>
                    <div className="plane-form-section-copy">
                      Infer colocates with the selected execution node unless legacy topology is enabled.
                    </div>
                  </div>
                )}
                <label className="field-label plane-checkbox">
                  <input
                    type="checkbox"
                    checked={form.inferStorageEnabled}
                    onChange={bindCheck("inferStorageEnabled")}
                  />
                  <InfoLabel info={FIELD_INFO.inferStorageEnabled} className="field-label-inline">
                    Infer storage
                  </InfoLabel>
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferStorageSizeGb}>Infer storage GB</InfoLabel>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    value={form.inferStorageSizeGb}
                    onChange={bindNumber("inferStorageSizeGb")}
                    disabled={!form.inferStorageEnabled}
                  />
                </label>
              </div>
              <div className="plane-form-grid plane-form-grid-wide">
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferStorageMountPath}>Infer storage mount path</InfoLabel>
                  <input
                    className="text-input"
                    value={form.inferStorageMountPath}
                    onChange={bindText("inferStorageMountPath")}
                    disabled={!form.inferStorageEnabled}
                  />
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.inferEnv}>Infer env</InfoLabel>
                  <textarea
                    className="editor-textarea plane-form-textarea"
                    value={form.inferEnvText}
                    onChange={bindText("inferEnvText")}
                  />
                </label>
              </div>
            </>
          ) : null}
        </AdvancedSection>
      ) : null}

      <AdvancedSection
        title="Storage and resources"
        description="Control shared-disk size and opt into explicit writable storage only where it is justified."
      >
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.sharedDiskGb}>Shared disk GB</InfoLabel>
            <input
              className="text-input"
              type="number"
              min="1"
              value={form.sharedDiskGb}
              onChange={bindNumber("sharedDiskGb")}
            />
          </label>
          <label className="field-label plane-checkbox">
            <input
              type="checkbox"
              checked={form.workerStorageEnabled}
              onChange={bindCheck("workerStorageEnabled")}
            />
            <InfoLabel info={FIELD_INFO.workerStorageEnabled} className="field-label-inline">
              Worker storage
            </InfoLabel>
          </label>
        </div>
        {form.workerStorageEnabled ? (
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerStorageSizeGb}>Worker storage GB</InfoLabel>
              <input
                className="text-input"
                type="number"
                min="1"
                value={form.workerStorageSizeGb}
                onChange={bindNumber("workerStorageSizeGb")}
              />
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.workerStorageMountPath}>Worker storage mount path</InfoLabel>
              <input
                className="text-input"
                value={form.workerStorageMountPath}
                onChange={bindText("workerStorageMountPath")}
              />
            </label>
          </div>
        ) : null}
      </AdvancedSection>

      {form.appEnabled ? (
        <AdvancedSection
          title="App advanced"
          description="Keep app storage and wiring explicit because app containers are treated as opaque user workloads."
        >
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.appImage}>App image</InfoLabel>
              <input
                className={inputClassName(Boolean(fieldError("App image is required when app is enabled")))}
                value={form.appImage}
                onChange={bindText("appImage")}
              />
              <FieldHint message={fieldError("App image is required when app is enabled")} />
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.appStartType}>App start type</InfoLabel>
              <select
                className="text-input"
                value={form.appStartType}
                onChange={bindText("appStartType")}
              >
                <option value="script">script</option>
                <option value="command">command</option>
              </select>
            </label>
          </div>
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.appStartValue}>
                {form.appStartType === "script" ? "App script ref" : "App command"}
              </InfoLabel>
              <input
                className="text-input"
                value={form.appStartValue}
                onChange={bindText("appStartValue")}
              />
              <FieldHint
                message={fieldWarning("App start is empty. The app container will rely on its image default command.")}
                severity="warning"
              />
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.appHostPort}>App host port</InfoLabel>
              <input
                className="text-input"
                type="number"
                min="1"
                value={form.appHostPort}
                onChange={bindNumber("appHostPort")}
              />
            </label>
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.appContainerPort}>App container port</InfoLabel>
              <input
                className="text-input"
                type="number"
                min="1"
                value={form.appContainerPort}
                onChange={bindNumber("appContainerPort")}
              />
            </label>
          </div>
          <div className="plane-form-grid">
            <label className="field-label plane-checkbox">
              <input
                type="checkbox"
                checked={form.appHostEnabled}
                onChange={bindCheck("appHostEnabled")}
              />
              <InfoLabel info={FIELD_INFO.appHostEnabled} className="field-label-inline">
                External app host
              </InfoLabel>
            </label>
            {form.topologyEnabled ? (
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.appNode}>App node</InfoLabel>
                <input className="text-input" value={form.appNode} onChange={bindText("appNode")} />
              </label>
            ) : (
              <div className="field-label">
                <InfoLabel info={FIELD_INFO.executionNode}>App placement</InfoLabel>
                <div className="plane-form-section-copy">
                  App runs on the selected execution node by default and moves only when External app host is enabled.
                </div>
              </div>
            )}
            <label className="field-label plane-checkbox">
              <input
                type="checkbox"
                checked={form.appVolumeEnabled}
                onChange={bindCheck("appVolumeEnabled")}
              />
              <InfoLabel info={FIELD_INFO.appVolumeEnabled} className="field-label-inline">
                App volume
              </InfoLabel>
            </label>
          </div>
          {form.appHostEnabled ? (
            <>
              <div className="plane-form-grid">
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appHostAddress}>App host address</InfoLabel>
                  <input
                    className={inputClassName(Boolean(fieldError("External app host address is required.")))}
                    value={form.appHostAddress}
                    onChange={bindText("appHostAddress")}
                  />
                  <FieldHint message={fieldError("External app host address is required.")} />
                  <FieldHint message={fieldError("External app host requires the app container to be enabled.")} />
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appHostAuthMode}>App host auth mode</InfoLabel>
                  <select
                    className="text-input"
                    value={form.appHostAuthMode}
                    onChange={bindText("appHostAuthMode")}
                  >
                    <option value="ssh-key">ssh-key</option>
                    <option value="password">password</option>
                  </select>
                </label>
              </div>
              {form.appHostAuthMode === "password" ? (
                <div className="plane-form-grid">
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.appHostUsername}>App host username</InfoLabel>
                    <input
                      className={inputClassName(Boolean(fieldError("External app host username and password are required together.")))}
                      value={form.appHostUsername}
                      onChange={bindText("appHostUsername")}
                    />
                  </label>
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.appHostPassword}>App host password</InfoLabel>
                    <input
                      className={inputClassName(Boolean(fieldError("External app host username and password are required together.")))}
                      type="password"
                      value={form.appHostPassword}
                      onChange={bindText("appHostPassword")}
                    />
                    <FieldHint message={fieldError("External app host username and password are required together.")} />
                  </label>
                </div>
              ) : (
                <div className="plane-form-grid">
                  <label className="field-label">
                    <InfoLabel info={FIELD_INFO.appHostSshKeyPath}>App host SSH key path</InfoLabel>
                    <input
                      className={inputClassName(Boolean(fieldError("External app host SSH key path is required.")))}
                      value={form.appHostSshKeyPath}
                      onChange={bindText("appHostSshKeyPath")}
                    />
                    <FieldHint message={fieldError("External app host SSH key path is required.")} />
                  </label>
                </div>
              )}
            </>
          ) : null}
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.appEnv}>App env</InfoLabel>
            <textarea
              className="editor-textarea plane-form-textarea"
              value={form.appEnvText}
              onChange={bindText("appEnvText")}
            />
          </label>
          {form.appVolumeEnabled ? (
            <>
              <div className="plane-form-grid">
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appVolumeName}>App volume name</InfoLabel>
                  <input
                    className="text-input"
                    value={form.appVolumeName}
                    onChange={bindText("appVolumeName")}
                  />
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appVolumeType}>App volume type</InfoLabel>
                  <select
                    className="text-input"
                    value={form.appVolumeType}
                    onChange={bindText("appVolumeType")}
                  >
                    <option value="persistent">persistent</option>
                    <option value="ephemeral">ephemeral</option>
                  </select>
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appVolumeSizeGb}>App volume size GB</InfoLabel>
                  <input
                    className="text-input"
                    type="number"
                    min="1"
                    value={form.appVolumeSizeGb}
                    onChange={bindNumber("appVolumeSizeGb")}
                  />
                </label>
              </div>
              <div className="plane-form-grid">
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appVolumeMountPath}>App volume mount path</InfoLabel>
                  <input
                    className="text-input"
                    value={form.appVolumeMountPath}
                    onChange={bindText("appVolumeMountPath")}
                  />
                </label>
                <label className="field-label">
                  <InfoLabel info={FIELD_INFO.appVolumeAccess}>App volume access</InfoLabel>
                  <select
                    className="text-input"
                    value={form.appVolumeAccess}
                    onChange={bindText("appVolumeAccess")}
                  >
                    <option value="rw">rw</option>
                    <option value="ro">ro</option>
                  </select>
                </label>
              </div>
            </>
          ) : null}
        </AdvancedSection>
      ) : null}

      <AdvancedSection
        title="Hooks"
        description="Run an optional post-deploy script after the plane has been materialized."
      >
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.postDeployScript}>Post-deploy script</InfoLabel>
          <input className="text-input" value={form.postDeployScript} onChange={bindText("postDeployScript")} />
        </label>
      </AdvancedSection>
    </div>
  );
}
