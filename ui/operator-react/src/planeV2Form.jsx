const DEFAULT_SUPPORTED_RESPONSE_LANGUAGES = ["en", "de", "uk", "ru"];

const FIELD_INFO = {
  planeName: "Unique plane identifier used by the controller, runtime artifacts, and API paths.",
  skillsEnabled: "Enable a dedicated plane-scoped Skills service backed by SQLite for storing and resolving reusable skills.",
  planeMode: "Choose llm for model serving planes or compute for custom GPU workloads without chat interaction.",
  protectedPlane: "Protected planes require an explicit confirmation before destructive actions such as delete.",
  runtimeEngine: "Runtime implementation used by infer and worker services.",
  workers: "How many worker instances should be created for this plane.",
  inferReplicas: "How many leaf infer replicas should be created behind the aggregator head.",
  maxModelLen: "Maximum context window exposed by the serving runtime.",
  maxNumSeqs: "Maximum number of sequences the runtime will batch concurrently.",
  gpuMemoryUtilization: "Target fraction of GPU memory reserved by the runtime for model weights and KV cache.",
  servedModelName: "Public model name returned by OpenAI-compatible endpoints for this plane.",
  modelSourceType: "Where the model definition comes from: local storage, Hugging Face, catalog, or direct URL.",
  modelRef: "Logical model reference, usually a Hugging Face id or catalog key.",
  modelUrl: "Primary remote URL used to download the model artifact.",
  modelUrls: "Additional URLs for multipart or sharded model downloads.",
  materializationMode: "Whether the model should be referenced in place, copied, or downloaded before serving.",
  materializationLocalPath: "Optional local path used when materializing a referenced model artifact.",
  defaultResponseLanguage: "Language the assistant should prefer when no explicit user language is given.",
  targetFilename: "Optional target filename used when downloading model artifacts.",
  sha256: "Optional checksum used to validate downloaded model artifacts.",
  followUserLanguage: "If enabled, the assistant follows the language of the latest user message.",
  thinkingEnabled: "If enabled, the model may use hidden reasoning internally, but only the final answer is shown to the user.",
  systemPrompt: "Default system prompt injected into LLM interactions for this plane.",
  gatewayPort: "HTTP port exposed by the plane gateway.",
  inferencePort: "Internal inference API port used by infer and worker services.",
  serverName: "Logical host name advertised by the gateway and status surfaces.",
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

function parseNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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

export function isDesiredStateV2(value) {
  return Boolean(value && typeof value === "object" && value.version === 2);
}

export function buildNewPlaneFormState() {
  return {
    planeName: "new-plane",
    skillsEnabled: false,
    planeMode: "llm",
    protectedPlane: false,
    modelSourceType: "local",
    modelRef: "",
    modelPath: "",
    modelUrl: "",
    modelUrls: "",
    materializationMode: "reference",
    materializationLocalPath: "",
    servedModelName: "new-plane-model",
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
    serverName: "new-plane.local",
    topologyEnabled: false,
    topologyNodes: [],
    inferImage: "",
    inferOverridesEnabled: false,
    inferStartType: "command",
    inferStartValue: "",
    inferEnvText: "",
    inferNode: "",
    inferStorageEnabled: false,
    inferStorageSizeGb: 8,
    inferStorageMountPath: "/comet/private",
    workerImage: "",
    workerStartType: "command",
    workerStartValue: "",
    workerEnvText: "",
    workerNode: "",
    workerGpuDevice: "",
    workerAssignmentsEnabled: false,
    workerAssignments: [],
    workerStorageEnabled: false,
    workerStorageSizeGb: 24,
    workerStorageMountPath: "/comet/private",
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
    appVolumeMountPath: "/comet/private",
    appVolumeAccess: "rw",
    placementMode: "auto",
    shareMode: "exclusive",
    gpuFraction: 1.0,
    memoryCapMb: 24576,
    sharedDiskGb: 40,
    postDeployScript: "bundle://deploy/scripts/post-deploy.sh",
  };
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
  return {
    ...defaults,
    planeName: value?.plane_name || defaults.planeName,
    skillsEnabled: Boolean(value?.skills?.enabled),
    planeMode: value?.plane_mode || defaults.planeMode,
    protectedPlane: Boolean(value?.protected),
    modelSourceType: source.type || defaults.modelSourceType,
    modelRef: source.ref || defaults.modelRef,
    modelPath: source.path || "",
    modelUrl: source.url || "",
    modelUrls: Array.isArray(source.urls) ? source.urls.join("\n") : "",
    materializationMode: materialization.mode || defaults.materializationMode,
    materializationLocalPath: materialization.local_path || "",
    servedModelName: value?.model?.served_model_name || defaults.servedModelName,
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
    serverName: network.server_name || defaults.serverName,
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
  const source = { type: form.modelSourceType };
  if (form.modelSourceType === "local") {
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
    plane_name: form.planeName.trim(),
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
      server_name: form.serverName.trim() || "new-plane.local",
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

  if (form.planeMode === "llm") {
    desiredState.model = {
      source,
      materialization: {
        mode: form.materializationMode,
      },
      served_model_name: form.servedModelName.trim() || form.planeName.trim(),
    };
    if (form.materializationLocalPath.trim()) {
      desiredState.model.materialization.local_path = form.materializationLocalPath.trim();
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
    if (form.inferNode.trim()) {
      desiredState.infer.node = form.inferNode.trim();
    }
    if (form.inferStorageEnabled) {
      desiredState.infer.storage = {
        size_gb: parseNumber(form.inferStorageSizeGb, 8),
        mount_path: form.inferStorageMountPath.trim() || "/comet/private",
      };
    }
  }

  if (
    form.workerImage.trim() ||
    form.workerStartValue.trim() ||
    form.workerEnvText.trim() ||
    form.workerNode.trim() ||
    form.workerGpuDevice.trim() ||
    form.workerStorageEnabled
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
    if (form.workerNode.trim()) {
      desiredState.worker.node = form.workerNode.trim();
    }
    if (form.workerGpuDevice.trim()) {
      desiredState.worker.gpu_device = form.workerGpuDevice.trim();
    }
    if (form.workerAssignmentsEnabled) {
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
        mount_path: form.workerStorageMountPath.trim() || "/comet/private",
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
    if (form.appNode.trim()) {
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
          mount_path: form.appVolumeMountPath.trim() || "/comet/private",
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
    if (!String(form?.servedModelName || "").trim()) {
      errors.push("Served model name is required for llm planes.");
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

  const referencedNodes = [
    String(form?.inferNode || "").trim(),
    String(form?.workerNode || "").trim(),
    String(form?.appNode || "").trim(),
    ...enabledAssignments.map((assignment) => String(assignment.node || "").trim()),
  ].filter(Boolean);
  if (form?.topologyEnabled && topologyNodeNames.length > 0) {
    const unknownNodes = referencedNodes.filter((name) => !topologyNodeNames.includes(name));
    if (unknownNodes.length > 0) {
      errors.push(`Referenced nodes are missing from topology: ${[...new Set(unknownNodes)].join(", ")}`);
    }
  }

  if (form?.workerAssignmentsEnabled && !form?.topologyEnabled) {
    warnings.push("Per-worker assignments are easier to reason about when custom topology is enabled.");
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

export function PlaneV2FormBuilder({ dialog, setDialog, languageOptions, modelLibraryItems = [] }) {
  const form = dialog.form || buildNewPlaneFormState();
  const validation = validatePlaneV2Form(form);
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
      updatePlaneDialogForm(setDialog, (current) => ({
        ...current,
        [key]: event.target.value,
      }));
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
      updatePlaneDialogForm(setDialog, (current) => ({
        ...current,
        [key]: event.target.checked,
      }));
  }

  function selectLocalModel(item) {
    updatePlaneDialogForm(setDialog, (current) => {
      const planeName = String(current.planeName || "").trim();
      const fallbackName = item?.name || item?.path?.split("/").pop() || planeName || "model";
      return {
        ...current,
        modelPath: item?.path || "",
        modelRef: item?.model_id || item?.name || item?.path || "",
        materializationMode: "reference",
        materializationLocalPath: item?.path || "",
        servedModelName:
          String(current.servedModelName || "").trim() && current.servedModelName !== "new-plane-model"
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
      topologyEnabled: true,
      topologyNodes: [{ name: "local-hostd", executionMode: "mixed", gpuMemoryText: "" }],
      inferNode: "local-hostd",
      workerNode: "local-hostd",
      appNode: current.appEnabled ? "local-hostd" : current.appNode,
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

  return (
    <div className="plane-form-builder">
      <SectionHeader
        title="Plane"
        description="Identity and mode for the plane you are about to create."
      />
      <div className="plane-form-grid">
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.skillsEnabled}
            onChange={bindCheck("skillsEnabled")}
            disabled={form.planeMode !== "llm"}
          />
          <InfoLabel info={FIELD_INFO.skillsEnabled} className="field-label-inline">Skills</InfoLabel>
        </label>
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
          <select className="text-input" value={form.planeMode} onChange={bindText("planeMode")}>
            <option value="llm">llm</option>
            <option value="compute">compute</option>
          </select>
        </label>
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.protectedPlane}
            onChange={bindCheck("protectedPlane")}
          />
          <InfoLabel info={FIELD_INFO.protectedPlane} className="field-label-inline">Protected plane</InfoLabel>
        </label>
      </div>

      <SectionHeader
        title="Runtime"
        description="Replica-parallel llama.cpp layout with one aggregator head and leaf infer replicas."
      />
      <div className="plane-form-grid">
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.runtimeEngine}>Runtime engine</InfoLabel>
          <input
            className="text-input"
            value={form.planeMode === "compute" ? "custom worker runtime" : "llama.cpp + llama_rpc"}
            disabled
            readOnly
          />
        </label>
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
            disabled={form.planeMode === "compute"}
          />
          <FieldHint message={fieldError("Infer replicas must be a positive integer")} />
          <FieldHint message={fieldError("Workers must be divisible by infer replicas")} />
        </label>
      </div>

      <div className="plane-form-grid">
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

      {form.planeMode === "llm" ? (
        <div className="plane-form-grid">
          <label className="field-label">
            <InfoLabel info={FIELD_INFO.servedModelName}>Served model name</InfoLabel>
            <input
              className={inputClassName(
                Boolean(fieldError("Served model name is required for llm planes")),
              )}
              value={form.servedModelName}
              onChange={bindText("servedModelName")}
            />
            <FieldHint message={fieldError("Served model name is required for llm planes")} />
          </label>
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
          <label className="field-label plane-checkbox">
            <input
              type="checkbox"
              checked={form.followUserLanguage}
              onChange={bindCheck("followUserLanguage")}
            />
            <InfoLabel info={FIELD_INFO.followUserLanguage} className="field-label-inline">Follow user language</InfoLabel>
          </label>
          <label className="field-label plane-checkbox">
            <input
              type="checkbox"
              checked={form.thinkingEnabled}
              onChange={bindCheck("thinkingEnabled")}
            />
            <InfoLabel info={FIELD_INFO.thinkingEnabled} className="field-label-inline">Enable thinking</InfoLabel>
          </label>
        </div>
      ) : null}

      {form.planeMode === "llm" ? (
        <>
          <SectionHeader
            title="Model"
            description="Choose a known model, a local path, or a download source."
          />
          <div className="plane-form-grid">
            <label className="field-label">
              <InfoLabel info={FIELD_INFO.modelSourceType}>Model source type</InfoLabel>
              <select
                className="text-input"
                value={form.modelSourceType}
                onChange={bindText("modelSourceType")}
              >
                <option value="local">local</option>
                <option value="huggingface">huggingface</option>
                <option value="catalog">catalog</option>
                <option value="url">url</option>
              </select>
            </label>
          </div>

          {form.modelSourceType === "local" ? (
            <div className="field-label">
              <InfoLabel info="Choose one locally available model from Model Library. The selected row becomes the plane model source." className="field-label-title">
                Model Library
              </InfoLabel>
              <ModelLibraryPicker
                items={modelLibraryItems}
                selectedPath={form.modelPath}
                onSelect={selectLocalModel}
              />
              <FieldHint
                message={fieldError("Local model path is required when model source type is local")}
              />
            </div>
          ) : null}

          {form.modelSourceType !== "local" ? (
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.modelRef}>Model ref</InfoLabel>
                <input
                  className={inputClassName(
                    Boolean(
                      fieldError("Model ref is required for catalog or huggingface sources"),
                    ),
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

          {form.modelSourceType !== "local" ? (
            <div className="plane-form-grid">
              <label className="field-label">
                <InfoLabel info={FIELD_INFO.materializationMode}>Materialization mode</InfoLabel>
                <select
                  className="text-input"
                  value={form.materializationMode}
                  onChange={bindText("materializationMode")}
                >
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
          ) : null}

          {form.modelSourceType !== "local" ? (
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
          ) : null}

          <label className="field-label">
            <InfoLabel info={FIELD_INFO.systemPrompt}>System prompt</InfoLabel>
            <textarea
              className="editor-textarea plane-form-textarea"
              value={form.systemPrompt}
              onChange={bindText("systemPrompt")}
            />
          </label>
        </>
      ) : null}

      <SectionHeader
        title="Network"
        description="Ports and server name exposed by the plane."
      />
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
        </label>
      </div>

      <SectionHeader
        title="Topology"
        description="Optional advanced node layout for single-host or split-host placement."
      />
      <SectionMeta>{topologySummary}</SectionMeta>
      <SectionActions>
        <button className="ghost-button" type="button" onClick={enableSingleHostLayout}>
          Use single-host layout
        </button>
        <button className="ghost-button" type="button" onClick={generateSplitHostLayout}>
          Generate split-host layout
        </button>
      </SectionActions>
      <div className="plane-form-grid">
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.topologyEnabled}
            onChange={bindCheck("topologyEnabled")}
          />
          <InfoLabel info={FIELD_INFO.topologyEnabled} className="field-label-inline">Custom topology</InfoLabel>
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

      <SectionHeader
        title="Worker"
        description="Resource policy plus optional custom worker container for compute or advanced runtimes."
      />
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
      </div>

      <div className="plane-form-grid">
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.workerStartValue}>{form.workerStartType === "script" ? "Worker script ref" : "Worker command"}</InfoLabel>
          <input
            className="text-input"
            value={form.workerStartValue}
            onChange={bindText("workerStartValue")}
          />
        </label>
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
      </div>

      <div className="plane-form-grid">
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.workerAssignmentsEnabled}
            onChange={bindCheck("workerAssignmentsEnabled")}
          />
          <InfoLabel info={FIELD_INFO.workerAssignmentsEnabled} className="field-label-inline">Per-worker assignments</InfoLabel>
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

      <label className="field-label">
        <InfoLabel info={FIELD_INFO.workerEnv}>Worker env</InfoLabel>
        <textarea
          className="editor-textarea plane-form-textarea"
          value={form.workerEnvText}
          onChange={bindText("workerEnvText")}
        />
      </label>

      <div className="plane-form-grid">
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.workerStorageEnabled}
            onChange={bindCheck("workerStorageEnabled")}
          />
          <InfoLabel info={FIELD_INFO.workerStorageEnabled} className="field-label-inline">Set worker storage size and location</InfoLabel>
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

      <div className="plane-form-toggle">
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.inferOverridesEnabled}
            onChange={bindCheck("inferOverridesEnabled")}
          />
          <InfoLabel info="Enable this only if you want to override the default infer container wiring." className="field-label-inline">Configure Infer Overrides</InfoLabel>
        </label>
        <div className="plane-form-toggle-copy">
          Leave this off to let the platform render the infer container automatically.
        </div>
      </div>
      {form.inferOverridesEnabled ? (
        <>
      <SectionHeader
        title="Infer Overrides"
        description="Optional container overrides for the infer service."
      />
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
          <InfoLabel info={FIELD_INFO.inferStartValue}>{form.inferStartType === "script" ? "Infer script ref" : "Infer command"}</InfoLabel>
          <input
            className="text-input"
            value={form.inferStartValue}
            onChange={bindText("inferStartValue")}
          />
        </label>
      </div>

      <div className="plane-form-grid">
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.inferNode}>Infer node</InfoLabel>
          <input className="text-input" value={form.inferNode} onChange={bindText("inferNode")} />
        </label>
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.inferStorageEnabled}
            onChange={bindCheck("inferStorageEnabled")}
          />
          <InfoLabel info={FIELD_INFO.inferStorageEnabled} className="field-label-inline">Infer storage</InfoLabel>
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

      <div className="plane-form-toggle">
        <label className="field-label plane-checkbox">
          <input type="checkbox" checked={form.appEnabled} onChange={bindCheck("appEnabled")} />
          <InfoLabel info={FIELD_INFO.appEnabled} className="field-label-inline">Enable App</InfoLabel>
        </label>
        <div className="plane-form-toggle-copy">
          Leave this off for backend-only planes without an app container.
        </div>
      </div>
      {form.appEnabled ? (
        <>
      <SectionHeader
        title="App"
        description="Optional app container and its exposed port and writable volume."
      />
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
          <InfoLabel info={FIELD_INFO.appStartValue}>{form.appStartType === "script" ? "App script ref" : "App command"}</InfoLabel>
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
        <label className="field-label">
          <InfoLabel info={FIELD_INFO.appNode}>App node</InfoLabel>
          <input
            className="text-input"
            value={form.appNode}
            onChange={bindText("appNode")}
          />
        </label>
        <label className="field-label plane-checkbox">
          <input
            type="checkbox"
            checked={form.appVolumeEnabled}
            onChange={bindCheck("appVolumeEnabled")}
          />
          <InfoLabel info={FIELD_INFO.appVolumeEnabled} className="field-label-inline">App volume</InfoLabel>
        </label>
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
      </div>

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
        </>
      ) : null}

      <SectionHeader
        title="Hooks"
        description="Optional post-deploy script run after the plane is materialized."
      />
      <label className="field-label">
        <InfoLabel info={FIELD_INFO.postDeployScript}>Post-deploy script</InfoLabel>
        <input className="text-input" value={form.postDeployScript} onChange={bindText("postDeployScript")} />
      </label>
    </div>
  );
}
