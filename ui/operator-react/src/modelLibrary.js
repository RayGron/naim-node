export const MODEL_LIBRARY_FORMAT_OPTIONS = [
  { value: "gguf", label: "GGUF" },
  { value: "safetensors", label: "safetensors" },
];

export const MODEL_LIBRARY_GGUF_QUANTIZATIONS = [
  "Q8_0",
  "Q5_K_M",
  "Q4_K_M",
  "IQ4_NL",
];

export const MODEL_LIBRARY_QUANTIZATION_FILTERS = [
  "base",
  ...MODEL_LIBRARY_GGUF_QUANTIZATIONS,
];

export function normalizeModelDownloadSourceUrls(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function detectModelSourceFormat(value) {
  const sourceUrls = normalizeModelDownloadSourceUrls(value);
  if (sourceUrls.length === 0) {
    return "unknown";
  }
  let sawGguf = false;
  let sawSafetensors = false;
  for (const sourceUrl of sourceUrls) {
    const normalized = String(sourceUrl).toLowerCase().split(/[?#]/, 1)[0];
    if (normalized.endsWith(".gguf")) {
      sawGguf = true;
      continue;
    }
    if (normalized.endsWith(".safetensors")) {
      sawSafetensors = true;
      continue;
    }
    if (
      normalized.endsWith(".json") ||
      normalized.endsWith(".model") ||
      normalized.endsWith(".txt")
    ) {
      continue;
    }
    return "unknown";
  }
  if (sawGguf && !sawSafetensors) {
    return "gguf";
  }
  if (sawSafetensors && !sawGguf) {
    return "safetensors";
  }
  return "unknown";
}

export function normalizeModelDownloadFormat(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "unknown";
}

export function shouldShowGgufConversionOptions(detectedSourceFormat, desiredFormat) {
  return (
    normalizeModelDownloadFormat(detectedSourceFormat) === "safetensors" &&
    normalizeModelDownloadFormat(desiredFormat) === "gguf"
  );
}

export function normalizeModelLibraryJobKind(value) {
  return String(value || "").trim().toLowerCase() === "quantization"
    ? "quantization"
    : "download";
}

export function normalizeModelLibraryItemQuantization(value) {
  const normalized = String(value || "").trim();
  return MODEL_LIBRARY_QUANTIZATION_FILTERS.includes(normalized) ? normalized : "base";
}

export function formatModelLibraryDisplayName(item) {
  const quantization = normalizeModelLibraryItemQuantization(item?.quantization);
  const rawName = String(item?.name || "").trim();
  const withoutExtension = rawName.replace(/\.gguf$/i, "");
  if (quantization === "base") {
    return withoutExtension || rawName;
  }
  const escapedQuantization = quantization.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const normalizedSuffix = new RegExp(`-${escapedQuantization}$`, "i");
  return `${withoutExtension.replace(normalizedSuffix, "")} - ${quantization}`;
}
