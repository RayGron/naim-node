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
