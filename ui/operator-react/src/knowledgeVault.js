export const KNOWLEDGE_GRAPH_LIMIT = 200;

export function knowledgeIdFromItem(item) {
  return item?.knowledge_id || item?.id || item?.block_id || "";
}

export function knowledgeTitleFromItem(item) {
  return item?.title || item?.name || item?.knowledge_id || item?.block_id || item?.id || "Untitled";
}

export function normalizeKnowledgeResults(items) {
  const byKnowledgeId = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    const knowledgeId = knowledgeIdFromItem(item);
    if (!knowledgeId || byKnowledgeId.has(knowledgeId)) {
      continue;
    }
    byKnowledgeId.set(knowledgeId, item);
  }
  return [...byKnowledgeId.values()];
}

export function buildKnowledgeGraphRequest(items, extras = [], limit = KNOWLEDGE_GRAPH_LIMIT) {
  const knowledgeIds = [];
  const seen = new Set();
  for (const candidate of [...(Array.isArray(items) ? items : []), ...extras]) {
    const knowledgeId = typeof candidate === "string" ? candidate : knowledgeIdFromItem(candidate);
    if (!knowledgeId || seen.has(knowledgeId)) {
      continue;
    }
    seen.add(knowledgeId);
    knowledgeIds.push(knowledgeId);
    if (knowledgeIds.length >= limit) {
      break;
    }
  }
  return {
    knowledge_ids: knowledgeIds,
    depth: 1,
  };
}

export function buildPlaneKnowledgeGraphRequest(results, selectedKnowledgeIds = [], limit = KNOWLEDGE_GRAPH_LIMIT) {
  const selected = Array.isArray(selectedKnowledgeIds)
    ? selectedKnowledgeIds.filter((item) => typeof item === "string" && item.trim())
    : [];
  if (selected.length > 0) {
    return buildKnowledgeGraphRequest([], selected, limit);
  }
  return buildKnowledgeGraphRequest(results, [], limit);
}

export function knowledgeGraphSignature(graph) {
  return JSON.stringify({
    nodes: Array.isArray(graph?.nodes) ? graph.nodes : [],
    edges: Array.isArray(graph?.edges) ? graph.edges : [],
    warnings: Array.isArray(graph?.warnings) ? graph.warnings : [],
  });
}

export function areKnowledgeGraphsEqual(left, right) {
  return knowledgeGraphSignature(left) === knowledgeGraphSignature(right);
}

export function summarizeKnowledgeGraph(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  return {
    nodeCount: nodes.length,
    edgeCount: edges.length,
  };
}
