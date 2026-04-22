import { describe, expect, it } from "vitest";

import {
  areKnowledgeGraphsEqual,
  buildKnowledgeGraphRequest,
  KNOWLEDGE_GRAPH_LIMIT,
  knowledgeTitleFromItem,
  normalizeKnowledgeResults,
  summarizeKnowledgeGraph,
} from "./knowledgeVault.js";

describe("knowledgeVault utils", () => {
  it("normalizes duplicate search results by canonical knowledge id", () => {
    const results = normalizeKnowledgeResults([
      { knowledge_id: "alpha", block_id: "alpha.v1", title: "Alpha" },
      { knowledge_id: "alpha", block_id: "alpha.v2", title: "Alpha duplicate" },
      { id: "beta", title: "Beta" },
      { block_id: "gamma.v1", title: "Gamma" },
      { title: "missing id" },
    ]);

    expect(results.map((item) => item.title)).toEqual(["Alpha", "Beta", "Gamma"]);
  });

  it("builds a bounded graph-neighborhood request", () => {
    expect(
      buildKnowledgeGraphRequest(
        [{ knowledge_id: "alpha" }, { knowledge_id: "beta" }],
        ["beta", "gamma"],
        3,
      ),
    ).toEqual({
      knowledge_ids: ["alpha", "beta", "gamma"],
      depth: 1,
    });
  });

  it("uses the 200 item Knowledge Vault graph limit by default", () => {
    const items = Array.from({ length: KNOWLEDGE_GRAPH_LIMIT + 5 }, (_, index) => ({
      knowledge_id: `item-${index}`,
    }));

    expect(buildKnowledgeGraphRequest(items).knowledge_ids).toHaveLength(KNOWLEDGE_GRAPH_LIMIT);
  });

  it("summarizes graph payloads and falls back to stable titles", () => {
    expect(
      summarizeKnowledgeGraph({
        nodes: [{ knowledge_id: "alpha" }, { knowledge_id: "beta" }],
        edges: [{ relation_id: "rel-1" }],
      }),
    ).toEqual({ nodeCount: 2, edgeCount: 1 });
    expect(knowledgeTitleFromItem({ block_id: "block-1" })).toBe("block-1");
  });

  it("compares graph payloads before replacing state", () => {
    expect(
      areKnowledgeGraphsEqual(
        { nodes: [{ knowledge_id: "alpha" }], edges: [], warnings: [] },
        { nodes: [{ knowledge_id: "alpha" }], edges: [], warnings: [] },
      ),
    ).toBe(true);
    expect(
      areKnowledgeGraphsEqual(
        { nodes: [{ knowledge_id: "alpha" }], edges: [], warnings: [] },
        { nodes: [{ knowledge_id: "beta" }], edges: [], warnings: [] },
      ),
    ).toBe(false);
  });
});
