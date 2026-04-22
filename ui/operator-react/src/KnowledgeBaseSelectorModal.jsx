import React, { useEffect, useState } from "react";
import { KnowledgeCubeGraph } from "./KnowledgeCubeGraph.jsx";
import {
  buildKnowledgeGraphRequest,
  KNOWLEDGE_GRAPH_LIMIT,
  knowledgeIdFromItem,
  normalizeKnowledgeResults,
} from "./knowledgeVault.js";

function InlineHint({ message, severity = "warning" }) {
  if (!message) {
    return null;
  }
  return <div className={`field-hint is-${severity}`}>{message}</div>;
}

export function KnowledgeBaseSelectorModal({
  open,
  selectedKnowledgeIds,
  onClose,
  onToggleKnowledge,
  onSelectAll,
  onUnselectAll,
}) {
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [results, setResults] = useState([]);
  const [graph, setGraph] = useState({ nodes: [], edges: [] });
  const selectedSet = new Set(Array.isArray(selectedKnowledgeIds) ? selectedKnowledgeIds : []);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const controller = new AbortController();
    async function loadKnowledge() {
      setBusy(true);
      setError("");
      try {
        const response = await fetch("/api/v1/knowledge-vault/search", {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            query,
            list_all: !query.trim(),
            limit: KNOWLEDGE_GRAPH_LIMIT,
          }),
          signal: controller.signal,
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload?.message || payload?.status || response.statusText);
        }
        setResults(normalizeKnowledgeResults(payload?.results));
      } catch (loadError) {
        if (loadError.name !== "AbortError") {
          setError(loadError.message || String(loadError));
          setResults([]);
        }
      } finally {
        setBusy(false);
      }
    }
    const timer = window.setTimeout(loadKnowledge, 220);
    return () => {
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [open, query]);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const graphRequest = buildKnowledgeGraphRequest(results, [...selectedSet], KNOWLEDGE_GRAPH_LIMIT);
    if (graphRequest.knowledge_ids.length === 0) {
      setGraph({ nodes: [], edges: [] });
      return undefined;
    }
    const controller = new AbortController();
    async function loadGraph() {
      try {
        const response = await fetch("/api/v1/knowledge-vault/graph-neighborhood", {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify(graphRequest),
          signal: controller.signal,
        });
        const payload = await response.json().catch(() => ({}));
        if (response.ok) {
          setGraph({
            nodes: Array.isArray(payload?.nodes) ? payload.nodes : [],
            edges: Array.isArray(payload?.edges) ? payload.edges : [],
          });
        }
      } catch (graphError) {
        if (graphError.name !== "AbortError") {
          setGraph({ nodes: [], edges: [] });
        }
      }
    }
    loadGraph();
    return () => controller.abort();
  }, [open, results, selectedKnowledgeIds]);

  if (!open) {
    return null;
  }

  const currentResultIds = results.map(knowledgeIdFromItem).filter(Boolean);
  return (
    <div className="modal-backdrop knowledge-modal-backdrop">
      <div className="modal-card knowledge-modal-card">
        <div className="modal-header">
          <div>
            <h2>Knowledge Base</h2>
            <p>Select canonical knowledge records for this plane.</p>
          </div>
          <button className="ghost-button compact-button" type="button" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="knowledge-selector-toolbar">
          <label className="field-label">
            <span className="field-label-title">Search</span>
            <input
              className="text-input"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search titles, relations, or content"
            />
          </label>
          <div className="plane-form-section-actions">
            <button
              className="ghost-button"
              type="button"
              disabled={currentResultIds.length === 0}
              onClick={() => onSelectAll(currentResultIds)}
            >
              Select All
            </button>
            <button
              className="ghost-button"
              type="button"
              disabled={currentResultIds.length === 0}
              onClick={() => onUnselectAll(currentResultIds)}
            >
              Unselect All
            </button>
          </div>
        </div>
        <InlineHint message={error} />
        <div className="knowledge-selector-layout">
          <div className="knowledge-table-shell">
            <table className="factory-skill-table knowledge-table">
              <thead>
                <tr>
                  <th>Select</th>
                  <th>Title</th>
                  <th>Knowledge id</th>
                  <th>Relations</th>
                  <th>Summary</th>
                </tr>
              </thead>
              <tbody>
                {results.map((item) => {
                  const knowledgeId = knowledgeIdFromItem(item);
                  const selected = selectedSet.has(knowledgeId);
                  return (
                    <tr
                      key={`${knowledgeId}:${item.block_id || ""}`}
                      className={selected ? "is-selected" : ""}
                      onClick={() => onToggleKnowledge(knowledgeId)}
                    >
                      <td>{selected ? "Selected" : "Select"}</td>
                      <td>{item.title || item.block_id || "Untitled"}</td>
                      <td>{knowledgeId}</td>
                      <td>{item.relation_count ?? 0}</td>
                      <td>{item.summary || ""}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {!busy && results.length === 0 ? (
              <InlineHint message="No knowledge records found." />
            ) : null}
            {busy ? <InlineHint message="Loading knowledge records..." /> : null}
          </div>
          <div className="knowledge-graph-shell">
            <KnowledgeCubeGraph
              graph={graph}
              selectedKnowledgeIds={selectedKnowledgeIds}
              onSelect={(node) => {
                const knowledgeId = knowledgeIdFromItem(node);
                if (knowledgeId) {
                  onToggleKnowledge(knowledgeId);
                }
              }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
