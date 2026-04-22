#include "app/controller_route_summary_builder.h"

namespace naim::controller::serve_support {

std::string ControllerRouteSummaryBuilder::BuildControllerRoutesSummary(
    bool webgateway_routes_enabled) const {
  RouteSummary routes;

  AppendRoutes(routes, {
      "/health",
      "/api/v1/health",
  });
  AppendRoutes(routes, {
      "/api/v1/bundles/validate",
      "/api/v1/bundles/preview",
      "/api/v1/bundles/import",
      "/api/v1/bundles/apply",
  });
  AppendRoutes(routes, {
      "/api/v1/model-library",
      "/api/v1/model-library/download",
      "/api/v1/model-library/jobs/stop",
      "/api/v1/model-library/jobs/resume",
      "/api/v1/model-library/jobs/hide",
      "/api/v1/model-library/jobs[DELETE]",
      "/api/v1/model-library/skills-factory-worker",
  });
  AppendRoutes(routes, {
      "/api/v1/knowledge-vault/status",
      "/api/v1/knowledge-vault/apply",
      "/api/v1/knowledge-vault/stop",
      "/api/v1/knowledge-vault/blocks",
      "/api/v1/knowledge-vault/heads",
      "/api/v1/knowledge-vault/relations",
      "/api/v1/knowledge-vault/search",
      "/api/v1/knowledge-vault/context",
      "/api/v1/knowledge-vault/query-route",
      "/api/v1/knowledge-vault/source-ingest",
      "/api/v1/knowledge-vault/capsules",
      "/api/v1/knowledge-vault/overlays",
      "/api/v1/knowledge-vault/replica-merges",
      "/api/v1/knowledge-vault/reviews",
      "/api/v1/knowledge-vault/repair",
      "/api/v1/knowledge-vault/markdown-export",
      "/api/v1/knowledge-vault/markdown-import",
      "/api/v1/knowledge-vault/graph-neighborhood",
      "/api/v1/knowledge-vault/catalog",
  });
  AppendRoutes(routes, {
      "/api/v1/planes",
      "/api/v1/planes/<plane>",
      "/api/v1/planes/<plane>/dashboard",
      "/api/v1/planes/<plane>/start",
      "/api/v1/planes/<plane>/stop",
      "/api/v1/planes/<plane>[DELETE]",
      "/api/v1/planes/<plane>/interaction/status",
      "/api/v1/planes/<plane>/interaction/models",
      "/api/v1/planes/<plane>/interaction/sessions",
      "/api/v1/planes/<plane>/interaction/sessions/<session_id>",
      "/api/v1/planes/<plane>/interaction/chat/completions",
      "/api/v1/planes/<plane>/interaction/chat/completions/stream",
  });
  if (webgateway_routes_enabled) {
    AppendRoutes(routes, {
        "/api/v1/planes/<plane>/webgateway/status",
        "/api/v1/planes/<plane>/webgateway/resolve",
        "/api/v1/planes/<plane>/webgateway/review-response",
        "/api/v1/planes/<plane>/webgateway/sessions",
    });
  }
  AppendRoutes(routes, {
      "/api/v1/planes/<plane>/skills",
      "/api/v1/skills-factory",
      "/api/v1/skills-factory/<skill>",
  });
  AppendRoutes(routes, {
      "/api/v1/state",
      "/api/v1/dashboard",
      "/api/v1/host-assignments",
      "/api/v1/host-observations",
      "/api/v1/host-health",
      "/api/v1/disk-state",
      "/api/v1/rollout-actions",
      "/api/v1/rebalance-plan",
      "/api/v1/events",
      "/api/v1/events/stream",
  });
  AppendRoutes(routes, {
      "/api/v1/scheduler-tick",
      "/api/v1/reconcile-rebalance-proposals",
      "/api/v1/reconcile-rollout-actions",
      "/api/v1/apply-rebalance-proposal",
      "/api/v1/set-rollout-action-status",
      "/api/v1/enqueue-rollout-eviction",
      "/api/v1/apply-ready-rollout-action",
      "/api/v1/node-availability",
      "/api/v1/retry-host-assignment",
  });
  AppendRoutes(routes, {
      "/api/v1/hostd/hosts",
      "/api/v1/hostd/peer-links",
      "/api/v1/hostd/file-transfer-tickets",
      "/api/v1/hostd/file-transfer-tickets/validate",
      "/api/v1/hostd/file-upload-tickets",
      "/api/v1/hostd/file-upload-tickets/validate",
      "/api/v1/hostd/hosts/<node>/revoke",
      "/api/v1/hostd/hosts/<node>/rotate-key",
      "/api/v1/hostd/hosts/<node>/reset-onboarding",
      "/api/v1/hostd/hosts/<node>/storage-role",
      "/api/v1/hostd/model-artifacts/chunks/request",
      "/api/v1/hostd/model-artifacts/chunks/poll",
      "/api/v1/hostd/model-artifacts/manifest/request",
      "/api/v1/hostd/model-artifacts/manifest/poll",
  });

  return JoinRoutes(routes);
}

std::string ControllerRouteSummaryBuilder::BuildSkillsFactoryRoutesSummary() const {
  RouteSummary routes;
  AppendRoutes(routes, {
      "/health",
      "/api/v1/health",
      "/api/v1/skills-factory",
      "/api/v1/skills-factory/<skill>",
  });
  return JoinRoutes(routes);
}

void ControllerRouteSummaryBuilder::AppendRoutes(
    RouteSummary& summary,
    std::initializer_list<std::string_view> routes) {
  summary.insert(summary.end(), routes.begin(), routes.end());
}

std::string ControllerRouteSummaryBuilder::JoinRoutes(const RouteSummary& routes) {
  std::string joined;
  for (const std::string_view route : routes) {
    if (!joined.empty()) {
      joined.push_back(',');
    }
    joined.append(route.data(), route.size());
  }
  return joined;
}

}  // namespace naim::controller::serve_support
