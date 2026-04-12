#include "app/remote_controller_cli_service.h"

#include <iostream>
#include <stdexcept>

#include "http/controller_http_server_support.h"
#include "infra/controller_remote_client.h"

namespace naim::controller {

int RemoteControllerCliService::EmitRemoteJsonPayload(
    const nlohmann::json& payload) const {
  const int http_status = payload.value("_http_status", 200);
  nlohmann::json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  std::cout << sanitized.dump(2) << "\n";
  return 0;
}

int RemoteControllerCliService::EmitRemoteControllerActionPayload(
    const nlohmann::json& payload) const {
  const int http_status = payload.value("_http_status", 200);
  nlohmann::json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  const std::string output = sanitized.value("output", "");
  if (!output.empty()) {
    std::cout << output;
    if (output.back() != '\n') {
      std::cout << "\n";
    }
  } else {
    std::cout << sanitized.dump(2) << "\n";
  }
  return sanitized.value("exit_code", 0);
}

int RemoteControllerCliService::ExecuteCommand(
    const ControllerEndpointTarget& target,
    const std::string& command,
    const ControllerCommandLine& cli) const {
  const auto plane_name = cli.plane();
  const auto node_name = cli.node();
  const auto stale_after = cli.stale_after();
  const auto bundle_dir = cli.bundle();
  const auto artifacts_root = cli.artifacts_root();
  const auto action_id = cli.id();
  const auto worker_name = cli.worker();
  const auto limit = cli.limit();
  const auto category = cli.category();
  const auto message = cli.message();
  const auto status = cli.status();
  const auto availability = cli.availability();

  if (command == "list-planes") {
    std::cout << SendControllerJsonRequest(target, "GET", "/api/v1/planes").dump(2) << "\n";
    return 0;
  }
  if (command == "show-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote show-plane");
    }
    std::cout
        << SendControllerJsonRequest(
               target,
               "GET",
               "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name))
               .dump(2)
        << "\n";
    return 0;
  }
  if (command == "start-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote start-plane");
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name) + "/start"));
  }
  if (command == "stop-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote stop-plane");
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name) + "/stop"));
  }
  if (command == "delete-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote delete-plane");
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "DELETE",
        "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name)));
  }
  if (command == "show-state") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(target, "GET", "/api/v1/state"));
  }
  if (command == "show-hostd-hosts") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/hostd/hosts",
        {{"node", node_name.value_or("")}}));
  }
  if (command == "revoke-hostd") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) +
            "/revoke",
        {{"message", message.value_or("")}}));
  }
  if (command == "rotate-hostd-key") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    const auto public_key = cli.public_key_base64();
    if (!public_key.has_value()) {
      std::cerr << "error: --public-key is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) +
        "/rotate-key",
        {{"public_key_base64", *public_key}, {"message", message.value_or("")}}));
  }
  if (command == "reset-hostd-onboarding") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) +
        "/reset-onboarding",
        {{"message", message.value_or("")}}));
  }
  if (command == "set-hostd-storage-role") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    if (!status.has_value()) {
      std::cerr << "error: --status is required\n";
      return 1;
    }
    if (*status != "enabled" && *status != "disabled") {
      std::cerr << "error: --status must be enabled or disabled\n";
      return 1;
    }
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) +
            "/storage-role",
        {{"enabled", *status == "enabled" ? "true" : "false"},
         {"message", message.value_or("")}}));
  }
  if (command == "show-host-assignments") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/host-assignments",
        {{"node", node_name.value_or("")}}));
  }
  if (command == "show-host-observations") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/host-observations",
        {{"plane", plane_name.value_or("")},
         {"node", node_name.value_or("")},
         {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-host-health") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/host-health",
        {{"node", node_name.value_or("")},
         {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-disk-state") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/disk-state",
        {{"node", node_name.value_or("")}, {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rollout-actions") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/rollout-actions",
        {{"node", node_name.value_or("")}, {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rebalance-plan") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/rebalance-plan",
        {{"node", node_name.value_or("")},
         {"plane", plane_name.value_or("")},
         {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-events") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/events",
        {{"plane", plane_name.value_or("")},
         {"node", node_name.value_or("")},
         {"worker", worker_name.value_or("")},
         {"category", category.value_or("")},
         {"limit", limit.has_value() ? std::to_string(*limit) : ""}}));
  }
  if (command == "show-node-availability") {
    return EmitRemoteJsonPayload(SendControllerJsonRequest(
        target,
        "GET",
        "/api/v1/node-availability",
        {{"node", node_name.value_or("")}}));
  }
  if (command == "validate-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target, "POST", "/api/v1/bundles/validate", {{"bundle", *bundle_dir}}));
  }
  if (command == "preview-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/bundles/preview",
        {{"bundle", *bundle_dir}, {"node", node_name.value_or("")}}));
  }
  if (command == "import-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target, "POST", "/api/v1/bundles/import", {{"bundle", *bundle_dir}}));
  }
  if (command == "apply-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/bundles/apply",
        {{"bundle", *bundle_dir}, {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "scheduler-tick") {
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/scheduler-tick",
        {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rebalance-proposals") {
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/reconcile-rebalance-proposals",
        {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rollout-actions") {
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/reconcile-rollout-actions",
        {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "apply-rebalance-proposal") {
    if (!worker_name.has_value()) {
      std::cerr << "error: --worker is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/apply-rebalance-proposal",
        {{"worker", *worker_name}, {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-rollout-action-status") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    if (!status.has_value()) {
      std::cerr << "error: --status is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/set-rollout-action-status",
        {{"id", std::to_string(*action_id)},
         {"status", *status},
         {"message", message.value_or("")}}));
  }
  if (command == "enqueue-rollout-eviction") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/enqueue-rollout-eviction",
        {{"id", std::to_string(*action_id)}}));
  }
  if (command == "apply-ready-rollout-action") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/apply-ready-rollout-action",
        {{"id", std::to_string(*action_id)},
         {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-node-availability") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    if (!availability.has_value()) {
      std::cerr << "error: --availability is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/node-availability",
        {{"node", *node_name},
         {"availability", *availability},
         {"message", message.value_or("")}}));
  }
  if (command == "retry-host-assignment") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(SendControllerJsonRequest(
        target,
        "POST",
        "/api/v1/retry-host-assignment",
        {{"id", std::to_string(*action_id)}}));
  }

  std::cerr << "error: command '" << command
            << "' is not available through --controller yet\n";
  return 1;
}

}  // namespace naim::controller
