#include "browsing/plane_browsing_service.h"

#include <algorithm>

namespace comet::controller {

namespace {

std::vector<std::pair<std::string, std::string>> DefaultJsonHeaders() {
  return {{"Content-Type", "application/json"}};
}

std::string NormalizeControllerTargetHost(const PublishedPort& port) {
  if (port.host_ip.empty() || port.host_ip == "0.0.0.0") {
    return "127.0.0.1";
  }
  return port.host_ip;
}

const InstanceSpec* FindBrowsingInstance(const DesiredState& desired_state) {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const InstanceSpec& instance) { return instance.role == InstanceRole::Browsing; });
  if (it == desired_state.instances.end()) {
    return nullptr;
  }
  return &*it;
}

std::string NormalizeBrowsingPathSuffix(const std::string& path_suffix) {
  if (path_suffix.empty() || path_suffix == "/") {
    return "/v1/browsing";
  }
  if (path_suffix.front() == '/') {
    return "/v1/browsing" + path_suffix;
  }
  return "/v1/browsing/" + path_suffix;
}

bool ProbeBrowsingTargetOk(const ControllerEndpointTarget& target) {
  try {
    const auto response = SendControllerHttpRequest(target, "GET", "/health");
    return response.status_code >= 200 && response.status_code < 300;
  } catch (...) {
    return false;
  }
}

}  // namespace

bool PlaneBrowsingService::IsEnabled(const DesiredState& desired_state) const {
  return desired_state.browsing.has_value() && desired_state.browsing->enabled;
}

std::optional<ControllerEndpointTarget> PlaneBrowsingService::ResolveTarget(
    const DesiredState& desired_state) const {
  const auto* browsing = FindBrowsingInstance(desired_state);
  if (browsing == nullptr) {
    return std::nullopt;
  }
  const auto published = std::find_if(
      browsing->published_ports.begin(),
      browsing->published_ports.end(),
      [](const PublishedPort& port) { return port.host_port > 0; });
  if (published == browsing->published_ports.end()) {
    return std::nullopt;
  }
  ControllerEndpointTarget target;
  target.host = NormalizeControllerTargetHost(*published);
  target.port = published->host_port;
  target.raw = "http://" + target.host + ":" + std::to_string(target.port);
  return target;
}

nlohmann::json PlaneBrowsingService::BuildStatusPayload(
    const DesiredState& desired_state,
    const std::optional<std::string>& plane_state) const {
  const bool enabled = IsEnabled(desired_state);
  const auto* browsing_instance = FindBrowsingInstance(desired_state);
  const auto target = ResolveTarget(desired_state);
  const bool running_plane = plane_state.has_value() && *plane_state == "running";
  const bool ready = enabled && running_plane && target.has_value() && ProbeBrowsingTargetOk(*target);

  std::string reason = "ready";
  if (!enabled) {
    reason = "browsing_disabled";
  } else if (!running_plane) {
    reason = "plane_not_running";
  } else if (!target.has_value()) {
    reason = "target_missing";
  } else if (!ready) {
    reason = "target_unreachable";
  }

  nlohmann::json payload = {
      {"status", "ok"},
      {"browsing_enabled", enabled},
      {"browsing_ready", ready},
      {"reason", reason},
      {"plane_name", desired_state.plane_name},
      {"plane_state", plane_state.has_value() ? nlohmann::json(*plane_state) : nlohmann::json(nullptr)},
      {"browsing_container_name",
       browsing_instance != nullptr ? nlohmann::json(browsing_instance->name) : nlohmann::json(nullptr)},
      {"browsing_target", target.has_value() ? nlohmann::json(target->raw) : nlohmann::json(nullptr)},
      {"browser_session_enabled",
       desired_state.browsing.has_value() && desired_state.browsing->policy.has_value()
           ? nlohmann::json(desired_state.browsing->policy->browser_session_enabled)
           : nlohmann::json(false)},
      {"rendered_browser_enabled",
       desired_state.browsing.has_value() && desired_state.browsing->policy.has_value()
           ? nlohmann::json(desired_state.browsing->policy->rendered_browser_enabled)
           : nlohmann::json(true)},
      {"login_enabled",
       desired_state.browsing.has_value() && desired_state.browsing->policy.has_value()
           ? nlohmann::json(desired_state.browsing->policy->login_enabled)
           : nlohmann::json(false)},
  };

  if (ready) {
    try {
      const auto response = SendControllerHttpRequest(*target, "GET", "/v1/browsing/status");
      if (!response.body.empty()) {
        const auto runtime_status = nlohmann::json::parse(response.body, nullptr, false);
        if (runtime_status.is_object()) {
          for (const auto& [key, value] : runtime_status.items()) {
            payload[key] = value;
          }
        }
      }
    } catch (...) {
    }
  }
  return payload;
}

std::optional<HttpResponse> PlaneBrowsingService::ProxyPlaneBrowsingRequest(
    const DesiredState& desired_state,
    const std::string& method,
    const std::string& path_suffix,
    const std::string& body,
    std::string* error_code,
    std::string* error_message) const {
  if (!IsEnabled(desired_state)) {
    if (error_code != nullptr) {
      *error_code = "browsing_disabled";
    }
    if (error_message != nullptr) {
      *error_message = "isolated browsing is not enabled for this plane";
    }
    return std::nullopt;
  }
  const auto target = ResolveTarget(desired_state);
  if (!target.has_value()) {
    if (error_code != nullptr) {
      *error_code = "browsing_target_missing";
    }
    if (error_message != nullptr) {
      *error_message = "browsing service target is not available";
    }
    return std::nullopt;
  }

  try {
    return SendControllerHttpRequest(
        *target,
        method,
        NormalizeBrowsingPathSuffix(path_suffix),
        body,
        DefaultJsonHeaders());
  } catch (const std::exception& error) {
    if (error_code != nullptr) {
      *error_code = "browsing_upstream_failed";
    }
    if (error_message != nullptr) {
      *error_message = error.what();
    }
    return std::nullopt;
  }
}

}  // namespace comet::controller
