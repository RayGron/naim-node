#include "skills/plane_skills_target_resolver.h"

#include <algorithm>

namespace naim::controller {

namespace {

std::string NormalizeControllerTargetHost(const PublishedPort& port) {
  if (port.host_ip.empty() || port.host_ip == "0.0.0.0") {
    return "127.0.0.1";
  }
  return port.host_ip;
}

}  // namespace

std::vector<std::pair<std::string, std::string>>
PlaneSkillsTargetResolver::DefaultJsonHeaders() {
  return {{"Content-Type", "application/json"}};
}

const InstanceSpec* PlaneSkillsTargetResolver::FindSkillsInstance(
    const DesiredState& desired_state) {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const InstanceSpec& instance) { return instance.role == InstanceRole::Skills; });
  if (it == desired_state.instances.end()) {
    return nullptr;
  }
  return &*it;
}

std::optional<ControllerEndpointTarget>
PlaneSkillsTargetResolver::ResolvePlaneLocalTarget(
    const DesiredState& desired_state) {
  const auto* skills = FindSkillsInstance(desired_state);
  if (skills == nullptr) {
    return std::nullopt;
  }
  const auto published = std::find_if(
      skills->published_ports.begin(),
      skills->published_ports.end(),
      [](const PublishedPort& port) { return port.host_port > 0; });
  if (published == skills->published_ports.end()) {
    return std::nullopt;
  }
  ControllerEndpointTarget target;
  target.host = NormalizeControllerTargetHost(*published);
  target.port = published->host_port;
  target.raw = "http://" + target.host + ":" + std::to_string(target.port);
  return target;
}

std::string PlaneSkillsTargetResolver::NormalizeSkillPathSuffix(
    const std::string& path_suffix) {
  if (path_suffix.empty() || path_suffix == "/") {
    return "/v1/skills";
  }
  if (path_suffix.front() == '/') {
    return "/v1/skills" + path_suffix;
  }
  return "/v1/skills/" + path_suffix;
}

}  // namespace naim::controller
