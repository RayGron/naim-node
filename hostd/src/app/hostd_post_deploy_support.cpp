#include "app/hostd_post_deploy_support.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include "app/hostd_repo_root_support.h"

namespace naim::hostd {

using nlohmann::json;

HostdPostDeploySupport::HostdPostDeploySupport(
    const HostdCommandSupport& command_support)
    : command_support_(command_support),
      local_state_path_support_() {}

bool HostdPostDeploySupport::NodeHasAppInstance(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) const {
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name && instance.role == naim::InstanceRole::App) {
      return true;
    }
  }
  return false;
}

bool HostdPostDeploySupport::ShouldRunForNode(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) const {
  if (!desired_node_state.post_deploy_script.has_value() ||
      desired_node_state.post_deploy_script->empty()) {
    return false;
  }
  if (!desired_node_state.inference.primary_infer_node.empty()) {
    return desired_node_state.inference.primary_infer_node == node_name;
  }
  if (NodeHasAppInstance(desired_node_state, node_name)) {
    return true;
  }
  return !desired_node_state.nodes.empty() &&
         desired_node_state.nodes.front().name == node_name;
}

std::string HostdPostDeploySupport::TailTextFile(
    const std::string& path,
    std::size_t max_bytes) const {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    return {};
  }
  input.seekg(0, std::ios::end);
  const auto size = static_cast<std::size_t>(input.tellg());
  const auto read_size = std::min(size, max_bytes);
  input.seekg(static_cast<std::streamoff>(size - read_size), std::ios::beg);
  std::string text(read_size, '\0');
  input.read(text.data(), static_cast<std::streamsize>(read_size));
  return text;
}

json HostdPostDeploySupport::BuildProgressPayload(
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name) const {
  return json{
      {"phase", phase},
      {"title", title},
      {"detail", detail},
      {"percent", std::max(0, std::min(100, percent))},
      {"plane_name", plane_name},
      {"node_name", node_name},
  };
}

void HostdPostDeploySupport::PublishProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const json& progress) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    return;
  }
  backend->UpdateHostAssignmentProgress(*assignment_id, progress);
}

void HostdPostDeploySupport::RunIfNeeded(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) const {
  if (!ShouldRunForNode(desired_node_state, node_name)) {
    return;
  }

  const auto script_path = repo_root_support_.ResolvePlaneOwnedPath(
      desired_node_state,
      *desired_node_state.post_deploy_script,
      artifacts_root);
  if (!script_path.has_value()) {
    throw std::runtime_error(
        "post_deploy_script was configured but could not be resolved: " +
        *desired_node_state.post_deploy_script);
  }

  const std::filesystem::path plane_root(
      local_state_path_support_.LocalPlaneRoot(
          state_root,
          node_name,
          desired_node_state.plane_name));
  std::filesystem::create_directories(plane_root);
  const std::string log_path = (plane_root / "post-deploy.log").string();

  PublishProgress(
      backend,
      assignment_id,
      BuildProgressPayload(
          "running-post-deploy",
          "Running post-deploy hook",
          "Executing plane post_deploy_script after runtime readiness.",
          99,
          desired_node_state.plane_name,
          node_name));

  std::ostringstream command;
  command << "cd " << command_support_.ShellQuote(script_path->parent_path().string()) << " && "
          << "NAIM_PLANE_NAME="
          << command_support_.ShellQuote(desired_node_state.plane_name) << " "
          << "NAIM_NODE_NAME=" << command_support_.ShellQuote(node_name) << " "
          << "NAIM_ARTIFACTS_ROOT=" << command_support_.ShellQuote(artifacts_root) << " "
          << "NAIM_STORAGE_ROOT=" << command_support_.ShellQuote(storage_root) << " "
          << "NAIM_STATE_ROOT=" << command_support_.ShellQuote(state_root) << " "
          << "NAIM_RUNTIME_ROOT="
          << command_support_.ShellQuote(
                 runtime_root.has_value() ? *runtime_root : std::string()) << " "
          << "NAIM_POST_DEPLOY_LOG=" << command_support_.ShellQuote(log_path) << " "
          << "NAIM_DESIRED_GENERATION="
          << command_support_.ShellQuote(
                 desired_generation.has_value() ? std::to_string(*desired_generation)
                                                : std::string())
          << " "
          << "NAIM_ASSIGNMENT_ID="
          << command_support_.ShellQuote(
                 assignment_id.has_value() ? std::to_string(*assignment_id) : std::string())
          << " "
          << command_support_.ShellQuote(script_path->string()) << " >"
          << command_support_.ShellQuote(log_path) << " 2>&1";
  const int rc = std::system(command.str().c_str());
  if (rc != 0) {
    const std::string tail = TailTextFile(log_path);
    throw std::runtime_error(
        "post_deploy_script failed with exit code " + std::to_string(rc) +
        (tail.empty() ? std::string()
                      : std::string(": ") + command_support_.Trim(tail)));
  }
}

}  // namespace naim::hostd
