#include "app/hostd_cli_actions.h"

namespace comet::hostd {

HostdCliActions::HostdCliActions(
    const HostdAssignmentService& assignment_service,
    const HostdObservationService& observation_service)
    : assignment_service_(assignment_service),
      observation_service_(observation_service) {}

void HostdCliActions::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  assignment_service_.ShowDemoOps(node_name, storage_root, runtime_root);
}

void HostdCliActions::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) {
  assignment_service_.ShowStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root);
}

void HostdCliActions::ShowLocalState(
    const std::string& node_name,
    const std::string& state_root) {
  observation_service_.ShowLocalState(node_name, state_root);
}

void HostdCliActions::ShowRuntimeStatus(
    const std::string& node_name,
    const std::string& state_root) {
  observation_service_.ShowRuntimeStatus(node_name, state_root);
}

void HostdCliActions::ReportLocalObservedState(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::string& node_name,
    const std::string& state_root) {
  observation_service_.ReportLocalObservedState(
      db_path,
      controller_url,
      host_private_key_path,
      controller_fingerprint,
      node_name,
      state_root);
}

void HostdCliActions::ApplyStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  assignment_service_.ApplyStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root,
      compose_mode);
}

void HostdCliActions::ApplyNextAssignment(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  assignment_service_.ApplyNextAssignment(
      db_path,
      controller_url,
      host_private_key_path,
      controller_fingerprint,
      node_name,
      storage_root,
      runtime_root,
      state_root,
      compose_mode);
}

}  // namespace comet::hostd
