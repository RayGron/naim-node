#pragma once

#include <functional>
#include <optional>
#include <string>

#include "controller_command_line.h"
#include "host_registry_service.h"
#include "plane_service.h"
#include "scheduler_service.h"
#include "web_ui_service.h"

namespace comet::controller {

class ControllerCli {
 public:
  using NullaryAction = std::function<int()>;
  using PlaneAction = std::function<int(const std::string&)>;
  using NodeQueryAction = std::function<int(const std::optional<std::string>&)>;
  using PlaneNodeQueryAction = std::function<int(
      const std::optional<std::string>&,
      const std::optional<std::string>&)>;
  using PlaneNodeStaleQueryAction = std::function<int(
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      int)>;
  using NodeStaleQueryAction =
      std::function<int(const std::optional<std::string>&, int)>;
  using IdAction = std::function<int(int)>;
  using EventsQueryAction = std::function<int(
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      int)>;
  using BundleQueryAction = std::function<int(const std::string&)>;
  using BundleNodeQueryAction = std::function<int(
      const std::string&,
      const std::optional<std::string>&)>;
  using BundleArtifactsAction = std::function<int(
      const std::string&,
      const std::string&)>;
  using BundleArtifactsNodeAction = std::function<int(
      const std::string&,
      const std::string&,
      const std::optional<std::string>&)>;
  using StateFileArtifactsAction = std::function<int(
      const std::string&,
      const std::string&)>;
  using WorkerAction = std::function<int(const std::string&)>;
  using RolloutStatusAction = std::function<int(
      int,
      const std::string&,
      const std::optional<std::string>&)>;
  using NodeAvailabilityAction = std::function<int(
      const std::string&,
      const std::string&,
      const std::optional<std::string>&)>;
  using BundleAction = std::function<int(const std::string&)>;
  using ServeAction = std::function<int(
      const std::string&,
      int,
      const std::optional<std::string>&,
      const std::string&)>;
  using RevokeHostAction =
      std::function<int(const std::string&, const std::optional<std::string>&)>;
  using RotateHostKeyAction = std::function<int(
      const std::string&,
      const std::string&,
      const std::optional<std::string>&)>;

  ControllerCli(
      const ControllerCommandLine& command_line,
      HostRegistryService& host_registry_service,
      PlaneService& plane_service,
      SchedulerService& scheduler_service,
      WebUiService& web_ui_service,
      NullaryAction init_db_action,
      NullaryAction seed_demo_action,
      NullaryAction show_state_action,
      NodeQueryAction show_host_assignments_action,
      PlaneNodeStaleQueryAction show_host_observations_action,
      NodeStaleQueryAction show_host_health_action,
      PlaneNodeQueryAction show_disk_state_action,
      BundleQueryAction validate_bundle_action,
      BundleNodeQueryAction preview_bundle_action,
      BundleQueryAction plan_bundle_action,
      BundleArtifactsAction apply_bundle_action,
      StateFileArtifactsAction apply_state_file_action,
      BundleArtifactsNodeAction plan_host_ops_action,
      NodeQueryAction show_node_availability_action,
      NodeAvailabilityAction set_node_availability_action,
      IdAction retry_host_assignment_action,
      BundleAction import_bundle_action,
      NodeQueryAction render_compose_action,
      NullaryAction render_infer_runtime_action,
      ServeAction serve_action,
      RevokeHostAction revoke_host_action,
      RotateHostKeyAction rotate_host_key_action);

  std::optional<int> TryRun() const;

 private:
  int MissingRequired(const char* option_name) const;

  const ControllerCommandLine& command_line_;
  HostRegistryService& host_registry_service_;
  PlaneService& plane_service_;
  SchedulerService& scheduler_service_;
  WebUiService& web_ui_service_;
  NullaryAction init_db_action_;
  NullaryAction seed_demo_action_;
  NullaryAction show_state_action_;
  NodeQueryAction show_host_assignments_action_;
  PlaneNodeStaleQueryAction show_host_observations_action_;
  NodeStaleQueryAction show_host_health_action_;
  PlaneNodeQueryAction show_disk_state_action_;
  BundleQueryAction validate_bundle_action_;
  BundleNodeQueryAction preview_bundle_action_;
  BundleQueryAction plan_bundle_action_;
  BundleArtifactsAction apply_bundle_action_;
  StateFileArtifactsAction apply_state_file_action_;
  BundleArtifactsNodeAction plan_host_ops_action_;
  NodeQueryAction show_node_availability_action_;
  NodeAvailabilityAction set_node_availability_action_;
  IdAction retry_host_assignment_action_;
  BundleAction import_bundle_action_;
  NodeQueryAction render_compose_action_;
  NullaryAction render_infer_runtime_action_;
  ServeAction serve_action_;
  RevokeHostAction revoke_host_action_;
  RotateHostKeyAction rotate_host_key_action_;
};

}  // namespace comet::controller
