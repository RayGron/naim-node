#pragma once

#include <optional>
#include <string>

#include "infra/controller_action.h"
#include "app/controller_command_line.h"

namespace naim::controller {

enum class WebUiComposeMode;
struct ControllerEndpointTarget;

}  // namespace naim::controller

namespace naim {

enum class NodeAvailability : int;

}

namespace naim::controller {

class IBundleCliService {
 public:
  virtual ~IBundleCliService() = default;

  virtual void ShowDemoPlan() const = 0;
  virtual int RenderDemoCompose(const std::optional<std::string>& node_name) const = 0;
  virtual int InitDb(const std::string& db_path) const = 0;
  virtual int SeedDemo(const std::string& db_path) const = 0;
  virtual int ValidateBundle(const std::string& bundle_dir) const = 0;
  virtual int PreviewBundle(
      const std::string& bundle_dir,
      const std::optional<std::string>& node_name) const = 0;
  virtual int PlanBundle(
      const std::string& db_path,
      const std::string& bundle_dir) const = 0;
  virtual int ApplyBundle(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root) const = 0;
  virtual int ApplyStateFile(
      const std::string& db_path,
      const std::string& state_path,
      const std::string& artifacts_root) const = 0;
  virtual int PlanHostOps(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root,
      const std::optional<std::string>& node_name) const = 0;
  virtual int ImportBundle(
      const std::string& db_path,
      const std::string& bundle_dir) const = 0;
  virtual int RenderCompose(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const = 0;
  virtual int RenderInferRuntime(const std::string& db_path) const = 0;
  virtual ControllerActionResult ExecuteValidateBundleAction(
      const std::string& bundle_dir) const = 0;
  virtual ControllerActionResult ExecutePreviewBundleAction(
      const std::string& bundle_dir,
      const std::optional<std::string>& node_name) const = 0;
  virtual ControllerActionResult ExecuteImportBundleAction(
      const std::string& db_path,
      const std::string& bundle_dir) const = 0;
  virtual ControllerActionResult ExecuteApplyBundleAction(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root) const = 0;
};

class IReadModelCliService {
 public:
  virtual ~IReadModelCliService() = default;

  virtual int ShowHostAssignments(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const = 0;
  virtual int ShowHostObservations(
      const std::string& db_path,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const = 0;
  virtual int ShowNodeAvailability(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const = 0;
  virtual int ShowHostHealth(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const = 0;
  virtual int ShowEvents(
      const std::string& db_path,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const = 0;
  virtual int ShowState(const std::string& db_path) const = 0;
  virtual int ShowDiskState(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const = 0;
};

class IHostRegistryService {
 public:
  virtual ~IHostRegistryService() = default;

  virtual int ShowHosts(const std::optional<std::string>& node_name) const = 0;
  virtual int RevokeHost(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const = 0;
  virtual int RotateHostKey(
      const std::string& node_name,
      const std::string& public_key_base64,
      const std::optional<std::string>& status_message) const = 0;
  virtual int ResetHostOnboarding(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const = 0;
  virtual int SetHostStorageRole(
      const std::string& node_name,
      bool enabled,
      const std::optional<std::string>& status_message) const = 0;
  virtual int StartManagedReleaseRollout(
      const std::string& manifest_path,
      const std::optional<std::string>& status_message) const = 0;
};

class IPlaneService {
 public:
  virtual ~IPlaneService() = default;

  virtual int ListPlanes() const = 0;
  virtual int ShowPlane(const std::string& plane_name) const = 0;
  virtual int StartPlane(const std::string& plane_name) const = 0;
  virtual int StopPlane(const std::string& plane_name) const = 0;
  virtual int DeletePlane(const std::string& plane_name) const = 0;
};

class ISchedulerService {
 public:
  virtual ~ISchedulerService() = default;

  virtual int ShowRolloutActions(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const = 0;
  virtual int ShowRebalancePlan(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const = 0;
  virtual int ShowEvents(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const = 0;
  virtual int ApplyRebalanceProposal(const std::string& worker_name) const = 0;
  virtual int ReconcileRebalanceProposals() const = 0;
  virtual int SchedulerTick() const = 0;
  virtual int SetRolloutActionStatus(
      int action_id,
      const std::string& requested_status,
      const std::optional<std::string>& message) const = 0;
  virtual int EnqueueRolloutEviction(int action_id) const = 0;
  virtual int ReconcileRolloutActions() const = 0;
  virtual int ApplyReadyRolloutAction(int action_id) const = 0;
};

class IAssignmentOrchestrationService {
 public:
  virtual ~IAssignmentOrchestrationService() = default;

  virtual int SetNodeAvailability(
      const std::string& db_path,
      const std::string& node_name,
      naim::NodeAvailability availability,
      const std::optional<std::string>& status_message) const = 0;
  virtual int RetryHostAssignment(
      const std::string& db_path,
      int assignment_id) const = 0;
  virtual ControllerActionResult ExecuteRetryHostAssignmentAction(
      const std::string& db_path,
      int assignment_id) const = 0;
};

class IWebUiService {
 public:
  virtual ~IWebUiService() = default;

  virtual int Ensure(
      const std::string& web_ui_root,
      int listen_port,
      const std::string& controller_upstream,
      WebUiComposeMode compose_mode) const = 0;
  virtual int ShowStatus(const std::string& web_ui_root) const = 0;
  virtual int Stop(
      const std::string& web_ui_root,
      WebUiComposeMode compose_mode) const = 0;
};

class IRemoteControllerCliService {
 public:
  virtual ~IRemoteControllerCliService() = default;

  virtual int ExecuteCommand(
      const ControllerEndpointTarget& target,
      const std::string& command,
      const ControllerCommandLine& cli) const = 0;
};

class IControllerServeService {
 public:
  virtual ~IControllerServeService() = default;

  virtual int Serve(
      const std::string& listen_host,
      int listen_port,
      const std::optional<std::string>& requested_ui_root,
      const std::optional<std::string>& skills_factory_upstream) = 0;
  virtual int ServeSkillsFactory(
      const std::string& listen_host,
      int listen_port) = 0;
};

}  // namespace naim::controller
