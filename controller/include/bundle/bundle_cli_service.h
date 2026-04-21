#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "infra/controller_action.h"
#include "app/controller_service_interfaces.h"
#include "infra/controller_print_service.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/desired_state_policy_service.h"
#include "plane/plane_realization_service.h"

#include "naim/planning/execution_plan.h"
#include "naim/state/models.h"
#include "naim/planning/scheduling_policy.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class BundleCliService : public IBundleCliService {
 public:
  BundleCliService(
      const ControllerPrintService& controller_print_service,
      const DesiredStatePolicyService& desired_state_policy_service,
      const PlaneRealizationService& plane_realization_service,
      ControllerRuntimeSupportService runtime_support_service = {},
      std::string default_artifacts_root = "var/artifacts",
      int default_stale_after_seconds = 300);

  void ShowDemoPlan() const override;
  int RenderDemoCompose(const std::optional<std::string>& node_name) const override;
  int InitDb(const std::string& db_path) const override;
  int ValidateBundle(const std::string& bundle_dir) const override;
  int PreviewBundle(
      const std::string& bundle_dir,
      const std::optional<std::string>& node_name) const override;
  int PlanBundle(
      const std::string& db_path,
      const std::string& bundle_dir) const override;
  int PlanHostOps(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root,
      const std::optional<std::string>& node_name) const override;
  int SeedDemo(const std::string& db_path) const override;
  int ImportBundle(
      const std::string& db_path,
      const std::string& bundle_dir) const override;
  int ApplyBundle(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root) const override;
  int ApplyDesiredState(
      const std::string& db_path,
      const naim::DesiredState& desired_state,
      const std::string& artifacts_root,
      const std::string& source_label) const;
  int ApplyStateFile(
      const std::string& db_path,
      const std::string& state_path,
      const std::string& artifacts_root) const override;
  int RenderCompose(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const override;
  int RenderInferRuntime(const std::string& db_path) const override;

  ControllerActionResult ExecuteValidateBundleAction(
      const std::string& bundle_dir) const override;
  ControllerActionResult ExecutePreviewBundleAction(
      const std::string& bundle_dir,
      const std::optional<std::string>& node_name) const override;
  ControllerActionResult ExecuteImportBundleAction(
      const std::string& db_path,
      const std::string& bundle_dir) const override;
 ControllerActionResult ExecuteApplyBundleAction(
      const std::string& db_path,
      const std::string& bundle_dir,
      const std::string& artifacts_root) const override;

 private:
  void AppendEvent(
      naim::ControllerStore& store,
      const std::string& category,
      const std::string& event_type,
      const std::string& message,
      const nlohmann::json& payload,
      const std::string& plane_name,
      const std::string& node_name = "",
      const std::string& worker_name = "",
      const std::optional<int>& assignment_id = std::nullopt,
      const std::optional<int>& rollout_action_id = std::nullopt,
      const std::string& severity = "info") const;

  const ControllerPrintService& controller_print_service_;
  const DesiredStatePolicyService& desired_state_policy_service_;
  const PlaneRealizationService& plane_realization_service_;
  ControllerRuntimeSupportService runtime_support_service_;
  std::string default_artifacts_root_;
  int default_stale_after_seconds_ = 300;
};

}  // namespace naim::controller
