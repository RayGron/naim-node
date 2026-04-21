#pragma once

#include <optional>
#include <string>
#include <vector>

#include "infra/controller_action.h"
#include "app/controller_service_interfaces.h"
#include "infra/controller_event_service.h"
#include "infra/controller_print_service.h"

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class AssignmentOrchestrationService : public IAssignmentOrchestrationService {
 public:
  AssignmentOrchestrationService(
      const ControllerEventService& controller_event_service,
      const ControllerPrintService& controller_print_service,
      std::string default_artifacts_root);

  std::optional<naim::HostAssignment> BuildResyncAssignmentForNode(
      const naim::DesiredState& desired_state,
      int desired_generation,
      const std::string& node_name,
      const std::vector<naim::HostAssignment>& existing_assignments,
      const std::optional<naim::HostObservation>& observation) const;

  std::optional<naim::HostAssignment> BuildDrainAssignmentForNode(
      const naim::DesiredState& desired_state,
      int desired_generation,
      const std::string& node_name,
      const std::vector<naim::HostAssignment>& existing_assignments) const;

  int SetNodeAvailability(
      const std::string& db_path,
      const std::string& node_name,
      naim::NodeAvailability availability,
      const std::optional<std::string>& status_message) const override;

  int RetryHostAssignment(
      const std::string& db_path,
      int assignment_id) const override;

  ControllerActionResult ExecuteSetNodeAvailabilityAction(
      const std::string& db_path,
      const std::string& node_name,
      naim::NodeAvailability availability,
      const std::optional<std::string>& status_message) const;

  ControllerActionResult ExecuteRetryHostAssignmentAction(
      const std::string& db_path,
      int assignment_id) const override;

 private:
  const ControllerEventService& controller_event_service_;
  const ControllerPrintService& controller_print_service_;
  std::string default_artifacts_root_;
};

}  // namespace naim::controller
