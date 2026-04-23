#include "app/controller_cli.h"

#include <iostream>

#include "web/web_ui_service.h"

namespace naim::controller {

ControllerCli::ControllerCli(
    const ControllerCommandLine& command_line,
    IHostRegistryService& host_registry_service,
    IPlaneService& plane_service,
    ISchedulerService& scheduler_service,
    IWebUiService& web_ui_service,
    IBundleCliService& bundle_cli_service,
    IReadModelCliService& read_model_cli_service,
    IAssignmentOrchestrationService& assignment_orchestration_service,
    IControllerServeService& serve_service)
    : command_line_(command_line),
      host_registry_service_(host_registry_service),
      plane_service_(plane_service),
      scheduler_service_(scheduler_service),
      web_ui_service_(web_ui_service),
      bundle_cli_service_(bundle_cli_service),
      read_model_cli_service_(read_model_cli_service),
      assignment_orchestration_service_(assignment_orchestration_service),
      serve_service_(serve_service) {}

std::optional<int> ControllerCli::TryRun() const {
  const std::string& command = command_line_.command();

  if (command == "init-db") {
    return bundle_cli_service_.InitDb(command_line_.db().value_or("var/controller.sqlite"));
  }

  if (command == "seed-demo") {
    return bundle_cli_service_.SeedDemo(command_line_.db().value_or("var/controller.sqlite"));
  }

  if (command == "show-hostd-hosts") {
    return host_registry_service_.ShowHosts(command_line_.node());
  }

  if (command == "show-state") {
    return read_model_cli_service_.ShowState(
        command_line_.db().value_or("var/controller.sqlite"));
  }

  if (command == "list-planes") {
    return plane_service_.ListPlanes();
  }

  if (command == "show-plane") {
    const auto plane_name = command_line_.plane();
    if (!plane_name.has_value()) {
      return MissingRequired("--plane");
    }
    return plane_service_.ShowPlane(*plane_name);
  }

  if (command == "start-plane") {
    const auto plane_name = command_line_.plane();
    if (!plane_name.has_value()) {
      return MissingRequired("--plane");
    }
    return plane_service_.StartPlane(*plane_name);
  }

  if (command == "stop-plane") {
    const auto plane_name = command_line_.plane();
    if (!plane_name.has_value()) {
      return MissingRequired("--plane");
    }
    return plane_service_.StopPlane(*plane_name);
  }

  if (command == "delete-plane") {
    const auto plane_name = command_line_.plane();
    if (!plane_name.has_value()) {
      return MissingRequired("--plane");
    }
    return plane_service_.DeletePlane(*plane_name);
  }

  if (command == "show-host-assignments") {
    return read_model_cli_service_.ShowHostAssignments(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.node());
  }

  if (command == "show-host-observations") {
    return read_model_cli_service_.ShowHostObservations(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.plane(),
        command_line_.node(),
        command_line_.stale_after().value_or(300));
  }

  if (command == "show-host-health") {
    return read_model_cli_service_.ShowHostHealth(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.node(),
        command_line_.stale_after().value_or(300));
  }

  if (command == "show-disk-state") {
    return read_model_cli_service_.ShowDiskState(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.node(),
        command_line_.plane());
  }

  if (command == "show-rollout-actions") {
    return scheduler_service_.ShowRolloutActions(
        command_line_.node(), command_line_.plane());
  }

  if (command == "show-rebalance-plan") {
    return scheduler_service_.ShowRebalancePlan(
        command_line_.node(), command_line_.plane());
  }

  if (command == "show-events") {
    return scheduler_service_.ShowEvents(
        command_line_.plane(),
        command_line_.node(),
        command_line_.worker(),
        command_line_.category(),
        command_line_.limit().value_or(100));
  }

  if (command == "validate-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.ValidateBundle(*bundle_dir);
  }

  if (command == "preview-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.PreviewBundle(*bundle_dir, command_line_.node());
  }

  if (command == "plan-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.PlanBundle(
        command_line_.db().value_or("var/controller.sqlite"),
        *bundle_dir);
  }

  if (command == "apply-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.ApplyBundle(
        command_line_.db().value_or("var/controller.sqlite"),
        *bundle_dir,
        command_line_.artifacts_root().value_or("var/artifacts"));
  }

  if (command == "apply-state-file") {
    const auto state_path = command_line_.state_file();
    if (!state_path.has_value()) {
      return MissingRequired("--state");
    }
    return bundle_cli_service_.ApplyStateFile(
        command_line_.db().value_or("var/controller.sqlite"),
        *state_path,
        command_line_.artifacts_root().value_or("var/artifacts"));
  }

  if (command == "plan-host-ops") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.PlanHostOps(
        command_line_.db().value_or("var/controller.sqlite"),
        *bundle_dir,
        command_line_.artifacts_root().value_or("var/artifacts"),
        command_line_.node());
  }

  if (command == "apply-rebalance-proposal") {
    const auto worker_name = command_line_.worker();
    if (!worker_name.has_value()) {
      return MissingRequired("--worker");
    }
    return scheduler_service_.ApplyRebalanceProposal(*worker_name);
  }

  if (command == "reconcile-rebalance-proposals") {
    return scheduler_service_.ReconcileRebalanceProposals();
  }

  if (command == "scheduler-tick") {
    return scheduler_service_.SchedulerTick();
  }

  if (command == "set-rollout-action-status") {
    const auto action_id = command_line_.id();
    if (!action_id.has_value()) {
      return MissingRequired("--id");
    }
    const auto requested_status = command_line_.status();
    if (!requested_status.has_value()) {
      return MissingRequired("--status");
    }
    return scheduler_service_.SetRolloutActionStatus(
        *action_id,
        *requested_status,
        command_line_.message());
  }

  if (command == "enqueue-rollout-eviction") {
    const auto action_id = command_line_.id();
    if (!action_id.has_value()) {
      return MissingRequired("--id");
    }
    return scheduler_service_.EnqueueRolloutEviction(*action_id);
  }

  if (command == "reconcile-rollout-actions") {
    return scheduler_service_.ReconcileRolloutActions();
  }

  if (command == "apply-ready-rollout-action") {
    const auto action_id = command_line_.id();
    if (!action_id.has_value()) {
      return MissingRequired("--id");
    }
    return scheduler_service_.ApplyReadyRolloutAction(*action_id);
  }

  if (command == "show-node-availability") {
    return read_model_cli_service_.ShowNodeAvailability(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.node());
  }

  if (command == "set-node-availability") {
    const auto requested_node_name = command_line_.node();
    if (!requested_node_name.has_value()) {
      return MissingRequired("--node");
    }
    const auto requested_availability = command_line_.availability();
    if (!requested_availability.has_value()) {
      return MissingRequired("--availability");
    }
    return assignment_orchestration_service_.SetNodeAvailability(
        command_line_.db().value_or("var/controller.sqlite"),
        *requested_node_name,
        naim::ParseNodeAvailability(*requested_availability),
        command_line_.message());
  }

  if (command == "retry-host-assignment") {
    const auto assignment_id = command_line_.id();
    if (!assignment_id.has_value()) {
      return MissingRequired("--id");
    }
    return assignment_orchestration_service_.RetryHostAssignment(
        command_line_.db().value_or("var/controller.sqlite"),
        *assignment_id);
  }

  if (command == "notify-release") {
    const auto manifest_path = command_line_.manifest();
    if (!manifest_path.has_value()) {
      return MissingRequired("--manifest");
    }
    return host_registry_service_.NotifyConnectedHostsOfRelease(
        *manifest_path,
        command_line_.message());
  }

  if (command == "import-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return bundle_cli_service_.ImportBundle(
        command_line_.db().value_or("var/controller.sqlite"),
        *bundle_dir);
  }

  if (command == "render-compose") {
    return bundle_cli_service_.RenderCompose(
        command_line_.db().value_or("var/controller.sqlite"),
        command_line_.node());
  }

  if (command == "render-infer-runtime") {
    return bundle_cli_service_.RenderInferRuntime(
        command_line_.db().value_or("var/controller.sqlite"));
  }

  if (command == "serve") {
    return serve_service_.Serve(
        command_line_.listen_host().value_or("127.0.0.1"),
        command_line_.listen_port().value_or(18080),
        command_line_.ui_root(),
        command_line_.skills_factory_upstream());
  }

  if (command == "serve-skills-factory") {
    return serve_service_.ServeSkillsFactory(
        command_line_.listen_host().value_or("127.0.0.1"),
        command_line_.listen_port().value_or(18082));
  }

  if (command == "revoke-hostd") {
    const auto node_name = command_line_.node();
    if (!node_name.has_value()) {
      return MissingRequired("--node");
    }
    return host_registry_service_.RevokeHost(*node_name, command_line_.message());
  }

  if (command == "rotate-hostd-key") {
    const auto node_name = command_line_.node();
    if (!node_name.has_value()) {
      return MissingRequired("--node");
    }
    const auto public_key = command_line_.public_key_base64();
    if (!public_key.has_value()) {
      return MissingRequired("--public-key");
    }
    return host_registry_service_.RotateHostKey(
        *node_name,
        *public_key,
        command_line_.message());
  }

  if (command == "reset-hostd-onboarding") {
    const auto node_name = command_line_.node();
    if (!node_name.has_value()) {
      return MissingRequired("--node");
    }
    return host_registry_service_.ResetHostOnboarding(
        *node_name,
        command_line_.message());
  }

  if (command == "set-hostd-storage-role") {
    const auto node_name = command_line_.node();
    if (!node_name.has_value()) {
      return MissingRequired("--node");
    }
    const auto status = command_line_.status();
    if (!status.has_value()) {
      return MissingRequired("--status");
    }
    if (*status != "enabled" && *status != "disabled") {
      std::cerr << "error: --status must be enabled or disabled\n";
      return 1;
    }
    return host_registry_service_.SetHostStorageRole(
        *node_name,
        *status == "enabled",
        command_line_.message());
  }

  if (command == "ensure-web-ui") {
    return web_ui_service_.Ensure(
        WebUiService::ResolveWebUiRoot(command_line_.web_ui_root()),
        command_line_.listen_port().value_or(WebUiService::DefaultWebUiPort()),
        command_line_.controller_upstream().value_or(WebUiService::DefaultControllerUpstream()),
        WebUiService::ResolveComposeMode(command_line_.compose_mode()));
  }

  if (command == "show-web-ui-status") {
    return web_ui_service_.ShowStatus(
        WebUiService::ResolveWebUiRoot(command_line_.web_ui_root()));
  }

  if (command == "stop-web-ui") {
    return web_ui_service_.Stop(
        WebUiService::ResolveWebUiRoot(command_line_.web_ui_root()),
        WebUiService::ResolveComposeMode(command_line_.compose_mode()));
  }

  return std::nullopt;
}

int ControllerCli::MissingRequired(const char* option_name) const {
  std::cerr << "error: " << option_name << " is required\n";
  return 1;
}

}  // namespace naim::controller
