#include "../include/controller_cli.h"

#include <iostream>
#include <utility>

namespace comet::controller {

ControllerCli::ControllerCli(
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
    RotateHostKeyAction rotate_host_key_action)
    : command_line_(command_line),
      host_registry_service_(host_registry_service),
      plane_service_(plane_service),
      scheduler_service_(scheduler_service),
      web_ui_service_(web_ui_service),
      init_db_action_(std::move(init_db_action)),
      seed_demo_action_(std::move(seed_demo_action)),
      show_state_action_(std::move(show_state_action)),
      show_host_assignments_action_(std::move(show_host_assignments_action)),
      show_host_observations_action_(std::move(show_host_observations_action)),
      show_host_health_action_(std::move(show_host_health_action)),
      show_disk_state_action_(std::move(show_disk_state_action)),
      validate_bundle_action_(std::move(validate_bundle_action)),
      preview_bundle_action_(std::move(preview_bundle_action)),
      plan_bundle_action_(std::move(plan_bundle_action)),
      apply_bundle_action_(std::move(apply_bundle_action)),
      apply_state_file_action_(std::move(apply_state_file_action)),
      plan_host_ops_action_(std::move(plan_host_ops_action)),
      show_node_availability_action_(std::move(show_node_availability_action)),
      set_node_availability_action_(std::move(set_node_availability_action)),
      retry_host_assignment_action_(std::move(retry_host_assignment_action)),
      import_bundle_action_(std::move(import_bundle_action)),
      render_compose_action_(std::move(render_compose_action)),
      render_infer_runtime_action_(std::move(render_infer_runtime_action)),
      serve_action_(std::move(serve_action)),
      revoke_host_action_(std::move(revoke_host_action)),
      rotate_host_key_action_(std::move(rotate_host_key_action)) {}

std::optional<int> ControllerCli::TryRun() const {
  const std::string& command = command_line_.command();

  if (command == "init-db") {
    return init_db_action_();
  }

  if (command == "seed-demo") {
    return seed_demo_action_();
  }

  if (command == "show-hostd-hosts") {
    return host_registry_service_.ShowHosts(command_line_.node());
  }

  if (command == "show-state") {
    return show_state_action_();
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
    return show_host_assignments_action_(command_line_.node());
  }

  if (command == "show-host-observations") {
    return show_host_observations_action_(
        command_line_.plane(),
        command_line_.node(),
        command_line_.stale_after().value_or(300));
  }

  if (command == "show-host-health") {
    return show_host_health_action_(
        command_line_.node(),
        command_line_.stale_after().value_or(300));
  }

  if (command == "show-disk-state") {
    return show_disk_state_action_(command_line_.node(), command_line_.plane());
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
    return validate_bundle_action_(*bundle_dir);
  }

  if (command == "preview-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return preview_bundle_action_(*bundle_dir, command_line_.node());
  }

  if (command == "plan-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return plan_bundle_action_(*bundle_dir);
  }

  if (command == "apply-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return apply_bundle_action_(
        *bundle_dir,
        command_line_.artifacts_root().value_or("var/artifacts"));
  }

  if (command == "apply-state-file") {
    const auto state_path = command_line_.state_file();
    if (!state_path.has_value()) {
      return MissingRequired("--state");
    }
    return apply_state_file_action_(
        *state_path,
        command_line_.artifacts_root().value_or("var/artifacts"));
  }

  if (command == "plan-host-ops") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return plan_host_ops_action_(
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
    return show_node_availability_action_(command_line_.node());
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
    return set_node_availability_action_(
        *requested_node_name,
        *requested_availability,
        command_line_.message());
  }

  if (command == "retry-host-assignment") {
    const auto assignment_id = command_line_.id();
    if (!assignment_id.has_value()) {
      return MissingRequired("--id");
    }
    return retry_host_assignment_action_(*assignment_id);
  }

  if (command == "import-bundle") {
    const auto bundle_dir = command_line_.bundle();
    if (!bundle_dir.has_value()) {
      return MissingRequired("--bundle");
    }
    return import_bundle_action_(*bundle_dir);
  }

  if (command == "render-compose") {
    return render_compose_action_(command_line_.node());
  }

  if (command == "render-infer-runtime") {
    return render_infer_runtime_action_();
  }

  if (command == "serve") {
    return serve_action_(
        command_line_.listen_host().value_or("127.0.0.1"),
        command_line_.listen_port().value_or(18080),
        command_line_.ui_root(),
        command_line_.artifacts_root().value_or("var/artifacts"));
  }

  if (command == "revoke-hostd") {
    const auto node_name = command_line_.node();
    if (!node_name.has_value()) {
      return MissingRequired("--node");
    }
    return revoke_host_action_(*node_name, command_line_.message());
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
    return rotate_host_key_action_(*node_name, *public_key, command_line_.message());
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

}  // namespace comet::controller
