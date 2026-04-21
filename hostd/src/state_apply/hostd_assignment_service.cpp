#include "state_apply/hostd_assignment_service.h"

#include <iostream>
#include <stdexcept>

#include "backend/local_db_hostd_backend.h"
#include "naim/state/state_json.h"
#include "naim/state/sqlite_store.h"
#include "state_apply/hostd_assignment_lock.h"

namespace naim::hostd {

HostdAssignmentService::HostdAssignmentService(
    const IHostdBackendFactory& backend_factory,
    const IHostdAssignmentSupport& support,
    const HostdObservationService& observation_service)
    : backend_factory_(backend_factory),
      support_(support),
      observation_service_(observation_service) {}

void HostdAssignmentService::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  support_.ShowDemoOps(node_name, storage_root, runtime_root);
}

void HostdAssignmentService::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) const {
  support_.ShowStateOps(db_path, node_name, artifacts_root, storage_root, runtime_root, state_root);
}

void HostdAssignmentService::ApplyStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const ComposeMode compose_mode) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  LocalDbHostdBackend backend(db_path);
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }
  const auto desired_generation = store.LoadDesiredGeneration();

  const naim::DesiredState rebased_full_state =
      support_.RebaseStateForRuntimeRoot(*state, storage_root, runtime_root);
  const naim::DesiredState desired_node_state =
      naim::SliceDesiredStateForNode(rebased_full_state, node_name);

  std::cout << "db=" << db_path << "\n";
  try {
    support_.ApplyDesiredNodeState(
        desired_node_state,
        artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        compose_mode,
        "hostd apply-state-ops",
        desired_generation,
        std::nullopt,
        &backend);
    observation_service_.ReportObservedState(
        backend,
        support_.BuildObservedStateSnapshot(
            node_name,
            storage_root,
            state_root,
            naim::HostObservationStatus::Applied,
            desired_generation.has_value()
                ? "applied desired generation " + std::to_string(*desired_generation)
                : "applied desired state"),
        "hostd observed-state-update");
  } catch (const std::exception& error) {
    observation_service_.ReportObservedState(
        backend,
        support_.BuildObservedStateSnapshot(
            node_name,
            storage_root,
            state_root,
            naim::HostObservationStatus::Failed,
            error.what()),
        "hostd observed-state-update");
    throw;
  }
}

void HostdAssignmentService::ApplyNextAssignment(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::optional<std::string>& onboarding_key,
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const ComposeMode compose_mode) const {
  const auto assignment_lock = HostdAssignmentLock::TryAcquire(state_root, node_name);
  if (!assignment_lock.has_value()) {
    std::cout << "assignment apply already in progress for node=" << node_name << "\n";
    return;
  }

  auto backend = backend_factory_.CreateBackend(
      db_path,
      controller_url,
      host_private_key_path,
      controller_fingerprint,
      onboarding_key,
      node_name,
      storage_root);
  const auto assignment = backend->ClaimNextHostAssignment(node_name);
  if (!assignment.has_value()) {
    std::cout << "no pending assignments for node=" << node_name << "\n";
    return;
  }

  std::cout << "hostd apply-next-assignment\n";
  if (controller_url.has_value()) {
    std::cout << "controller=" << *controller_url << "\n";
  } else {
    std::cout << "db=" << db_path.value_or((std::filesystem::path("var") / "controller.sqlite").string())
              << "\n";
  }
  std::cout << "assignment_id=" << assignment->id << "\n";
  std::cout << "assignment_type=" << assignment->assignment_type << "\n";
  std::cout << "desired_generation=" << assignment->desired_generation << "\n";
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  std::cout << "state_root=" << state_root << "\n";
  std::cout << "compose_mode=" << (compose_mode == ComposeMode::Exec ? "exec" : "skip") << "\n";

  const std::string assignment_context =
      assignment->status_message.empty() ? "" : " [" + assignment->status_message + "]";

  try {
    if (assignment->assignment_type == "model-library-download") {
      support_.PublishAssignmentProgress(
          backend.get(),
          assignment->id,
          support_.BuildAssignmentProgressPayload(
              "starting",
              "Starting model download",
              "Storage node accepted the model library download assignment.",
              5,
              assignment->plane_name,
              node_name));
      support_.DownloadModelLibraryArtifacts(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      support_.PublishAssignmentProgress(
          backend.get(),
          assignment->id,
          support_.BuildAssignmentProgressPayload(
              "completed",
              "Model download completed",
              "Storage node finished the model library download assignment.",
              100,
              assignment->plane_name,
              node_name));
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "downloaded model library artifacts on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      support_.AppendHostdEvent(
          *backend,
          "model-library",
          "downloaded",
          "downloaded model library artifacts on node " + node_name,
          nlohmann::json{
              {"assignment_type", assignment->assignment_type},
              {"attempt_count", assignment->attempt_count},
              {"max_attempts", assignment->max_attempts},
          },
          assignment->plane_name,
          node_name,
          "",
          assignment->id,
          std::nullopt,
          "info");
      return;
    }
    if (assignment->assignment_type == "model-artifact-read-chunk") {
      support_.ReadModelArtifactChunk(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "read model artifact chunk on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }
    if (assignment->assignment_type == "model-artifact-build-manifest") {
      support_.BuildModelArtifactManifest(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "built model artifact manifest on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }
    if (assignment->assignment_type == "runtime-http-proxy") {
      support_.ExecuteRuntimeHttpProxy(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "executed runtime HTTP proxy request on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }
    if (assignment->assignment_type == "knowledge-vault-apply") {
      support_.ApplyKnowledgeVaultService(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          storage_root,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "applied knowledge vault service on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }
    if (assignment->assignment_type == "knowledge-vault-stop") {
      support_.StopKnowledgeVaultService(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "stopped knowledge vault service on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }
    if (assignment->assignment_type == "knowledge-vault-http-proxy") {
      support_.ExecuteKnowledgeVaultHttpProxy(
          nlohmann::json::parse(assignment->desired_state_json),
          node_name,
          backend.get(),
          assignment->id);
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Applied,
          "executed knowledge vault HTTP proxy request on attempt " +
              std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts));
      return;
    }

    if (assignment->assignment_type != "apply-node-state" &&
        assignment->assignment_type != "drain-node-state" &&
        assignment->assignment_type != "stop-plane-state" &&
        assignment->assignment_type != "delete-plane-state" &&
        assignment->assignment_type != "evict-workers") {
      throw std::runtime_error("unsupported assignment type '" + assignment->assignment_type + "'");
    }

    const naim::DesiredState rebased_state = support_.RebaseStateForRuntimeRoot(
        naim::DeserializeDesiredStateJson(assignment->desired_state_json),
        storage_root,
        runtime_root);
    const bool is_drain_assignment = assignment->assignment_type == "drain-node-state";
    const bool is_stop_assignment = assignment->assignment_type == "stop-plane-state";
    const bool is_delete_assignment = assignment->assignment_type == "delete-plane-state";
    const bool is_eviction_assignment = assignment->assignment_type == "evict-workers";
    const auto victim_names =
        is_eviction_assignment
            ? support_.ParseTaggedCsv(assignment->status_message, "victims")
            : std::vector<std::string>{};
    const auto victim_host_pids =
        is_eviction_assignment ? support_.CaptureServiceHostPids(victim_names)
                               : std::map<std::string, int>{};
    const std::string applying_status_message =
        (is_drain_assignment ? "draining node for desired generation "
                             : (is_stop_assignment
                                    ? "stopping plane state for desired generation "
                             : (is_delete_assignment
                                    ? "deleting plane state for desired generation "
                             : (is_eviction_assignment
                                    ? "evicting rollout workers for desired generation "
                                    : "applying desired generation ")))) +
        std::to_string(assignment->desired_generation) + assignment_context;
    const std::string apply_trace_label =
        is_drain_assignment
            ? "hostd drain-assignment-ops"
            : (is_stop_assignment
                   ? "hostd stop-plane-assignment-ops"
                   : (is_delete_assignment
                          ? "hostd delete-plane-assignment-ops"
                          : (is_eviction_assignment
                                 ? "hostd eviction-assignment-ops"
                                 : "hostd apply-assignment-ops")));

    observation_service_.ReportObservedState(
        *backend,
        support_.BuildObservedStateSnapshot(
            node_name,
            storage_root,
            state_root,
            naim::HostObservationStatus::Applying,
            applying_status_message,
            assignment->id),
        "hostd observed-state-update");

    support_.ApplyDesiredNodeState(
        rebased_state,
        assignment->artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        compose_mode,
        apply_trace_label,
        assignment->desired_generation,
        assignment->id,
        backend.get());

    if (is_eviction_assignment &&
        compose_mode == ComposeMode::Exec &&
        !support_.VerifyEvictionAssignment(
            rebased_state,
            node_name,
            state_root,
            assignment->status_message,
            victim_host_pids)) {
      throw std::runtime_error("eviction verification timed out; gpu resources were not released");
    }

    observation_service_.ReportObservedState(
        *backend,
        support_.BuildObservedStateSnapshot(
            node_name,
            storage_root,
            state_root,
            naim::HostObservationStatus::Applied,
            (is_drain_assignment ? "drained node for desired generation "
                                 : (is_stop_assignment
                                        ? "stopped plane state for desired generation "
                                 : (is_eviction_assignment
                                        ? "evicted rollout workers for desired generation "
                                        : "applied desired generation "))) +
                std::to_string(assignment->desired_generation) + assignment_context,
            assignment->id),
        "hostd observed-state-update");

    backend->TransitionClaimedHostAssignment(
        assignment->id,
        naim::HostAssignmentStatus::Applied,
        (is_drain_assignment ? "drained node for desired generation "
                             : (is_stop_assignment
                                    ? "stopped plane state for desired generation "
                             : (is_eviction_assignment
                                    ? "evicted rollout workers for desired generation "
                                    : "applied desired generation "))) +
            std::to_string(assignment->desired_generation) +
            assignment_context +
            " on attempt " + std::to_string(assignment->attempt_count) + "/" +
            std::to_string(assignment->max_attempts));
    support_.AppendHostdEvent(
        *backend,
        "host-assignment",
        "applied",
        "applied assignment on node " + node_name,
        nlohmann::json{
            {"assignment_type", assignment->assignment_type},
            {"desired_generation", assignment->desired_generation},
            {"attempt_count", assignment->attempt_count},
            {"max_attempts", assignment->max_attempts},
        },
        assignment->plane_name,
        node_name,
        "",
        assignment->id,
        std::nullopt,
        "info");
  } catch (const std::exception& error) {
    const std::string error_message = error.what();
    support_.PublishAssignmentProgress(
        backend.get(),
        assignment->id,
        support_.BuildAssignmentProgressPayload(
            "failed",
            "Assignment failed",
            error_message,
            100,
            assignment->plane_name,
            node_name));
    observation_service_.ReportObservedState(
        *backend,
        support_.BuildObservedStateSnapshot(
            node_name,
            storage_root,
            state_root,
            naim::HostObservationStatus::Failed,
            error_message,
            assignment->id),
        "hostd observed-state-update");
    if (assignment->attempt_count < assignment->max_attempts) {
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Pending,
          "attempt " + std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts) + " failed: " +
              error_message + assignment_context);
    } else {
      backend->TransitionClaimedHostAssignment(
          assignment->id,
          naim::HostAssignmentStatus::Failed,
          "attempt " + std::to_string(assignment->attempt_count) + "/" +
              std::to_string(assignment->max_attempts) + " exhausted: " +
              error_message + assignment_context);
    }
    support_.AppendHostdEvent(
        *backend,
        "host-assignment",
        "failed",
        error_message,
        nlohmann::json{
            {"assignment_type", assignment->assignment_type},
            {"desired_generation", assignment->desired_generation},
            {"attempt_count", assignment->attempt_count},
            {"max_attempts", assignment->max_attempts},
        },
        assignment->plane_name,
        node_name,
        "",
        assignment->id,
        std::nullopt,
        "error");
    throw;
  }
}

}  // namespace naim::hostd
