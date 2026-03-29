#include "app/controller_component_factory_support.h"

#include "app/controller_composition_support.h"
#include "app/controller_http_service_support.h"
#include "app/controller_main_includes.h"
#include "app/controller_plane_support.h"
#include "app/controller_read_model_support.h"
#include "app/controller_scheduler_service_builder.h"

namespace {

using nlohmann::json;

using SchedulerService = comet::controller::SchedulerService;
using SchedulerViewService = ::SchedulerViewService;
using PlaneService = comet::controller::PlaneService;
using ReadModelService = comet::controller::ReadModelService;
using HostRegistryService = comet::controller::HostRegistryService;
using BundleCliService = comet::controller::BundleCliService;
using ReadModelCliService = comet::controller::ReadModelCliService;
using AssignmentOrchestrationService = comet::controller::AssignmentOrchestrationService;
using SchedulerHttpService = ::SchedulerHttpService;
using ReadModelHttpService = ::ReadModelHttpService;
using ModelLibraryService = ::ModelLibraryService;
using ModelLibraryHttpService = ::ModelLibraryHttpService;
using PlaneHttpService = ::PlaneHttpService;
using HostdHttpService = ::HostdHttpService;
using AuthHttpService = ::AuthHttpService;
using InteractionHttpService = ::InteractionHttpService;

using comet::controller::composition_support::AppendControllerEvent;
using comet::controller::composition_support::CanFinalizeDeletedPlane;
using comet::controller::composition_support::FilterHostObservationsForPlane;

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

int DefaultStaleAfterSeconds() {
  return 300;
}

int MinimumSafeDirectRebalanceScore() {
  return 100;
}

int MaximumRebalanceIterationsPerGeneration() {
  return 1;
}

int WorkerMinimumResidencySeconds() {
  return 300;
}

int NodeCooldownAfterMoveSeconds() {
  return 60;
}

int VerificationStableSamplesRequired() {
  return 3;
}

int VerificationTimeoutSeconds() {
  return 45;
}

comet::controller::ControllerRuntimeSupportService MakeControllerRuntimeSupportService() {
  return comet::controller::ControllerRuntimeSupportService{};
}

comet::controller::DesiredStatePolicyService MakeDesiredStatePolicyService() {
  return comet::controller::DesiredStatePolicyService{};
}

comet::controller::PlaneRealizationService MakePlaneRealizationService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  return comet::controller::PlaneRealizationService(
      &runtime_support_service,
      DefaultStaleAfterSeconds());
}

InteractionHttpService MakeInteractionHttpService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  static const comet::controller::DesiredStatePolicyService desired_state_policy_service =
      MakeDesiredStatePolicyService();
  static const comet::controller::InteractionRuntimeSupportService
      interaction_runtime_support_service;
  return comet::controller::http_service_support::CreateInteractionHttpService(
      runtime_support_service,
      desired_state_policy_service,
      interaction_runtime_support_service);
}

HostdHttpService MakeHostdHttpService() {
  return comet::controller::http_service_support::CreateHostdHttpService();
}

AuthHttpService MakeAuthHttpService(AuthSupportService& auth_support) {
  return comet::controller::http_service_support::CreateAuthHttpService(auth_support);
}

comet::controller::SchedulerDomainService MakeSchedulerDomainService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  return comet::controller::SchedulerDomainService({
      [&](const std::string& heartbeat_at) {
        return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
      },
      [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
        return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseRuntimeStatus(observation);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseGpuTelemetry(observation);
      },
      [&](const std::vector<comet::NodeAvailabilityOverride>& overrides) {
        return runtime_support_service.BuildAvailabilityOverrideMap(overrides);
      },
      [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [](comet::NodeAvailability availability) {
        return availability == comet::NodeAvailability::Active;
      },
      [&](const std::string& timestamp_text) {
        return runtime_support_service.TimestampAgeSeconds(timestamp_text);
      },
      [&](const std::vector<comet::HostObservation>& observations,
          const std::string& node_name,
          int stale_after_seconds) {
        return MakePlaneRealizationService().ObservedSchedulingGateReason(
            observations, node_name, stale_after_seconds);
      },
      DefaultStaleAfterSeconds(),
      MinimumSafeDirectRebalanceScore(),
      WorkerMinimumResidencySeconds(),
      NodeCooldownAfterMoveSeconds(),
      85,
      1024,
  });
}

comet::controller::StateAggregateLoader MakeStateAggregateLoader(
    const comet::controller::SchedulerDomainService& scheduler_domain_service,
    const SchedulerViewService& scheduler_view_service) {
  return comet::controller::StateAggregateLoader(
      scheduler_domain_service,
      scheduler_view_service,
      MakeControllerRuntimeSupportService(),
      MaximumRebalanceIterationsPerGeneration());
}

BundleCliService MakeBundleCliService() {
  static const comet::controller::ControllerPrintService controller_print_service =
      comet::controller::plane_support::CreateControllerPrintService(
          MakeControllerRuntimeSupportService());
  static const comet::controller::DesiredStatePolicyService desired_state_policy_service =
      MakeDesiredStatePolicyService();
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  static const comet::controller::PlaneRealizationService plane_realization_service =
      MakePlaneRealizationService();
  return comet::controller::plane_support::CreateBundleCliService(
      controller_print_service,
      desired_state_policy_service,
      runtime_support_service,
      plane_realization_service,
      DefaultArtifactsRoot(),
      DefaultStaleAfterSeconds());
}

comet::controller::ControllerPrintService MakeControllerPrintService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  return comet::controller::plane_support::CreateControllerPrintService(
      runtime_support_service);
}

comet::controller::PlaneMutationService MakePlaneMutationService() {
  static const BundleCliService bundle_cli_service = MakeBundleCliService();
  return comet::controller::plane_support::CreatePlaneMutationService(
      bundle_cli_service,
      [](const std::string& db_path) {
        static const comet::controller::ControllerPrintService controller_print_service =
            MakeControllerPrintService();
        static const comet::controller::DesiredStatePolicyService desired_state_policy_service =
            MakeDesiredStatePolicyService();
        static const comet::controller::PlaneRealizationService plane_realization_service =
            MakePlaneRealizationService();
        return comet::controller::plane_support::CreatePlaneService(
            db_path,
            controller_print_service,
            desired_state_policy_service,
            plane_realization_service,
            DefaultArtifactsRoot());
      });
}

AssignmentOrchestrationService MakeAssignmentOrchestrationService() {
  static const comet::controller::ControllerEventService controller_event_service;
  static const comet::controller::ControllerPrintService controller_print_service =
      MakeControllerPrintService();
  return AssignmentOrchestrationService(
      controller_event_service,
      controller_print_service,
      DefaultArtifactsRoot());
}

PlaneService MakePlaneService(const std::string& db_path) {
  static const comet::controller::ControllerPrintService controller_print_service =
      MakeControllerPrintService();
  static const comet::controller::DesiredStatePolicyService desired_state_policy_service =
      MakeDesiredStatePolicyService();
  static const comet::controller::PlaneRealizationService plane_realization_service =
      MakePlaneRealizationService();
  return comet::controller::plane_support::CreatePlaneService(
      db_path,
      controller_print_service,
      desired_state_policy_service,
      plane_realization_service,
      DefaultArtifactsRoot());
}

PlaneHttpService MakePlaneHttpService() {
  static const comet::controller::ControllerRequestSupport request_support;
  static const comet::controller::PlaneMutationService plane_mutation_service =
      MakePlaneMutationService();
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  static const comet::controller::PlaneRegistryService plane_registry_service(
      comet::controller::PlaneRegistryService::Deps{
          [](comet::ControllerStore& store, const std::string& plane_name) {
            return CanFinalizeDeletedPlane(store, plane_name);
          },
          [](comet::ControllerStore& store,
              const std::string& category,
              const std::string& event_type,
              const std::string& message,
              const json& payload,
              const std::string& plane_name) {
            AppendControllerEvent(
                store,
                category,
                event_type,
                message,
                payload,
                plane_name);
          },
          [](const std::vector<comet::HostObservation>& observations,
              const std::string& plane_name) {
            return FilterHostObservationsForPlane(observations, plane_name);
          },
          [&](const comet::PlaneRecord& plane,
              const std::optional<comet::DesiredState>& desired_state,
              const std::optional<int>& desired_generation,
              const std::vector<comet::HostObservation>& observations) {
            if (!desired_state.has_value() || !desired_generation.has_value()) {
              return plane.applied_generation;
            }
            if (*desired_generation <= plane.applied_generation) {
              return plane.applied_generation;
            }
            for (const auto& node : desired_state->nodes) {
              const auto observation =
                  runtime_support_service.FindHostObservationForNode(
                      observations, node.name);
              if (!observation.has_value()) {
                return plane.applied_generation;
              }
              if (!observation->applied_generation.has_value() ||
                  *observation->applied_generation < *desired_generation ||
                  observation->status == comet::HostObservationStatus::Failed) {
                return plane.applied_generation;
              }
            }
            return *desired_generation;
          },
          [](const std::vector<comet::HostAssignment>& assignments) {
            std::map<std::string, comet::HostAssignment> latest_by_node;
            for (const auto& assignment : assignments) {
              auto it = latest_by_node.find(assignment.node_name);
              if (it == latest_by_node.end() || assignment.id >= it->second.id) {
                latest_by_node[assignment.node_name] = assignment;
              }
            }
            return latest_by_node;
          },
      });
  static const comet::controller::ControllerStateService controller_state_service(
      comet::controller::ControllerStateService::Deps{
          [](comet::ControllerStore& store, const std::string& plane_name) {
            return CanFinalizeDeletedPlane(store, plane_name);
          },
          [](comet::ControllerStore& store,
              const std::string& category,
              const std::string& event_type,
              const std::string& message,
              const json& payload,
              const std::string& plane_name) {
            AppendControllerEvent(
                store,
                category,
                event_type,
                message,
                payload,
                plane_name);
          },
      });
  static const SchedulerViewService scheduler_view_service;
  static const comet::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const comet::controller::StateAggregateLoader state_aggregate_loader =
      MakeStateAggregateLoader(scheduler_domain_service, scheduler_view_service);
  static const ReadModelService read_model_service =
      comet::controller::read_model_support::CreateReadModelService(
          runtime_support_service);
  static const comet::controller::DashboardService dashboard_service(
      comet::controller::DashboardService::Deps{
          &state_aggregate_loader,
          [&](const comet::EventRecord& event) {
            return read_model_service.BuildEventPayloadItem(event);
          },
          [&](const std::vector<comet::NodeAvailabilityOverride>& overrides) {
            return runtime_support_service.BuildAvailabilityOverrideMap(overrides);
          },
          [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
              const std::string& node_name) {
            return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
          },
          [&](const std::string& heartbeat_at) {
            return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
          },
          [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
            return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
          },
          [&](const comet::HostObservation& observation) {
            return runtime_support_service.ParseRuntimeStatus(observation);
          },
          [&](const comet::HostObservation& observation) {
            return runtime_support_service.ParseGpuTelemetry(observation);
          },
      });
  return comet::controller::plane_support::CreatePlaneHttpService(
      request_support,
      plane_mutation_service,
      plane_registry_service,
      controller_state_service,
      dashboard_service,
      DefaultStaleAfterSeconds());
}

ModelLibraryService MakeModelLibraryService() {
  static const comet::controller::ControllerRequestSupport request_support;
  return comet::controller::http_service_support::CreateModelLibraryService(request_support);
}

ModelLibraryHttpService MakeModelLibraryHttpService(
    const ModelLibraryService& model_library_service) {
  return comet::controller::http_service_support::CreateModelLibraryHttpService(
      model_library_service);
}

ReadModelService MakeReadModelService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  return comet::controller::read_model_support::CreateReadModelService(
      runtime_support_service);
}

::BundleHttpService MakeBundleHttpService(
    const comet::controller::IBundleCliService& bundle_cli_service) {
  return comet::controller::read_model_support::CreateBundleHttpService(bundle_cli_service);
}

ReadModelHttpService MakeReadModelHttpService(
    const ReadModelService& read_model_service) {
  static const SchedulerViewService scheduler_view_service;
  static const comet::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const comet::controller::StateAggregateLoader state_aggregate_loader =
      MakeStateAggregateLoader(scheduler_domain_service, scheduler_view_service);
  return comet::controller::read_model_support::CreateReadModelHttpService(
      read_model_service,
      scheduler_view_service,
      state_aggregate_loader,
      DefaultStaleAfterSeconds());
}

SchedulerHttpService MakeSchedulerHttpService(
    const ReadModelService& read_model_service) {
  static const AssignmentOrchestrationService assignment_orchestration_service =
      MakeAssignmentOrchestrationService();
  static const comet::controller::ControllerSchedulerServiceFactory scheduler_service_factory;
  return comet::controller::read_model_support::CreateSchedulerHttpService(
      read_model_service,
      assignment_orchestration_service,
      scheduler_service_factory);
}

ReadModelCliService MakeReadModelCliService() {
  static const comet::controller::ControllerPrintService controller_print_service =
      MakeControllerPrintService();
  static const SchedulerViewService scheduler_view_service;
  static const comet::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const comet::controller::StateAggregateLoader state_aggregate_loader =
      MakeStateAggregateLoader(scheduler_domain_service, scheduler_view_service);
  return comet::controller::read_model_support::CreateReadModelCliService(
      controller_print_service,
      state_aggregate_loader,
      scheduler_view_service,
      DefaultStaleAfterSeconds(),
      VerificationStableSamplesRequired());
}

SchedulerService MakeSchedulerServiceInternal(
    const std::string& db_path,
    const std::string& artifacts_root) {
  static const comet::controller::ControllerEventService controller_event_service;
  static const comet::controller::ControllerPrintService controller_print_service =
      MakeControllerPrintService();
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service =
      MakeControllerRuntimeSupportService();
  static const comet::controller::PlaneRealizationService plane_realization_service =
      MakePlaneRealizationService();
  static const SchedulerViewService scheduler_view_service;
  static const comet::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const comet::controller::StateAggregateLoader state_aggregate_loader =
      MakeStateAggregateLoader(scheduler_domain_service, scheduler_view_service);
  static const ReadModelCliService read_model_cli_service =
      MakeReadModelCliService();
  static const comet::controller::SchedulerExecutionSupport scheduler_execution_support({
      [&](const std::vector<comet::HostAssignment>& assignments, const std::string& node_name) {
        return plane_realization_service.FindLatestHostAssignmentForNode(assignments, node_name);
      },
      [&](const std::vector<comet::HostAssignment>& assignments, const std::string& plane_name) {
        return plane_realization_service.FindLatestHostAssignmentForPlane(assignments, plane_name);
      },
      []() { return DefaultArtifactsRoot(); },
      [&](const std::vector<comet::HostObservation>& observations, const std::string& node_name) {
        return runtime_support_service.FindHostObservationForNode(observations, node_name);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseInstanceRuntimeStatuses(observation);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseGpuTelemetry(observation);
      },
      [&](const std::string& timestamp_text) {
        return runtime_support_service.TimestampAgeSeconds(timestamp_text);
      },
      []() { return VerificationTimeoutSeconds(); },
      []() { return VerificationStableSamplesRequired(); },
      [&]() { return runtime_support_service.UtcNowSqlTimestamp(); },
  });
  return SchedulerService(
      db_path,
      artifacts_root,
      DefaultStaleAfterSeconds(),
      VerificationStableSamplesRequired(),
      state_aggregate_loader,
      scheduler_view_service,
      read_model_cli_service,
      controller_print_service,
      runtime_support_service,
      scheduler_execution_support,
      plane_realization_service,
      controller_event_service);
}

HostRegistryService MakeHostRegistryService(const std::string& db_path) {
  return comet::controller::plane_support::CreateHostRegistryService(db_path);
}

}  // namespace

namespace comet::controller {

SchedulerService BuildControllerSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return MakeSchedulerServiceInternal(db_path, artifacts_root);
}

::BundleHttpService BuildBundleHttpService(const IBundleCliService& bundle_cli_service) {
  return MakeBundleHttpService(bundle_cli_service);
}

}  // namespace comet::controller

namespace comet::controller::component_factory_support {

std::unique_ptr<BundleCliService> CreateBundleCliService() {
  return std::make_unique<BundleCliService>(MakeBundleCliService());
}

std::unique_ptr<IReadModelCliService> CreateReadModelCliService() {
  return std::make_unique<ReadModelCliService>(MakeReadModelCliService());
}

std::unique_ptr<IHostRegistryService> CreateHostRegistryService(const std::string& db_path) {
  return std::make_unique<HostRegistryService>(MakeHostRegistryService(db_path));
}

std::unique_ptr<IPlaneService> CreatePlaneService(const std::string& db_path) {
  return std::make_unique<PlaneService>(MakePlaneService(db_path));
}

std::unique_ptr<IAssignmentOrchestrationService> CreateAssignmentOrchestrationService() {
  return std::make_unique<AssignmentOrchestrationService>(MakeAssignmentOrchestrationService());
}

std::unique_ptr<ISchedulerService> CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return std::make_unique<SchedulerService>(
      BuildControllerSchedulerService(db_path, artifacts_root));
}

std::unique_ptr<IWebUiService> CreateWebUiService(const std::string& db_path) {
  return std::make_unique<WebUiService>(
      db_path,
      [](comet::ControllerStore& store,
          const std::string& event_type,
          const std::string& message,
          const json& payload) {
        AppendControllerEvent(store, "web-ui", event_type, message, payload);
      });
}

InteractionHttpService CreateInteractionHttpService() {
  return MakeInteractionHttpService();
}

AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) {
  return MakeAuthHttpService(auth_support);
}

HostdHttpService CreateHostdHttpService() {
  return MakeHostdHttpService();
}

ModelLibraryService CreateModelLibraryService() {
  return MakeModelLibraryService();
}

ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) {
  return MakeModelLibraryHttpService(model_library_service);
}

PlaneHttpService CreatePlaneHttpService() {
  return MakePlaneHttpService();
}

ReadModelService CreateReadModelService() {
  return MakeReadModelService();
}

ReadModelHttpService CreateReadModelHttpService(const ReadModelService& read_model_service) {
  return MakeReadModelHttpService(read_model_service);
}

SchedulerHttpService CreateSchedulerHttpService(const ReadModelService& read_model_service) {
  return MakeSchedulerHttpService(read_model_service);
}

}  // namespace comet::controller::component_factory_support
