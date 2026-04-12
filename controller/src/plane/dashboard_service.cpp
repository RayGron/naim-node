#include "plane/dashboard_service.h"

#include "browsing/plane_browsing_service.h"
#include "http/controller_http_transport.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/plane_dashboard_skills_summary_service.h"
#include "plane/plane_placement_payload_builder.h"
#include "read_model/state_aggregate_loader.h"
#include "web/web_ui_service.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <set>
#include <stdexcept>
#include <utility>

#include "host/host_assignment_reconciliation_service.h"
#include "naim/state/state_json.h"

using nlohmann::json;

namespace naim::controller {

namespace {

struct ObservedPlaneRuntimeSummary {
  bool available = false;
  int instance_count = 0;
  int disk_count = 0;
};

struct NodeDemandSummary {
  int desired_instance_count = 0;
  int desired_disk_count = 0;
  std::set<std::string> plane_names;
};

ObservedPlaneRuntimeSummary SummarizeObservedPlaneRuntime(
    const naim::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  if (observation.observed_state_json.empty()) {
    return {};
  }
  try {
    const auto observed_state =
        naim::DeserializeDesiredStateJson(observation.observed_state_json);
    const std::string target_plane =
        plane_name.has_value() ? *plane_name : observed_state.plane_name;
    if (target_plane.empty()) {
      return {};
    }

    ObservedPlaneRuntimeSummary summary;
    for (const auto& instance : observed_state.instances) {
      if (instance.node_name == node_name && instance.plane_name == target_plane) {
        ++summary.instance_count;
      }
    }
    for (const auto& disk : observed_state.disks) {
      if (disk.node_name == node_name && disk.plane_name == target_plane) {
        ++summary.disk_count;
      }
    }
    summary.available =
        observed_state.plane_name == target_plane || summary.instance_count > 0 ||
        summary.disk_count > 0;
    return summary;
  } catch (...) {
    return {};
  }
}

NodeDemandSummary SummarizeNodeDemand(
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    const naim::DesiredState& desired_state,
    const std::vector<naim::DesiredState>& desired_states) {
  NodeDemandSummary summary;
  const auto accumulate = [&](const naim::DesiredState& state) {
    for (const auto& instance : state.instances) {
      if (instance.node_name == node_name) {
        ++summary.desired_instance_count;
        summary.plane_names.insert(state.plane_name);
      }
    }
    for (const auto& disk : state.disks) {
      if (disk.node_name == node_name) {
        ++summary.desired_disk_count;
        summary.plane_names.insert(state.plane_name);
      }
    }
  };

  if (plane_name.has_value()) {
    accumulate(desired_state);
  } else {
    for (const auto& state : desired_states) {
      accumulate(state);
    }
  }

  return summary;
}

std::map<std::string, naim::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<naim::HostAssignment>& assignments) {
  std::map<std::string, naim::HostAssignment> latest_by_node;
  for (const auto& assignment : assignments) {
    auto it = latest_by_node.find(assignment.node_name);
    if (it == latest_by_node.end() || assignment.id >= it->second.id) {
      latest_by_node[assignment.node_name] = assignment;
    }
  }
  return latest_by_node;
}

int ComputeEffectivePlaneAppliedGeneration(
    const naim::PlaneRecord& plane,
    const std::optional<naim::DesiredState>& desired_state,
    const std::optional<int>& desired_generation,
    const std::vector<naim::HostObservation>& observations) {
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    return plane.applied_generation;
  }
  if (*desired_generation <= plane.applied_generation) {
    return plane.applied_generation;
  }
  for (const auto& node : desired_state->nodes) {
    const auto observation = std::find_if(
        observations.begin(),
        observations.end(),
        [&](const naim::HostObservation& candidate) {
          return candidate.node_name == node.name;
        });
    if (observation == observations.end()) {
      return plane.applied_generation;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation < *desired_generation ||
        observation->status == naim::HostObservationStatus::Failed) {
      return plane.applied_generation;
    }
  }
  return *desired_generation;
}

struct LocalHealthProbeResult {
  bool attempted = false;
  bool reachable = false;
  std::string checked_at;
  int status_code = 0;
  json payload = nullptr;
  std::string error;
};

std::string EnvValue(const char* name) {
  const char* value = std::getenv(name);
  return value != nullptr && *value != '\0' ? std::string(value) : std::string{};
}

LocalHealthProbeResult ProbeLocalJsonHealth(
    const std::optional<std::string>& upstream,
    const std::string& path) {
  LocalHealthProbeResult result;
  if (!upstream.has_value() || upstream->empty()) {
    return result;
  }

  const ControllerRuntimeSupportService runtime_support_service;
  result.attempted = true;
  result.checked_at = runtime_support_service.UtcNowSqlTimestamp();
  try {
    const auto response =
        SendControllerHttpRequest(ParseControllerEndpointTarget(*upstream), "GET", path);
    result.status_code = response.status_code;
    result.reachable = response.status_code >= 200 && response.status_code < 300;
    if (!response.body.empty()) {
      try {
        result.payload = json::parse(response.body);
      } catch (...) {
        result.payload = json::object();
      }
    }
    if (!result.reachable && result.error.empty()) {
      result.error = "http status " + std::to_string(response.status_code);
    }
  } catch (const std::exception& error) {
    result.error = error.what();
  }
  return result;
}

bool ProbeLooksHealthy(
    const LocalHealthProbeResult& probe,
    const std::optional<std::string>& expected_service = std::nullopt) {
  if (!probe.attempted || !probe.reachable) {
    return false;
  }
  if (!probe.payload.is_object()) {
    return true;
  }
  if (probe.payload.value("status", std::string()) != "ok") {
    return false;
  }
  if (expected_service.has_value()) {
    return probe.payload.value("service", std::string()) == *expected_service;
  }
  return true;
}

json BuildServiceTargetPayload(
    const std::string& label,
    const std::string& value,
    const std::optional<bool>& reachable,
    const std::optional<std::string>& detail = std::nullopt) {
  return json{
      {"label", label},
      {"value", value},
      {"reachable", reachable.has_value() ? json(*reachable) : json(nullptr)},
      {"detail", detail.has_value() ? json(*detail) : json(nullptr)},
  };
}

std::string JoinDetails(const std::vector<std::string>& parts) {
  std::ostringstream out;
  bool first = true;
  for (const auto& part : parts) {
    if (part.empty()) {
      continue;
    }
    if (!first) {
      out << "; ";
    }
    out << part;
    first = false;
  }
  return out.str();
}

std::optional<std::string> PickLatestTimestamp(
    const std::optional<std::string>& left,
    const std::optional<std::string>& right) {
  if (!left.has_value()) {
    return right;
  }
  if (!right.has_value()) {
    return left;
  }
  return *left >= *right ? left : right;
}

std::optional<std::string> LatestHostHeartbeatAt(
    const naim::RegisteredHostRecord& host,
    const std::vector<naim::HostObservation>& observations) {
  std::optional<std::string> latest =
      host.last_heartbeat_at.empty() ? std::nullopt
                                     : std::optional<std::string>(host.last_heartbeat_at);
  for (const auto& observation : observations) {
    if (!observation.heartbeat_at.empty()) {
      latest = PickLatestTimestamp(latest, observation.heartbeat_at);
    }
  }
  return latest;
}

std::optional<naim::RuntimeStatus> SelectPlaneRuntimeStatus(
    const std::optional<naim::RuntimeStatus>& runtime_status,
    const std::optional<std::string>& plane_name) {
  if (!runtime_status.has_value() || !plane_name.has_value()) {
    return runtime_status;
  }
  if (!runtime_status->plane_name.empty() &&
      runtime_status->plane_name != *plane_name) {
    return std::nullopt;
  }
  return runtime_status;
}

json BuildControllerSelfServicePayload() {
  const ControllerRuntimeSupportService runtime_support_service;
  const std::string checked_at = runtime_support_service.UtcNowSqlTimestamp();
  const std::string admin_upstream = EnvValue("NAIM_CONTROLLER_ADMIN_UPSTREAM");
  const std::string internal_upstream = EnvValue("NAIM_CONTROLLER_INTERNAL_UPSTREAM");

  std::string health = "healthy";
  std::string state = "running";
  std::vector<std::string> details = {"dashboard API responding"};
  json targets = json::array();

  const auto add_target_probe =
      [&](const std::string& label, const std::string& upstream) {
        if (upstream.empty()) {
          return;
        }
        const auto probe = ProbeLocalJsonHealth(upstream, "/health");
        const bool target_healthy = ProbeLooksHealthy(probe, "naim-controller");
        targets.push_back(BuildServiceTargetPayload(
            label,
            upstream,
            probe.attempted ? std::optional<bool>(target_healthy) : std::nullopt,
            !probe.error.empty() ? std::optional<std::string>(probe.error) : std::nullopt));
        if (probe.attempted && !target_healthy) {
          health = "warning";
          details.push_back(label + " target probe failed");
        }
      };

  add_target_probe("admin", admin_upstream);
  if (internal_upstream != admin_upstream) {
    add_target_probe("internal", internal_upstream);
  }

  return json{
      {"id", "controller"},
      {"label", "Controller"},
      {"kind", "process"},
      {"health", health},
      {"state", state},
      {"detail", JoinDetails(details)},
      {"targets", std::move(targets)},
      {"updated_at", checked_at},
  };
}

json BuildSkillsFactorySelfServicePayload() {
  const std::optional<std::string> upstream =
      [&]() -> std::optional<std::string> {
    const std::string value = EnvValue("NAIM_SKILLS_FACTORY_UPSTREAM");
    return value.empty() ? std::nullopt : std::optional<std::string>(value);
  }();

  if (!upstream.has_value()) {
    return json{
        {"id", "skills-factory"},
        {"label", "Skills Factory"},
        {"kind", "process"},
        {"health", "unknown"},
        {"state", "unknown"},
        {"detail", "skills factory upstream is not configured"},
        {"targets", json::array()},
        {"updated_at", nullptr},
    };
  }

  const auto probe = ProbeLocalJsonHealth(upstream, "/health");
  const bool healthy = ProbeLooksHealthy(probe, "naim-skills-factory");
  return json{
      {"id", "skills-factory"},
      {"label", "Skills Factory"},
      {"kind", "process"},
      {"health", healthy ? json("healthy") : json("critical")},
      {"state", healthy ? json("running") : json("unreachable")},
      {"detail",
       healthy ? json("health endpoint responded")
               : json(!probe.error.empty() ? probe.error : "health endpoint probe failed")},
      {"targets",
       json::array({BuildServiceTargetPayload(
           "local",
           *upstream,
           probe.attempted ? std::optional<bool>(healthy) : std::nullopt,
           !probe.error.empty() ? std::optional<std::string>(probe.error) : std::nullopt)})},
      {"updated_at", probe.checked_at.empty() ? json(nullptr) : json(probe.checked_at)},
  };
}

json BuildHostdSelfServicePayload(
    naim::ControllerStore& store,
    int stale_after_seconds) {
  const ControllerRuntimeSupportService runtime_support_service;
  const std::string node_name = [&]() {
    const std::string configured = EnvValue("NAIM_HOSTD_NODE_NAME");
    return configured.empty() ? std::string("local-hostd") : configured;
  }();

  const auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    return json{
        {"id", "hostd"},
        {"label", "Hostd"},
        {"kind", "process"},
        {"health", "critical"},
        {"state", "missing"},
        {"detail", "registered host record not found"},
        {"targets", json::array({BuildServiceTargetPayload("node", node_name, std::nullopt)})},
        {"updated_at", nullptr},
    };
  }

  const auto observations = store.LoadHostObservations(node_name);
  const auto latest_heartbeat_at = LatestHostHeartbeatAt(*host, observations);
  const auto heartbeat_age =
      latest_heartbeat_at.has_value()
          ? runtime_support_service.HeartbeatAgeSeconds(*latest_heartbeat_at)
          : std::optional<long long>{};
  const std::string heartbeat_health =
      runtime_support_service.HealthFromAge(heartbeat_age, stale_after_seconds);

  std::string health = "healthy";
  if (host->registration_state != "registered" || host->session_state != "connected" ||
      !latest_heartbeat_at.has_value()) {
    health = "critical";
  } else if (heartbeat_health == "stale") {
    health = "warning";
  }

  std::string detail =
      "registration=" + host->registration_state + ", session=" + host->session_state;
  if (latest_heartbeat_at.has_value()) {
    detail += ", last heartbeat " + *latest_heartbeat_at;
  } else {
    detail += ", heartbeat missing";
  }

  json targets = json::array();
  targets.push_back(BuildServiceTargetPayload("node", node_name, std::nullopt));
  if (!host->advertised_address.empty()) {
    targets.push_back(
        BuildServiceTargetPayload("advertised", host->advertised_address, std::nullopt));
  }

  return json{
      {"id", "hostd"},
      {"label", "Hostd"},
      {"kind", "process"},
      {"health", health},
      {"state", host->session_state.empty() ? json("unknown") : json(host->session_state)},
      {"detail", detail},
      {"targets", std::move(targets)},
      {"updated_at",
       latest_heartbeat_at.has_value()
           ? json(*latest_heartbeat_at)
           : (host->updated_at.empty() ? json(nullptr) : json(host->updated_at))},
  };
}

json BuildWebUiSelfServicePayload() {
  const ControllerRuntimeSupportService runtime_support_service;
  const std::string web_ui_root = [&]() {
    const std::string configured = EnvValue("NAIM_WEB_UI_ROOT");
    return configured.empty() ? WebUiService::DefaultWebUiRoot() : configured;
  }();
  const std::filesystem::path state_path =
      std::filesystem::path(web_ui_root) / "web-ui-state.json";
  const std::string checked_at = runtime_support_service.UtcNowSqlTimestamp();

  if (!std::filesystem::exists(state_path)) {
    return json{
        {"id", "web-ui"},
        {"label", "Web UI"},
        {"kind", "container"},
        {"health", "critical"},
        {"state", "missing"},
        {"detail", "web-ui state file is missing"},
        {"targets",
         json::array({BuildServiceTargetPayload(
             "local",
             "http://127.0.0.1:" + std::to_string(WebUiService::DefaultWebUiPort()),
             std::nullopt)})},
        {"updated_at", nullptr},
    };
  }

  json state = json::object();
  try {
    std::ifstream input(state_path);
    input >> state;
  } catch (const std::exception& error) {
    return json{
        {"id", "web-ui"},
        {"label", "Web UI"},
        {"kind", "container"},
        {"health", "critical"},
        {"state", "error"},
        {"detail", std::string("failed to parse web-ui state: ") + error.what()},
        {"targets", json::array()},
        {"updated_at", checked_at},
    };
  }

  const int listen_port = state.value("listen_port", WebUiService::DefaultWebUiPort());
  const std::string local_upstream =
      "http://127.0.0.1:" + std::to_string(listen_port);
  const std::string controller_upstream = state.value("controller_upstream", std::string());
  const bool materialized = state.value("materialized", false);
  const bool running = state.value("running", false);
  const std::string persisted_status = state.value("status", std::string("unknown"));

  LocalHealthProbeResult probe;
  if (materialized || running || persisted_status == "running") {
    probe = ProbeLocalJsonHealth(local_upstream, "/api/v1/health");
  }
  const bool healthy = ProbeLooksHealthy(probe);

  std::string health = "warning";
  std::string rendered_state = persisted_status;
  std::vector<std::string> details;
  if (!controller_upstream.empty()) {
    details.push_back("controller upstream " + controller_upstream);
  }
  if (running && healthy) {
    health = "healthy";
    rendered_state = "running";
    details.push_back("container responded to health probe");
  } else if ((running || persisted_status == "running") && !healthy) {
    health = "critical";
    rendered_state = "unreachable";
    details.push_back(!probe.error.empty() ? probe.error : "health endpoint probe failed");
  } else if (materialized) {
    health = "warning";
    rendered_state = "materialized";
    details.push_back("compose state exists but container is not running");
  } else {
    health = "warning";
    details.push_back("web-ui is not running");
  }

  json targets = json::array();
  targets.push_back(BuildServiceTargetPayload(
      "local",
      local_upstream,
      probe.attempted ? std::optional<bool>(healthy) : std::nullopt,
      !probe.error.empty() ? std::optional<std::string>(probe.error) : std::nullopt));

  return json{
      {"id", "web-ui"},
      {"label", "Web UI"},
      {"kind", "container"},
      {"health", health},
      {"state", rendered_state},
      {"detail", JoinDetails(details)},
      {"targets", std::move(targets)},
      {"updated_at", checked_at},
  };
}

json BuildSelfServicesPayload(
    naim::ControllerStore& store,
    int stale_after_seconds) {
  json items = json::array();
  items.push_back(BuildControllerSelfServicePayload());
  items.push_back(BuildSkillsFactorySelfServicePayload());
  items.push_back(BuildHostdSelfServicePayload(store, stale_after_seconds));
  items.push_back(BuildWebUiSelfServicePayload());

  int healthy = 0;
  int warning = 0;
  int critical = 0;
  int unknown = 0;
  for (const auto& item : items) {
    const std::string service_health = item.value("health", std::string("unknown"));
    if (service_health == "healthy") {
      ++healthy;
    } else if (service_health == "warning") {
      ++warning;
    } else if (service_health == "critical") {
      ++critical;
    } else {
      ++unknown;
    }
  }

  return json{
      {"summary",
       {
           {"total", items.size()},
           {"healthy", healthy},
           {"warning", warning},
           {"critical", critical},
           {"unknown", unknown},
       }},
      {"items", std::move(items)},
  };
}

}  // namespace

DashboardService::DashboardService(Deps deps) : deps_(std::move(deps)) {}

void DashboardService::AlertSummary::Push(
    const std::string& severity,
    const std::string& kind,
    const std::string& title,
    const std::string& detail,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& event_id) {
  if (severity == "critical") {
    ++critical;
  } else if (severity == "warning") {
    ++warning;
  } else if (severity == "booting") {
    ++booting;
  }
  items.push_back(json{
      {"severity", severity},
      {"kind", kind},
      {"title", title},
      {"detail", detail},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"worker_name", worker_name.has_value() ? json(*worker_name) : json(nullptr)},
      {"assignment_id",
       assignment_id.has_value() ? json(*assignment_id) : json(nullptr)},
      {"event_id", event_id.has_value() ? json(*event_id) : json(nullptr)},
  });
}

json DashboardService::AlertSummary::ToJson() const {
  return json{
      {"critical", critical},
      {"warning", warning},
      {"booting", booting},
      {"total", critical + warning + booting},
      {"items", items},
  };
}

nlohmann::json DashboardService::BuildPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) const {
  if (deps_.state_aggregate_loader == nullptr ||
      !deps_.build_event_payload ||
      !deps_.build_availability_override_map ||
      !deps_.resolve_node_availability ||
      !deps_.heartbeat_age_seconds ||
      !deps_.health_from_age ||
      !deps_.parse_runtime_status ||
      !deps_.parse_gpu_telemetry) {
    throw std::runtime_error("dashboard service dependencies are not configured");
  }

  const auto view = deps_.state_aggregate_loader->LoadStateAggregateViewData(
      db_path,
      stale_after_seconds,
      plane_name);
  naim::ControllerStore store(db_path);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;
  const auto recent_events =
      store.LoadEvents(plane_name, std::nullopt, std::nullopt, std::nullopt, 10);
  const auto rollout_actions =
      plane_name.has_value() ? store.LoadRolloutActions(*plane_name)
                             : store.LoadRolloutActions();

  json payload{
      {"service", "naim-controller"},
      {"db_path", db_path},
      {"stale_after_seconds", stale_after_seconds},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"desired_generation",
       view.desired_generation.has_value() ? json(*view.desired_generation)
                                           : json(nullptr)},
  };
  payload["self_services"] = BuildSelfServicesPayload(store, stale_after_seconds);
  if (!view.desired_state.has_value()) {
    payload["plane"] = nullptr;
    payload["planes"] = json::array();
    payload["nodes"] = json::array();
    payload["runtime"] = {
        {"observed_nodes", 0},
        {"ready_nodes", 0},
        {"not_ready_nodes", 0},
        {"degraded_gpu_telemetry_nodes", 0},
    };
    payload["assignments"] = {
        {"total", 0},
        {"pending", 0},
        {"claimed", 0},
        {"applied", 0},
        {"failed", 0},
        {"by_node", json::array()},
    };
    payload["rollout"] = {
        {"total_actions", 0},
        {"pending", 0},
        {"acknowledged", 0},
        {"ready_to_retry", 0},
        {"workers", json::array()},
    };
    payload["alerts"] = {
        {"critical", 0},
        {"warning", 0},
        {"booting", 0},
        {"total", 0},
        {"items", json::array()},
    };
    payload["recent_events"] = json::array();
    return payload;
  }

  std::map<std::string, naim::HostObservation> observation_by_node;
  for (const auto& observation : view.observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  int effective_plane_applied_generation = 0;
  std::optional<naim::PlaneRecord> dashboard_plane_record;
  for (const auto& plane : view.planes) {
    if (plane.name != view.desired_state->plane_name) {
      continue;
    }
    dashboard_plane_record = plane;
    effective_plane_applied_generation = ComputeEffectivePlaneAppliedGeneration(
        plane,
        view.desired_state,
        view.desired_generation,
        view.observations);
    if (effective_plane_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_plane_applied_generation);
    }
    (void)reconciliation_service.Reconcile(store, plane.name);
    break;
  }
  const auto availability_override_map =
      deps_.build_availability_override_map(view.availability_overrides);

  payload["plane"] = BuildPlanePayload(
      *view.desired_state,
      view.desired_generation,
      dashboard_plane_record,
      effective_plane_applied_generation);
  payload["planes"] = BuildPlanesPayload(
      view.planes,
      view.desired_states,
      view.desired_state->plane_name,
      effective_plane_applied_generation);

  const std::optional<std::string> selected_plane_state =
      ResolveSelectedPlaneState(view.planes, plane_name);
  const auto dashboard_nodes =
      BuildDashboardNodes(plane_name, *view.desired_state, view.desired_states);
  const auto nodes_payload = BuildNodesPayload(
      dashboard_nodes,
      observation_by_node,
      availability_override_map,
      *view.desired_state,
      view.desired_states,
      plane_name,
      selected_plane_state,
      view.desired_generation.value_or(0),
      stale_after_seconds,
      deps_.heartbeat_age_seconds,
      deps_.health_from_age,
      deps_.resolve_node_availability,
      deps_.parse_runtime_status,
      deps_.parse_gpu_telemetry);
  payload["nodes"] = nodes_payload.items;
  payload["runtime"] = BuildRuntimePayload(
      nodes_payload.observed_nodes,
      nodes_payload.ready_nodes,
      nodes_payload.not_ready_nodes,
      nodes_payload.degraded_gpu_nodes,
      nodes_payload.kv_cache_bytes);
  payload["skills"] = PlaneDashboardSkillsSummaryService::BuildPayload(
      *view.desired_state,
      store.LoadPlaneSkillBindings(view.desired_state->plane_name, std::nullopt));
  payload["webgateway"] = PlaneBrowsingService().BuildStatusPayload(
      *view.desired_state,
      selected_plane_state);

  const auto latest_assignments_by_node =
      BuildLatestPlaneAssignmentsByNode(
          plane_name.has_value()
              ? store.LoadHostAssignments(std::nullopt, std::nullopt, *plane_name)
              : store.LoadHostAssignments());
  payload["assignments"] = BuildAssignmentsPayload(latest_assignments_by_node);
  payload["rollout"] = BuildRolloutPayload(
      rollout_actions,
      view.loop_status.state,
      view.loop_status.reason);

  AlertSummary alerts;
  BuildAssignmentAlerts(&alerts, latest_assignments_by_node);
  BuildNodeAlerts(
      &alerts,
      dashboard_nodes,
      observation_by_node,
      availability_override_map,
      *view.desired_state,
      plane_name,
      selected_plane_state,
      view.desired_generation.value_or(0),
      stale_after_seconds,
      deps_.heartbeat_age_seconds,
      deps_.health_from_age,
      deps_.resolve_node_availability,
      deps_.parse_runtime_status,
      deps_.parse_gpu_telemetry);
  BuildRolloutAlerts(&alerts, rollout_actions);
  BuildRecentEventAlerts(&alerts, recent_events);
  payload["alerts"] = alerts.ToJson();
  payload["recent_events"] =
      BuildRecentEventsPayload(recent_events, deps_.build_event_payload);
  return payload;
}

DashboardService::RuntimeFallback DashboardService::DetermineRuntimeFallback(
    const naim::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& plane_state,
    int desired_generation,
    int desired_instance_count,
    int desired_disk_count,
    const std::string& health) {
  RuntimeFallback fallback;
  if (health == "stale" || health == "failed") {
    return fallback;
  }

  const auto observed_runtime =
      SummarizeObservedPlaneRuntime(observation, node_name, plane_name);
  const bool has_applied_generation =
      observation.applied_generation.has_value() &&
      *observation.applied_generation >= desired_generation;
  const bool has_observed_runtime =
      observed_runtime.available || has_applied_generation;
  if (!has_observed_runtime) {
    return fallback;
  }

  fallback.available = true;
  const std::string state = plane_state.value_or("");
  if (state == "stopped") {
    const bool stop_converged =
        has_applied_generation &&
        observed_runtime.instance_count == 0 &&
        (desired_disk_count == 0 || observed_runtime.disk_count >= desired_disk_count);
    fallback.launch_ready = stop_converged;
    fallback.runtime_phase = stop_converged ? "stopped" : "stopping";
    return fallback;
  }

  if (state == "running") {
    const bool start_converged =
        has_applied_generation &&
        (desired_instance_count == 0 ||
         observed_runtime.instance_count >= desired_instance_count);
    fallback.launch_ready = start_converged;
    fallback.runtime_phase = start_converged ? "applied" : "starting";
    return fallback;
  }

  fallback.launch_ready =
      has_applied_generation && observed_runtime.instance_count >= desired_instance_count;
  fallback.runtime_phase = fallback.launch_ready ? "applied" : "pending";
  return fallback;
}

json DashboardService::BuildBootstrapModelPayload(
    const std::optional<naim::BootstrapModelSpec>& bootstrap_model) {
  if (!bootstrap_model.has_value()) {
    return nullptr;
  }
  json item{
      {"model_id", bootstrap_model->model_id},
      {"materialization_mode", bootstrap_model->materialization_mode},
      {"served_model_name",
       bootstrap_model->served_model_name.has_value() ? json(*bootstrap_model->served_model_name)
                                                      : json(nullptr)},
      {"local_path",
       bootstrap_model->local_path.has_value() ? json(*bootstrap_model->local_path)
                                               : json(nullptr)},
      {"source_node_name",
       bootstrap_model->source_node_name.has_value()
           ? json(*bootstrap_model->source_node_name)
           : json(nullptr)},
      {"source_paths", bootstrap_model->source_paths},
      {"source_url",
       bootstrap_model->source_url.has_value() ? json(*bootstrap_model->source_url)
                                               : json(nullptr)},
      {"source_urls", bootstrap_model->source_urls},
      {"target_filename",
       bootstrap_model->target_filename.has_value()
           ? json(*bootstrap_model->target_filename)
           : json(nullptr)},
      {"sha256",
       bootstrap_model->sha256.has_value() ? json(*bootstrap_model->sha256) : json(nullptr)},
  };
  return item;
}

json DashboardService::BuildPlanePayload(
    const naim::DesiredState& desired_state,
    const std::optional<int>& desired_generation,
    const std::optional<naim::PlaneRecord>& plane_record,
    int effective_plane_applied_generation) {
  return json{
      {"plane_name", desired_state.plane_name},
      {"plane_mode", naim::ToString(desired_state.plane_mode)},
      {"state",
       plane_record.has_value() ? json(plane_record->state) : json(nullptr)},
      {"desired_generation",
       desired_generation.has_value() ? json(*desired_generation)
                                      : json(nullptr)},
      {"applied_generation",
       plane_record.has_value() ? json(effective_plane_applied_generation)
                                : json(nullptr)},
      {"staged_update",
       plane_record.has_value()
           ? plane_record->generation > effective_plane_applied_generation
           : false},
      {"node_count", desired_state.nodes.size()},
      {"instance_count", desired_state.instances.size()},
      {"disk_count", desired_state.disks.size()},
      {"shared_disk_name", desired_state.plane_shared_disk_name},
      {"control_root", desired_state.control_root},
      {"placement", PlanePlacementPayloadBuilder(desired_state).Build()},
      {"bootstrap_model", BuildBootstrapModelPayload(desired_state.bootstrap_model)},
  };
}

json DashboardService::BuildPlanesPayload(
    const std::vector<naim::PlaneRecord>& planes,
    const std::vector<naim::DesiredState>& desired_states,
    const std::string& selected_plane_name,
    int effective_plane_applied_generation) {
  json plane_items = json::array();
  for (const auto& plane : planes) {
    const auto desired_state_it = std::find_if(
        desired_states.begin(),
        desired_states.end(),
        [&](const naim::DesiredState& candidate) {
          return candidate.plane_name == plane.name;
        });
    const int plane_applied_generation =
        plane.name == selected_plane_name ? effective_plane_applied_generation
                                          : plane.applied_generation;
    plane_items.push_back(json{
        {"plane_name", plane.name},
        {"plane_mode",
         desired_state_it != desired_states.end()
             ? json(naim::ToString(desired_state_it->plane_mode))
             : json(plane.plane_mode)},
        {"state", plane.state},
        {"generation", plane.generation},
        {"applied_generation", plane_applied_generation},
        {"staged_update", plane.generation > plane_applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"node_count",
         desired_state_it != desired_states.end()
             ? json(desired_state_it->nodes.size())
             : json(nullptr)},
        {"instance_count",
         desired_state_it != desired_states.end()
             ? json(desired_state_it->instances.size())
             : json(nullptr)},
        {"disk_count",
         desired_state_it != desired_states.end()
             ? json(desired_state_it->disks.size())
             : json(nullptr)},
    });
  }
  return plane_items;
}

json DashboardService::BuildAssignmentsPayload(
    const std::map<std::string, naim::HostAssignment>& latest_assignments_by_node) {
  int pending_assignments = 0;
  int claimed_assignments = 0;
  int applied_assignments = 0;
  int failed_assignments = 0;
  json latest_progress = nullptr;
  int latest_progress_assignment_id = -1;
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    switch (assignment.status) {
      case naim::HostAssignmentStatus::Pending:
        ++pending_assignments;
        break;
      case naim::HostAssignmentStatus::Claimed:
        ++claimed_assignments;
        break;
      case naim::HostAssignmentStatus::Applied:
        ++applied_assignments;
        break;
      case naim::HostAssignmentStatus::Failed:
        ++failed_assignments;
        break;
      default:
        break;
    }
    if ((assignment.status == naim::HostAssignmentStatus::Pending ||
         assignment.status == naim::HostAssignmentStatus::Claimed) &&
        !assignment.progress_json.empty() &&
        assignment.progress_json != "{}" &&
        assignment.id > latest_progress_assignment_id) {
      latest_progress = json::parse(assignment.progress_json);
      latest_progress_assignment_id = assignment.id;
    }
  }

  json assignment_nodes = json::array();
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    assignment_nodes.push_back(json{
        {"node_name", assignment.node_name},
        {"latest_assignment_id", assignment.id},
        {"latest_status", naim::ToString(assignment.status)},
        {"latest_progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
        {"pending", assignment.status == naim::HostAssignmentStatus::Pending ? 1 : 0},
        {"claimed", assignment.status == naim::HostAssignmentStatus::Claimed ? 1 : 0},
        {"failed", assignment.status == naim::HostAssignmentStatus::Failed ? 1 : 0},
    });
  }

  return json{
      {"total", latest_assignments_by_node.size()},
      {"pending", pending_assignments},
      {"claimed", claimed_assignments},
      {"applied", applied_assignments},
      {"failed", failed_assignments},
      {"latest_progress", latest_progress},
      {"by_node", std::move(assignment_nodes)},
  };
}

json DashboardService::BuildRolloutPayload(
    const std::vector<naim::RolloutActionRecord>& rollout_actions,
    const std::string& loop_state,
    const std::string& loop_reason) {
  int pending_rollout = 0;
  int acknowledged_rollout = 0;
  int ready_rollout = 0;
  std::set<std::string> rollout_workers;
  for (const auto& action : rollout_actions) {
    rollout_workers.insert(action.worker_name);
    if (action.status == naim::RolloutActionStatus::Pending) {
      ++pending_rollout;
    } else if (action.status == naim::RolloutActionStatus::Acknowledged) {
      ++acknowledged_rollout;
    } else if (action.status == naim::RolloutActionStatus::ReadyToRetry) {
      ++ready_rollout;
    }
  }
  return json{
      {"total_actions", rollout_actions.size()},
      {"pending", pending_rollout},
      {"acknowledged", acknowledged_rollout},
      {"ready_to_retry", ready_rollout},
      {"workers", json(rollout_workers)},
      {"loop_status", loop_state},
      {"loop_reason", loop_reason},
  };
}

json DashboardService::BuildRuntimePayload(
    int observed_nodes,
    int ready_nodes,
    int not_ready_nodes,
    int degraded_gpu_nodes,
    const std::optional<std::uint64_t>& kv_cache_bytes) {
  return json{
      {"observed_nodes", observed_nodes},
      {"ready_nodes", ready_nodes},
      {"not_ready_nodes", not_ready_nodes},
      {"degraded_gpu_telemetry_nodes", degraded_gpu_nodes},
      {"kv_cache_bytes",
       kv_cache_bytes.has_value() ? json(*kv_cache_bytes) : json(nullptr)},
  };
}

json DashboardService::BuildRecentEventsPayload(
    const std::vector<naim::EventRecord>& recent_events,
    const std::function<json(const naim::EventRecord&)>& build_event_payload) {
  json recent_items = json::array();
  for (const auto& event : recent_events) {
    recent_items.push_back(build_event_payload(event));
  }
  return recent_items;
}

std::optional<std::string> DashboardService::ResolveSelectedPlaneState(
    const std::vector<naim::PlaneRecord>& planes,
    const std::optional<std::string>& plane_name) {
  if (!plane_name.has_value()) {
    return std::nullopt;
  }
  for (const auto& plane : planes) {
    if (plane.name == *plane_name) {
      return plane.state;
    }
  }
  return std::nullopt;
}

std::map<std::string, naim::NodeInventory> DashboardService::BuildDashboardNodes(
    const std::optional<std::string>& plane_name,
    const naim::DesiredState& desired_state,
    const std::vector<naim::DesiredState>& desired_states) {
  std::map<std::string, naim::NodeInventory> dashboard_nodes;
  if (plane_name.has_value()) {
    for (const auto& node : desired_state.nodes) {
      dashboard_nodes.emplace(node.name, node);
    }
    return dashboard_nodes;
  }

  for (const auto& state : desired_states) {
    for (const auto& node : state.nodes) {
      dashboard_nodes.emplace(node.name, node);
    }
  }
  return dashboard_nodes;
}

DashboardService::NodesPayload DashboardService::BuildNodesPayload(
    const std::map<std::string, naim::NodeInventory>& dashboard_nodes,
    const std::map<std::string, naim::HostObservation>& observation_by_node,
    const std::map<std::string, naim::NodeAvailabilityOverride>&
        availability_override_map,
    const naim::DesiredState& desired_state,
    const std::vector<naim::DesiredState>& desired_states,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& selected_plane_state,
    int desired_generation,
    int stale_after_seconds,
    const std::function<std::optional<long long>(const std::string&)>&
        heartbeat_age_seconds,
    const std::function<std::string(
        const std::optional<long long>&,
        int)>& health_from_age,
    const std::function<naim::NodeAvailability(
        const std::map<std::string, naim::NodeAvailabilityOverride>&,
        const std::string&)>& resolve_node_availability,
    const std::function<std::optional<naim::RuntimeStatus>(
        const naim::HostObservation&)>& parse_runtime_status,
    const std::function<std::optional<naim::GpuTelemetrySnapshot>(
        const naim::HostObservation&)>& parse_gpu_telemetry) {
  NodesPayload payload;
  for (const auto& [dashboard_node_name, node] : dashboard_nodes) {
    (void)dashboard_node_name;
    const auto demand =
        SummarizeNodeDemand(node.name, plane_name, desired_state, desired_states);

    json item{
        {"node_name", node.name},
        {"availability",
         naim::ToString(resolve_node_availability(availability_override_map, node.name))},
        {"plane_count", static_cast<int>(demand.plane_names.size())},
        {"planes", json(demand.plane_names)},
        {"desired_instance_count", demand.desired_instance_count},
        {"desired_disk_count", demand.desired_disk_count},
        {"gpu_count", node.gpu_devices.size()},
    };
    const auto observation_it = observation_by_node.find(node.name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      item["runtime_launch_ready"] = nullptr;
      item["runtime_phase"] = nullptr;
      payload.items.push_back(std::move(item));
      continue;
    }

    ++payload.observed_nodes;
    const auto age_seconds =
        heartbeat_age_seconds(observation_it->second.heartbeat_at);
    item["health"] = health_from_age(age_seconds, stale_after_seconds);
    item["status"] = naim::ToString(observation_it->second.status);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status = SelectPlaneRuntimeStatus(
            parse_runtime_status(observation_it->second), plane_name);
        runtime_status.has_value()) {
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_phase"] =
          runtime_status->runtime_phase.empty() ? json(nullptr)
                                                : json(runtime_status->runtime_phase);
      item["runtime_backend"] =
          runtime_status->runtime_backend.empty()
              ? json(nullptr)
              : json(runtime_status->runtime_backend);
      item["kv_cache_bytes"] =
          runtime_status->kv_cache_bytes.has_value()
              ? json(*runtime_status->kv_cache_bytes)
              : json(nullptr);
      if (runtime_status->kv_cache_bytes.has_value()) {
        payload.kv_cache_bytes =
            payload.kv_cache_bytes.value_or(0) + *runtime_status->kv_cache_bytes;
      }
      if (runtime_status->launch_ready) {
        ++payload.ready_nodes;
      } else {
        ++payload.not_ready_nodes;
      }
    } else {
      const auto fallback = DetermineRuntimeFallback(
          observation_it->second,
          node.name,
          plane_name,
          selected_plane_state,
          desired_generation,
          demand.desired_instance_count,
          demand.desired_disk_count,
          item.value("health", std::string("unknown")));
      if (fallback.available) {
        item["runtime_launch_ready"] = fallback.launch_ready;
        item["runtime_phase"] =
            fallback.runtime_phase.empty() ? json(nullptr)
                                           : json(fallback.runtime_phase);
        item["kv_cache_bytes"] = nullptr;
        if (fallback.launch_ready) {
          ++payload.ready_nodes;
        } else {
          ++payload.not_ready_nodes;
        }
      } else {
        item["runtime_launch_ready"] = nullptr;
        item["runtime_phase"] = nullptr;
        item["kv_cache_bytes"] = nullptr;
        ++payload.not_ready_nodes;
      }
    }
    if (const auto gpu_telemetry = parse_gpu_telemetry(observation_it->second);
        gpu_telemetry.has_value()) {
      item["telemetry_degraded"] = gpu_telemetry->degraded;
      if (gpu_telemetry->degraded) {
        ++payload.degraded_gpu_nodes;
      }
    }
    payload.items.push_back(std::move(item));
  }

  return payload;
}

void DashboardService::BuildAssignmentAlerts(
    AlertSummary* alerts,
    const std::map<std::string, naim::HostAssignment>&
        latest_assignments_by_node) {
  if (alerts == nullptr) {
    return;
  }
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    if (assignment.status == naim::HostAssignmentStatus::Failed) {
      alerts->Push(
          "critical",
          "failed-assignment",
          "Assignment failed",
          "Host assignment failed and requires retry or operator action.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    } else if (
        assignment.status == naim::HostAssignmentStatus::Pending ||
        assignment.status == naim::HostAssignmentStatus::Claimed) {
      alerts->Push(
          "booting",
          "assignment-in-flight",
          "Assignment in progress",
          "Host assignment is still pending or claimed.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    }
  }
}

void DashboardService::BuildNodeAlerts(
    AlertSummary* alerts,
    const std::map<std::string, naim::NodeInventory>& dashboard_nodes,
    const std::map<std::string, naim::HostObservation>& observation_by_node,
    const std::map<std::string, naim::NodeAvailabilityOverride>&
        availability_override_map,
    const naim::DesiredState& desired_state,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& selected_plane_state,
    int desired_generation,
    int stale_after_seconds,
    const std::function<std::optional<long long>(const std::string&)>&
        heartbeat_age_seconds,
    const std::function<std::string(
        const std::optional<long long>&,
        int)>& health_from_age,
    const std::function<naim::NodeAvailability(
        const std::map<std::string, naim::NodeAvailabilityOverride>&,
        const std::string&)>& resolve_node_availability,
    const std::function<std::optional<naim::RuntimeStatus>(
        const naim::HostObservation&)>& parse_runtime_status,
    const std::function<std::optional<naim::GpuTelemetrySnapshot>(
        const naim::HostObservation&)>& parse_gpu_telemetry) {
  if (alerts == nullptr) {
    return;
  }
  for (const auto& [dashboard_node_name, item] : dashboard_nodes) {
    (void)dashboard_node_name;
    const std::string node_name = item.name;
    const auto observation_it = observation_by_node.find(item.name);
    if (observation_it == observation_by_node.end()) {
      alerts->Push(
          "warning",
          "missing-observation",
          "Node has no observation",
          "Controller does not have a recent observation for this node.",
          node_name);
      continue;
    }

    const auto age_seconds =
        heartbeat_age_seconds(observation_it->second.heartbeat_at);
    const std::string health =
        health_from_age(age_seconds, stale_after_seconds);
    if (health == "failed" || health == "stale") {
      alerts->Push(
          "critical",
          "node-health",
          "Node heartbeat is stale",
          "Observed state for this node is stale or failed.",
          node_name);
    }

    const auto availability =
        resolve_node_availability(availability_override_map, node_name);
    if (availability != naim::NodeAvailability::Active) {
      alerts->Push(
          "warning",
          "node-availability",
          "Node is not active",
          "Node availability override is blocking normal scheduling.",
          node_name);
    }

    if (const auto runtime_status = SelectPlaneRuntimeStatus(
            parse_runtime_status(observation_it->second), plane_name);
        runtime_status.has_value()) {
      if (!runtime_status->launch_ready) {
        alerts->Push(
            "booting",
            "runtime-not-ready",
            "Runtime still starting",
            "Node runtime is not launch-ready yet.",
            node_name);
      }
    } else {
      const auto fallback = DetermineRuntimeFallback(
          observation_it->second,
          node_name,
          plane_name,
          selected_plane_state,
          desired_generation,
          std::count_if(
              desired_state.instances.begin(),
              desired_state.instances.end(),
              [&](const auto& instance) {
                return instance.node_name == node_name;
              }),
          std::count_if(
              desired_state.disks.begin(),
              desired_state.disks.end(),
              [&](const auto& disk) { return disk.node_name == node_name; }),
          health);
      if (!fallback.available) {
        alerts->Push(
            "booting",
            "runtime-missing",
            "Runtime status missing",
            "No runtime status has been reported yet for this node.",
            node_name);
      } else if (!fallback.launch_ready) {
        alerts->Push(
            "booting",
            "runtime-transition",
            "Runtime transition in progress",
            "Observed runtime state is converging even though low-level runtime status is not available yet.",
            item.name);
      }
    }

    if (const auto gpu_telemetry = parse_gpu_telemetry(observation_it->second);
        gpu_telemetry.has_value() && gpu_telemetry->degraded) {
      alerts->Push(
          "warning",
          "gpu-telemetry-degraded",
          "GPU telemetry degraded",
          "GPU telemetry is running in degraded mode on this node.",
          item.name);
    }
  }
}

void DashboardService::BuildRolloutAlerts(
    AlertSummary* alerts,
    const std::vector<naim::RolloutActionRecord>& rollout_actions) {
  if (alerts == nullptr) {
    return;
  }
  for (const auto& action : rollout_actions) {
    alerts->Push(
        "warning",
        "rollout-action",
        "Deferred rollout requires follow-up",
        action.action + " for worker " + action.worker_name,
        action.target_node_name.empty() ? std::nullopt
                                        : std::optional<std::string>(
                                              action.target_node_name),
        action.worker_name);
  }
}

void DashboardService::BuildRecentEventAlerts(
    AlertSummary* alerts,
    const std::vector<naim::EventRecord>& recent_events) {
  if (alerts == nullptr) {
    return;
  }
  int surfaced_event_alerts = 0;
  for (const auto& event : recent_events) {
    if ((event.severity == "error" || event.severity == "warning") &&
        surfaced_event_alerts < 5) {
      alerts->Push(
          event.severity == "error" ? "critical" : "warning",
          "event-log",
          event.category + "." + event.event_type,
          event.message,
          event.node_name.empty() ? std::nullopt
                                  : std::optional<std::string>(event.node_name),
          event.worker_name.empty()
              ? std::nullopt
              : std::optional<std::string>(event.worker_name),
          std::nullopt,
          event.id);
      ++surfaced_event_alerts;
    }
  }
}

}  // namespace naim::controller
