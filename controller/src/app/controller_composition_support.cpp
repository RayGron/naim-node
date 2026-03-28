#include "app/controller_composition_support.h"

#include "app/controller_request_context.h"

namespace comet::controller::composition_support {

namespace {

using nlohmann::json;

std::string SerializeEventPayload(const json& payload) {
  return payload.dump();
}

bool ObservationBlocksPlaneDeletion(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (!ObservationMatchesPlane(observation, plane_name)) {
    return false;
  }
  if (observation.status != comet::HostObservationStatus::Idle) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }
  try {
    const auto observed_state =
        comet::DeserializeDesiredStateJson(observation.observed_state_json);
    for (const auto& disk : observed_state.disks) {
      if (disk.plane_name == plane_name) {
        return true;
      }
    }
    for (const auto& instance : observed_state.instances) {
      if (instance.plane_name == plane_name) {
        return true;
      }
    }
    return false;
  } catch (const std::exception&) {
    return true;
  }
}

bool HasBlockingPlaneObservations(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  return std::any_of(
      observations.begin(),
      observations.end(),
      [&](const auto& observation) {
        return ObservationBlocksPlaneDeletion(observation, plane_name);
      });
}

}  // namespace

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key) {
  const auto value = FindQueryString(request, key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) {
  store.AppendEvent(comet::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializeEventPayload(payload),
      "",
  });
}

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation) {
  if (observation.instance_runtime_json.empty()) {
    return {};
  }
  return comet::DeserializeRuntimeStatusListJson(observation.instance_runtime_json);
}

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (observation.plane_name == plane_name) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }

  const auto observed_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  if (observed_state.plane_name == plane_name) {
    return true;
  }
  for (const auto& disk : observed_state.disks) {
    if (disk.plane_name == plane_name) {
      return true;
    }
  }
  for (const auto& instance : observed_state.instances) {
    if (instance.plane_name == plane_name) {
      return true;
    }
  }
  try {
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    for (const auto& status : instance_statuses) {
      const std::string worker_prefix = "worker-" + plane_name + "-";
      if (status.instance_name == "infer-" + plane_name ||
          status.instance_name == "worker-" + plane_name ||
          status.instance_name.rfind(worker_prefix, 0) == 0) {
        return true;
      }
    }
  } catch (const std::exception&) {
  }
  return false;
}

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  std::vector<comet::HostObservation> result;
  for (const auto& observation : observations) {
    if (ObservationMatchesPlane(observation, plane_name)) {
      result.push_back(observation);
    }
  }
  return result;
}

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name) {
  const auto pending_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Pending, plane_name);
  const auto claimed_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Claimed, plane_name);
  if (!pending_assignments.empty() || !claimed_assignments.empty()) {
    return false;
  }
  return !HasBlockingPlaneObservations(store.LoadHostObservations(), plane_name);
}

HttpResponse BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) {
  json enriched = payload;
  if (enriched.is_object()) {
    if (!enriched.contains("api_version")) {
      enriched["api_version"] = "v1";
    }
    const HttpRequest* current_request = comet::controller::ControllerRequestContext::Current();
    if (current_request != nullptr && !enriched.contains("request")) {
      enriched["request"] = {
          {"path", current_request->path},
          {"method", current_request->method},
      };
    }
    if (status_code >= 400) {
      if (!enriched.contains("error") || !enriched.at("error").is_object()) {
        json error{
            {"code", enriched.value("status", "error")},
            {"message",
             enriched.value(
                 "message",
                 ControllerNetworkManager::ReasonPhrase(status_code))},
        };
        if (enriched.contains("details")) {
          error["details"] = enriched["details"];
        }
        enriched["error"] = error;
      } else {
        if (!enriched["error"].contains("code")) {
          enriched["error"]["code"] = enriched.value("status", "error");
        }
        if (!enriched["error"].contains("message")) {
          enriched["error"]["message"] = enriched.value(
              "message",
              ControllerNetworkManager::ReasonPhrase(status_code));
        }
      }
      enriched["status"] = "error";
      enriched.erase("message");
      enriched.erase("details");
      enriched.erase("path");
      enriched.erase("method");
    }
  }
  return HttpResponse{status_code, "application/json", enriched.dump(), headers};
}

}  // namespace comet::controller::composition_support
