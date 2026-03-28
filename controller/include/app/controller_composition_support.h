#pragma once

#include "app/controller_main_includes.h"

namespace comet::controller::composition_support {

std::string Trim(const std::string& value);

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key);

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key);

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload = nlohmann::json::object(),
    const std::string& plane_name = "",
    const std::string& node_name = "",
    const std::string& worker_name = "",
    const std::optional<int>& assignment_id = std::nullopt,
    const std::optional<int>& rollout_action_id = std::nullopt,
    const std::string& severity = "info");

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name);

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name);

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name);

HttpResponse BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers = {});

}  // namespace comet::controller::composition_support
