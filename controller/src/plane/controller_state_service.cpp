#include "plane/controller_state_service.h"

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "naim/state/sqlite_store.h"
#include "naim/state/state_json.h"

using nlohmann::json;

namespace naim::controller {

ControllerStateService::ControllerStateService(Deps deps) : deps_(std::move(deps)) {}

json ControllerStateService::BuildPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name) const {
  json payload{
      {"service", "naim-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"desired_state_contract",
       {
           {"preferred_create_contract_version", 2},
           {"accepted_create_contract_versions", json::array({1, 2})},
           {"stored_state_format", "v2"},
           {"returned_state_format", "expanded-rendered"},
       }},
  };

  naim::ControllerStore store(db_path);
  store.Initialize();

  if (plane_name.has_value()) {
    plane_deletion_support::FinalizeDeletedPlaneIfReady(
        store,
        *plane_name,
        deps_.can_finalize_deleted_plane,
        deps_.event_appender);
  } else {
    plane_deletion_support::FinalizeDeletedPlanesIfReady(
        store,
        deps_.can_finalize_deleted_plane,
        deps_.event_appender);
  }

  const auto planes = store.LoadPlanes();
  const auto generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  const auto desired_states =
      plane_name.has_value() ? std::vector<naim::DesiredState>{}
                             : store.LoadDesiredStates();

  json plane_items = json::array();
  for (const auto& plane : planes) {
    plane_items.push_back(json{
        {"name", plane.name},
        {"plane_mode", plane.plane_mode},
        {"generation", plane.generation},
        {"applied_generation", plane.applied_generation},
        {"staged_update", plane.generation > plane.applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"state", plane.state},
        {"created_at", plane.created_at},
    });
  }

  payload["desired_generation"] =
      generation.has_value() ? json(*generation) : json(nullptr);
  payload["planes"] = std::move(plane_items);

  if (plane_name.has_value()) {
    payload["desired_states"] = json::array();
  } else {
    json desired_state_items = json::array();
    json desired_state_v2_items = json::array();
    for (const auto& state : desired_states) {
      desired_state_items.push_back(
          json::parse(naim::SerializeDesiredStateJson(state)));
      desired_state_v2_items.push_back(
          json::parse(naim::SerializeDesiredStateV2Json(state)));
    }
    payload["desired_states"] = std::move(desired_state_items);
    payload["desired_states_v2"] = std::move(desired_state_v2_items);
  }
  if (plane_name.has_value()) {
    payload["desired_states_v2"] = json::array();
  }

  payload["desired_state"] =
      desired_state.has_value()
          ? json::parse(naim::SerializeDesiredStateJson(*desired_state))
          : json(nullptr);
  payload["desired_state_v2"] =
      desired_state.has_value()
          ? json::parse(naim::SerializeDesiredStateV2Json(*desired_state))
          : json(nullptr);
  return payload;
}

}  // namespace naim::controller
