#include "interaction/interaction_request_contract_support.h"
#include "interaction/interaction_service.h"

namespace naim::controller {

nlohmann::json InteractionContractResponder::BuildStandaloneErrorPayload(
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& reason,
    const std::optional<std::string>& served_model_name,
    const std::optional<std::string>& active_model_id,
    const nlohmann::json& details) const {
  nlohmann::json payload{
      {"request_id", request_id},
      {"plane_name", plane_name.has_value() ? nlohmann::json(*plane_name)
                                            : nlohmann::json(nullptr)},
      {"reason", reason.has_value() ? nlohmann::json(*reason)
                                    : nlohmann::json(nullptr)},
      {"served_model_name",
       served_model_name.has_value() ? nlohmann::json(*served_model_name)
                                     : nlohmann::json(nullptr)},
      {"active_model_id",
       active_model_id.has_value() ? nlohmann::json(*active_model_id)
                                   : nlohmann::json(nullptr)},
      {"error",
       nlohmann::json{
           {"code", code},
           {"message", message},
           {"retryable", retryable},
       }},
      {"naim",
       nlohmann::json{
           {"request_id", request_id},
           {"plane_name", plane_name.has_value() ? nlohmann::json(*plane_name)
                                                 : nlohmann::json(nullptr)},
           {"served_model_name",
            served_model_name.has_value() ? nlohmann::json(*served_model_name)
                                          : nlohmann::json(nullptr)},
           {"active_model_id",
            active_model_id.has_value() ? nlohmann::json(*active_model_id)
                                        : nlohmann::json(nullptr)},
       }},
  };
  if (!details.empty()) {
    payload["error"]["details"] = details;
  }
  return payload;
}

nlohmann::json InteractionContractResponder::BuildPlaneErrorPayload(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const nlohmann::json& details) const {
  nlohmann::json payload = resolution.status_payload;
  payload["request_id"] = request_id;
  payload["error"] = nlohmann::json{
      {"code", code},
      {"message", message},
      {"retryable", retryable},
  };
  payload["naim"] =
      InteractionRequestContractSupport{}.BuildInteractionContractMetadata(
          resolution, request_id);
  if (!details.empty()) {
    payload["error"]["details"] = details;
  }
  return payload;
}

}  // namespace naim::controller
