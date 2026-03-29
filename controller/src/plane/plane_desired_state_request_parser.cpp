#include "plane/plane_desired_state_request_parser.h"

#include <stdexcept>

PlaneDesiredStateRequestParser::ParsedUpsertRequest
PlaneDesiredStateRequestParser::ParseUpsertRequestBody(const nlohmann::json& body) const {
  if (!body.is_object()) {
    throw std::invalid_argument("request body must be a JSON object");
  }

  const bool has_desired_state = body.contains("desired_state");
  const bool has_desired_state_v2 = body.contains("desired_state_v2");
  if (has_desired_state && has_desired_state_v2) {
    throw std::invalid_argument(
        "request body must contain only one of desired_state or desired_state_v2");
  }

  nlohmann::json payload = has_desired_state_v2   ? body.at("desired_state_v2")
                          : has_desired_state    ? body.at("desired_state")
                                                 : StripEnvelopeFields(body);
  if (!payload.is_object()) {
    throw std::invalid_argument("desired state payload must be a JSON object");
  }

  const std::string source_label =
      has_desired_state_v2 || IsDesiredStateV2(payload) ? "api/v2" : "api";
  return ParsedUpsertRequest{
      std::move(payload),
      source_label,
  };
}

bool PlaneDesiredStateRequestParser::IsDesiredStateV2(const nlohmann::json& value) const {
  return value.is_object() && value.value("version", 0) == 2;
}

nlohmann::json PlaneDesiredStateRequestParser::StripEnvelopeFields(
    const nlohmann::json& body) const {
  nlohmann::json payload = body;
  payload.erase("artifacts_root");
  return payload;
}
