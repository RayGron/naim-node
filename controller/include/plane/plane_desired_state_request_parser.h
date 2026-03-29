#pragma once

#include <string>

#include <nlohmann/json.hpp>

class PlaneDesiredStateRequestParser {
 public:
  struct ParsedUpsertRequest {
    nlohmann::json desired_state_payload;
    std::string source_label;
  };

  ParsedUpsertRequest ParseUpsertRequestBody(const nlohmann::json& body) const;

 private:
  bool IsDesiredStateV2(const nlohmann::json& value) const;
  nlohmann::json StripEnvelopeFields(const nlohmann::json& body) const;
};
