#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "plane/plane_desired_state_request_parser.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void ExpectInvalid(const json& body, const std::string& name) {
  PlaneDesiredStateRequestParser parser;
  try {
    static_cast<void>(parser.ParseUpsertRequestBody(body));
  } catch (const std::exception&) {
    std::cout << "ok-invalid: " << name << '\n';
    return;
  }
  throw std::runtime_error("expected invalid parser scenario '" + name + "'");
}

}  // namespace

int main() {
  try {
    PlaneDesiredStateRequestParser parser;

    {
      const auto parsed = parser.ParseUpsertRequestBody(json{
          {"plane_name", "alpha"},
          {"plane_mode", "llm"},
      });
      Expect(parsed.source_label == "api", "raw expanded body should map to api");
      Expect(parsed.desired_state_payload.at("plane_name").get<std::string>() == "alpha",
             "raw expanded body should preserve plane_name");
      std::cout << "ok: raw-expanded-body" << '\n';
    }

    {
      const auto parsed = parser.ParseUpsertRequestBody(json{
          {"desired_state_v2",
           {
               {"version", 2},
               {"plane_name", "beta"},
               {"plane_mode", "compute"},
               {"runtime", {{"engine", "custom"}, {"workers", 1}}},
           }},
          {"artifacts_root", "var/artifacts"},
      });
      Expect(parsed.source_label == "api/v2", "desired_state_v2 wrapper should map to api/v2");
      Expect(parsed.desired_state_payload.value("version", 0) == 2,
             "desired_state_v2 wrapper should preserve version");
      std::cout << "ok: desired-state-v2-wrapper" << '\n';
    }

    {
      const auto parsed = parser.ParseUpsertRequestBody(json{
          {"version", 2},
          {"plane_name", "gamma"},
          {"plane_mode", "llm"},
          {"runtime", {{"engine", "vllm"}, {"workers", 1}}},
          {"artifacts_root", "var/artifacts"},
      });
      Expect(parsed.source_label == "api/v2", "raw v2 body should map to api/v2");
      Expect(!parsed.desired_state_payload.contains("artifacts_root"),
             "raw v2 body should strip artifacts_root envelope field");
      std::cout << "ok: raw-v2-body-with-envelope-field" << '\n';
    }

    ExpectInvalid(
        json{
            {"desired_state", {{"plane_name", "alpha"}}},
            {"desired_state_v2", {{"version", 2}, {"plane_name", "beta"}}},
        },
        "mixed-wrappers");

    ExpectInvalid(
        json{
            {"desired_state_v2", json::array()},
        },
        "non-object-v2-payload");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "plane_desired_state_request_parser_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
