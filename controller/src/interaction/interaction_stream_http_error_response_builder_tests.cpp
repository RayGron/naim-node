#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_stream_http_error_response_builder.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestBuildStandaloneErrorResponse() {
  const naim::controller::InteractionStreamHttpErrorResponseBuilder builder;
  const ::HttpResponse response = builder.Build(
      404,
      "req-1",
      "plane_not_found",
      "plane not found for interaction stream path",
      false);
  const auto payload = nlohmann::json::parse(response.body);
  Expect(response.status_code == 404, "standalone error should preserve status code");
  Expect(payload.at("request_id").get<std::string>() == "req-1",
         "standalone error should preserve request id");
  Expect(payload.at("error").at("code").get<std::string>() == "plane_not_found",
         "standalone error should preserve error code");
  std::cout << "ok: interaction-stream-error-response-standalone" << '\n';
}

void TestBuildPlaneErrorResponse() {
  const naim::controller::InteractionStreamHttpErrorResponseBuilder builder;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = {
      {"plane_name", "demo-plane"},
      {"served_model_name", "served"},
      {"active_model_id", "model"},
      {"reason", "not_ready"},
  };

  const ::HttpResponse response = builder.Build(
      409,
      "req-2",
      "plane_not_ready",
      "plane interaction target is not ready",
      true,
      std::string("demo-plane"),
      resolution,
      nlohmann::json{{"detail", "missing target"}});
  const auto payload = nlohmann::json::parse(response.body);
  Expect(response.status_code == 409, "plane error should preserve status code");
  Expect(payload.at("plane_name").get<std::string>() == "demo-plane",
         "plane error should preserve plane name");
  Expect(payload.at("error").at("code").get<std::string>() == "plane_not_ready",
         "plane error should preserve error code");
  Expect(payload.at("error").at("details").at("detail").get<std::string>() == "missing target",
         "plane error should preserve details");
  std::cout << "ok: interaction-stream-error-response-plane" << '\n';
}

}  // namespace

int main() {
  try {
    TestBuildStandaloneErrorResponse();
    TestBuildPlaneErrorResponse();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_stream_http_error_response_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
