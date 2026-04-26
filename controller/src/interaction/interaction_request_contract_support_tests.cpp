#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_request_contract_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestParsesTrimmedPublicSessionId() {
  const naim::controller::InteractionRequestContractSupport support;
  const nlohmann::json payload{{"session_id", "  sess-123  "}};
  const auto session_id = support.ParsePublicSessionId(payload);
  Expect(session_id.has_value(), "support should parse session_id");
  Expect(*session_id == "sess-123", "support should trim session_id");
  std::cout << "ok: interaction-request-contract-parses-session-id" << '\n';
}

void TestDetectsUnsupportedField() {
  const naim::controller::InteractionRequestContractSupport support;
  const nlohmann::json payload{{"messages", nlohmann::json::array()}, {"tools", nlohmann::json::array()}};
  std::string field_name;
  Expect(
      support.PayloadContainsUnsupportedInteractionField(payload, &field_name),
      "support should detect unsupported tool fields");
  Expect(field_name == "tools", "support should report offending field name");
  std::cout << "ok: interaction-request-contract-detects-unsupported-field" << '\n';
}

void TestBuildsDefaultBrowsingSummary() {
  const naim::controller::InteractionRequestContractSupport support;
  naim::controller::InteractionRequestContext context;
  const auto browsing = support.RequestBrowsingSummary(context);
  Expect(browsing.value("mode", std::string{}) == "disabled",
         "default browsing summary should disable browsing");
  Expect(
      browsing.at("indicator").value("compact", std::string{}) == "web:off",
      "default browsing summary should expose compact disabled indicator");
  std::cout << "ok: interaction-request-contract-default-browsing-summary" << '\n';
}

void TestParsesStreamPlaneNameAndHeaders() {
  const naim::controller::InteractionRequestContractSupport support;
  const auto plane_name = support.ParseInteractionStreamPlaneName(
      "POST",
      "/api/v1/planes/demo-plane/interaction/chat/completions/stream");
  Expect(plane_name.has_value() && *plane_name == "demo-plane",
         "support should parse stream plane name");
  const auto headers = support.BuildInteractionResponseHeaders("req-1");
  Expect(headers.at("X-Naim-Request-Id") == "req-1",
         "support should build request id response header");
  std::cout << "ok: interaction-request-contract-parses-stream-path" << '\n';
}

void TestBuildsContractMetadataPreferringRuntimeIdentity() {
  const naim::controller::InteractionRequestContractSupport support;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = {
      {"plane_name", "demo-plane"},
      {"reason", "ready"},
      {"served_model_name", "status-served"},
      {"active_model_id", "status-active"},
  };
  naim::RuntimeStatus runtime_status;
  runtime_status.active_served_model_name = "runtime-served";
  runtime_status.active_model_id = "runtime-active";
  resolution.runtime_status = runtime_status;
  resolution.desired_state.interaction = naim::InteractionSettings{};
  naim::InteractionSettings::CompletionPolicy completion_policy;
  completion_policy.response_mode = "normal";
  completion_policy.max_tokens = 512;
  resolution.desired_state.interaction->completion_policy = completion_policy;
  resolution.desired_state.interaction->thinking_enabled = true;

  const auto metadata = support.BuildInteractionContractMetadata(
      resolution,
      "req-1",
      std::string("sess-1"),
      3,
      2);
  Expect(metadata.at("served_model_name").get<std::string>() == "runtime-served",
         "metadata should prefer runtime served model");
  Expect(metadata.at("active_model_id").get<std::string>() == "runtime-active",
         "metadata should prefer runtime active model");
  Expect(metadata.at("segment_count").get<int>() == 3,
         "metadata should include segment count");
  Expect(metadata.at("completion_policy").at("thinking_enabled").get<bool>(),
         "metadata should include thinking flag");
  std::cout << "ok: interaction-request-contract-builds-metadata" << '\n';
}

void TestBuildsHostdProxyTransportMetadata() {
  const naim::controller::InteractionRequestContractSupport support;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = {
      {"plane_name", "demo-plane"},
      {"reason", "ready"},
  };
  resolution.target = naim::controller::ControllerEndpointTarget{
      "http://127.0.0.1:53156",
      "127.0.0.1",
      53156,
      "",
      "hpc1",
      true,
      "hostd-runtime-proxy",
  };

  const auto metadata = support.BuildInteractionContractMetadata(resolution, "req-2");
  const auto transport = metadata.at("transport");
  Expect(
      transport.value("mode", std::string{}) == "hostd-runtime-proxy",
      "metadata should expose hostd runtime proxy mode");
  Expect(
      transport.value("supports_hostd_proxy", false),
      "metadata should expose hostd proxy support");
  Expect(
      !transport.value("supports_direct_routing", true),
      "metadata should not claim direct routing for remote loopback proxy");
  Expect(
      !transport.value("supports_sse", true),
      "metadata should not claim upstream SSE support for assignment proxy");
  Expect(
      transport.value("node_name", std::string{}) == "hpc1",
      "metadata should expose proxy node");
  std::cout << "ok: interaction-request-contract-hostd-proxy-transport" << '\n';
}

}  // namespace

int main() {
  try {
    TestParsesTrimmedPublicSessionId();
    TestDetectsUnsupportedField();
    TestBuildsDefaultBrowsingSummary();
    TestParsesStreamPlaneNameAndHeaders();
    TestBuildsContractMetadataPreferringRuntimeIdentity();
    TestBuildsHostdProxyTransportMetadata();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_request_contract_support_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
