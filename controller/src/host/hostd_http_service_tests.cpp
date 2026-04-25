#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "host/hostd_http_service.h"
#include "host/hostd_http_support.h"
#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_store.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "naim-hostd-http-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

naim::controller::HostRegistryEventSink TestEventSink() {
  return [](naim::ControllerStore& store,
            const std::string& event_type,
            const std::string& message,
            const json& payload,
            const std::string& node_name,
            const std::string& severity) {
    store.AppendEvent(naim::EventRecord{
        0,
        "",
        node_name,
        "",
        std::nullopt,
        std::nullopt,
        "host-registry",
        event_type,
        severity,
        message,
        payload.dump(),
        "",
    });
  };
}

HttpResponse Send(
    const HostdHttpService& service,
    const std::string& db_path,
    HttpRequest request) {
  const auto response = service.HandleRequest(db_path, request);
  Expect(response.has_value(), "hostd service should handle request " + request.path);
  return *response;
}

std::string OpenSession(
    const HostdHttpService& service,
    const std::string& db_path,
    const std::string& node_name,
    const std::string& private_key_base64,
    const std::string& status_message) {
  const std::string nonce = naim::RandomTokenBase64(24);
  const std::string timestamp =
      std::to_string(std::chrono::system_clock::to_time_t(
          std::chrono::system_clock::now()));
  const std::string signed_message =
      "hostd-session-open\n" + node_name + "\n" + timestamp + "\n" + nonce;
  HttpRequest request;
  request.method = "POST";
  request.path = "/api/v1/hostd/session/open";
  request.body =
      json{
          {"node_name", node_name},
          {"timestamp", timestamp},
          {"nonce", nonce},
          {"signature", naim::SignDetachedBase64(signed_message, private_key_base64)},
          {"status_message", status_message},
      }.dump();
  const auto response = Send(service, db_path, request);
  Expect(response.status_code == 200, "session open should succeed");
  const auto payload = json::parse(response.body);
  const std::string token = payload.value("session_token", std::string{});
  Expect(!token.empty(), "session open should return token");
  return token;
}

HttpRequest EncryptedHostRequest(
    const std::string& path,
    const std::string& node_name,
    const std::string& session_token,
    const std::string& message_type,
    std::uint64_t sequence_number,
    const json& payload) {
  const auto envelope = naim::EncryptEnvelopeBase64(
      payload.dump(),
      session_token,
      "request\n" + message_type + "\n" + node_name + "\n" +
          std::to_string(sequence_number));
  HttpRequest request;
  request.method = "POST";
  request.path = path;
  request.headers["x-naim-host-session"] = session_token;
  request.headers["x-naim-host-node"] = node_name;
  request.body =
      json{
          {"encrypted", true},
          {"sequence_number", sequence_number},
          {"nonce", envelope.nonce_base64},
          {"ciphertext", envelope.ciphertext_base64},
      }.dump();
  return request;
}

json DecryptResponse(
    const HttpResponse& response,
    const std::string& node_name,
    const std::string& session_token,
    const std::string& message_type) {
  Expect(response.status_code == 200, "encrypted response should be HTTP 200");
  const auto payload = json::parse(response.body);
  Expect(payload.value("encrypted", false), "response should be encrypted");
  const auto sequence_number =
      payload.value("sequence_number", static_cast<std::uint64_t>(0));
  const naim::EncryptedEnvelope envelope{
      payload.value("nonce", std::string{}),
      payload.value("ciphertext", std::string{}),
  };
  const std::string decrypted = naim::DecryptEnvelopeBase64(
      envelope,
      session_token,
      "response\n" + message_type + "\n" + node_name + "\n" +
          std::to_string(sequence_number));
  return json::parse(decrypted);
}

void TestLongPollResponseUsesOriginalSessionToken() {
  const std::string db_path = MakeTempDbPath("long-poll-session-race");
  const std::string node_name = "hpc1";
  const auto keypair = naim::GenerateSigningKeypair();
  {
    naim::ControllerStore store(db_path);
    store.Initialize();
    naim::RegisteredHostRecord host;
    host.node_name = node_name;
    host.registration_state = "registered";
    host.onboarding_state = "completed";
    host.session_state = "disconnected";
    host.transport_mode = "out";
    host.execution_mode = "mixed";
    host.public_key_base64 = keypair.public_key_base64;
    host.capabilities_json = json::object().dump();
    store.UpsertRegisteredHost(host);
  }

  HostdHttpService service{HostdHttpSupport(TestEventSink())};
  const std::string first_token =
      OpenSession(service, db_path, node_name, keypair.private_key_base64, "first");
  const auto long_poll_request = EncryptedHostRequest(
      "/api/v1/hostd/assignments/next",
      node_name,
      first_token,
      "assignments/next",
      1,
      json{
          {"node_name", node_name},
          {"preferred_control_transport", "http-long-poll"},
          {"wait_ms", 1000},
      });

  std::promise<HttpResponse> response_promise;
  auto response_future = response_promise.get_future();
  std::thread long_poll_thread([&]() {
    response_promise.set_value(Send(service, db_path, long_poll_request));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  const std::string second_token =
      OpenSession(service, db_path, node_name, keypair.private_key_base64, "second");
  Expect(second_token != first_token, "second session should rotate token");

  const auto response = response_future.get();
  long_poll_thread.join();
  const auto decrypted =
      DecryptResponse(response, node_name, first_token, "assignments/next");
  Expect(decrypted.value("node_name", std::string{}) == node_name,
         "long-poll response should decrypt with original token");
  Expect(decrypted.contains("assignment") && decrypted["assignment"].is_null(),
         "test should receive empty assignment response");

  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto host = store.LoadRegisteredHost(node_name);
  Expect(host.has_value(), "host should remain registered");
  Expect(host->session_token == second_token,
         "late long-poll response must not restore the old session token");
}

}  // namespace

int main() {
  try {
    naim::InitializeCrypto();
    TestLongPollResponseUsesOriginalSessionToken();
    std::cout << "ok: long-poll-response-uses-original-session-token\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "test failure: " << error.what() << "\n";
    return 1;
  }
}
