#include "backend/http_hostd_backend.h"

#include <chrono>
#include <ctime>
#include <stdexcept>
#include <thread>

#include "naim/security/crypto_utils.h"

namespace naim::hostd {

namespace {

std::string JsonNullableStringOrEmpty(
    const nlohmann::json& payload,
    const char* key) {
  const auto it = payload.find(key);
  if (it == payload.end() || it->is_null() || !it->is_string()) {
    return {};
  }
  return it->get<std::string>();
}

}  // namespace

HttpHostdBackend::HttpHostdBackend(
    std::string controller_url,
    std::string private_key_base64,
    std::string trusted_controller_fingerprint,
    std::string onboarding_key,
    std::string node_name,
    std::string storage_root,
    const IHttpHostdBackendSupport& support)
    : controller_url_(std::move(controller_url)),
      private_key_base64_(std::move(private_key_base64)),
      trusted_controller_fingerprint_(std::move(trusted_controller_fingerprint)),
      onboarding_key_(std::move(onboarding_key)),
      configured_node_name_(std::move(node_name)),
      storage_root_(std::move(storage_root)),
      support_(support) {}

std::optional<naim::HostAssignment> HttpHostdBackend::ClaimNextHostAssignment(
    const std::string& node_name) {
  EnsureSession(node_name, "claiming next assignment");
  const auto payload = SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/assignments/next",
      nlohmann::json{
          {"node_name", node_name},
          {"preferred_control_transport", "http-long-poll"},
          {"wait_ms", 15000},
      },
      "assignments/next");
  if (!payload.contains("assignment") || payload["assignment"].is_null()) {
    return std::nullopt;
  }
  return support_.ParseAssignmentPayload(payload["assignment"]);
}

bool HttpHostdBackend::TransitionClaimedHostAssignment(
    const int assignment_id,
    const naim::HostAssignmentStatus status,
    const std::string& status_message) {
  if (status == naim::HostAssignmentStatus::Applied) {
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/applied",
        nlohmann::json{{"status_message", status_message}},
        "assignments/" + std::to_string(assignment_id) + "/applied");
    return true;
  }
  if (status == naim::HostAssignmentStatus::Pending ||
      status == naim::HostAssignmentStatus::Failed) {
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/failed",
        nlohmann::json{
            {"status_message", status_message},
            {"retry", status == naim::HostAssignmentStatus::Pending},
        },
        "assignments/" + std::to_string(assignment_id) + "/failed");
    return true;
  }
  throw std::runtime_error("unsupported remote assignment transition");
}

bool HttpHostdBackend::UpdateHostAssignmentProgress(
    const int assignment_id,
    const nlohmann::json& progress) {
  EnsureSession(configured_node_name_, "updating assignment progress");
  SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/progress",
      progress,
      "assignments/" + std::to_string(assignment_id) + "/progress");
  return true;
}

nlohmann::json HttpHostdBackend::RequestModelArtifactChunk(
    const std::string& requester_node_name,
    const std::string& source_node_name,
    const std::string& source_path,
    const std::uintmax_t offset,
    const std::uintmax_t max_bytes) {
  EnsureSession(requester_node_name, "requesting model artifact chunk");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/model-artifacts/chunks/request",
      nlohmann::json{
          {"requester_node_name", requester_node_name},
          {"source_node_name", source_node_name},
          {"source_path", source_path},
          {"offset", offset},
          {"max_bytes", max_bytes},
      },
      "model-artifacts/chunks/request");
}

nlohmann::json HttpHostdBackend::LoadModelArtifactChunk(
    const std::string& requester_node_name,
    const int assignment_id) {
  EnsureSession(requester_node_name, "loading model artifact chunk");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/model-artifacts/chunks/poll",
      nlohmann::json{
          {"requester_node_name", requester_node_name},
          {"assignment_id", assignment_id},
      },
      "model-artifacts/chunks/poll");
}

nlohmann::json HttpHostdBackend::RequestModelArtifactManifest(
    const std::string& requester_node_name,
    const std::string& source_node_name,
    const std::vector<std::string>& source_paths) {
  EnsureSession(requester_node_name, "requesting model artifact manifest");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/model-artifacts/manifest/request",
      nlohmann::json{
          {"requester_node_name", requester_node_name},
          {"source_node_name", source_node_name},
          {"source_paths", source_paths},
      },
      "model-artifacts/manifest/request");
}

nlohmann::json HttpHostdBackend::LoadModelArtifactManifest(
    const std::string& requester_node_name,
    const int assignment_id) {
  EnsureSession(requester_node_name, "loading model artifact manifest");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/model-artifacts/manifest/poll",
      nlohmann::json{
          {"requester_node_name", requester_node_name},
          {"assignment_id", assignment_id},
      },
      "model-artifacts/manifest/poll");
}

nlohmann::json HttpHostdBackend::RequestFileTransferTicket(
    const std::string& requester_node_name,
    const std::string& source_node_name,
    const std::vector<std::string>& source_paths) {
  EnsureSession(requester_node_name, "requesting LAN file transfer ticket");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/file-transfer-tickets",
      nlohmann::json{
          {"requester_node_name", requester_node_name},
          {"source_node_name", source_node_name},
          {"source_paths", source_paths},
      },
      "file-transfer-tickets/create");
}

nlohmann::json HttpHostdBackend::ValidateFileTransferTicket(
    const std::string& source_node_name,
    const std::string& ticket_id) {
  EnsureSession(source_node_name, "validating LAN file transfer ticket");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/file-transfer-tickets/validate",
      nlohmann::json{{"ticket_id", ticket_id}},
      "file-transfer-tickets/validate");
}

nlohmann::json HttpHostdBackend::RequestFileUploadTicket(
    const std::string& uploader_node_name,
    const std::string& target_node_name,
    const std::string& target_relative_path,
    const std::string& sha256,
    std::uintmax_t size_bytes,
    bool if_missing) {
  EnsureSession(uploader_node_name, "requesting LAN file upload ticket");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/file-upload-tickets",
      nlohmann::json{
          {"uploader_node_name", uploader_node_name},
          {"target_node_name", target_node_name},
          {"target_relative_path", target_relative_path},
          {"sha256", sha256},
          {"size_bytes", size_bytes},
          {"if_missing", if_missing},
      },
      "file-upload-tickets/create");
}

nlohmann::json HttpHostdBackend::ValidateFileUploadTicket(
    const std::string& target_node_name,
    const std::string& ticket_id) {
  EnsureSession(target_node_name, "validating LAN file upload ticket");
  return SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/file-upload-tickets/validate",
      nlohmann::json{{"ticket_id", ticket_id}},
      "file-upload-tickets/validate");
}

void HttpHostdBackend::UpsertHostObservation(const naim::HostObservation& observation) {
  EnsureSession(observation.node_name, "uploading observation");
  SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/observations",
      support_.BuildHostObservationPayload(observation),
      "observations/upsert");
}

void HttpHostdBackend::AppendEvent(const naim::EventRecord& event) {
  if (!event.node_name.empty()) {
    EnsureSession(event.node_name, "appending event");
  }
  SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/events",
      nlohmann::json{
          {"plane_name", event.plane_name},
          {"node_name", event.node_name},
          {"worker_name", event.worker_name},
          {"assignment_id", event.assignment_id.has_value() ? nlohmann::json(*event.assignment_id)
                                                            : nlohmann::json(nullptr)},
          {"rollout_action_id",
           event.rollout_action_id.has_value() ? nlohmann::json(*event.rollout_action_id)
                                               : nlohmann::json(nullptr)},
          {"category", event.category},
          {"event_type", event.event_type},
          {"severity", event.severity},
          {"message", event.message},
          {"payload_json", event.payload_json},
      },
      "events/append");
}

void HttpHostdBackend::UpsertDiskRuntimeState(const naim::DiskRuntimeState& state) {
  EnsureSession(state.node_name, "upserting disk runtime state");
  SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/disk-runtime-state",
      support_.BuildDiskRuntimeStatePayload(state),
      "disk-runtime-state/upsert");
}

std::optional<naim::DiskRuntimeState> HttpHostdBackend::LoadDiskRuntimeState(
    const std::string& disk_name,
    const std::string& node_name) {
  EnsureSession(node_name, "loading disk runtime state");
  const auto payload = SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/disk-runtime-state/load",
      nlohmann::json{{"disk_name", disk_name}, {"node_name", node_name}},
      "disk-runtime-state/load");
  if (!payload.contains("runtime_state") || payload["runtime_state"].is_null()) {
    return std::nullopt;
  }
  return support_.ParseDiskRuntimeStatePayload(payload["runtime_state"]);
}

bool HttpHostdBackend::IsRecoverableSessionErrorMessage(const std::string& message) {
  return message.find("invalid or missing host session") != std::string::npos ||
         message.find("stale or replayed host session request") != std::string::npos;
}

bool IsOnboardingRegistrationNeededMessage(const std::string& message) {
  return message.find("host node is not registered") != std::string::npos ||
         message.find("registered host is missing public key") != std::string::npos;
}

void HttpHostdBackend::ResetSessionState() {
  session_token_.clear();
  session_node_name_.clear();
  host_sequence_ = 0;
  controller_sequence_ = 0;
}

void HttpHostdBackend::EnsureRegistered(const std::string& node_name) {
  if (registration_attempted_) {
    return;
  }
  registration_attempted_ = true;
  if (onboarding_key_.empty()) {
    return;
  }
  if (!configured_node_name_.empty() && configured_node_name_ != node_name) {
    throw std::runtime_error(
        "configured hostd node name '" + configured_node_name_ +
        "' does not match requested node '" + node_name + "'");
  }
  const auto response = support_.SendControllerJsonRequest(
      controller_url_,
      "POST",
      "/api/v1/hostd/register",
      nlohmann::json{
          {"node_name", node_name},
          {"public_key_base64", naim::DerivePublicKeyBase64(private_key_base64_)},
          {"onboarding_key", onboarding_key_},
          {"transport_mode", "out"},
          {"execution_mode", "mixed"},
          {"capabilities_json",
           nlohmann::json{
               {"storage_root", storage_root_},
               {"transport",
                nlohmann::json{
                    {"preferred_control_transport", "http-long-poll"},
                    {"supported_control_transports",
                     nlohmann::json::array({"http-poll", "http-long-poll", "websocket"})},
                    {"supports_keep_alive", true},
                    {"supports_long_poll", true},
                    {"supports_websocket", true},
                    {"supports_resumable_transfer", true},
                    {"supports_udp_discovery", true},
                }},
           }.dump()},
          {"status_message", "registered via naim-node remote hostd onboarding"},
      });
  const std::string controller_fingerprint =
      JsonNullableStringOrEmpty(response, "controller_public_key_fingerprint");
  if (!trusted_controller_fingerprint_.empty() &&
      !controller_fingerprint.empty() &&
      controller_fingerprint != trusted_controller_fingerprint_) {
    throw std::runtime_error("controller fingerprint mismatch during host registration");
  }
}

std::string HttpHostdBackend::BuildRequestAad(
    const std::string& message_type,
    const std::uint64_t sequence_number) const {
  return "request\n" + message_type + "\n" + session_node_name_ + "\n" +
         std::to_string(sequence_number);
}

std::string HttpHostdBackend::BuildResponseAad(
    const std::string& message_type,
    const std::uint64_t sequence_number) const {
  return "response\n" + message_type + "\n" + session_node_name_ + "\n" +
         std::to_string(sequence_number);
}

nlohmann::json HttpHostdBackend::SendEncryptedControllerJsonRequest(
    const std::string& path,
    const nlohmann::json& payload,
    const std::string& message_type) {
  return RetryOnRecoverableSessionError(
      message_type,
      "recovering host session",
      [&]() {
        if (session_token_.empty()) {
          throw std::runtime_error("missing host session token");
        }
        host_sequence_ += 1;
        const naim::EncryptedEnvelope envelope = naim::EncryptEnvelopeBase64(
            payload.dump(),
            session_token_,
            BuildRequestAad(message_type, host_sequence_));
        const auto response = support_.SendControllerJsonRequest(
            controller_url_,
            "POST",
            path,
            nlohmann::json{
                {"encrypted", true},
                {"sequence_number", host_sequence_},
                {"nonce", envelope.nonce_base64},
                {"ciphertext", envelope.ciphertext_base64},
            },
            SessionHeaders());
        if (!response.value("encrypted", false)) {
          return response;
        }
        const std::uint64_t controller_sequence =
            response.value("sequence_number", static_cast<std::uint64_t>(0));
        if (controller_sequence <= controller_sequence_) {
          throw std::runtime_error("stale or replayed controller session response");
        }
        const naim::EncryptedEnvelope response_envelope{
            response.value("nonce", std::string{}),
            response.value("ciphertext", std::string{}),
        };
        const std::string decrypted = naim::DecryptEnvelopeBase64(
            response_envelope,
            session_token_,
            BuildResponseAad(message_type, controller_sequence));
        controller_sequence_ = controller_sequence;
        return decrypted.empty() ? nlohmann::json::object() : nlohmann::json::parse(decrypted);
      });
}

void HttpHostdBackend::EnsureSession(const std::string& node_name, const std::string& status_message) {
  if (!session_token_.empty() &&
      (host_sequence_ >= SessionRekeyMessageLimit() ||
       controller_sequence_ >= SessionRekeyMessageLimit())) {
    ResetSessionState();
  }
  if (!session_token_.empty() && session_node_name_ == node_name) {
    try {
      SendEncryptedControllerJsonRequest(
          "/api/v1/hostd/session/heartbeat",
          nlohmann::json{
              {"node_name", node_name},
              {"session_state", "connected"},
              {"status_message", status_message}},
          "session/heartbeat");
      return;
    } catch (const std::exception&) {
      ResetSessionState();
    }
  }
  const std::string nonce = naim::RandomTokenBase64(24);
  const std::string timestamp = std::to_string(std::time(nullptr));
  const std::string message = "hostd-session-open\n" + node_name + "\n" + timestamp + "\n" + nonce;
  const std::string signature = naim::SignDetachedBase64(message, private_key_base64_);
  nlohmann::json response;
  try {
    response = support_.SendControllerJsonRequest(
        controller_url_,
        "POST",
        "/api/v1/hostd/session/open",
        nlohmann::json{
            {"node_name", node_name},
            {"timestamp", timestamp},
            {"nonce", nonce},
            {"signature", signature},
            {"status_message", status_message},
        });
  } catch (const std::exception& error) {
    if (onboarding_key_.empty() ||
        !IsOnboardingRegistrationNeededMessage(error.what())) {
      throw;
    }
    EnsureRegistered(node_name);
    response = support_.SendControllerJsonRequest(
        controller_url_,
        "POST",
        "/api/v1/hostd/session/open",
        nlohmann::json{
            {"node_name", node_name},
            {"timestamp", timestamp},
            {"nonce", nonce},
            {"signature", signature},
            {"status_message", status_message},
        });
  }
  const std::string controller_fingerprint =
      JsonNullableStringOrEmpty(response, "controller_public_key_fingerprint");
  if (!trusted_controller_fingerprint_.empty() &&
      controller_fingerprint != trusted_controller_fingerprint_) {
    throw std::runtime_error("controller fingerprint mismatch during host session open");
  }
  session_token_ = response.value("session_token", std::string{});
  session_node_name_ = node_name;
  host_sequence_ = 0;
  controller_sequence_ =
      response.value("controller_sequence", static_cast<std::uint64_t>(0));
  SendEncryptedControllerJsonRequest(
      "/api/v1/hostd/session/heartbeat",
      nlohmann::json{
          {"node_name", node_name},
          {"session_state", "connected"},
          {"status_message", status_message}},
      "session/heartbeat");
}

std::map<std::string, std::string> HttpHostdBackend::SessionHeaders() const {
  if (session_token_.empty()) {
    return {};
  }
  return {
      {"X-Naim-Host-Session", session_token_},
      {"X-Naim-Host-Node", session_node_name_},
  };
}

template <typename Fn>
nlohmann::json HttpHostdBackend::RetryOnRecoverableSessionError(
    const std::string& message_type,
    const char* recovery_status_message,
    Fn&& fn) {
  constexpr int kMaxAttempts = 5;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    try {
      return fn();
    } catch (const std::exception& error) {
      if (session_node_name_.empty() ||
          message_type == "session/heartbeat" ||
          !IsRecoverableSessionErrorMessage(error.what()) ||
          attempt + 1 >= kMaxAttempts) {
        throw;
      }
      const std::string node_name = session_node_name_;
      ResetSessionState();
      EnsureSession(node_name, recovery_status_message);
      std::this_thread::sleep_for(std::chrono::milliseconds(25 * (attempt + 1)));
    }
  }
  throw std::runtime_error("unreachable host session retry state");
}

}  // namespace naim::hostd
