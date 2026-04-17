#pragma once

#include <cstdint>
#include <string>

#include "backend/hostd_backend.h"
#include "backend/http_hostd_backend_support.h"

namespace naim::hostd {

class HttpHostdBackend final : public HostdBackend {
 public:
  HttpHostdBackend(
      std::string controller_url,
      std::string private_key_base64,
      std::string trusted_controller_fingerprint,
      std::string onboarding_key,
      std::string node_name,
      std::string storage_root,
      const IHttpHostdBackendSupport& support);

  std::optional<naim::HostAssignment> ClaimNextHostAssignment(
      const std::string& node_name) override;
  bool TransitionClaimedHostAssignment(
      int assignment_id,
      naim::HostAssignmentStatus status,
      const std::string& status_message) override;
  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const nlohmann::json& progress) override;
  nlohmann::json RequestModelArtifactChunk(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::string& source_path,
      std::uintmax_t offset,
      std::uintmax_t max_bytes) override;
  nlohmann::json LoadModelArtifactChunk(
      const std::string& requester_node_name,
      int assignment_id) override;
  nlohmann::json RequestModelArtifactManifest(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::vector<std::string>& source_paths) override;
  nlohmann::json LoadModelArtifactManifest(
      const std::string& requester_node_name,
      int assignment_id) override;
  nlohmann::json RequestFileTransferTicket(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::vector<std::string>& source_paths) override;
  nlohmann::json ValidateFileTransferTicket(
      const std::string& source_node_name,
      const std::string& ticket_id) override;
  void UpsertHostObservation(const naim::HostObservation& observation) override;
  void AppendEvent(const naim::EventRecord& event) override;
  void UpsertDiskRuntimeState(const naim::DiskRuntimeState& state) override;
  std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) override;

 private:
  static bool IsRecoverableSessionErrorMessage(const std::string& message);

  void ResetSessionState();
  void EnsureRegistered(const std::string& node_name);
  std::string BuildRequestAad(const std::string& message_type, std::uint64_t sequence_number) const;
  std::string BuildResponseAad(const std::string& message_type, std::uint64_t sequence_number) const;
  nlohmann::json SendEncryptedControllerJsonRequest(
      const std::string& path,
      const nlohmann::json& payload,
      const std::string& message_type);
  void EnsureSession(const std::string& node_name, const std::string& status_message);
  std::map<std::string, std::string> SessionHeaders() const;

  template <typename Fn>
  nlohmann::json RetryOnRecoverableSessionError(
      const std::string& message_type,
      const char* recovery_status_message,
      Fn&& fn);

  static constexpr std::uint64_t SessionRekeyMessageLimit() {
    return 64;
  }

  std::string controller_url_;
  std::string private_key_base64_;
  std::string trusted_controller_fingerprint_;
  std::string onboarding_key_;
  std::string configured_node_name_;
  std::string storage_root_;
  const IHttpHostdBackendSupport& support_;
  std::string session_token_;
  std::string session_node_name_;
  std::uint64_t host_sequence_ = 0;
  std::uint64_t controller_sequence_ = 0;
  bool registration_attempted_ = false;
};

}  // namespace naim::hostd
