#pragma once

#include <string>

#include "backend/hostd_backend.h"

namespace naim::hostd {

class LocalDbHostdBackend final : public HostdBackend {
 public:
  explicit LocalDbHostdBackend(std::string db_path);

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
  nlohmann::json RequestFileUploadTicket(
      const std::string& uploader_node_name,
      const std::string& target_node_name,
      const std::string& target_relative_path,
      const std::string& sha256,
      std::uintmax_t size_bytes,
      bool if_missing) override;
  nlohmann::json ValidateFileUploadTicket(
      const std::string& target_node_name,
      const std::string& ticket_id) override;
  void UpsertHostObservation(const naim::HostObservation& observation) override;
  void AppendEvent(const naim::EventRecord& event) override;
  void UpsertDiskRuntimeState(const naim::DiskRuntimeState& state) override;
  std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) override;

 private:
  naim::ControllerStore store_;
};

}  // namespace naim::hostd
