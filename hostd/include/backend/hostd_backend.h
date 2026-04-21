#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::hostd {

class HostdBackend {
 public:
  virtual ~HostdBackend() = default;

  virtual std::optional<naim::HostAssignment> ClaimNextHostAssignment(
      const std::string& node_name) = 0;
  virtual bool TransitionClaimedHostAssignment(
      int assignment_id,
      naim::HostAssignmentStatus status,
      const std::string& status_message) = 0;
  virtual bool UpdateHostAssignmentProgress(
      int assignment_id,
      const nlohmann::json& progress) = 0;
  virtual nlohmann::json RequestModelArtifactChunk(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::string& source_path,
      std::uintmax_t offset,
      std::uintmax_t max_bytes) = 0;
  virtual nlohmann::json LoadModelArtifactChunk(
      const std::string& requester_node_name,
      int assignment_id) = 0;
  virtual nlohmann::json RequestModelArtifactManifest(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::vector<std::string>& source_paths) = 0;
  virtual nlohmann::json LoadModelArtifactManifest(
      const std::string& requester_node_name,
      int assignment_id) = 0;
  virtual nlohmann::json RequestFileTransferTicket(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::vector<std::string>& source_paths) = 0;
  virtual nlohmann::json ValidateFileTransferTicket(
      const std::string& source_node_name,
      const std::string& ticket_id) = 0;
  virtual nlohmann::json RequestFileUploadTicket(
      const std::string& uploader_node_name,
      const std::string& target_node_name,
      const std::string& target_relative_path,
      const std::string& sha256,
      std::uintmax_t size_bytes,
      bool if_missing) = 0;
  virtual nlohmann::json ValidateFileUploadTicket(
      const std::string& target_node_name,
      const std::string& ticket_id) = 0;
  virtual void UpsertHostObservation(const naim::HostObservation& observation) = 0;
  virtual void AppendEvent(const naim::EventRecord& event) = 0;
  virtual void UpsertDiskRuntimeState(const naim::DiskRuntimeState& state) = 0;
  virtual std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) = 0;
};

}  // namespace naim::hostd
