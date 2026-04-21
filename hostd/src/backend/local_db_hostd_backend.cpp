#include "backend/local_db_hostd_backend.h"

#include <stdexcept>

namespace naim::hostd {

LocalDbHostdBackend::LocalDbHostdBackend(std::string db_path) : store_(std::move(db_path)) {
  store_.Initialize();
}

std::optional<naim::HostAssignment> LocalDbHostdBackend::ClaimNextHostAssignment(
    const std::string& node_name) {
  return store_.ClaimNextHostAssignment(node_name);
}

bool LocalDbHostdBackend::TransitionClaimedHostAssignment(
    const int assignment_id,
    const naim::HostAssignmentStatus status,
    const std::string& status_message) {
  return store_.TransitionClaimedHostAssignment(assignment_id, status, status_message);
}

bool LocalDbHostdBackend::UpdateHostAssignmentProgress(
    const int assignment_id,
    const nlohmann::json& progress) {
  return store_.UpdateHostAssignmentProgress(assignment_id, progress.dump());
}

nlohmann::json LocalDbHostdBackend::RequestModelArtifactChunk(
    const std::string&,
    const std::string&,
    const std::string&,
    std::uintmax_t,
    std::uintmax_t) {
  throw std::runtime_error("model artifact chunk relay requires a controller backend");
}

nlohmann::json LocalDbHostdBackend::LoadModelArtifactChunk(
    const std::string&,
    int) {
  throw std::runtime_error("model artifact chunk relay requires a controller backend");
}

nlohmann::json LocalDbHostdBackend::RequestModelArtifactManifest(
    const std::string&,
    const std::string&,
    const std::vector<std::string>&) {
  throw std::runtime_error("model artifact manifest relay requires a controller backend");
}

nlohmann::json LocalDbHostdBackend::LoadModelArtifactManifest(
    const std::string&,
    int) {
  throw std::runtime_error("model artifact manifest relay requires a controller backend");
}

nlohmann::json LocalDbHostdBackend::RequestFileTransferTicket(
    const std::string&,
    const std::string&,
    const std::vector<std::string>&) {
  return nlohmann::json{{"status", "not_available"}};
}

nlohmann::json LocalDbHostdBackend::ValidateFileTransferTicket(
    const std::string&,
    const std::string&) {
  return nlohmann::json{{"status", "denied"}};
}

nlohmann::json LocalDbHostdBackend::RequestFileUploadTicket(
    const std::string&,
    const std::string&,
    const std::string&,
    const std::string&,
    std::uintmax_t,
    bool) {
  return nlohmann::json{{"status", "not_available"}};
}

nlohmann::json LocalDbHostdBackend::ValidateFileUploadTicket(
    const std::string&,
    const std::string&) {
  return nlohmann::json{{"status", "denied"}};
}

void LocalDbHostdBackend::UpsertHostObservation(const naim::HostObservation& observation) {
  store_.UpsertHostObservation(observation);
}

void LocalDbHostdBackend::AppendEvent(const naim::EventRecord& event) {
  store_.AppendEvent(event);
}

void LocalDbHostdBackend::UpsertDiskRuntimeState(const naim::DiskRuntimeState& state) {
  store_.UpsertDiskRuntimeState(state);
}

std::optional<naim::DiskRuntimeState> LocalDbHostdBackend::LoadDiskRuntimeState(
    const std::string& disk_name,
    const std::string& node_name) {
  return store_.LoadDiskRuntimeState(disk_name, node_name);
}

}  // namespace naim::hostd
