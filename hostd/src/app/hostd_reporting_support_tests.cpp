#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/hostd_reporting_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class RecordingHostdBackend : public naim::hostd::HostdBackend {
 public:
  std::optional<int> updated_assignment_id;
  nlohmann::json updated_progress = nlohmann::json::object();
  std::vector<naim::EventRecord> events;

  std::optional<naim::HostAssignment> ClaimNextHostAssignment(
      const std::string&) override {
    return std::nullopt;
  }

  bool TransitionClaimedHostAssignment(
      int,
      naim::HostAssignmentStatus,
      const std::string&) override {
    return false;
  }

  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const nlohmann::json& progress) override {
    updated_assignment_id = assignment_id;
    updated_progress = progress;
    return true;
  }

  nlohmann::json RequestModelArtifactChunk(
      const std::string&,
      const std::string&,
      const std::string&,
      std::uintmax_t,
      std::uintmax_t) override {
    return nlohmann::json::object();
  }

  nlohmann::json LoadModelArtifactChunk(
      const std::string&,
      int) override {
    return nlohmann::json::object();
  }

  nlohmann::json RequestModelArtifactManifest(
      const std::string&,
      const std::string&,
      const std::vector<std::string>&) override {
    return nlohmann::json::object();
  }

  nlohmann::json LoadModelArtifactManifest(
      const std::string&,
      int) override {
    return nlohmann::json::object();
  }

  nlohmann::json RequestFileTransferTicket(
      const std::string&,
      const std::string&,
      const std::vector<std::string>&) override {
    return nlohmann::json::object();
  }

  nlohmann::json ValidateFileTransferTicket(
      const std::string&,
      const std::string&) override {
    return nlohmann::json::object();
  }

  nlohmann::json RequestFileUploadTicket(
      const std::string&,
      const std::string&,
      const std::string&,
      const std::string&,
      std::uintmax_t,
      bool) override {
    return nlohmann::json::object();
  }

  nlohmann::json ValidateFileUploadTicket(
      const std::string&,
      const std::string&) override {
    return nlohmann::json::object();
  }

  void UpsertHostObservation(const naim::HostObservation&) override {}

  void AppendEvent(const naim::EventRecord& event) override {
    events.push_back(event);
  }

  void UpsertDiskRuntimeState(const naim::DiskRuntimeState&) override {}

  std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string&,
      const std::string&) override {
    return std::nullopt;
  }
};

class ThrowingProgressHostdBackend final : public RecordingHostdBackend {
 public:
  bool UpdateHostAssignmentProgress(
      int,
      const nlohmann::json&) override {
    throw std::runtime_error("transient progress update failure");
  }
};

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;

    const naim::hostd::HostdReportingSupport support;

    {
      const auto payload = support.BuildAssignmentProgressPayload(
          "pull",
          "Pull image",
          "Downloading",
          140,
          "plane-a",
          "node-a",
          10,
          20);
      Expect(payload.at("percent").get<int>() == 100, "progress percent should clamp to 100");
      Expect(
          payload.at("bytes_done").get<std::uintmax_t>() == 10,
          "progress payload should include bytes_done");
      Expect(
          payload.at("plane_name").get<std::string>() == "plane-a",
          "progress payload should include plane name");
    }

    {
      RecordingHostdBackend backend;
      support.PublishAssignmentProgress(&backend, 42, nlohmann::json{{"phase", "apply"}});
      Expect(
          backend.updated_assignment_id.has_value() && *backend.updated_assignment_id == 42,
          "PublishAssignmentProgress should forward assignment id");
      Expect(
          backend.updated_progress.at("phase").get<std::string>() == "apply",
          "PublishAssignmentProgress should forward payload");

      support.PublishAssignmentProgress(&backend, std::nullopt, nlohmann::json{{"phase", "skip"}});
      Expect(
          backend.updated_progress.at("phase").get<std::string>() == "apply",
          "PublishAssignmentProgress should ignore empty assignment id");
    }

    {
      ThrowingProgressHostdBackend backend;
      support.PublishAssignmentProgress(&backend, 43, nlohmann::json{{"phase", "apply"}});
    }

    {
      RecordingHostdBackend backend;
      support.AppendHostdEvent(
          backend,
          "hostd",
          "assignment-progress",
          "apply started",
          nlohmann::json{{"phase", "apply"}},
          "plane-a",
          "node-a",
          "",
          99,
          std::nullopt,
          "info");
      Expect(backend.events.size() == 1, "AppendHostdEvent should append one event");
      Expect(
          backend.events.front().payload_json.find("\"phase\":\"apply\"") != std::string::npos,
          "AppendHostdEvent should serialize payload");
      Expect(
          backend.events.front().assignment_id.has_value() &&
              *backend.events.front().assignment_id == 99,
          "AppendHostdEvent should keep assignment id");
    }

    {
      const auto victims = support.ParseTaggedCsv(
          "status=running victims=worker-a,worker-b",
          "victims");
      Expect(victims.size() == 2, "ParseTaggedCsv should extract two values");
      Expect(victims.front() == "worker-a", "ParseTaggedCsv should keep first victim");
    }

    {
      const fs::path temp_root =
          fs::temp_directory_path() / "naim-hostd-reporting-support-tests";
      std::error_code cleanup_error;
      fs::remove_all(temp_root, cleanup_error);
      fs::create_directories(temp_root);

      const auto observation = support.BuildObservedStateSnapshot(
          "node-a",
          temp_root.string(),
          temp_root.string(),
          naim::HostObservationStatus::Applied,
          "stable",
          17);
      Expect(observation.node_name == "node-a", "observation should keep node name");
      Expect(
          observation.last_assignment_id.has_value() && *observation.last_assignment_id == 17,
          "observation should keep assignment id");
      Expect(
          observation.status_message == "stable",
          "observation should keep status message");
      Expect(
          !observation.disk_telemetry_json.empty(),
          "observation should include disk telemetry payload");

      fs::remove_all(temp_root, cleanup_error);
    }

    std::cout << "ok: hostd-reporting-support-progress-payload\n";
    std::cout << "ok: hostd-reporting-support-publish-progress\n";
    std::cout << "ok: hostd-reporting-support-progress-best-effort\n";
    std::cout << "ok: hostd-reporting-support-append-event\n";
    std::cout << "ok: hostd-reporting-support-parse-tagged-csv\n";
    std::cout << "ok: hostd-reporting-support-build-observation\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
