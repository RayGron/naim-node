#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"
#include "infra/controller_request_support.h"
#include "model/model_library_service.h"

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string ReadFile(const fs::path& path) {
  std::ifstream input(path);
  std::string contents;
  std::getline(input, contents);
  return contents;
}

std::string FileUrlForPath(const fs::path& path) {
  return "file://" + fs::absolute(path).string();
}

HttpRequest JsonRequest(
    const std::string& method,
    const std::string& path,
    const nlohmann::json& body) {
  HttpRequest request;
  request.method = method;
  request.path = path;
  request.body = body.dump();
  request.headers["Content-Type"] = "application/json";
  return request;
}

}  // namespace

int main() {
  try {
    const fs::path temp_root = fs::temp_directory_path() / "comet-model-library-tests";
    const fs::path db_path = temp_root / "controller.sqlite";
    const fs::path src_root = temp_root / "src";
    const fs::path dst_root = temp_root / "dst";
    const fs::path source_path = src_root / "smoke.gguf";
    const fs::path target_path = dst_root / "smoke.gguf";
    std::error_code error;
    fs::remove_all(temp_root, error);
    fs::create_directories(src_root);
    fs::create_directories(dst_root);
    {
      std::ofstream out(source_path);
      out << "persistent-model-library-job";
    }

    comet::ControllerStore store(db_path.string());
    store.Initialize();
    const std::string now = "2026-03-30 00:00:00";
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        "job-1",
        "queued",
        "model-1",
        dst_root.string(),
        "",
        {FileUrlForPath(source_path)},
        {target_path.string()},
        "",
        std::nullopt,
        0,
        1,
        "",
        false,
        now,
        now,
    });

    comet::controller::ControllerRequestSupport request_support;
    ModelLibraryService service{ModelLibrarySupport(request_support)};
    const auto payload = service.BuildPayload(db_path.string());
    Expect(payload.at("jobs").is_array(), "jobs payload should be an array");
    Expect(payload.at("jobs").size() == 1, "jobs payload should contain persisted queued job");

    bool completed = false;
    for (int attempt = 0; attempt < 50; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto job = store.LoadModelLibraryDownloadJob("job-1");
      Expect(job.has_value(), "persisted model library job should remain loadable");
      if (job->status == "completed") {
        completed = true;
        break;
      }
    }

    Expect(completed, "persisted queued model library job should complete after resume");
    Expect(fs::exists(target_path), "download target should exist");
    Expect(
        ReadFile(target_path) == "persistent-model-library-job",
        "downloaded model payload should match source contents");

    const fs::path second_target_path = dst_root / "resume.gguf";
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        "job-2",
        "queued",
        "model-2",
        dst_root.string(),
        "",
        {FileUrlForPath(source_path)},
        {second_target_path.string()},
        "",
        std::nullopt,
        0,
        1,
        "",
        false,
        now,
        now,
    });

    const auto stop_response = service.StopDownloadJob(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/jobs/stop",
            nlohmann::json{{"job_id", "job-2"}}));
    Expect(stop_response.status_code == 200, "stop download job should succeed");
    auto stopped_job = store.LoadModelLibraryDownloadJob("job-2");
    Expect(stopped_job.has_value(), "stopped job should remain persisted");
    Expect(stopped_job->status == "stopped", "queued job should become stopped");

    const auto resume_response = service.ResumeDownloadJob(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/jobs/resume",
            nlohmann::json{{"job_id", "job-2"}}));
    Expect(resume_response.status_code == 202, "resume download job should be accepted");

    bool resumed_completed = false;
    for (int attempt = 0; attempt < 50; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto job = store.LoadModelLibraryDownloadJob("job-2");
      Expect(job.has_value(), "resumed model library job should remain loadable");
      if (job->status == "completed") {
        resumed_completed = true;
        break;
      }
    }

    Expect(resumed_completed, "stopped model library job should complete after resume");
    Expect(fs::exists(second_target_path), "resumed download target should exist");

    const auto hide_response = service.HideDownloadJob(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/jobs/hide",
            nlohmann::json{{"job_id", "job-2"}}));
    Expect(hide_response.status_code == 200, "hide download job should succeed");
    const auto visible_after_hide = service.BuildPayload(db_path.string());
    Expect(
        visible_after_hide.at("jobs").size() == 1,
        "hidden download job should disappear from visible payload");
    auto hidden_job = store.LoadModelLibraryDownloadJob("job-2");
    Expect(hidden_job.has_value(), "hidden job should remain in the database");
    Expect(hidden_job->hidden, "hidden job should be marked as hidden");

    const auto delete_response = service.DeleteDownloadJob(
        db_path.string(),
        JsonRequest(
            "DELETE",
            "/api/v1/model-library/jobs",
            nlohmann::json{{"job_id", "job-2"}}));
    Expect(delete_response.status_code == 200, "delete download job should succeed");
    Expect(
        !store.LoadModelLibraryDownloadJob("job-2").has_value(),
        "deleted model library job should be removed from the database");
    Expect(!fs::exists(second_target_path), "delete download job should remove downloaded file");

    fs::remove_all(temp_root, error);
    std::cout << "model library service tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
