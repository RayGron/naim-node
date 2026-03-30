#include <chrono>
#include <cstddef>
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

#if !defined(_WIN32)
    const fs::path recovered_dir = dst_root / "recovered";
    const fs::path recovered_target = recovered_dir / "recovered.gguf";
    const fs::path recovered_part = recovered_dir / "recovered.gguf.part";
    const fs::path recovered_metadata =
        recovered_dir / ".comet-model-job-job-3.json";
    fs::create_directories(recovered_dir);
    {
      std::ofstream out(recovered_part);
      out << "partial-model-download";
    }
    {
      std::ofstream out(recovered_metadata);
      out << nlohmann::json{
          {"id", "job-3"},
          {"status", "stopped"},
          {"model_id", "model-3"},
          {"target_root", dst_root.string()},
          {"target_subdir", "recovered"},
          {"source_urls", nlohmann::json::array({FileUrlForPath(source_path)})},
          {"target_paths", nlohmann::json::array({recovered_target.string()})},
          {"current_item", "recovered.gguf"},
          {"bytes_total", 1024},
          {"bytes_done", 0},
          {"part_count", 1},
          {"error_message", ""},
          {"hidden", false},
          {"created_at", now},
          {"updated_at", now},
      }.dump(2);
    }
    setenv("COMET_NODE_MODEL_LIBRARY_ROOTS", dst_root.string().c_str(), 1);
    ModelLibraryService recovery_service{ModelLibrarySupport(request_support)};
    const auto recovered_payload = recovery_service.BuildPayload(db_path.string());
    unsetenv("COMET_NODE_MODEL_LIBRARY_ROOTS");
    auto recovered_job = store.LoadModelLibraryDownloadJob("job-3");
    Expect(recovered_job.has_value(), "metadata-backed job should be restored into the database");
    Expect(recovered_job->status == "stopped", "restored metadata job should preserve status");
    Expect(
        recovered_job->bytes_done == fs::file_size(recovered_part),
        "restored metadata job should recover bytes_done from partial file");
    Expect(
        recovered_payload.at("jobs").size() == 2,
        "metadata-backed job should be visible in jobs payload after recovery");

    const fs::path resumable_source_path = src_root / "resumable.gguf";
    const fs::path resumed_target_path = dst_root / "resumable.gguf";
    const fs::path resumed_part_path = dst_root / "resumable.gguf.part";
    {
      std::ofstream out(resumable_source_path, std::ios::binary);
      const std::string chunk = "resume-download-payload-";
      for (int index = 0; index < 40000; ++index) {
        out.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
      }
    }
    {
      const auto full = fs::file_size(resumable_source_path);
      const std::size_t prefix_size =
          static_cast<std::size_t>(std::min<std::uintmax_t>(full / 3, 200000));
      std::ifstream input(resumable_source_path, std::ios::binary);
      std::ofstream out(resumed_part_path, std::ios::binary);
      std::string prefix(prefix_size, '\0');
      input.read(prefix.data(), static_cast<std::streamsize>(prefix.size()));
      out.write(prefix.data(), static_cast<std::streamsize>(input.gcount()));
    }
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        "job-5",
        "stopped",
        "model-5",
        dst_root.string(),
        "",
        {FileUrlForPath(resumable_source_path)},
        {resumed_target_path.string()},
        resumed_target_path.filename().string(),
        fs::file_size(resumable_source_path),
        fs::file_size(resumed_part_path),
        1,
        "",
        false,
        now,
        now,
    });
    const auto resumable_resume_response = service.ResumeDownloadJob(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/jobs/resume",
            nlohmann::json{{"job_id", "job-5"}}));
    Expect(
        resumable_resume_response.status_code == 202,
        "resume on partial download should be accepted");
    bool resumable_completed = false;
    for (int attempt = 0; attempt < 50; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto job = store.LoadModelLibraryDownloadJob("job-5");
      Expect(job.has_value(), "resumed partial job should remain persisted");
      if (job->status == "completed") {
        resumable_completed = true;
        break;
      }
    }
    Expect(
        resumable_completed,
        "partial download should complete after resume without restarting from zero");
    Expect(fs::exists(resumed_target_path), "resumed target should exist");
    Expect(
        !fs::exists(resumed_part_path),
        "resumed partial file should be renamed into final target after completion");
    Expect(
        fs::file_size(resumed_target_path) == fs::file_size(resumable_source_path),
        "resumed target should match source file size");

    const fs::path multipart_dir = dst_root / "multipart";
    fs::create_directories(multipart_dir);
    const fs::path part1 = multipart_dir / "Qwen3.5-122B-00001-of-00002.gguf";
    const fs::path part2 = multipart_dir / "Qwen3.5-122B-00002-of-00002.gguf";
    {
      std::ofstream out(part1);
      out << "multipart-part-1";
    }
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        "job-4",
        "running",
        "model-4",
        dst_root.string(),
        "multipart",
        {FileUrlForPath(source_path), FileUrlForPath(source_path)},
        {part1.string(), part2.string()},
        part1.filename().string(),
        2048,
        512,
        2,
        "",
        false,
        now,
        now,
    });

    const auto payload_with_running_multipart = service.BuildPayload(db_path.string());
    bool found_running_multipart_entry = false;
    for (const auto& item : payload_with_running_multipart.at("items")) {
      if (item.value("name", std::string{}) == "Qwen3.5-122B" &&
          item.value("kind", std::string{}) == "multipart-gguf") {
        found_running_multipart_entry = true;
        break;
      }
    }
    Expect(
        !found_running_multipart_entry,
        "multipart model with unfinished download job must not appear in catalog");

    {
      std::ofstream out(part2);
      out << "multipart-part-2";
    }
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        "job-4",
        "completed",
        "model-4",
        dst_root.string(),
        "multipart",
        {FileUrlForPath(source_path), FileUrlForPath(source_path)},
        {part1.string(), part2.string()},
        "",
        2048,
        2048,
        2,
        "",
        false,
        now,
        now,
    });
    const auto payload_with_completed_multipart = service.BuildPayload(db_path.string());
    bool found_completed_multipart_entry = false;
    for (const auto& item : payload_with_completed_multipart.at("items")) {
      if (item.value("name", std::string{}) == "Qwen3.5-122B" &&
          item.value("kind", std::string{}) == "multipart-gguf" &&
          item.value("part_count", 0) == 2) {
        found_completed_multipart_entry = true;
        break;
      }
    }
    Expect(
        found_completed_multipart_entry,
        "completed multipart model should appear in catalog after all parts finish");
#endif

    fs::remove_all(temp_root, error);
    std::cout << "model library service tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
