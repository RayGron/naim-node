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

void WriteExecutableScript(const fs::path& path, const std::string& body) {
  {
    std::ofstream out(path);
    out << body;
  }
  fs::permissions(
      path,
      fs::perms::owner_exec | fs::perms::owner_read | fs::perms::owner_write,
      fs::perm_options::replace);
}

comet::ModelLibraryDownloadJobRecord WaitForJobStatus(
    comet::ControllerStore& store,
    const std::string& job_id,
    const std::string& expected_status) {
  for (int attempt = 0; attempt < 80; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto job = store.LoadModelLibraryDownloadJob(job_id);
    Expect(job.has_value(), "job should remain persisted while waiting");
    if (job->status == expected_status) {
      return *job;
    }
  }
  throw std::runtime_error("timed out waiting for model library job status " + expected_status);
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
    const fs::path tools_root = temp_root / "tools";
    fs::create_directories(tools_root);
    const fs::path fake_convert = tools_root / "fake-convert.sh";
    const fs::path fake_quantize = tools_root / "fake-quantize.sh";
    const fs::path fake_quantize_fail = tools_root / "fake-quantize-fail.sh";
    WriteExecutableScript(
        fake_convert,
        "#!/bin/sh\n"
        "out=\"\"\n"
        "model=\"\"\n"
        "while [ $# -gt 0 ]; do\n"
        "  if [ \"$1\" = \"--outfile\" ]; then out=\"$2\"; shift 2; continue; fi\n"
        "  model=\"$1\"\n"
        "  shift\n"
        "done\n"
        "input=$(find \"$model\" -name '*.safetensors' | sort | head -n 1)\n"
        "mkdir -p \"$(dirname \"$out\")\"\n"
        "printf 'GGUF:' > \"$out\"\n"
        "cat \"$input\" >> \"$out\"\n");
    WriteExecutableScript(
        fake_quantize,
        "#!/bin/sh\n"
        "input=\"$1\"\n"
        "output=\"$2\"\n"
        "quant=\"$3\"\n"
        "mkdir -p \"$(dirname \"$output\")\"\n"
        "printf '%s:' \"$quant\" > \"$output\"\n"
        "cat \"$input\" >> \"$output\"\n");
    WriteExecutableScript(
        fake_quantize_fail,
        "#!/bin/sh\n"
        "exit 17\n");
    setenv("COMET_MODEL_LIBRARY_PYTHON", "/bin/sh", 1);
    setenv("COMET_MODEL_LIBRARY_CONVERT_SCRIPT", fake_convert.string().c_str(), 1);
    setenv("COMET_MODEL_LIBRARY_QUANTIZE_BIN", fake_quantize.string().c_str(), 1);

    comet::ControllerStore store(db_path.string());
    store.Initialize();
    const std::string now = "2026-03-30 00:00:00";
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        .id = "job-1",
        .status = "queued",
        .phase = "queued",
        .model_id = "model-1",
        .target_root = dst_root.string(),
        .target_subdir = "",
        .detected_source_format = "gguf",
        .desired_output_format = "gguf",
        .source_urls = {FileUrlForPath(source_path)},
        .target_paths = {target_path.string()},
        .quantizations = {},
        .retained_output_paths = {target_path.string()},
        .current_item = "",
        .staging_directory = "",
        .bytes_total = std::nullopt,
        .bytes_done = 0,
        .part_count = 1,
        .keep_base_gguf = true,
        .error_message = "",
        .hidden = false,
        .created_at = now,
        .updated_at = now,
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

    const auto set_worker_response = service.SetSkillsFactoryWorker(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/skills-factory-worker",
            nlohmann::json{{"path", target_path.string()}}));
    Expect(
        set_worker_response.status_code == 200,
        "set skills factory worker should succeed for discovered model");
    const auto worker_payload = service.BuildPayload(db_path.string());
    bool found_worker = false;
    for (const auto& item : worker_payload.at("items")) {
      if (item.at("path").get<std::string>() == target_path.string()) {
        found_worker = item.value("skills_factory_worker", false);
      }
    }
    Expect(found_worker, "selected model should be marked as skills_factory_worker");

    const fs::path second_target_path = dst_root / "resume.gguf";
    store.UpsertModelLibraryDownloadJob(comet::ModelLibraryDownloadJobRecord{
        .id = "job-2",
        .status = "queued",
        .phase = "queued",
        .model_id = "model-2",
        .target_root = dst_root.string(),
        .target_subdir = "",
        .detected_source_format = "gguf",
        .desired_output_format = "gguf",
        .source_urls = {FileUrlForPath(source_path)},
        .target_paths = {second_target_path.string()},
        .quantizations = {},
        .retained_output_paths = {second_target_path.string()},
        .current_item = "",
        .staging_directory = "",
        .bytes_total = std::nullopt,
        .bytes_done = 0,
        .part_count = 1,
        .keep_base_gguf = true,
        .error_message = "",
        .hidden = false,
        .created_at = now,
        .updated_at = now,
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

    const auto replace_worker_response = service.SetSkillsFactoryWorker(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/skills-factory-worker",
            nlohmann::json{{"path", second_target_path.string()}}));
    Expect(
        replace_worker_response.status_code == 200,
        "replacing skills factory worker should succeed");
    const auto replaced_worker_payload = service.BuildPayload(db_path.string());
    bool old_worker_active = false;
    bool new_worker_active = false;
    for (const auto& item : replaced_worker_payload.at("items")) {
      const auto path = item.at("path").get<std::string>();
      if (path == target_path.string()) {
        old_worker_active = item.value("skills_factory_worker", false);
      } else if (path == second_target_path.string()) {
        new_worker_active = item.value("skills_factory_worker", false);
      }
    }
    Expect(!old_worker_active, "previous skills factory worker should be cleared");
    Expect(new_worker_active, "replacement model should become the skills factory worker");

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
          {"phase", "stopped"},
          {"model_id", "model-3"},
          {"target_root", dst_root.string()},
          {"target_subdir", "recovered"},
          {"detected_source_format", "gguf"},
          {"desired_output_format", "gguf"},
          {"source_urls", nlohmann::json::array({FileUrlForPath(source_path)})},
          {"target_paths", nlohmann::json::array({recovered_target.string()})},
          {"quantizations", nlohmann::json::array()},
          {"retained_output_paths", nlohmann::json::array({recovered_target.string()})},
          {"current_item", "recovered.gguf"},
          {"staging_directory", ""},
          {"bytes_total", 1024},
          {"bytes_done", 0},
          {"part_count", 1},
          {"keep_base_gguf", true},
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
        .id = "job-5",
        .status = "stopped",
        .phase = "stopped",
        .model_id = "model-5",
        .target_root = dst_root.string(),
        .target_subdir = "",
        .detected_source_format = "gguf",
        .desired_output_format = "gguf",
        .source_urls = {FileUrlForPath(resumable_source_path)},
        .target_paths = {resumed_target_path.string()},
        .quantizations = {},
        .retained_output_paths = {resumed_target_path.string()},
        .current_item = resumed_target_path.filename().string(),
        .staging_directory = "",
        .bytes_total = fs::file_size(resumable_source_path),
        .bytes_done = fs::file_size(resumed_part_path),
        .part_count = 1,
        .keep_base_gguf = true,
        .error_message = "",
        .hidden = false,
        .created_at = now,
        .updated_at = now,
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
        .id = "job-4",
        .status = "running",
        .phase = "running",
        .model_id = "model-4",
        .target_root = dst_root.string(),
        .target_subdir = "multipart",
        .detected_source_format = "gguf",
        .desired_output_format = "gguf",
        .source_urls = {FileUrlForPath(source_path), FileUrlForPath(source_path)},
        .target_paths = {part1.string(), part2.string()},
        .quantizations = {},
        .retained_output_paths = {part1.string(), part2.string()},
        .current_item = part1.filename().string(),
        .staging_directory = "",
        .bytes_total = 2048,
        .bytes_done = 512,
        .part_count = 2,
        .keep_base_gguf = true,
        .error_message = "",
        .hidden = false,
        .created_at = now,
        .updated_at = now,
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
        .id = "job-4",
        .status = "completed",
        .phase = "completed",
        .model_id = "model-4",
        .target_root = dst_root.string(),
        .target_subdir = "multipart",
        .detected_source_format = "gguf",
        .desired_output_format = "gguf",
        .source_urls = {FileUrlForPath(source_path), FileUrlForPath(source_path)},
        .target_paths = {part1.string(), part2.string()},
        .quantizations = {},
        .retained_output_paths = {part1.string(), part2.string()},
        .current_item = "",
        .staging_directory = "",
        .bytes_total = 2048,
        .bytes_done = 2048,
        .part_count = 2,
        .keep_base_gguf = true,
        .error_message = "",
        .hidden = false,
        .created_at = now,
        .updated_at = now,
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

    const fs::path safetensors_source = src_root / "sample-model.safetensors";
    {
      std::ofstream out(safetensors_source);
      out << "safetensors-payload";
    }
    const auto conversion_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "google/gemma-4-E2B"},
                {"target_root", dst_root.string()},
                {"target_subdir", "converted-base"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(safetensors_source)})},
                {"format", "gguf"},
                {"quantizations", nlohmann::json::array()},
                {"keep_base_gguf", true},
            }));
    Expect(conversion_response.status_code == 202, "safetensors conversion job should be accepted");
    const auto conversion_job_id =
        nlohmann::json::parse(conversion_response.body).at("job").at("id").get<std::string>();
    const auto completed_conversion_job = WaitForJobStatus(store, conversion_job_id, "completed");
    const fs::path converted_base_path = dst_root / "converted-base" / "gemma-4-E2B.gguf";
    Expect(fs::exists(converted_base_path), "converted base GGUF should exist");
    Expect(
        ReadFile(converted_base_path) == "GGUF:safetensors-payload",
        "converted base GGUF should come from fake converter output");
    Expect(
        completed_conversion_job.phase == "completed",
        "completed conversion job should expose completed phase");
    Expect(
        !completed_conversion_job.staging_directory.empty(),
        "conversion job should persist its staging directory");
    Expect(
        !fs::exists(completed_conversion_job.staging_directory),
        "staging directory should be removed after successful conversion");

    const auto quantized_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "google/gemma-4-E2B"},
                {"target_root", dst_root.string()},
                {"target_subdir", "converted-quants"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(safetensors_source)})},
                {"format", "gguf"},
                {"quantizations", nlohmann::json::array({"Q4_K_M", "Q8_0"})},
                {"keep_base_gguf", false},
            }));
    Expect(
        quantized_response.status_code == 202,
        "safetensors quantization job should be accepted");
    const auto quantized_job_id =
        nlohmann::json::parse(quantized_response.body).at("job").at("id").get<std::string>();
    const auto completed_quantized_job = WaitForJobStatus(store, quantized_job_id, "completed");
    const fs::path quantized_root = dst_root / "converted-quants";
    Expect(
        fs::exists(quantized_root / "gemma-4-E2B-Q4_K_M.gguf"),
        "Q4_K_M output should exist after quantization");
    Expect(
        fs::exists(quantized_root / "gemma-4-E2B-Q8_0.gguf"),
        "Q8_0 output should exist after quantization");
    Expect(
        !fs::exists(quantized_root / "gemma-4-E2B.gguf"),
        "base GGUF should not be retained when keep_base_gguf is false");
    Expect(
        completed_quantized_job.retained_output_paths.size() == 2,
        "quantized job should persist retained output paths");

    const fs::path mixed_config_path = src_root / "config.json";
    const fs::path mixed_tokenizer_path = src_root / "tokenizer.json";
    {
      std::ofstream out(mixed_config_path);
      out << "{\"architectures\":[\"FakeModel\"]}";
    }
    {
      std::ofstream out(mixed_tokenizer_path);
      out << "{\"tokenizer\":\"fake\"}";
    }
    const auto mixed_bundle_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "google/gemma-4-31B-it"},
                {"target_root", dst_root.string()},
                {"target_subdir", "converted-mixed"},
                {"source_urls",
                 nlohmann::json::array(
                     {FileUrlForPath(mixed_config_path),
                      FileUrlForPath(mixed_tokenizer_path),
                      FileUrlForPath(safetensors_source)})},
                {"format", "gguf"},
                {"quantizations", nlohmann::json::array()},
                {"keep_base_gguf", true},
            }));
    Expect(
        mixed_bundle_response.status_code == 202,
        "mixed HF metadata plus safetensors bundle should be accepted");
    const auto mixed_job_id =
        nlohmann::json::parse(mixed_bundle_response.body).at("job").at("id").get<std::string>();
    const auto completed_mixed_job = WaitForJobStatus(store, mixed_job_id, "completed");
    Expect(
        completed_mixed_job.detected_source_format == "safetensors",
        "mixed HF metadata bundle should persist safetensors source format");
    Expect(
        fs::exists(dst_root / "converted-mixed" / "gemma-4-31B-it.gguf"),
        "mixed HF metadata bundle should still produce a GGUF output");

    setenv("COMET_MODEL_LIBRARY_QUANTIZE_BIN", fake_quantize_fail.string().c_str(), 1);
    const auto failed_quantized_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "google/gemma-4-E2B"},
                {"target_root", dst_root.string()},
                {"target_subdir", "converted-failed"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(safetensors_source)})},
                {"format", "gguf"},
                {"quantizations", nlohmann::json::array({"Q4_K_M"})},
                {"keep_base_gguf", true},
            }));
    Expect(
        failed_quantized_response.status_code == 202,
        "failed quantization job should still be accepted");
    const auto failed_job_id =
        nlohmann::json::parse(failed_quantized_response.body).at("job").at("id").get<std::string>();
    const auto failed_job = WaitForJobStatus(store, failed_job_id, "failed");
    Expect(
        failed_job.phase == "quantizing",
        "failed quantization job should preserve failing phase");
    Expect(
        !failed_job.staging_directory.empty() && fs::exists(failed_job.staging_directory),
        "failed quantization job should retain staging directory for inspection or resume");
    unsetenv("COMET_MODEL_LIBRARY_PYTHON");
    unsetenv("COMET_MODEL_LIBRARY_CONVERT_SCRIPT");
    unsetenv("COMET_MODEL_LIBRARY_QUANTIZE_BIN");

    fs::remove_all(temp_root, error);
    std::cout << "model library service tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
