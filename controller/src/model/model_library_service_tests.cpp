#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"
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

naim::ModelLibraryDownloadJobRecord WaitForJobStatus(
    naim::ControllerStore& store,
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

void UpsertHost(
    naim::ControllerStore& store,
    const std::string& node_name,
    const std::string& derived_role,
    const fs::path& storage_root,
    std::uint64_t free_bytes) {
  naim::RegisteredHostRecord host;
  host.node_name = node_name;
  host.registration_state = "registered";
  host.session_state = "connected";
  host.execution_mode = "mixed";
  host.derived_role = derived_role;
  host.role_reason = "test";
  host.capabilities_json =
      nlohmann::json{
          {"storage_root", storage_root.string()},
          {"storage_total_bytes", 500ULL * 1024ULL * 1024ULL * 1024ULL},
          {"storage_free_bytes", free_bytes},
      }
          .dump();
  host.created_at = "2026-03-30 00:00:00";
  host.updated_at = host.created_at;
  store.UpsertRegisteredHost(host);
}

}  // namespace

int main() {
  try {
    const fs::path temp_root = fs::temp_directory_path() / "naim-model-library-tests";
    const fs::path db_path = temp_root / "controller.sqlite";
    const fs::path src_root = temp_root / "src";
    const fs::path dst_root = temp_root / "dst";
    const fs::path storage_root = temp_root / "storage-dst";
    const fs::path source_path = src_root / "smoke.gguf";
    const fs::path target_path = dst_root / "smoke.gguf";
    std::error_code error;
    fs::remove_all(temp_root, error);
    fs::create_directories(src_root);
    fs::create_directories(dst_root);
    fs::create_directories(storage_root);
	    {
	      std::ofstream out(source_path);
	      out << "persistent-model-library-job";
	    }
	    const auto source_payload_size = fs::file_size(source_path);
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
    setenv("NAIM_MODEL_LIBRARY_PYTHON", "/bin/sh", 1);
    setenv("NAIM_MODEL_LIBRARY_CONVERT_SCRIPT", fake_convert.string().c_str(), 1);
    setenv("NAIM_MODEL_LIBRARY_QUANTIZE_BIN", fake_quantize.string().c_str(), 1);

    naim::ControllerStore store(db_path.string());
    store.Initialize();
    UpsertHost(
        store,
        "worker-node-a",
        "worker",
        dst_root,
        500ULL * 1024ULL * 1024ULL * 1024ULL);
    UpsertHost(
        store,
        "storage-node-a",
        "storage",
        storage_root,
        500ULL * 1024ULL * 1024ULL * 1024ULL);
    UpsertHost(
        store,
        "storage-node-low",
        "storage",
        temp_root / "storage-low",
        4);
    const std::string now = "2026-03-30 00:00:00";
    store.UpsertModelLibraryDownloadJob(naim::ModelLibraryDownloadJobRecord{
        .id = "job-1",
        .status = "queued",
        .phase = "queued",
        .model_id = "model-1",
        .node_name = "",
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

    naim::controller::ControllerRequestSupport request_support;
    ModelLibraryService service{ModelLibrarySupport(request_support)};
    const auto payload = service.BuildPayload(db_path.string());
    Expect(payload.at("jobs").is_array(), "jobs payload should be an array");
    Expect(payload.at("jobs").size() == 1, "jobs payload should contain persisted queued job");
    bool found_worker_node = false;
    bool found_storage_node = false;
    for (const auto& node : payload.at("nodes")) {
      if (node.at("node_name").get<std::string>() == "worker-node-a") {
        found_worker_node =
            node.at("derived_role").get<std::string>() == "worker" &&
            node.at("storage_root").get<std::string>() == dst_root.string();
      }
      if (node.at("node_name").get<std::string>() == "storage-node-a") {
        found_storage_node =
            node.at("derived_role").get<std::string>() == "storage";
      }
    }
    Expect(found_worker_node, "payload should expose connected worker node capacity");
    Expect(found_storage_node, "payload should expose connected storage node capacity");

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

    const auto node_download_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "model-node-aware"},
                {"target_node_name", "worker-node-a"},
                {"target_subdir", "node-aware"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(source_path)})},
                {"format", "gguf"},
            }));
    Expect(node_download_response.status_code == 202, "node-aware download should be accepted");
    const auto node_download_job_id =
        nlohmann::json::parse(node_download_response.body).at("job").at("id").get<std::string>();
    const auto node_download_job = store.LoadModelLibraryDownloadJob(node_download_job_id);
    Expect(node_download_job.has_value(), "node-aware download job should be persisted");
    Expect(node_download_job->node_name == "worker-node-a",
           "node-aware download job should persist target node_name");
    Expect(node_download_job->status == "queued",
           "node-aware download should wait for hostd assignment");
    const auto node_assignments =
        store.LoadHostAssignments(
            std::make_optional<std::string>("worker-node-a"),
            naim::HostAssignmentStatus::Pending);
    Expect(
        std::any_of(
            node_assignments.begin(),
            node_assignments.end(),
            [&](const naim::HostAssignment& assignment) {
              return assignment.assignment_type == "model-library-download" &&
                     assignment.desired_state_json.find(node_download_job_id) !=
                         std::string::npos;
            }),
        "node-aware download should enqueue hostd model-library-download assignment");

    const auto storage_download_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "model-storage-aware"},
                {"target_node_name", "storage-node-a"},
                {"target_subdir", "storage-aware"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(source_path)})},
                {"format", "gguf"},
            }));
    Expect(storage_download_response.status_code == 202,
           "storage node should be accepted for non-quantized download");
    const auto storage_download_job_id =
        nlohmann::json::parse(storage_download_response.body).at("job").at("id").get<std::string>();
    const auto storage_download_job = store.LoadModelLibraryDownloadJob(storage_download_job_id);
    Expect(storage_download_job.has_value(), "storage download job should be persisted");
    Expect(storage_download_job->node_name == "storage-node-a",
           "storage download job should persist target node_name");
    Expect(storage_download_job->status == "queued",
           "storage download should wait for hostd assignment");
    const auto storage_assignments =
        store.LoadHostAssignments(
            std::make_optional<std::string>("storage-node-a"),
            naim::HostAssignmentStatus::Pending);
	    Expect(
	        std::any_of(
	            storage_assignments.begin(),
	            storage_assignments.end(),
	            [&](const naim::HostAssignment& assignment) {
	              return assignment.assignment_type == "model-library-download" &&
	                     assignment.desired_state_json.find(storage_download_job_id) !=
	                         std::string::npos;
	            }),
	        "storage download should enqueue hostd model-library-download assignment");
	    auto completed_storage_job = *storage_download_job;
	    completed_storage_job.status = "completed";
	    completed_storage_job.phase = "completed";
	    completed_storage_job.bytes_done = source_payload_size;
	    completed_storage_job.bytes_total = source_payload_size;
	    store.UpsertModelLibraryDownloadJob(completed_storage_job);
	    const auto remote_storage_payload = service.BuildPayload(db_path.string());
	    bool found_remote_storage_item = false;
	    for (const auto& item : remote_storage_payload.at("items")) {
	      if (item.at("path").get<std::string>() == completed_storage_job.target_paths.front()) {
	        found_remote_storage_item =
	            item.at("node_name").get<std::string>() == "storage-node-a" &&
	            item.at("size_bytes").get<std::uintmax_t>() == source_payload_size;
	      }
	    }
	    Expect(
	        found_remote_storage_item,
	        "completed storage-node downloads should remain visible without controller-local files");

	    const auto low_capacity_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "model-storage-low"},
                {"target_node_name", "storage-node-low"},
                {"target_subdir", "storage-aware"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(source_path)})},
                {"format", "gguf"},
            }));
    Expect(low_capacity_response.status_code == 409,
           "download should reject nodes without enough free capacity");

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
    store.UpsertModelLibraryDownloadJob(naim::ModelLibraryDownloadJobRecord{
        .id = "job-2",
        .status = "queued",
        .phase = "queued",
        .model_id = "model-2",
        .node_name = "",
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
    bool hidden_job_visible = false;
    for (const auto& job : visible_after_hide.at("jobs")) {
      if (job.at("id").get<std::string>() == "job-2") {
        hidden_job_visible = true;
      }
    }
    Expect(
        !hidden_job_visible,
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
        recovered_dir / ".naim-model-job-job-3.json";
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
    setenv("NAIM_NODE_MODEL_LIBRARY_ROOTS", dst_root.string().c_str(), 1);
    ModelLibraryService recovery_service{ModelLibrarySupport(request_support)};
    const auto recovered_payload = recovery_service.BuildPayload(db_path.string());
    unsetenv("NAIM_NODE_MODEL_LIBRARY_ROOTS");
    auto recovered_job = store.LoadModelLibraryDownloadJob("job-3");
    Expect(recovered_job.has_value(), "metadata-backed job should be restored into the database");
    Expect(recovered_job->status == "stopped", "restored metadata job should preserve status");
    Expect(
        recovered_job->bytes_done == fs::file_size(recovered_part),
        "restored metadata job should recover bytes_done from partial file");
    bool recovered_job_visible = false;
    for (const auto& job : recovered_payload.at("jobs")) {
      if (job.at("id").get<std::string>() == "job-3") {
        recovered_job_visible = true;
      }
    }
    Expect(
        recovered_job_visible,
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
    store.UpsertModelLibraryDownloadJob(naim::ModelLibraryDownloadJobRecord{
        .id = "job-5",
        .status = "stopped",
        .phase = "stopped",
        .model_id = "model-5",
        .node_name = "",
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
    store.UpsertModelLibraryDownloadJob(naim::ModelLibraryDownloadJobRecord{
        .id = "job-4",
        .status = "running",
        .phase = "running",
        .model_id = "model-4",
        .node_name = "",
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
    store.UpsertModelLibraryDownloadJob(naim::ModelLibraryDownloadJobRecord{
        .id = "job-4",
        .status = "completed",
        .phase = "completed",
        .model_id = "model-4",
        .node_name = "",
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
    unsetenv("NAIM_MODEL_LIBRARY_QUANTIZE_BIN");
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
    Expect(
        completed_conversion_job.job_kind == "download",
        "base conversion job should persist as a download job");
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

    setenv("NAIM_MODEL_LIBRARY_QUANTIZE_BIN", fake_quantize.string().c_str(), 1);
    const auto normalized_download_response = service.EnqueueDownload(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/download",
            nlohmann::json{
                {"model_id", "google/gemma-4-E2B"},
                {"target_root", dst_root.string()},
                {"target_subdir", "converted-ignored-quants"},
                {"source_urls", nlohmann::json::array({FileUrlForPath(safetensors_source)})},
                {"format", "gguf"},
                {"quantizations", nlohmann::json::array({"Q4_K_M", "Q8_0"})},
                {"keep_base_gguf", false},
            }));
    Expect(
        normalized_download_response.status_code == 202,
        "download with deprecated quantization options should still be accepted");
    const auto normalized_download_job_id =
        nlohmann::json::parse(normalized_download_response.body).at("job").at("id").get<std::string>();
    const auto normalized_download_job =
        WaitForJobStatus(store, normalized_download_job_id, "completed");
    const fs::path quantized_root = dst_root / "converted-ignored-quants";
    Expect(
        normalized_download_job.quantizations.empty(),
        "download jobs should normalize quantizations away from upload flow");
    Expect(
        fs::exists(quantized_root / "gemma-4-E2B.gguf"),
        "upload flow should retain only the base GGUF");
    Expect(
        !fs::exists(quantized_root / "gemma-4-E2B-Q4_K_M.gguf"),
        "upload flow should no longer emit quantized variants");

    const auto quantized_response = service.EnqueueQuantization(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/quantize",
            nlohmann::json{
                {"source_path", converted_base_path.string()},
                {"replace_existing", true},
            }));
    Expect(
        quantized_response.status_code == 202,
        "dedicated quantization job should accept the default Q8_0 preset");
    const auto quantized_job_id =
        nlohmann::json::parse(quantized_response.body).at("job").at("id").get<std::string>();
    const auto completed_quantized_job = WaitForJobStatus(store, quantized_job_id, "completed");
    Expect(
        completed_quantized_job.job_kind == "quantization",
        "quantization endpoint should persist job_kind=quantization");
    Expect(
        completed_quantized_job.node_name == "worker-node-a",
        "quantization job should persist the worker node that stores the source model");
    Expect(
        fs::exists(dst_root / "converted-base" / "gemma-4-E2B-Q8_0.gguf"),
        "Q8_0 output should exist after dedicated quantization");
    Expect(
        completed_quantized_job.retained_output_paths.size() == 1,
        "quantization job should persist one retained output path");
    {
      std::ofstream out(dst_root / "converted-base" / "gemma-4-E2B-Q8_0.gguf", std::ios::trunc);
      out << "stale-quantized-output";
    }
    const auto replaced_quantized_response = service.EnqueueQuantization(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/quantize",
            nlohmann::json{
                {"source_path", converted_base_path.string()},
                {"replace_existing", true},
            }));
    Expect(
        replaced_quantized_response.status_code == 202,
        "re-quantization should be accepted for the same source/type");
    const auto replaced_quantized_job_id =
        nlohmann::json::parse(replaced_quantized_response.body).at("job").at("id").get<std::string>();
    const auto replaced_quantized_job =
        WaitForJobStatus(store, replaced_quantized_job_id, "completed");
    Expect(
        ReadFile(dst_root / "converted-base" / "gemma-4-E2B-Q8_0.gguf") ==
            "Q8_0:GGUF:safetensors-payload",
        "re-quantization should replace the previous quantized variant contents");
    Expect(
        replaced_quantized_job.job_kind == "quantization",
        "replacement quantization should remain a quantization job");

    const auto normalized_fp16_response = service.EnqueueQuantization(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/quantize",
            nlohmann::json{
                {"source_path", converted_base_path.string()},
                {"quantization", "fp16"},
                {"replace_existing", true},
            }));
    Expect(
        normalized_fp16_response.status_code == 202,
        "FP16 should be accepted as a supported quantization alias");
    const auto normalized_fp16_job_id =
        nlohmann::json::parse(normalized_fp16_response.body).at("job").at("id").get<std::string>();
    const auto normalized_fp16_job =
        WaitForJobStatus(store, normalized_fp16_job_id, "completed");
    Expect(
        fs::exists(dst_root / "converted-base" / "gemma-4-E2B-FP16.gguf"),
        "FP16 output should exist after explicit FP16 quantization");
    Expect(
        ReadFile(dst_root / "converted-base" / "gemma-4-E2B-FP16.gguf") ==
            "FP16:GGUF:safetensors-payload",
        "explicit FP16 quantization should preserve normalized output naming");
    Expect(
        normalized_fp16_job.quantizations ==
            std::vector<std::string>{"FP16"},
        "FP16 quantization jobs should persist canonical quantization names");

    const fs::path storage_base_path = storage_root / "storage-aware" / "model-storage-aware.gguf";
    fs::create_directories(storage_base_path.parent_path());
    {
      std::ofstream out(storage_base_path);
      out << "storage-node-base-gguf";
    }
    const auto storage_quantization_response = service.EnqueueQuantization(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/quantize",
            nlohmann::json{
                {"source_path", storage_base_path.string()},
                {"replace_existing", true},
            }));
    Expect(
        storage_quantization_response.status_code == 409,
        "quantization should reject base GGUF files that live on storage nodes");

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

    setenv("NAIM_MODEL_LIBRARY_QUANTIZE_BIN", fake_quantize_fail.string().c_str(), 1);
    const auto failed_quantized_response = service.EnqueueQuantization(
        db_path.string(),
        JsonRequest(
            "POST",
            "/api/v1/model-library/quantize",
            nlohmann::json{
                {"source_path", converted_base_path.string()},
                {"quantization", "Q5_K_M"},
                {"replace_existing", true},
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
        failed_job.job_kind == "quantization",
        "failed dedicated quantization job should still persist quantization job kind");
    Expect(
        !failed_job.staging_directory.empty() && fs::exists(failed_job.staging_directory),
        "failed quantization job should retain staging directory for inspection or resume");
    unsetenv("NAIM_MODEL_LIBRARY_PYTHON");
    unsetenv("NAIM_MODEL_LIBRARY_CONVERT_SCRIPT");
    unsetenv("NAIM_MODEL_LIBRARY_QUANTIZE_BIN");

    fs::remove_all(temp_root, error);
    std::cout << "model library service tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
