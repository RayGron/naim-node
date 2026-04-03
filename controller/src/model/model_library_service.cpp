#include "model/model_library_service.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <future>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

#if !defined(_WIN32)
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"

using nlohmann::json;

namespace {

constexpr std::string_view kJobMetadataPrefix = ".comet-model-job-";
constexpr std::string_view kJobMetadataSuffix = ".json";
constexpr std::array<std::string_view, 4> kKnownGgufQuantizations = {
    "Q8_0",
    "Q5_K_M",
    "Q4_K_M",
    "IQ4_NL",
};

struct MultipartGroup {
  std::string root;
  std::string name;
  std::vector<std::string> paths;
  std::uintmax_t size_bytes = 0;
  int part_total = 0;
};

struct PendingDownloadState {
  std::set<std::string> target_paths;
  std::set<std::string> multipart_group_keys;
};

class DownloadStoppedError : public std::runtime_error {
 public:
  DownloadStoppedError() : std::runtime_error("download stopped") {}
};

bool LooksLikeJobMetadataFilename(const std::string& filename) {
  return filename.size() >
             kJobMetadataPrefix.size() + kJobMetadataSuffix.size() &&
         filename.rfind(kJobMetadataPrefix.data(), 0) == 0 &&
         filename.size() >= kJobMetadataSuffix.size() &&
         filename.compare(
             filename.size() - kJobMetadataSuffix.size(),
             kJobMetadataSuffix.size(),
             kJobMetadataSuffix.data()) == 0;
}

bool ShouldScanDefaultModelRoots(const std::string& db_path) {
  std::error_code error;
  const auto normalized = std::filesystem::weakly_canonical(db_path, error);
  if (error) {
    return false;
  }
  return normalized == "/var/lib/comet-node/hostd-state/controller.sqlite" ||
         normalized == "/var/lib/comet-node/controller.sqlite";
}

bool IsDownloadJobComplete(
    const comet::ModelLibraryDownloadJobRecord& job) {
  if (job.status != "completed") {
    return false;
  }
  if (job.target_paths.empty()) {
    return false;
  }
  for (const auto& target_path_text : job.target_paths) {
    const std::filesystem::path target_path(target_path_text);
    std::error_code error;
    if (!std::filesystem::exists(target_path, error) || error) {
      return false;
    }
    error.clear();
    if (std::filesystem::exists(target_path.string() + ".part", error) && !error) {
      return false;
    }
  }
  return true;
}

std::uintmax_t ExistingDownloadBytesForTarget(
    const std::filesystem::path& target_path) {
  std::error_code error;
  if (std::filesystem::exists(target_path, error) && !error) {
    return std::filesystem::file_size(target_path, error);
  }
  error.clear();
  const std::filesystem::path part_path = target_path.string() + ".part";
  if (std::filesystem::exists(part_path, error) && !error) {
    return std::filesystem::file_size(part_path, error);
  }
  return 0;
}

std::uintmax_t ExistingDownloadBytesForJob(
    const comet::ModelLibraryDownloadJobRecord& job) {
  std::uintmax_t total = 0;
  for (const auto& target_path_text : job.target_paths) {
    total += ExistingDownloadBytesForTarget(std::filesystem::path(target_path_text));
  }
  return total;
}

}  // namespace

ModelLibraryService::ModelLibraryService(ModelLibrarySupport support)
    : support_(std::move(support)),
      state_(std::make_shared<State>()),
      conversion_service_(std::make_shared<ModelConversionService>()) {}

json ModelLibraryService::BuildPayload(const std::string& db_path) const {
  ResumePersistentJobs(db_path);
  const auto roots = DiscoverRoots(db_path);
  const auto entries = ScanEntries(db_path);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto worker_path = store.LoadControllerSetting("skills_factory_worker_model_path");
  json items = json::array();
  for (const auto& entry : entries) {
    items.push_back(json{
        {"path", entry.path},
        {"name", entry.name},
        {"kind", entry.kind},
        {"format", entry.format},
        {"quantization", entry.quantization},
        {"quantized_from_path",
         entry.quantized_from_path.empty() ? json(nullptr)
                                           : json(entry.quantized_from_path)},
        {"root", entry.root},
        {"paths", entry.paths},
        {"size_bytes", entry.size_bytes},
        {"part_count", entry.part_count},
        {"referenced_by", entry.referenced_by},
        {"deletable", entry.deletable},
        {"skills_factory_worker", worker_path.has_value() && *worker_path == entry.path},
    });
  }
  json jobs = json::array();
  for (const auto& job : store.LoadModelLibraryDownloadJobs()) {
    if (job.hidden) {
      continue;
    }
    jobs.push_back(BuildJobPayload(job));
  }
  return json{{"items", items}, {"roots", roots}, {"jobs", jobs}};
}

HttpResponse ModelLibraryService::SetSkillsFactoryWorker(
    const std::string& db_path,
    const HttpRequest& request) const {
  const json body = support_.parse_json_request_body(request);
  const std::string path = body.value("path", std::string{});
  if (path.empty() || !IsUsableAbsoluteHostPath(path)) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "path must be an absolute host path"}},
        {});
  }
  const auto entries = ScanEntries(db_path);
  const auto normalized_path = NormalizePathString(path);
  const auto it = std::find_if(
      entries.begin(),
      entries.end(),
      [&](const ModelLibraryEntry& entry) { return entry.path == normalized_path; });
  if (it == entries.end()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "model entry not found"}},
        {});
  }

  comet::ControllerStore store(db_path);
  store.Initialize();
  store.UpsertControllerSetting("skills_factory_worker_model_path", normalized_path);
  return support_.build_json_response(
      200,
      json{{"status", "updated"}, {"path", normalized_path}},
      {});
}

HttpResponse ModelLibraryService::DeleteEntryByPath(
    const std::string& db_path,
    const HttpRequest& request) const {
  json body = json::object();
  if (!request.body.empty()) {
    body = support_.parse_json_request_body(request);
  }
  const std::string path = [&]() {
    if (body.contains("path") && body.at("path").is_string()) {
      return body.at("path").get<std::string>();
    }
    const auto query = support_.find_query_string(request, "path");
    return query.value_or(std::string{});
  }();
  if (path.empty() || !IsUsableAbsoluteHostPath(path)) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "path must be an absolute host path"}},
        {});
  }
  const auto entries = ScanEntries(db_path);
  const auto normalized_path = NormalizePathString(path);
  const auto it = std::find_if(
      entries.begin(),
      entries.end(),
      [&](const ModelLibraryEntry& entry) {
        return entry.path == normalized_path;
      });
  if (it == entries.end()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "model entry not found"}},
        {});
  }
  if (!it->deletable) {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "model is referenced by one or more planes"},
             {"referenced_by", it->referenced_by}},
        {});
  }

  std::vector<std::string> deleted_paths;
  std::error_code error;
  if (it->kind == "directory") {
    std::filesystem::remove_all(it->path, error);
    if (error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.message()}},
          {});
    }
    deleted_paths.push_back(it->path);
  } else {
    for (const auto& current_path : it->paths) {
      std::filesystem::remove(current_path, error);
      if (error) {
        return support_.build_json_response(
            500,
            json{{"status", "internal_error"}, {"message", error.message()}},
            {});
      }
      deleted_paths.push_back(current_path);
    }
  }
  return support_.build_json_response(
      200,
      json{{"status", "deleted"},
           {"path", it->path},
           {"deleted_paths", deleted_paths}},
      {});
}

HttpResponse ModelLibraryService::EnqueueDownload(
    const std::string& db_path,
    const HttpRequest& request) const {
  const json body = support_.parse_json_request_body(request);
  const std::string target_root = body.value("target_root", std::string{});
  const std::string target_subdir = body.value("target_subdir", std::string{});
  const std::string model_id = body.value("model_id", std::string{});
  const std::string source_url = body.value("source_url", std::string{});
  std::vector<std::string> source_urls;
  if (body.contains("source_urls") && body.at("source_urls").is_array()) {
    source_urls = body.at("source_urls").get<std::vector<std::string>>();
  } else if (!source_url.empty()) {
    source_urls.push_back(source_url);
  }
  const std::string detected_source_format = DetectModelSourceFormat(source_urls);
  const std::string desired_output_format = NormalizeModelOutputFormat(
      body.value("format", detected_source_format));
  const std::vector<std::string> quantizations;
  const bool keep_base_gguf = true;
  if (target_root.empty() || !IsUsableAbsoluteHostPath(target_root)) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "target_root must be an absolute host path"}},
        {});
  }
  if (source_urls.empty()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "source_url or source_urls is required"}},
        {});
  }
  if (detected_source_format == "unknown") {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "failed to detect source format from source_urls"}},
        {});
  }
  if (desired_output_format != "gguf" && desired_output_format != "safetensors") {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "format must be gguf or safetensors"}},
        {});
  }
  if (detected_source_format == "gguf" && desired_output_format != "gguf") {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "gguf sources can only retain GGUF output format"}},
        {});
  }

  std::filesystem::path destination_root(target_root);
  if (!target_subdir.empty()) {
    destination_root /= target_subdir;
  }

  std::vector<std::string> target_paths;
  try {
    if (detected_source_format == "safetensors" &&
        desired_output_format == "gguf") {
      const std::string job_id = GenerateJobId();
      const std::filesystem::path staging_directory =
          destination_root /
          (std::string(kJobMetadataPrefix) + job_id + "-staging");
      const auto plan = conversion_service_->BuildPlan(ModelConversionService::Request{
          .job_id = job_id,
          .model_id = model_id,
          .destination_root = destination_root,
          .source_urls = source_urls,
          .detected_source_format = detected_source_format,
          .desired_output_format = desired_output_format,
          .quantizations = NormalizeQuantizationValues(quantizations),
          .keep_base_gguf = keep_base_gguf,
          .staging_directory = staging_directory,
      });
      for (const auto& retained_output_path : plan.retained_output_paths) {
        target_paths.push_back(NormalizePathString(retained_output_path));
      }
      ModelLibraryDownloadJob job;
      job.id = job_id;
      job.job_kind = "download";
      job.model_id = model_id;
      job.target_root = NormalizePathString(std::filesystem::path(target_root));
      job.target_subdir = target_subdir;
      job.detected_source_format = detected_source_format;
      job.desired_output_format = desired_output_format;
      job.source_urls = source_urls;
      job.target_paths = target_paths;
      job.quantizations = NormalizeQuantizationValues(quantizations);
      job.retained_output_paths = target_paths;
      job.staging_directory = NormalizePathString(plan.staging_directory);
      job.keep_base_gguf = keep_base_gguf;
      job.part_count = static_cast<int>(source_urls.size());
      job.created_at = support_.utc_now_sql_timestamp();
      job.updated_at = job.created_at;
      comet::ControllerStore store(db_path);
      store.Initialize();
      store.UpsertModelLibraryDownloadJob(job);
      PersistDownloadJobMetadata(job);
      StartDownloadJob(db_path, job_id);
      return support_.build_json_response(
          202,
          json{{"status", "accepted"}, {"job", BuildJobPayload(job)}},
          {});
    }
    target_paths.reserve(source_urls.size());
    for (std::size_t index = 0; index < source_urls.size(); ++index) {
      const auto filename =
          source_urls.size() == 1 && body.contains("target_filename") &&
                  body.at("target_filename").is_string() &&
                  !body.at("target_filename").get<std::string>().empty()
              ? body.at("target_filename").get<std::string>()
              : FilenameFromUrl(source_urls.at(index));
      target_paths.push_back(NormalizePathString(destination_root / filename));
    }
  } catch (const std::exception& error) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", error.what()}},
        {});
  }

  const std::string job_id = GenerateJobId();
  ModelLibraryDownloadJob job;
  job.id = job_id;
  job.job_kind = "download";
  job.model_id = model_id;
  job.target_root = NormalizePathString(std::filesystem::path(target_root));
  job.target_subdir = target_subdir;
  job.detected_source_format = detected_source_format;
  job.desired_output_format = desired_output_format;
  job.source_urls = source_urls;
  job.target_paths = target_paths;
  job.retained_output_paths = target_paths;
  job.quantizations = NormalizeQuantizationValues(quantizations);
  job.keep_base_gguf = keep_base_gguf;
  job.part_count = static_cast<int>(source_urls.size());
  job.created_at = support_.utc_now_sql_timestamp();
  job.updated_at = job.created_at;
  comet::ControllerStore store(db_path);
  store.Initialize();
  store.UpsertModelLibraryDownloadJob(job);
  PersistDownloadJobMetadata(job);
  StartDownloadJob(db_path, job_id);
  return support_.build_json_response(
      202,
      json{{"status", "accepted"}, {"job", BuildJobPayload(job)}},
      {});
}

HttpResponse ModelLibraryService::EnqueueQuantization(
    const std::string& db_path,
    const HttpRequest& request) const {
  const json body = support_.parse_json_request_body(request);
  const std::string source_path_value = body.value("source_path", std::string{});
  const std::string quantization = Trim(body.value("quantization", std::string{}));
  const bool replace_existing = body.value("replace_existing", true);
  if (source_path_value.empty() || !IsUsableAbsoluteHostPath(source_path_value)) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "source_path must be an absolute host path"}},
        {});
  }
  if (quantization.empty()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "quantization is required"}},
        {});
  }
  const auto normalized_quantizations = NormalizeQuantizationValues({quantization});
  if (normalized_quantizations.size() != 1 ||
      std::none_of(
          kKnownGgufQuantizations.begin(),
          kKnownGgufQuantizations.end(),
          [&](std::string_view value) { return value == normalized_quantizations.front(); })) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "quantization must be a single supported value"}},
        {});
  }

  const std::filesystem::path source_path(source_path_value);
  std::error_code error;
  if (!std::filesystem::exists(source_path, error) || error ||
      !std::filesystem::is_regular_file(source_path, error) || error ||
      !EndsWithIgnoreCase(source_path.filename().string(), ".gguf")) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"},
             {"message", "source GGUF model not found"}},
        {});
  }

  const auto entries = ScanEntries(db_path);
  const auto normalized_source_path = NormalizePathString(source_path);
  const auto entry_it = std::find_if(
      entries.begin(),
      entries.end(),
      [&](const ModelLibraryEntry& entry) {
        return entry.kind == "file" && entry.format == "gguf" &&
               entry.path == normalized_source_path;
      });
  if (entry_it == entries.end()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"},
             {"message", "source GGUF entry not found in model library"}},
        {});
  }
  const auto source_quantization = entry_it->quantization.empty() ? "base" : entry_it->quantization;
  if (source_quantization != "base") {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"},
             {"message", "only base GGUF models can be quantized"}},
        {});
  }

  const auto base_stem = StripKnownQuantizationSuffix(source_path.stem().string());
  const auto retained_output_path =
      source_path.parent_path() / (base_stem + "-" + normalized_quantizations.front() + ".gguf");
  const std::string job_id = GenerateJobId();
  const auto staging_directory =
      source_path.parent_path() /
      (std::string(kJobMetadataPrefix) + job_id + "-staging");
  try {
    const auto plan = conversion_service_->BuildQuantizationPlan(
        ModelConversionService::QuantizationRequest{
            .job_id = job_id,
            .source_path = source_path,
            .quantization = normalized_quantizations.front(),
            .staging_directory = staging_directory,
            .retained_output_path = retained_output_path,
            .replace_existing = replace_existing,
        });
    ModelLibraryDownloadJob job;
    job.id = job_id;
    job.job_kind = "quantization";
    job.model_id = BuildQuantizedDisplayName(entry_it->name, normalized_quantizations.front());
    job.target_root = NormalizePathString(source_path.parent_path());
    job.target_subdir = "";
    job.detected_source_format = "gguf";
    job.desired_output_format = "gguf";
    job.source_urls = {normalized_source_path};
    job.target_paths = {NormalizePathString(plan.retained_output_path)};
    job.quantizations = {normalized_quantizations.front()};
    job.retained_output_paths = {NormalizePathString(plan.retained_output_path)};
    job.current_item = source_path.filename().string();
    job.staging_directory = NormalizePathString(plan.staging_directory);
    job.bytes_total = FileSizeIfExists(source_path);
    job.bytes_done = 0;
    job.part_count = 1;
    job.keep_base_gguf = true;
    job.created_at = support_.utc_now_sql_timestamp();
    job.updated_at = job.created_at;
    comet::ControllerStore store(db_path);
    store.Initialize();
    store.UpsertModelLibraryDownloadJob(job);
    PersistDownloadJobMetadata(job);
    StartDownloadJob(db_path, job_id);
    return support_.build_json_response(
        202,
        json{{"status", "accepted"}, {"job", BuildJobPayload(job)}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", error.what()}},
        {});
  }
}

HttpResponse ModelLibraryService::StopDownloadJob(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto job_id = ExtractJobId(request);
  if (!job_id.has_value()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", "job_id is required"}},
        {});
  }
  const auto job = LoadDownloadJob(db_path, *job_id);
  if (!job.has_value()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "download job not found"}},
        {});
  }
  if (job->status == "completed" || job->status == "failed") {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "only queued or running downloads can be stopped"},
             {"job", BuildJobPayload(*job)}},
        {});
  }
  if (job->status == "stopped" || job->status == "stopping") {
    return support_.build_json_response(
        200,
        json{{"status", "ok"}, {"job", BuildJobPayload(*job)}},
        {});
  }
  if (job->status == "queued") {
    UpdateJob(
        db_path,
        *job_id,
        [](ModelLibraryDownloadJob& current) {
          current.status = "stopped";
          current.phase = "stopped";
          current.current_item.clear();
          current.error_message.clear();
        });
    const auto stopped = LoadDownloadJob(db_path, *job_id);
    return support_.build_json_response(
        200,
        json{{"status", "stopped"},
             {"job", stopped.has_value() ? BuildJobPayload(*stopped)
                                          : json{{"id", *job_id}}}},
        {});
  }
  RequestStop(*job_id);
  UpdateJob(
      db_path,
      *job_id,
      [](ModelLibraryDownloadJob& current) {
        current.status = "stopping";
        current.phase = current.phase.empty() ? "stopping" : current.phase;
      });
  const auto stopping = LoadDownloadJob(db_path, *job_id);
  return support_.build_json_response(
      202,
      json{{"status", "stopping"},
           {"job", stopping.has_value() ? BuildJobPayload(*stopping)
                                        : json{{"id", *job_id}}}},
      {});
}

HttpResponse ModelLibraryService::ResumeDownloadJob(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto job_id = ExtractJobId(request);
  if (!job_id.has_value()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", "job_id is required"}},
        {});
  }
  const auto job = LoadDownloadJob(db_path, *job_id);
  if (!job.has_value()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "download job not found"}},
        {});
  }
  if (job->status == "running" || job->status == "stopping") {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "running downloads cannot be resumed"},
             {"job", BuildJobPayload(*job)}},
        {});
  }
  if (job->status == "completed") {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "completed downloads cannot be resumed"},
             {"job", BuildJobPayload(*job)}},
        {});
  }

  ClearStopRequest(*job_id);
  UpdateJob(
      db_path,
      *job_id,
      [](ModelLibraryDownloadJob& current) {
        current.status = "queued";
        current.phase = "queued";
        current.current_item.clear();
        current.error_message.clear();
        current.bytes_done = ExistingDownloadBytesForJob(current);
      });
  StartDownloadJob(db_path, *job_id);
  const auto resumed = LoadDownloadJob(db_path, *job_id);
  return support_.build_json_response(
      202,
      json{{"status", "accepted"},
           {"job", resumed.has_value() ? BuildJobPayload(*resumed)
                                       : json{{"id", *job_id}}}},
      {});
}

HttpResponse ModelLibraryService::DeleteDownloadJob(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto job_id = ExtractJobId(request);
  if (!job_id.has_value()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", "job_id is required"}},
        {});
  }
  const auto job = LoadDownloadJob(db_path, *job_id);
  if (!job.has_value()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "download job not found"}},
        {});
  }
  if (job->status == "running" || job->status == "stopping") {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "stop the download before deleting it"},
             {"job", BuildJobPayload(*job)}},
        {});
  }

  std::vector<std::string> deleted_paths;
  std::string delete_error_message;
  for (const auto& target_path_text : job->target_paths) {
    if (!RemovePathIfExists(
            std::filesystem::path(target_path_text),
            &deleted_paths,
            &delete_error_message)) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", delete_error_message},
               {"job", BuildJobPayload(*job)}},
          {});
    }
    if (!RemovePathIfExists(
            std::filesystem::path(target_path_text).string() + ".part",
            &deleted_paths,
            &delete_error_message)) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", delete_error_message},
               {"job", BuildJobPayload(*job)}},
          {});
    }
  }
  if (!job->staging_directory.empty()) {
    if (!RemovePathIfExists(
            std::filesystem::path(job->staging_directory),
            &deleted_paths,
            &delete_error_message)) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", delete_error_message},
               {"job", BuildJobPayload(*job)}},
          {});
    }
  }
  ClearStopRequest(*job_id);
  comet::ControllerStore store(db_path);
  store.Initialize();
  RemoveDownloadJobMetadata(*job);
  store.DeleteModelLibraryDownloadJob(*job_id);
  return support_.build_json_response(
      200,
      json{{"status", "deleted"},
           {"job_id", *job_id},
           {"deleted_paths", deleted_paths}},
      {});
}

HttpResponse ModelLibraryService::HideDownloadJob(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto job_id = ExtractJobId(request);
  if (!job_id.has_value()) {
    return support_.build_json_response(
        400,
        json{{"status", "bad_request"}, {"message", "job_id is required"}},
        {});
  }
  const auto job = LoadDownloadJob(db_path, *job_id);
  if (!job.has_value()) {
    return support_.build_json_response(
        404,
        json{{"status", "not_found"}, {"message", "download job not found"}},
        {});
  }
  if (job->status == "running" || job->status == "stopping" ||
      job->status == "queued") {
    return support_.build_json_response(
        409,
        json{{"status", "conflict"},
             {"message", "only completed, failed, or stopped downloads can be hidden"},
             {"job", BuildJobPayload(*job)}},
        {});
  }

  UpdateJob(
      db_path,
      *job_id,
      [](ModelLibraryDownloadJob& current) {
        current.hidden = true;
      });
  return support_.build_json_response(
      200,
      json{{"status", "hidden"}, {"job_id", *job_id}},
      {});
}

bool ModelLibraryService::EndsWithIgnoreCase(
    const std::string& value,
    const std::string& suffix) {
  if (value.size() < suffix.size()) {
    return false;
  }
  const std::size_t offset = value.size() - suffix.size();
  for (std::size_t index = 0; index < suffix.size(); ++index) {
    if (std::tolower(static_cast<unsigned char>(value[offset + index])) !=
        std::tolower(static_cast<unsigned char>(suffix[index]))) {
      return false;
    }
  }
  return true;
}

bool ModelLibraryService::IsAllDigits(const std::string& value) {
  return !value.empty() &&
         std::all_of(
             value.begin(),
             value.end(),
             [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

std::string ModelLibraryService::Trim(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

std::string ModelLibraryService::Lowercase(const std::string& value) {
  std::string result = value;
  std::transform(
      result.begin(),
      result.end(),
      result.begin(),
      [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
      });
  return result;
}

std::string ModelLibraryService::NormalizePathString(
    const std::filesystem::path& path) {
  return path.lexically_normal().string();
}

bool ModelLibraryService::IsUsableAbsoluteHostPath(const std::string& value) {
  return !value.empty() && value.front() == '/' &&
         value.rfind("/comet/", 0) != 0;
}

std::string ModelLibraryService::FilenameFromUrl(
    const std::string& source_url) {
  std::string trimmed = source_url;
  const auto query_pos = trimmed.find('?');
  if (query_pos != std::string::npos) {
    trimmed = trimmed.substr(0, query_pos);
  }
  const auto fragment_pos = trimmed.find('#');
  if (fragment_pos != std::string::npos) {
    trimmed = trimmed.substr(0, fragment_pos);
  }
  const auto slash_pos = trimmed.find_last_of('/');
  const std::string filename =
      slash_pos == std::string::npos ? trimmed : trimmed.substr(slash_pos + 1);
  if (filename.empty()) {
    throw std::runtime_error(
        "failed to infer filename from URL: " + source_url);
  }
  return filename;
}

std::string ModelLibraryService::DetectModelSourceFormat(
    const std::vector<std::string>& source_urls) {
  return ModelConversionService::DetectSourceFormat(source_urls);
}

std::string ModelLibraryService::NormalizeModelOutputFormat(
    const std::string& value) {
  return ModelConversionService::NormalizeOutputFormat(value);
}

std::vector<std::string> ModelLibraryService::NormalizeQuantizationValues(
    const std::vector<std::string>& values) {
  return ModelConversionService::NormalizeQuantizations(values);
}

std::optional<std::uintmax_t> ModelLibraryService::ProbeContentLength(
    const std::string& source_url) const {
  const std::string temp_headers =
      (std::filesystem::temp_directory_path() /
       ("comet-model-head-" +
        std::to_string(comet::platform::CurrentProcessId()) + "-" +
        std::to_string(state_->job_counter.fetch_add(1)) + ".txt"))
          .string();
  const std::string command =
      "/usr/bin/curl --connect-timeout 5 --max-time 15 -fsSLI '" + source_url +
      "' > '" + temp_headers + "' 2>/dev/null || true";
  std::system(command.c_str());
  std::ifstream input(temp_headers);
  std::filesystem::remove(temp_headers);
  std::string line;
  std::optional<std::uintmax_t> last_content_length;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key = trimmed.substr(0, colon);
    if (Lowercase(key) != "content-length") {
      continue;
    }
    try {
      last_content_length = static_cast<std::uintmax_t>(
          std::stoull(Trim(trimmed.substr(colon + 1))));
    } catch (...) {
      return std::nullopt;
    }
  }
  return last_content_length;
}

std::optional<std::uintmax_t> ModelLibraryService::FileSizeIfExists(
    const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error) {
    return std::nullopt;
  }
  if (std::filesystem::is_regular_file(path, error)) {
    const auto size = std::filesystem::file_size(path, error);
    return error ? std::nullopt : std::optional<std::uintmax_t>(size);
  }
  if (!std::filesystem::is_directory(path, error) || error) {
    return std::nullopt;
  }
  std::uintmax_t total = 0;
  for (std::filesystem::recursive_directory_iterator iterator(
           path,
           std::filesystem::directory_options::skip_permission_denied,
           error);
       !error && iterator != std::filesystem::recursive_directory_iterator();
       iterator.increment(error)) {
    if (!iterator->is_regular_file(error) || error) {
      error.clear();
      continue;
    }
    total += iterator->file_size(error);
    if (error) {
      return std::nullopt;
    }
  }
  return error ? std::nullopt : std::optional<std::uintmax_t>(total);
}

bool ModelLibraryService::RemovePathIfExists(
    const std::filesystem::path& path,
    std::vector<std::string>* removed_paths,
    std::string* error_message) {
  std::error_code error;
  if (!std::filesystem::exists(path, error)) {
    return !error;
  }
  if (error) {
    if (error_message != nullptr) {
      *error_message = error.message();
    }
    return false;
  }
  const auto normalized = NormalizePathString(path);
  if (std::filesystem::is_directory(path, error)) {
    std::filesystem::remove_all(path, error);
  } else {
    std::filesystem::remove(path, error);
  }
  if (error) {
    if (error_message != nullptr) {
      *error_message = error.message();
    }
    return false;
  }
  if (removed_paths != nullptr) {
    removed_paths->push_back(normalized);
  }
  return true;
}

bool ModelLibraryService::LooksLikeRecognizedModelDirectory(
    const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error ||
      !std::filesystem::is_directory(path, error) || error) {
    return false;
  }
  return std::filesystem::exists(path / "config.json", error) ||
         std::filesystem::exists(path / "params.json", error);
}

bool ModelLibraryService::ParseMultipartGgufFilename(
    const std::string& filename,
    std::string* prefix,
    int* part_index,
    int* part_total) {
  if (!EndsWithIgnoreCase(filename, ".gguf")) {
    return false;
  }
  const std::string stem = filename.substr(0, filename.size() - 5);
  const auto of_pos = stem.rfind("-of-");
  if (of_pos == std::string::npos) {
    return false;
  }
  const auto part_sep = stem.rfind('-', of_pos - 1);
  if (part_sep == std::string::npos) {
    return false;
  }
  const std::string part_index_text =
      stem.substr(part_sep + 1, of_pos - (part_sep + 1));
  const std::string part_total_text = stem.substr(of_pos + 4);
  if (!IsAllDigits(part_index_text) || !IsAllDigits(part_total_text)) {
    return false;
  }
  if (prefix != nullptr) {
    *prefix = stem.substr(0, part_sep);
  }
  if (part_index != nullptr) {
    *part_index = std::stoi(part_index_text);
  }
  if (part_total != nullptr) {
    *part_total = std::stoi(part_total_text);
  }
  return true;
}

std::string ModelLibraryService::NormalizeJobKind(const std::string& value) {
  const auto normalized = Lowercase(Trim(value));
  return normalized == "quantization" ? "quantization" : "download";
}

bool ModelLibraryService::IsQuantizationJob(const ModelLibraryDownloadJob& job) {
  return NormalizeJobKind(job.job_kind) == "quantization";
}

std::string ModelLibraryService::DetectEntryQuantization(const std::string& stem_or_prefix) {
  for (const auto quantization : kKnownGgufQuantizations) {
    const std::string suffix = "-" + std::string(quantization);
    if (stem_or_prefix.size() > suffix.size() &&
        stem_or_prefix.compare(stem_or_prefix.size() - suffix.size(), suffix.size(), suffix) == 0) {
      return std::string(quantization);
    }
  }
  return "base";
}

std::string ModelLibraryService::StripKnownQuantizationSuffix(const std::string& stem) {
  const auto quantization = DetectEntryQuantization(stem);
  if (quantization == "base") {
    return stem;
  }
  const std::string suffix = "-" + quantization;
  return stem.substr(0, stem.size() - suffix.size());
}

std::string ModelLibraryService::BuildQuantizedDisplayName(
    const std::string& raw_name,
    const std::string& quantization) {
  const std::string normalized =
      EndsWithIgnoreCase(raw_name, ".gguf")
          ? raw_name.substr(0, raw_name.size() - 5)
          : raw_name;
  if (quantization == "base") {
    return normalized;
  }
  return StripKnownQuantizationSuffix(normalized) + " - " + quantization;
}

std::vector<std::string> ModelLibraryService::DiscoverRoots(
    const std::string& db_path) const {
  comet::ControllerStore store(db_path);
  const auto desired_states = store.LoadDesiredStates();
  const auto jobs = store.LoadModelLibraryDownloadJobs();
  std::set<std::string> roots;
  if (ShouldScanDefaultModelRoots(db_path)) {
    for (const std::string& candidate : {
             std::string("/mnt/shared-storage/models"),
             std::string("/mnt/shared-storage/models/gguf"),
         }) {
      if (IsUsableAbsoluteHostPath(candidate)) {
        roots.insert(NormalizePathString(candidate));
      }
    }
  }
  if (const char* env_value = std::getenv("COMET_NODE_MODEL_LIBRARY_ROOTS");
      env_value != nullptr && *env_value != '\0') {
    std::string current;
    for (char ch : std::string(env_value)) {
      if (ch == ':' || ch == ';') {
        const std::string trimmed = Trim(current);
        if (IsUsableAbsoluteHostPath(trimmed)) {
          roots.insert(NormalizePathString(trimmed));
        }
        current.clear();
      } else {
        current.push_back(ch);
      }
    }
    const std::string trimmed = Trim(current);
    if (IsUsableAbsoluteHostPath(trimmed)) {
      roots.insert(NormalizePathString(trimmed));
    }
  }
  for (const auto& desired_state : desired_states) {
    if (!desired_state.bootstrap_model.has_value() ||
        !desired_state.bootstrap_model->local_path.has_value()) {
      continue;
    }
    const std::string& local_path = *desired_state.bootstrap_model->local_path;
    if (!IsUsableAbsoluteHostPath(local_path)) {
      continue;
    }
    std::filesystem::path path(local_path);
    std::error_code error;
    if (std::filesystem::exists(path, error) && !error &&
        std::filesystem::is_regular_file(path, error)) {
      roots.insert(NormalizePathString(path.parent_path()));
      continue;
    }
    if (!error && std::filesystem::exists(path) &&
        std::filesystem::is_directory(path, error) && !error) {
      roots.insert(NormalizePathString(path.parent_path()));
      continue;
    }
    roots.insert(NormalizePathString(path.parent_path()));
  }
  for (const auto& job : jobs) {
    if (IsUsableAbsoluteHostPath(job.target_root)) {
      roots.insert(NormalizePathString(std::filesystem::path(job.target_root)));
    }
    for (const auto& target_path : job.target_paths) {
      if (!IsUsableAbsoluteHostPath(target_path)) {
        continue;
      }
      roots.insert(
          NormalizePathString(std::filesystem::path(target_path).parent_path()));
    }
  }
  return std::vector<std::string>(roots.begin(), roots.end());
}

std::filesystem::path ModelLibraryService::DownloadJobMetadataDirectory(
    const ModelLibraryDownloadJob& job) const {
  if (!IsUsableAbsoluteHostPath(job.target_root)) {
    return {};
  }
  std::filesystem::path directory(job.target_root);
  if (!job.target_subdir.empty()) {
    directory /= job.target_subdir;
  }
  return directory;
}

std::filesystem::path ModelLibraryService::DownloadJobMetadataPath(
    const ModelLibraryDownloadJob& job) const {
  const auto directory = DownloadJobMetadataDirectory(job);
  if (directory.empty()) {
    return {};
  }
  return directory /
         (std::string(kJobMetadataPrefix) + job.id + std::string(kJobMetadataSuffix));
}

void ModelLibraryService::PersistDownloadJobMetadata(
    const ModelLibraryDownloadJob& job) const {
  const auto metadata_path = DownloadJobMetadataPath(job);
  if (metadata_path.empty()) {
    return;
  }
  std::error_code error;
  std::filesystem::create_directories(metadata_path.parent_path(), error);
  if (error) {
    return;
  }
  json payload{
      {"id", job.id},
      {"job_kind", NormalizeJobKind(job.job_kind)},
      {"status", job.status},
      {"phase", job.phase},
      {"model_id", job.model_id},
      {"target_root", job.target_root},
      {"target_subdir", job.target_subdir},
      {"detected_source_format", job.detected_source_format},
      {"desired_output_format", job.desired_output_format},
      {"source_urls", job.source_urls},
      {"target_paths", job.target_paths},
      {"quantizations", job.quantizations},
      {"retained_output_paths", job.retained_output_paths},
      {"current_item", job.current_item},
      {"staging_directory", job.staging_directory},
      {"bytes_total",
       job.bytes_total.has_value() ? json(*job.bytes_total) : json(nullptr)},
      {"bytes_done", job.bytes_done},
      {"part_count", job.part_count},
      {"keep_base_gguf", job.keep_base_gguf},
      {"error_message", job.error_message},
      {"hidden", job.hidden},
      {"created_at", job.created_at},
      {"updated_at", job.updated_at},
  };
  const auto temp_path = metadata_path.string() + ".tmp";
  {
    std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return;
    }
    out << payload.dump(2);
  }
  std::filesystem::rename(temp_path, metadata_path, error);
  if (error) {
    std::filesystem::remove(temp_path, error);
  }
}

void ModelLibraryService::RemoveDownloadJobMetadata(
    const ModelLibraryDownloadJob& job) const {
  const auto metadata_path = DownloadJobMetadataPath(job);
  if (metadata_path.empty()) {
    return;
  }
  std::error_code error;
  std::filesystem::remove(metadata_path, error);
}

void ModelLibraryService::RecoverPersistentJobsFromMetadata(
    const std::string& db_path) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  std::set<std::string> existing_job_ids;
  for (const auto& job : store.LoadModelLibraryDownloadJobs()) {
    existing_job_ids.insert(job.id);
  }
  for (const auto& root_text : DiscoverRoots(db_path)) {
    const std::filesystem::path root(root_text);
    std::error_code error;
    if (!std::filesystem::exists(root, error) || error ||
        !std::filesystem::is_directory(root, error) || error) {
      continue;
    }
    for (std::filesystem::recursive_directory_iterator iterator(
             root,
             std::filesystem::directory_options::skip_permission_denied,
             error);
         !error && iterator != std::filesystem::recursive_directory_iterator();
         iterator.increment(error)) {
      if (error) {
        break;
      }
      if (iterator.depth() > 6) {
        iterator.disable_recursion_pending();
        continue;
      }
      if (!iterator->is_regular_file(error)) {
        error.clear();
        continue;
      }
      error.clear();
      const auto filename = iterator->path().filename().string();
      if (!LooksLikeJobMetadataFilename(filename)) {
        continue;
      }
      std::ifstream input(iterator->path());
      if (!input.is_open()) {
        continue;
      }
      json payload;
      try {
        input >> payload;
      } catch (...) {
        continue;
      }
      if (!payload.is_object() || !payload.contains("id") ||
          !payload.at("id").is_string()) {
        continue;
      }
      ModelLibraryDownloadJob job;
      job.id = payload.value("id", std::string{});
      if (job.id.empty() || existing_job_ids.count(job.id) != 0) {
        continue;
      }
      job.job_kind = NormalizeJobKind(payload.value("job_kind", std::string("download")));
      job.status = payload.value("status", std::string("queued"));
      job.phase = payload.value("phase", job.status);
      job.model_id = payload.value("model_id", std::string{});
      job.target_root = payload.value("target_root", std::string{});
      job.target_subdir = payload.value("target_subdir", std::string{});
      job.detected_source_format =
          payload.value("detected_source_format", std::string{});
      job.desired_output_format =
          payload.value("desired_output_format", std::string{});
      if (payload.contains("source_urls") && payload.at("source_urls").is_array()) {
        job.source_urls = payload.at("source_urls").get<std::vector<std::string>>();
      }
      if (payload.contains("target_paths") && payload.at("target_paths").is_array()) {
        job.target_paths = payload.at("target_paths").get<std::vector<std::string>>();
      }
      if (payload.contains("quantizations") && payload.at("quantizations").is_array()) {
        job.quantizations = payload.at("quantizations").get<std::vector<std::string>>();
      }
      if (payload.contains("retained_output_paths") &&
          payload.at("retained_output_paths").is_array()) {
        job.retained_output_paths =
            payload.at("retained_output_paths").get<std::vector<std::string>>();
      }
      job.current_item = payload.value("current_item", std::string{});
      job.staging_directory = payload.value("staging_directory", std::string{});
      if (payload.contains("bytes_total") && !payload.at("bytes_total").is_null()) {
        job.bytes_total = payload.at("bytes_total").get<std::uintmax_t>();
      }
      job.bytes_done = payload.value("bytes_done", std::uintmax_t{0});
      job.part_count = payload.value("part_count", 0);
      job.keep_base_gguf = payload.value("keep_base_gguf", true);
      job.error_message = payload.value("error_message", std::string{});
      job.hidden = payload.value("hidden", false);
      job.created_at = payload.value("created_at", support_.utc_now_sql_timestamp());
      job.updated_at = payload.value("updated_at", job.created_at);

      std::uintmax_t observed_bytes_done = 0;
      bool all_targets_complete = !job.target_paths.empty();
      if (IsQuantizationJob(job) && job.status != "completed") {
        all_targets_complete = false;
      }
      for (const auto& target_path_text : job.target_paths) {
        std::error_code size_error;
        const std::filesystem::path target_path(target_path_text);
        if (std::filesystem::exists(target_path, size_error) && !size_error) {
          observed_bytes_done += std::filesystem::file_size(target_path, size_error);
          all_targets_complete = all_targets_complete && !size_error;
          continue;
        }
        size_error.clear();
        const std::filesystem::path part_path = target_path.string() + ".part";
        if (std::filesystem::exists(part_path, size_error) && !size_error) {
          observed_bytes_done += std::filesystem::file_size(part_path, size_error);
          all_targets_complete = false;
          continue;
        }
        all_targets_complete = false;
      }
      if (observed_bytes_done > 0) {
        job.bytes_done = observed_bytes_done;
      }
      if (job.bytes_total.has_value() && all_targets_complete &&
          observed_bytes_done >= *job.bytes_total) {
        job.status = "completed";
      } else if (job.status == "completed" && !all_targets_complete) {
        job.status = "running";
      }

      store.UpsertModelLibraryDownloadJob(job);
      existing_job_ids.insert(job.id);
    }
  }
}

std::map<std::string, std::vector<std::string>>
ModelLibraryService::BuildReferenceMap(const std::string& db_path) const {
  comet::ControllerStore store(db_path);
  const auto desired_states = store.LoadDesiredStates();
  std::map<std::string, std::vector<std::string>> references;
  for (const auto& desired_state : desired_states) {
    if (!desired_state.bootstrap_model.has_value() ||
        !desired_state.bootstrap_model->local_path.has_value()) {
      continue;
    }
    const std::string& local_path = *desired_state.bootstrap_model->local_path;
    if (!IsUsableAbsoluteHostPath(local_path)) {
      continue;
    }
    references[NormalizePathString(local_path)].push_back(
        desired_state.plane_name);
  }
  return references;
}

json ModelLibraryService::BuildJobPayload(
    const ModelLibraryDownloadJob& job) const {
  return json{
      {"id", job.id},
      {"job_kind", NormalizeJobKind(job.job_kind)},
      {"status", job.status},
      {"phase", job.phase.empty() ? job.status : job.phase},
      {"model_id", job.model_id},
      {"target_root", job.target_root},
      {"target_subdir", job.target_subdir},
      {"detected_source_format",
       job.detected_source_format.empty() ? json(nullptr)
                                          : json(job.detected_source_format)},
      {"desired_output_format",
       job.desired_output_format.empty() ? json(nullptr)
                                         : json(job.desired_output_format)},
      {"source_urls", job.source_urls},
      {"target_paths", job.target_paths},
      {"quantizations", job.quantizations},
      {"keep_base_gguf", job.keep_base_gguf},
      {"staging_directory",
       job.staging_directory.empty() ? json(nullptr) : json(job.staging_directory)},
      {"retained_output_paths", job.retained_output_paths},
      {"current_item", job.current_item},
      {"can_stop", job.status == "queued" || job.status == "running"},
      {"can_resume", job.status == "stopped" || job.status == "failed"},
      {"can_hide",
       !job.hidden &&
           (job.status == "completed" || job.status == "failed" ||
            job.status == "stopped")},
      {"can_delete", job.status != "running" && job.status != "stopping"},
      {"hidden", job.hidden},
      {"bytes_total",
       job.bytes_total.has_value() ? json(*job.bytes_total) : json(nullptr)},
      {"bytes_done", job.bytes_done},
      {"part_count", job.part_count},
      {"error_message",
       job.error_message.empty() ? json(nullptr) : json(job.error_message)},
      {"created_at", job.created_at},
      {"updated_at", job.updated_at},
  };
}

void ModelLibraryService::ResumePersistentJobs(const std::string& db_path) const {
  {
    std::lock_guard<std::mutex> lock(state_->jobs_mutex);
    if (state_->resumed_db_paths.count(db_path) != 0) {
      return;
    }
    state_->resumed_db_paths.insert(db_path);
  }
  RecoverPersistentJobsFromMetadata(db_path);
  comet::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& job : store.LoadModelLibraryDownloadJobs()) {
    if (job.status == "queued" || job.status == "running") {
      StartDownloadJob(db_path, job.id);
    }
  }
}

std::optional<std::string> ModelLibraryService::ExtractJobId(
    const HttpRequest& request) const {
  json body = json::object();
  if (!request.body.empty()) {
    body = support_.parse_json_request_body(request);
  }
  if (body.contains("job_id") && body.at("job_id").is_string()) {
    const auto value = Trim(body.at("job_id").get<std::string>());
    if (!value.empty()) {
      return value;
    }
  }
  if (const auto query = support_.find_query_string(request, "job_id");
      query.has_value()) {
    const auto value = Trim(*query);
    if (!value.empty()) {
      return value;
    }
  }
  return std::nullopt;
}

std::optional<ModelLibraryService::ModelLibraryDownloadJob>
ModelLibraryService::LoadDownloadJob(
    const std::string& db_path,
    const std::string& job_id) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return store.LoadModelLibraryDownloadJob(job_id);
}

void ModelLibraryService::UpdateJob(
    const std::string& db_path,
    const std::string& job_id,
    const std::function<void(ModelLibraryDownloadJob&)>& update) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  auto job = store.LoadModelLibraryDownloadJob(job_id);
  if (!job.has_value()) {
    return;
  }
  update(*job);
  job->updated_at = support_.utc_now_sql_timestamp();
  store.UpsertModelLibraryDownloadJob(*job);
  PersistDownloadJobMetadata(*job);
}

bool ModelLibraryService::IsStopRequested(const std::string& job_id) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  return state_->stop_requested_job_ids.count(job_id) != 0;
}

void ModelLibraryService::ClearStopRequest(const std::string& job_id) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  state_->stop_requested_job_ids.erase(job_id);
}

void ModelLibraryService::RequestStop(const std::string& job_id) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  state_->stop_requested_job_ids.insert(job_id);
#if !defined(_WIN32)
  if (const auto it = state_->active_job_pids.find(job_id);
      it != state_->active_job_pids.end() && it->second > 0) {
    kill(static_cast<pid_t>(it->second), SIGTERM);
  }
#endif
}

void ModelLibraryService::RegisterActiveJobProcess(
    const std::string& job_id,
    int pid) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  state_->active_job_pids[job_id] = pid;
}

void ModelLibraryService::ClearActiveJobProcess(
    const std::string& job_id) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  state_->active_job_pids.erase(job_id);
}

std::vector<ModelLibraryService::ModelLibraryEntry>
ModelLibraryService::ScanEntries(const std::string& db_path) const {
  const auto roots = DiscoverRoots(db_path);
  const auto reference_map = BuildReferenceMap(db_path);
  comet::ControllerStore store(db_path);
  store.Initialize();
  PendingDownloadState pending_downloads;
  for (const auto& job : store.LoadModelLibraryDownloadJobs()) {
    if (IsDownloadJobComplete(job)) {
      continue;
    }
    for (const auto& target_path_text : job.target_paths) {
      if (!IsUsableAbsoluteHostPath(target_path_text)) {
        continue;
      }
      const std::filesystem::path target_path(target_path_text);
      const auto normalized_target = NormalizePathString(target_path);
      pending_downloads.target_paths.insert(normalized_target);
      std::string multipart_prefix;
      int part_index = 0;
      int part_total = 0;
      if (ParseMultipartGgufFilename(
              target_path.filename().string(),
              &multipart_prefix,
              &part_index,
              &part_total)) {
        pending_downloads.multipart_group_keys.insert(
            NormalizePathString(target_path.parent_path() / multipart_prefix));
      }
    }
  }
  std::map<std::string, ModelLibraryEntry> entries_by_path;
  std::map<std::string, MultipartGroup> multipart_groups;

  for (const auto& root_text : roots) {
    const std::filesystem::path root(root_text);
    std::error_code error;
    if (!std::filesystem::exists(root, error) || error ||
        !std::filesystem::is_directory(root, error) || error) {
      continue;
    }
    for (std::filesystem::recursive_directory_iterator iterator(
             root,
             std::filesystem::directory_options::skip_permission_denied,
             error);
         !error && iterator != std::filesystem::recursive_directory_iterator();
         iterator.increment(error)) {
      if (error) {
        break;
      }
      const auto depth = iterator.depth();
      if (depth > 4) {
        iterator.disable_recursion_pending();
        continue;
      }
      const std::filesystem::path current_path = iterator->path();
      const std::string current_name = current_path.filename().string();
      if (!current_name.empty() && current_name.front() == '.') {
        if (iterator->is_directory(error)) {
          iterator.disable_recursion_pending();
        }
        error.clear();
        continue;
      }
      if (iterator->is_directory(error)) {
        error.clear();
        if (!LooksLikeRecognizedModelDirectory(current_path)) {
          continue;
        }
        iterator.disable_recursion_pending();
        ModelLibraryEntry entry;
        entry.path = NormalizePathString(current_path);
        entry.name = current_path.filename().string();
        entry.kind = "directory";
        entry.format = "model-directory";
        entry.root = root_text;
        entry.paths = {entry.path};
        entry.size_bytes = FileSizeIfExists(current_path).value_or(0);
        const auto reference_it = reference_map.find(entry.path);
        if (reference_it != reference_map.end()) {
          entry.referenced_by = reference_it->second;
          entry.deletable = false;
        }
        entries_by_path[entry.path] = std::move(entry);
        continue;
      }
      if (!iterator->is_regular_file(error)) {
        error.clear();
        continue;
      }
      error.clear();
      if (!EndsWithIgnoreCase(current_name, ".gguf")) {
        continue;
      }
      const auto normalized_file_path = NormalizePathString(current_path);
      if (pending_downloads.target_paths.count(normalized_file_path) != 0) {
        continue;
      }
      std::string multipart_prefix;
      int part_index = 0;
      int part_total = 0;
      if (ParseMultipartGgufFilename(
              current_name, &multipart_prefix, &part_index, &part_total)) {
        const std::string group_key =
            NormalizePathString(current_path.parent_path() / multipart_prefix);
        auto& group = multipart_groups[group_key];
        group.root = root_text;
        group.name = multipart_prefix;
        group.part_total = std::max(group.part_total, part_total);
        group.paths.push_back(NormalizePathString(current_path));
        group.size_bytes += iterator->file_size(error);
        continue;
      }
      ModelLibraryEntry entry;
      entry.path = NormalizePathString(current_path);
      entry.name = current_name;
      entry.kind = "file";
      entry.format = "gguf";
      entry.quantization = DetectEntryQuantization(current_path.stem().string());
      if (entry.quantization != "base") {
        const auto base_path =
            current_path.parent_path() /
            (StripKnownQuantizationSuffix(current_path.stem().string()) + ".gguf");
        std::error_code base_error;
        if (std::filesystem::exists(base_path, base_error) && !base_error) {
          entry.quantized_from_path = NormalizePathString(base_path);
        }
      }
      entry.root = root_text;
      entry.paths = {entry.path};
      entry.size_bytes = iterator->file_size(error);
      const auto reference_it = reference_map.find(entry.path);
      if (reference_it != reference_map.end()) {
        entry.referenced_by = reference_it->second;
        entry.deletable = false;
      }
      entries_by_path[entry.path] = std::move(entry);
    }
  }

  for (auto& [group_key, group] : multipart_groups) {
    std::sort(group.paths.begin(), group.paths.end());
    group.paths.erase(
        std::unique(group.paths.begin(), group.paths.end()),
        group.paths.end());
    if (group.paths.empty()) {
      continue;
    }
    if (pending_downloads.multipart_group_keys.count(group_key) != 0) {
      continue;
    }
    if (group.part_total > 0 &&
        static_cast<int>(group.paths.size()) < group.part_total) {
      continue;
    }
    ModelLibraryEntry entry;
    entry.path = group.paths.front();
    entry.name = group.name;
    entry.kind = "multipart-gguf";
    entry.format = "gguf";
    entry.quantization = DetectEntryQuantization(group.name);
    entry.root = group.root;
    entry.paths = group.paths;
    entry.size_bytes = group.size_bytes;
    entry.part_count = static_cast<int>(group.paths.size());
    bool referenced = false;
    for (const auto& path : group.paths) {
      const auto reference_it = reference_map.find(path);
      if (reference_it == reference_map.end()) {
        continue;
      }
      referenced = true;
      entry.referenced_by.insert(
          entry.referenced_by.end(),
          reference_it->second.begin(),
          reference_it->second.end());
    }
    if (const auto reference_it = reference_map.find(group_key);
        reference_it != reference_map.end()) {
      referenced = true;
      entry.referenced_by.insert(
          entry.referenced_by.end(),
          reference_it->second.begin(),
          reference_it->second.end());
    }
    if (referenced) {
      std::sort(entry.referenced_by.begin(), entry.referenced_by.end());
      entry.referenced_by.erase(
          std::unique(entry.referenced_by.begin(), entry.referenced_by.end()),
          entry.referenced_by.end());
      entry.deletable = false;
    }
    entries_by_path[group_key] = std::move(entry);
  }

  std::vector<ModelLibraryEntry> entries;
  entries.reserve(entries_by_path.size());
  for (auto& [_, entry] : entries_by_path) {
    entries.push_back(std::move(entry));
  }
  std::sort(
      entries.begin(),
      entries.end(),
      [](const ModelLibraryEntry& left, const ModelLibraryEntry& right) {
        if (left.root != right.root) {
          return left.root < right.root;
        }
        return left.name < right.name;
      });
  return entries;
}

std::string ModelLibraryService::GenerateJobId() const {
  return "mdl-" +
         std::to_string(
             static_cast<unsigned long long>(std::time(nullptr))) +
         "-" +
         std::to_string(static_cast<unsigned long long>(
             state_->job_counter.fetch_add(1)));
}

void ModelLibraryService::DownloadFile(
    const std::string& db_path,
    const std::string& job_id,
    const std::string& source_url,
    const std::filesystem::path& target_path,
    std::uintmax_t aggregate_prefix,
    const std::optional<std::uintmax_t>& aggregate_total) const {
  std::filesystem::create_directories(target_path.parent_path());
  const std::filesystem::path temp_path = target_path.string() + ".part";
  std::error_code cleanup_error;
  if (std::filesystem::exists(target_path, cleanup_error) && !cleanup_error &&
      !std::filesystem::exists(temp_path, cleanup_error)) {
    const auto final_size = FileSizeIfExists(target_path).value_or(0);
    UpdateJob(
        db_path,
        job_id,
        [&](ModelLibraryDownloadJob& job) {
          job.bytes_done = aggregate_prefix + final_size;
          job.bytes_total = aggregate_total;
          job.current_item = target_path.filename().string();
          job.status = "running";
          job.phase = "running";
        });
    return;
  }
  cleanup_error.clear();
#if defined(_WIN32)
  auto future = std::async(
      std::launch::async,
      [temp_path_text = temp_path.string(), source_url]() {
        const std::string command =
            "/usr/bin/curl -fL -C - --silent --show-error --output '" +
            temp_path_text + "' '" + source_url + "'";
        return std::system(command.c_str());
      });
  while (future.wait_for(std::chrono::milliseconds(500)) !=
         std::future_status::ready) {
    const auto bytes_done = FileSizeIfExists(temp_path).value_or(0);
    UpdateJob(
        db_path,
        job_id,
        [&](ModelLibraryDownloadJob& job) {
          job.bytes_done = aggregate_prefix + bytes_done;
          job.bytes_total = aggregate_total;
          job.current_item = target_path.filename().string();
          job.status = "running";
          job.phase = "running";
        });
    if (IsStopRequested(job_id)) {
      std::filesystem::remove(temp_path, cleanup_error);
      throw DownloadStoppedError();
    }
  }
  const int rc = future.get();
  if (rc != 0 || IsStopRequested(job_id)) {
    std::filesystem::remove(temp_path, cleanup_error);
    if (IsStopRequested(job_id)) {
      throw DownloadStoppedError();
    }
    throw std::runtime_error(
        "failed to download model artifact from " + source_url);
  }
#else
  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed while starting model download");
  }
  if (pid == 0) {
    std::vector<char*> argv;
    const std::string curl_path = "/usr/bin/curl";
    const std::string flag_fail = "-fL";
    const std::string flag_continue = "-C";
    const std::string flag_continue_value = "-";
    const std::string flag_silent = "--silent";
    const std::string flag_show_error = "--show-error";
    const std::string flag_output = "--output";
    argv.push_back(const_cast<char*>(curl_path.c_str()));
    argv.push_back(const_cast<char*>(flag_fail.c_str()));
    argv.push_back(const_cast<char*>(flag_continue.c_str()));
    argv.push_back(const_cast<char*>(flag_continue_value.c_str()));
    argv.push_back(const_cast<char*>(flag_silent.c_str()));
    argv.push_back(const_cast<char*>(flag_show_error.c_str()));
    argv.push_back(const_cast<char*>(flag_output.c_str()));
    argv.push_back(const_cast<char*>(temp_path.c_str()));
    argv.push_back(const_cast<char*>(source_url.c_str()));
    argv.push_back(nullptr);
    execv(curl_path.c_str(), argv.data());
    std::perror("execv curl");
    _exit(127);
  }
  RegisterActiveJobProcess(job_id, static_cast<int>(pid));
  int wait_status = 0;
  while (true) {
    const pid_t wait_result = waitpid(pid, &wait_status, WNOHANG);
    if (wait_result == pid) {
      break;
    }
    if (wait_result < 0) {
      ClearActiveJobProcess(job_id);
      throw std::runtime_error("waitpid failed while downloading model artifact");
    }
    const auto bytes_done = FileSizeIfExists(temp_path).value_or(0);
    UpdateJob(
        db_path,
        job_id,
        [&](ModelLibraryDownloadJob& job) {
          job.bytes_done = aggregate_prefix + bytes_done;
          job.bytes_total = aggregate_total;
          job.current_item = target_path.filename().string();
          job.status = "running";
          job.phase = "running";
        });
    if (IsStopRequested(job_id)) {
      kill(pid, SIGTERM);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  ClearActiveJobProcess(job_id);
  if (IsStopRequested(job_id)) {
    throw DownloadStoppedError();
  }
  if (!WIFEXITED(wait_status) || WEXITSTATUS(wait_status) != 0) {
    throw std::runtime_error(
        "failed to download model artifact from " + source_url);
  }
#endif
  std::filesystem::rename(temp_path, target_path);
  const auto final_size = FileSizeIfExists(target_path).value_or(0);
  UpdateJob(
      db_path,
      job_id,
      [&](ModelLibraryDownloadJob& job) {
        job.bytes_done = aggregate_prefix + final_size;
        job.bytes_total = aggregate_total;
        job.current_item = target_path.filename().string();
        job.phase = "running";
      });
}

void ModelLibraryService::StartDownloadJob(
    const std::string& db_path,
    const std::string& job_id) const {
  {
    std::lock_guard<std::mutex> lock(state_->jobs_mutex);
    if (state_->active_job_ids.count(job_id) != 0) {
      return;
    }
    state_->active_job_ids.insert(job_id);
  }
  std::thread([service = *this, db_path, job_id]() {
    const auto clear_active = [&service, &job_id]() {
      std::lock_guard<std::mutex> lock(service.state_->jobs_mutex);
      service.state_->active_job_ids.erase(job_id);
    };
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      auto snapshot = store.LoadModelLibraryDownloadJob(job_id);
      if (!snapshot.has_value()) {
        service.ClearStopRequest(job_id);
        clear_active();
        return;
      }
      if (snapshot->status == "completed" || snapshot->status == "failed" ||
          snapshot->status == "stopped" || snapshot->status == "stopping") {
        service.ClearStopRequest(job_id);
        clear_active();
        return;
      }
      const bool requires_conversion =
          NormalizeModelOutputFormat(snapshot->detected_source_format) == "safetensors" &&
          NormalizeModelOutputFormat(snapshot->desired_output_format) == "gguf";
      const bool is_quantization_job = IsQuantizationJob(*snapshot);
      service.UpdateJob(
          db_path,
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "running";
            job.phase = "queued";
            job.current_item = "probing";
            job.bytes_done = is_quantization_job ? 0 : ExistingDownloadBytesForJob(job);
          });
      std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
      if (!is_quantization_job) {
        for (const auto& source_url : snapshot->source_urls) {
          if (service.IsStopRequested(job_id)) {
            throw DownloadStoppedError();
          }
          const auto content_length = service.ProbeContentLength(source_url);
          if (!content_length.has_value()) {
            aggregate_total = std::nullopt;
            break;
          }
          *aggregate_total += *content_length;
        }
      }
      service.UpdateJob(
          db_path,
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "running";
            job.phase = is_quantization_job ? "quantizing" : "running";
            if (!is_quantization_job) {
              job.bytes_total = aggregate_total;
            }
            if (job.current_item == "probing") {
              job.current_item.clear();
            }
          });
      std::uintmax_t aggregate_prefix = 0;
      std::vector<std::filesystem::path> download_targets;
      if (is_quantization_job) {
        if (snapshot->target_paths.empty() || snapshot->quantizations.empty()) {
          throw std::runtime_error("quantization job is missing target_paths or quantizations");
        }
        const std::filesystem::path source_path =
            snapshot->source_urls.empty()
                ? std::filesystem::path(snapshot->target_root) /
                      (StripKnownQuantizationSuffix(
                           std::filesystem::path(snapshot->current_item).stem().string()) +
                       ".gguf")
                : std::filesystem::path(snapshot->source_urls.front());
        const auto plan = service.conversion_service_->BuildQuantizationPlan(
            ModelConversionService::QuantizationRequest{
                .job_id = snapshot->id,
                .source_path = source_path,
                .quantization = snapshot->quantizations.front(),
                .staging_directory = snapshot->staging_directory.empty()
                    ? std::filesystem::path(snapshot->target_root) /
                          (std::string(kJobMetadataPrefix) + snapshot->id + "-staging")
                    : std::filesystem::path(snapshot->staging_directory),
                .retained_output_path = std::filesystem::path(snapshot->target_paths.front()),
                .replace_existing = true,
            });
        service.conversion_service_->ExecuteQuantization(
            ModelConversionService::QuantizationRequest{
                .job_id = snapshot->id,
                .source_path = source_path,
                .quantization = snapshot->quantizations.front(),
                .staging_directory = plan.staging_directory,
                .retained_output_path = plan.retained_output_path,
                .replace_existing = true,
            },
            plan,
            ModelConversionService::JobHooks{
                .stop_requested =
                    [&service, &job_id]() { return service.IsStopRequested(job_id); },
                .register_pid =
                    [&service, &job_id](int pid) {
                      service.RegisterActiveJobProcess(job_id, pid);
                    },
                .clear_pid =
                    [&service, &job_id]() { service.ClearActiveJobProcess(job_id); },
                .update_job =
                    [&service, &db_path, &job_id](
                        const std::string& phase,
                        const std::string& current_item,
                        const std::optional<std::uintmax_t>& bytes_total,
                        std::uintmax_t bytes_done) {
                      service.UpdateJob(
                          db_path,
                          job_id,
                          [&](ModelLibraryDownloadJob& job) {
                            job.status = "running";
                            job.phase = phase;
                            job.current_item = current_item;
                            job.bytes_total = bytes_total;
                            job.bytes_done = bytes_done;
                          });
                    },
            });
        aggregate_prefix = FileSizeIfExists(plan.retained_output_path).value_or(0);
      } else if (requires_conversion) {
        const std::filesystem::path destination_root =
            snapshot->target_subdir.empty()
                ? std::filesystem::path(snapshot->target_root)
                : std::filesystem::path(snapshot->target_root) / snapshot->target_subdir;
        const auto plan = service.conversion_service_->BuildPlan(
            ModelConversionService::Request{
                .job_id = snapshot->id,
                .model_id = snapshot->model_id,
                .destination_root = destination_root,
                .source_urls = snapshot->source_urls,
                .detected_source_format = snapshot->detected_source_format,
                .desired_output_format = snapshot->desired_output_format,
                .quantizations = snapshot->quantizations,
                .keep_base_gguf = snapshot->keep_base_gguf,
                .staging_directory = snapshot->staging_directory.empty()
                    ? destination_root /
                          (std::string(kJobMetadataPrefix) + snapshot->id + "-staging")
                    : std::filesystem::path(snapshot->staging_directory),
            });
        download_targets = plan.downloaded_source_paths;
        for (std::size_t index = 0; index < snapshot->source_urls.size(); ++index) {
          if (service.IsStopRequested(job_id)) {
            throw DownloadStoppedError();
          }
          service.DownloadFile(
              db_path,
              job_id,
              snapshot->source_urls.at(index),
              download_targets.at(index),
              aggregate_prefix,
              aggregate_total);
          aggregate_prefix += FileSizeIfExists(download_targets.at(index)).value_or(0);
        }
        service.UpdateJob(
            db_path,
            job_id,
            [&](ModelLibraryDownloadJob& job) {
              job.status = "running";
              job.phase = "converting";
              job.bytes_total = std::nullopt;
              job.bytes_done = 0;
            });
        service.conversion_service_->Execute(
            ModelConversionService::Request{
                .job_id = snapshot->id,
                .model_id = snapshot->model_id,
                .destination_root = destination_root,
                .source_urls = snapshot->source_urls,
                .detected_source_format = snapshot->detected_source_format,
                .desired_output_format = snapshot->desired_output_format,
                .quantizations = snapshot->quantizations,
                .keep_base_gguf = snapshot->keep_base_gguf,
                .staging_directory = plan.staging_directory,
            },
            plan,
            ModelConversionService::JobHooks{
                .stop_requested =
                    [&service, &job_id]() { return service.IsStopRequested(job_id); },
                .register_pid =
                    [&service, &job_id](int pid) {
                      service.RegisterActiveJobProcess(job_id, pid);
                    },
                .clear_pid =
                    [&service, &job_id]() { service.ClearActiveJobProcess(job_id); },
                .update_job =
                    [&service, &db_path, &job_id](
                        const std::string& phase,
                        const std::string& current_item,
                        const std::optional<std::uintmax_t>& bytes_total,
                        std::uintmax_t bytes_done) {
                      service.UpdateJob(
                          db_path,
                          job_id,
                          [&](ModelLibraryDownloadJob& job) {
                            job.status = "running";
                            job.phase = phase;
                            job.current_item = current_item;
                            job.bytes_total = bytes_total;
                            job.bytes_done = bytes_done;
                          });
                    },
            });
      } else {
        for (std::size_t index = 0; index < snapshot->source_urls.size();
             ++index) {
          if (service.IsStopRequested(job_id)) {
            throw DownloadStoppedError();
          }
          const std::filesystem::path target_path(snapshot->target_paths.at(index));
          service.DownloadFile(
              db_path,
              job_id,
              snapshot->source_urls.at(index),
              target_path,
              aggregate_prefix,
              aggregate_total);
          aggregate_prefix += FileSizeIfExists(target_path).value_or(0);
        }
      }
      service.UpdateJob(
          db_path,
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "completed";
            job.phase = "completed";
            job.bytes_done = aggregate_prefix;
            job.bytes_total =
                (requires_conversion || is_quantization_job) ? std::nullopt : aggregate_total;
            job.current_item.clear();
            job.error_message.clear();
          });
      service.ClearStopRequest(job_id);
    } catch (const DownloadStoppedError&) {
      service.UpdateJob(
          db_path,
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "stopped";
            if (job.phase.empty() || job.phase == "queued") {
              job.phase = "stopped";
            }
            job.current_item.clear();
            job.error_message.clear();
          });
      service.ClearStopRequest(job_id);
    } catch (const std::exception& error) {
      if (service.IsStopRequested(job_id)) {
        service.UpdateJob(
            db_path,
            job_id,
            [&](ModelLibraryDownloadJob& job) {
              job.status = "stopped";
              if (job.phase.empty() || job.phase == "queued") {
                job.phase = "stopped";
              }
              job.current_item.clear();
              job.error_message.clear();
            });
        service.ClearStopRequest(job_id);
        clear_active();
        return;
      }
      service.UpdateJob(
          db_path,
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "failed";
            if (job.phase.empty()) {
              job.phase = "failed";
            }
            job.error_message = error.what();
            job.current_item.clear();
          });
      service.ClearStopRequest(job_id);
    }
    clear_active();
  }).detach();
}
