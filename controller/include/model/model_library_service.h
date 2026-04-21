#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "model/model_conversion_service.h"
#include "model/model_library_support.h"

class ModelLibraryService {
 public:
  explicit ModelLibraryService(ModelLibrarySupport support);

  nlohmann::json BuildPayload(const std::string& db_path) const;
  nlohmann::json BuildJobsPayload(const std::string& db_path) const;
  HttpResponse DeleteEntryByPath(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse EnqueueDownload(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse EnqueueQuantization(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse StopDownloadJob(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse ResumeDownloadJob(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HideDownloadJob(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse DeleteDownloadJob(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse SetSkillsFactoryWorker(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  struct ModelLibraryEntry {
    std::string path;
    std::string name;
    std::string kind;
    std::string format;
    std::string root;
    std::vector<std::string> paths;
    std::uintmax_t size_bytes = 0;
    int part_count = 1;
    std::vector<std::string> referenced_by;
    bool deletable = true;
    std::string quantization = "base";
    std::string quantized_from_path;
  };

  using ModelLibraryDownloadJob = naim::ModelLibraryDownloadJobRecord;

  struct State {
    std::mutex jobs_mutex;
    std::set<std::string> active_job_ids;
    std::set<std::string> stop_requested_job_ids;
    std::map<std::string, int> active_job_pids;
    std::set<std::string> resumed_db_paths;
    std::atomic<std::uint64_t> job_counter{0};
  };

  static bool EndsWithIgnoreCase(
      const std::string& value,
      const std::string& suffix);
  static bool IsAllDigits(const std::string& value);
  static std::string Trim(const std::string& value);
  static std::string Lowercase(const std::string& value);
  static std::string NormalizePathString(const std::filesystem::path& path);
  static bool IsUsableAbsoluteHostPath(const std::string& value);
  static std::string FilenameFromUrl(const std::string& source_url);
  static std::string DetectModelSourceFormat(
      const std::vector<std::string>& source_urls);
  static std::string NormalizeModelOutputFormat(const std::string& value);
  static std::vector<std::string> NormalizeQuantizationValues(
      const std::vector<std::string>& values);
  std::optional<std::uintmax_t> ProbeContentLength(
      const std::string& source_url) const;
  static std::optional<std::uintmax_t> FileSizeIfExists(
      const std::filesystem::path& path);
  static bool RemovePathIfExists(
      const std::filesystem::path& path,
      std::vector<std::string>* removed_paths,
      std::string* error_message);
  static bool LooksLikeRecognizedModelDirectory(
      const std::filesystem::path& path);
  static bool ParseMultipartGgufFilename(
      const std::string& filename,
      std::string* prefix,
      int* part_index,
      int* part_total);
  static std::string NormalizeJobKind(const std::string& value);
  static bool IsQuantizationJob(const ModelLibraryDownloadJob& job);
  static std::string DetectEntryQuantization(const std::string& stem_or_prefix);
  static std::string StripKnownQuantizationSuffix(const std::string& stem);
  static std::string BuildQuantizedDisplayName(
      const std::string& raw_name,
      const std::string& quantization);
  std::filesystem::path DownloadJobMetadataDirectory(
      const ModelLibraryDownloadJob& job) const;
  std::filesystem::path DownloadJobMetadataPath(
      const ModelLibraryDownloadJob& job) const;
  void PersistDownloadJobMetadata(const ModelLibraryDownloadJob& job) const;
  void RemoveDownloadJobMetadata(const ModelLibraryDownloadJob& job) const;
  void RecoverPersistentJobsFromMetadata(const std::string& db_path) const;
  std::vector<std::string> DiscoverRoots(const std::string& db_path) const;
  std::map<std::string, std::vector<std::string>> BuildReferenceMap(
      const std::string& db_path) const;
  nlohmann::json BuildJobPayload(const ModelLibraryDownloadJob& job) const;
  void ResumePersistentJobs(const std::string& db_path) const;
  std::optional<std::string> ExtractJobId(const HttpRequest& request) const;
  std::optional<ModelLibraryDownloadJob> LoadDownloadJob(
      const std::string& db_path,
      const std::string& job_id) const;
  void UpdateJob(
      const std::string& db_path,
      const std::string& job_id,
      const std::function<void(ModelLibraryDownloadJob&)>& update) const;
  bool IsStopRequested(const std::string& job_id) const;
  void ClearStopRequest(const std::string& job_id) const;
  void RequestStop(const std::string& job_id) const;
  void RegisterActiveJobProcess(const std::string& job_id, int pid) const;
  void ClearActiveJobProcess(const std::string& job_id) const;
  std::vector<ModelLibraryEntry> ScanEntries(const std::string& db_path) const;
  std::string GenerateJobId() const;
  void DownloadFile(
      const std::string& db_path,
      const std::string& job_id,
      const std::string& source_url,
      const std::filesystem::path& target_path,
      std::uintmax_t aggregate_prefix,
      const std::optional<std::uintmax_t>& aggregate_total) const;
  void StartDownloadJob(const std::string& db_path, const std::string& job_id) const;

  ModelLibrarySupport support_;
  std::shared_ptr<State> state_;
  std::shared_ptr<ModelConversionService> conversion_service_;
};
