#pragma once

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "model/model_library_support.h"

class ModelLibraryService {
 public:
  explicit ModelLibraryService(ModelLibrarySupport support);

  nlohmann::json BuildPayload(const std::string& db_path) const;
  HttpResponse DeleteEntryByPath(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse EnqueueDownload(const HttpRequest& request) const;

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
  };

  struct ModelLibraryDownloadJob {
    std::string id;
    std::string status = "queued";
    std::string model_id;
    std::string target_root;
    std::string target_subdir;
    std::vector<std::string> source_urls;
    std::vector<std::string> target_paths;
    std::string current_item;
    std::optional<std::uintmax_t> bytes_total;
    std::uintmax_t bytes_done = 0;
    int part_count = 0;
    std::string error_message;
    std::string created_at;
    std::string updated_at;
  };

  struct State {
    std::mutex jobs_mutex;
    std::map<std::string, ModelLibraryDownloadJob> jobs;
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
  std::optional<std::uintmax_t> ProbeContentLength(
      const std::string& source_url) const;
  static std::optional<std::uintmax_t> FileSizeIfExists(
      const std::filesystem::path& path);
  static bool LooksLikeRecognizedModelDirectory(
      const std::filesystem::path& path);
  static bool ParseMultipartGgufFilename(
      const std::string& filename,
      std::string* prefix,
      int* part_index,
      int* part_total);
  std::vector<std::string> DiscoverRoots(const std::string& db_path) const;
  std::map<std::string, std::vector<std::string>> BuildReferenceMap(
      const std::string& db_path) const;
  nlohmann::json BuildJobPayload(const ModelLibraryDownloadJob& job) const;
  void UpdateJob(
      const std::string& job_id,
      const std::function<void(ModelLibraryDownloadJob&)>& update) const;
  std::vector<ModelLibraryEntry> ScanEntries(const std::string& db_path) const;
  std::string GenerateJobId() const;
  void DownloadFile(
      const std::string& job_id,
      const std::string& source_url,
      const std::filesystem::path& target_path,
      std::uintmax_t aggregate_prefix,
      const std::optional<std::uintmax_t>& aggregate_total) const;
  void StartDownloadJob(const std::string& job_id) const;

  ModelLibrarySupport support_;
  std::shared_ptr<State> state_;
};
