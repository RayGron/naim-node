#include "model/model_library_service.h"

#include <algorithm>
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

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"

using nlohmann::json;

namespace {

struct MultipartGroup {
  std::string root;
  std::string name;
  std::vector<std::string> paths;
  std::uintmax_t size_bytes = 0;
  int part_total = 0;
};

}  // namespace

ModelLibraryService::ModelLibraryService(ModelLibrarySupport support)
    : support_(std::move(support)), state_(std::make_shared<State>()) {}

json ModelLibraryService::BuildPayload(const std::string& db_path) const {
  const auto roots = DiscoverRoots(db_path);
  const auto entries = ScanEntries(db_path);
  json items = json::array();
  for (const auto& entry : entries) {
    items.push_back(json{
        {"path", entry.path},
        {"name", entry.name},
        {"kind", entry.kind},
        {"format", entry.format},
        {"root", entry.root},
        {"paths", entry.paths},
        {"size_bytes", entry.size_bytes},
        {"part_count", entry.part_count},
        {"referenced_by", entry.referenced_by},
        {"deletable", entry.deletable},
    });
  }
  json jobs = json::array();
  {
    std::lock_guard<std::mutex> lock(state_->jobs_mutex);
    for (const auto& [_, job] : state_->jobs) {
      jobs.push_back(BuildJobPayload(job));
    }
  }
  return json{{"items", items}, {"roots", roots}, {"jobs", jobs}};
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

  std::filesystem::path destination_root(target_root);
  if (!target_subdir.empty()) {
    destination_root /= target_subdir;
  }

  std::vector<std::string> target_paths;
  target_paths.reserve(source_urls.size());
  try {
    for (std::size_t index = 0; index < source_urls.size(); ++index) {
      const auto filename =
          source_urls.size() == 1 && body.contains("target_filename") &&
                  body.at("target_filename").is_string() &&
                  !body.at("target_filename").get<std::string>().empty()
              ? body.at("target_filename").get<std::string>()
              : FilenameFromUrl(source_urls.at(index));
      target_paths.push_back(
          NormalizePathString(destination_root / filename));
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
  job.model_id = model_id;
  job.target_root = NormalizePathString(std::filesystem::path(target_root));
  job.target_subdir = target_subdir;
  job.source_urls = source_urls;
  job.target_paths = target_paths;
  job.part_count = static_cast<int>(source_urls.size());
  job.created_at = support_.utc_now_sql_timestamp();
  job.updated_at = job.created_at;
  {
    std::lock_guard<std::mutex> lock(state_->jobs_mutex);
    state_->jobs.emplace(job_id, job);
  }
  StartDownloadJob(job_id);
  return support_.build_json_response(
      202,
      json{{"status", "accepted"}, {"job", BuildJobPayload(job)}},
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

std::optional<std::uintmax_t> ModelLibraryService::ProbeContentLength(
    const std::string& source_url) const {
  const std::string temp_headers =
      (std::filesystem::temp_directory_path() /
       ("comet-model-head-" +
        std::to_string(comet::platform::CurrentProcessId()) + "-" +
        std::to_string(state_->job_counter.fetch_add(1)) + ".txt"))
          .string();
  const std::string command =
      "/usr/bin/curl -fsSLI '" + source_url + "' > '" + temp_headers +
      "' 2>/dev/null || true";
  std::system(command.c_str());
  std::ifstream input(temp_headers);
  std::filesystem::remove(temp_headers);
  std::string line;
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
      return static_cast<std::uintmax_t>(
          std::stoull(Trim(trimmed.substr(colon + 1))));
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
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

std::vector<std::string> ModelLibraryService::DiscoverRoots(
    const std::string& db_path) const {
  comet::ControllerStore store(db_path);
  const auto desired_states = store.LoadDesiredStates();
  std::set<std::string> roots;
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
  return std::vector<std::string>(roots.begin(), roots.end());
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
      {"status", job.status},
      {"model_id", job.model_id},
      {"target_root", job.target_root},
      {"target_subdir", job.target_subdir},
      {"source_urls", job.source_urls},
      {"target_paths", job.target_paths},
      {"current_item", job.current_item},
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

void ModelLibraryService::UpdateJob(
    const std::string& job_id,
    const std::function<void(ModelLibraryDownloadJob&)>& update) const {
  std::lock_guard<std::mutex> lock(state_->jobs_mutex);
  const auto it = state_->jobs.find(job_id);
  if (it == state_->jobs.end()) {
    return;
  }
  update(it->second);
  it->second.updated_at = support_.utc_now_sql_timestamp();
}

std::vector<ModelLibraryService::ModelLibraryEntry>
ModelLibraryService::ScanEntries(const std::string& db_path) const {
  const auto roots = DiscoverRoots(db_path);
  const auto reference_map = BuildReferenceMap(db_path);
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
    ModelLibraryEntry entry;
    entry.path = group.paths.front();
    entry.name = group.name;
    entry.kind = "multipart-gguf";
    entry.format = "gguf";
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
    const std::string& job_id,
    const std::string& source_url,
    const std::filesystem::path& target_path,
    std::uintmax_t aggregate_prefix,
    const std::optional<std::uintmax_t>& aggregate_total) const {
  std::filesystem::create_directories(target_path.parent_path());
  const std::filesystem::path temp_path = target_path.string() + ".part";
  std::filesystem::remove(temp_path);
  auto future = std::async(
      std::launch::async,
      [temp_path_text = temp_path.string(), source_url]() {
        const std::string command =
            "/usr/bin/curl -fL --silent --show-error --output '" +
            temp_path_text + "' '" + source_url + "'";
        return std::system(command.c_str());
      });
  while (future.wait_for(std::chrono::milliseconds(500)) !=
         std::future_status::ready) {
    const auto bytes_done = FileSizeIfExists(temp_path).value_or(0);
    UpdateJob(
        job_id,
        [&](ModelLibraryDownloadJob& job) {
          job.bytes_done = aggregate_prefix + bytes_done;
          job.bytes_total = aggregate_total;
          job.current_item = target_path.filename().string();
          job.status = "running";
        });
  }
  const int rc = future.get();
  if (rc != 0) {
    throw std::runtime_error(
        "failed to download model artifact from " + source_url);
  }
  std::filesystem::rename(temp_path, target_path);
  const auto final_size = FileSizeIfExists(target_path).value_or(0);
  UpdateJob(
      job_id,
      [&](ModelLibraryDownloadJob& job) {
        job.bytes_done = aggregate_prefix + final_size;
        job.bytes_total = aggregate_total;
        job.current_item = target_path.filename().string();
      });
}

void ModelLibraryService::StartDownloadJob(const std::string& job_id) const {
  std::thread([service = *this, job_id]() {
    try {
      ModelLibraryDownloadJob snapshot;
      {
        std::lock_guard<std::mutex> lock(service.state_->jobs_mutex);
        snapshot = service.state_->jobs.at(job_id);
      }
      std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
      for (const auto& source_url : snapshot.source_urls) {
        const auto content_length = service.ProbeContentLength(source_url);
        if (!content_length.has_value()) {
          aggregate_total = std::nullopt;
          break;
        }
        *aggregate_total += *content_length;
      }
      service.UpdateJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "running";
            job.bytes_total = aggregate_total;
          });
      std::uintmax_t aggregate_prefix = 0;
      for (std::size_t index = 0; index < snapshot.source_urls.size();
           ++index) {
        const std::filesystem::path target_path(snapshot.target_paths.at(index));
        service.DownloadFile(
            job_id,
            snapshot.source_urls.at(index),
            target_path,
            aggregate_prefix,
            aggregate_total);
        aggregate_prefix +=
            FileSizeIfExists(target_path).value_or(0);
      }
      service.UpdateJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "completed";
            job.bytes_done = aggregate_prefix;
            job.bytes_total = aggregate_total;
            job.current_item.clear();
          });
    } catch (const std::exception& error) {
      service.UpdateJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "failed";
            job.error_message = error.what();
            job.current_item.clear();
          });
    }
  }).detach();
}
