#include "app/hostd_app_assignment_support.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <netdb.h>

#include "naim/core/platform_compat.h"
#include "naim/security/crypto_utils.h"

namespace naim::hostd {

namespace {

constexpr std::uintmax_t kMaxModelArtifactChunkBytes = 4ULL * 1024ULL * 1024ULL;

std::string NormalizePathString(const std::filesystem::path& path) {
  return path.lexically_normal().string();
}

bool IsSafeRelativePath(const std::filesystem::path& path) {
  const auto normalized = path.lexically_normal();
  const std::string text = normalized.generic_string();
  return !text.empty() && text != "." && text.front() != '/' &&
         text != ".." && text.rfind("../", 0) != 0;
}

std::optional<std::uintmax_t> JsonUintmax(const nlohmann::json& payload, const std::string& key) {
  if (!payload.contains(key) || !payload.at(key).is_number_unsigned()) {
    return std::nullopt;
  }
  return payload.at(key).get<std::uintmax_t>();
}

std::string BuildManifestCanonicalText(const nlohmann::json& files) {
  std::vector<nlohmann::json> sorted_files = files.get<std::vector<nlohmann::json>>();
  std::sort(
      sorted_files.begin(),
      sorted_files.end(),
      [](const nlohmann::json& lhs, const nlohmann::json& rhs) {
        const int lhs_root = lhs.value("root_index", 0);
        const int rhs_root = rhs.value("root_index", 0);
        if (lhs_root != rhs_root) {
          return lhs_root < rhs_root;
        }
        return lhs.value("relative_path", std::string{}) <
               rhs.value("relative_path", std::string{});
      });
  std::string canonical = "naim-model-manifest-v1\n";
  for (const auto& file : sorted_files) {
    canonical += "file ";
    canonical += std::to_string(file.value("root_index", 0));
    canonical += " ";
    canonical += file.value("relative_path", std::string{});
    canonical += " ";
    canonical += std::to_string(JsonUintmax(file, "size_bytes").value_or(0));
    canonical += " ";
    canonical += file.value("sha256", std::string{});
    canonical += "\n";
  }
  return canonical;
}

struct RuntimeHttpResponse {
  int status_code = 502;
  std::string content_type = "application/json";
  std::string body;
  std::map<std::string, std::string> headers;
};

std::string TrimAscii(std::string value) {
  const auto begin = std::find_if(
      value.begin(),
      value.end(),
      [](unsigned char ch) { return std::isspace(ch) == 0; });
  const auto end = std::find_if(
      value.rbegin(),
      value.rend(),
      [](unsigned char ch) { return std::isspace(ch) == 0; }).base();
  if (begin >= end) {
    return {};
  }
  return std::string(begin, end);
}

std::string LowercaseAscii(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

bool IsLoopbackRuntimeHost(const std::string& host) {
  return host == "127.0.0.1" || host == "localhost" || host == "::1";
}

bool IsAllowedRuntimeProxyPath(const std::string& method, const std::string& path) {
  const std::string route = path.substr(0, path.find('?'));
  if (method == "GET" && route == "/health") {
    return true;
  }
  if (method == "GET" && route.rfind("/v1/models", 0) == 0) {
    return true;
  }
  if (method == "POST" && route.rfind("/v1/chat/completions", 0) == 0) {
    return true;
  }
  return false;
}

bool IsAllowedKnowledgeVaultProxyPath(const std::string& method, const std::string& path) {
  const std::string route = path.substr(0, path.find('?'));
  if (method == "GET" && route == "/health") {
    return true;
  }
  if (method == "GET" && route == "/v1/status") {
    return true;
  }
  if (method == "GET" && route.rfind("/v1/blocks/", 0) == 0) {
    return true;
  }
  if (method == "GET" && route.rfind("/v1/heads/", 0) == 0) {
    return true;
  }
  if (method == "GET" && route.rfind("/v1/capsules/", 0) == 0) {
    return true;
  }
  if (method == "GET" && (route == "/v1/reviews" || route == "/v1/catalog")) {
    return true;
  }
  if (method == "GET" && route == "/v1/replica-merges/status") {
    return true;
  }
  if (method == "POST" &&
      (route == "/v1/blocks" || route == "/v1/relations" || route == "/v1/search" ||
       route == "/v1/context" || route == "/v1/source-ingest" || route == "/v1/capsules" ||
       route == "/v1/overlays" || route == "/v1/replica-merges/trigger" ||
       route == "/v1/replica-merges/schedule" || route == "/v1/replica-merges/run-due" ||
       route == "/v1/repair" || route == "/v1/markdown-export" ||
       route == "/v1/graph-neighborhood" || route == "/v1/catalog")) {
    return true;
  }
  if (method == "PUT" &&
      (route.rfind("/v1/heads/", 0) == 0 || route.rfind("/v1/reviews/", 0) == 0)) {
    return true;
  }
  return false;
}

RuntimeHttpResponse ParseRuntimeHttpResponse(const std::string& response_text) {
  RuntimeHttpResponse response;
  const std::size_t headers_end = response_text.find("\r\n\r\n");
  const std::string header_text =
      headers_end == std::string::npos ? response_text : response_text.substr(0, headers_end);
  response.body =
      headers_end == std::string::npos ? std::string{} : response_text.substr(headers_end + 4);

  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line =
      line_end == std::string::npos ? header_text : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  std::string http_version;
  stream >> http_version >> response.status_code;

  std::size_t offset = line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = LowercaseAscii(TrimAscii(line.substr(0, colon)));
      const std::string value = TrimAscii(line.substr(colon + 1));
      response.headers[key] = value;
      if (key == "content-type") {
        response.content_type = value;
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return response;
}

RuntimeHttpResponse SendRuntimeHttpRequest(
    const std::string& host,
    int port,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::map<std::string, std::string>& headers,
    bool (*is_allowed_path)(const std::string&, const std::string&),
    const std::string& proxy_label) {
  if (!IsLoopbackRuntimeHost(host)) {
    throw std::runtime_error(proxy_label + " target host must be loopback");
  }
  if (port <= 0) {
    throw std::runtime_error(proxy_label + " target port is invalid");
  }
  if (!is_allowed_path(method, path)) {
    throw std::runtime_error(proxy_label + " rejected unsupported path: " + path);
  }

  naim::platform::EnsureSocketsInitialized();
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(port);
  const int lookup = getaddrinfo(host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error(
        "failed to resolve " + proxy_label + " target: " + std::string(gai_strerror(lookup)));
  }

  naim::platform::SocketHandle fd = naim::platform::kInvalidSocket;
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    fd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (!naim::platform::IsSocketValid(fd)) {
      continue;
    }
    if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    naim::platform::CloseSocket(fd);
    fd = naim::platform::kInvalidSocket;
  }
  freeaddrinfo(results);
  if (!naim::platform::IsSocketValid(fd)) {
    throw std::runtime_error("failed to connect to " + proxy_label + " target");
  }

  std::ostringstream request;
  request << method << " " << path << " HTTP/1.1\r\n";
  request << "Host: " << host << ":" << port << "\r\n";
  request << "Connection: close\r\n";
  for (const auto& [key, value] : headers) {
    request << key << ": " << value << "\r\n";
  }
  if (!body.empty()) {
    if (headers.find("Content-Type") == headers.end() &&
        headers.find("content-type") == headers.end()) {
      request << "Content-Type: application/json\r\n";
    }
    request << "Content-Length: " << body.size() << "\r\n";
  }
  request << "\r\n";
  request << body;

  const std::string request_text = request.str();
  const char* data = request_text.c_str();
  std::size_t remaining = request_text.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, data, remaining, 0);
    if (written <= 0) {
      const std::string error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(fd);
      throw std::runtime_error("failed to write " + proxy_label + " request: " + error);
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }

  std::string response_text;
  std::array<char, 8192> buffer{};
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count < 0) {
      const std::string error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(fd);
      throw std::runtime_error("failed to read " + proxy_label + " response: " + error);
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  naim::platform::CloseSocket(fd);
  return ParseRuntimeHttpResponse(response_text);
}

std::string SafeContainerToken(std::string value) {
  for (char& ch : value) {
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (std::isalnum(uch) == 0 && ch != '-' && ch != '_') {
      ch = '-';
    }
  }
  if (value.empty()) {
    return "default";
  }
  return value;
}

std::map<std::string, std::string> ParseRuntimeProxyHeaders(const nlohmann::json& headers_json) {
  std::map<std::string, std::string> headers;
  if (!headers_json.is_array()) {
    return headers;
  }
  for (const auto& item : headers_json) {
    if (item.is_array() && item.size() == 2 && item[0].is_string() && item[1].is_string()) {
      const std::string key = item[0].get<std::string>();
      if (LowercaseAscii(key) == "host" || LowercaseAscii(key) == "content-length" ||
          LowercaseAscii(key) == "connection") {
        continue;
      }
      headers[key] = item[1].get<std::string>();
    }
  }
  return headers;
}

}  // namespace

HostdAppAssignmentSupport::HostdAppAssignmentSupport()
    : path_support_(),
      runtime_telemetry_support_(),
      local_state_path_support_(),
      local_state_repository_(local_state_path_support_),
      local_runtime_state_support_(
          path_support_,
          local_state_repository_,
          runtime_telemetry_support_),
      command_support_(),
      file_support_(),
      compose_runtime_support_(command_support_),
      disk_runtime_support_(command_support_, path_support_, file_support_),
      apply_plan_support_(
          command_support_,
          compose_runtime_support_,
          disk_runtime_support_,
          file_support_),
      post_deploy_support_(command_support_),
      reporting_support_(),
      model_library_transfer_support_(
          command_support_,
          file_support_,
          reporting_support_),
      bootstrap_model_support_factory_(
          path_support_,
          command_support_,
          file_support_,
          reporting_support_),
      bootstrap_model_support_(bootstrap_model_support_factory_.Create()),
      display_support_(path_support_),
      apply_support_(
          path_support_,
          display_support_,
          apply_plan_support_,
          disk_runtime_support_,
          post_deploy_support_,
          local_state_repository_,
          local_runtime_state_support_,
          bootstrap_model_support_),
      observation_support_() {}

naim::DesiredState HostdAppAssignmentSupport::RebaseStateForRuntimeRoot(
    naim::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  return path_support_.RebaseStateForRuntimeRoot(std::move(state), storage_root, runtime_root);
}

nlohmann::json HostdAppAssignmentSupport::BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& phase_label,
    const std::string& message,
    int progress_percent,
    const std::string& plane_name,
    const std::string& node_name) const {
  return reporting_support_.BuildAssignmentProgressPayload(
      phase,
      phase_label,
      message,
      progress_percent,
      plane_name,
      node_name);
}

void HostdAppAssignmentSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress) const {
  reporting_support_.PublishAssignmentProgress(backend, assignment_id, progress);
}

std::vector<std::string> HostdAppAssignmentSupport::ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag) const {
  return reporting_support_.ParseTaggedCsv(tagged_message, tag);
}

naim::HostObservation HostdAppAssignmentSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& storage_root,
    const std::string& state_root,
    naim::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return observation_support_.BuildObservedStateSnapshot(
      node_name,
      storage_root,
      state_root,
      status,
      status_message,
      assignment_id);
}

std::map<std::string, int> HostdAppAssignmentSupport::CaptureServiceHostPids(
    const std::vector<std::string>& service_names) const {
  return reporting_support_.CaptureServiceHostPids(service_names);
}

bool HostdAppAssignmentSupport::VerifyEvictionAssignment(
    const naim::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids) const {
  return reporting_support_.VerifyEvictionAssignment(
      desired_state,
      node_name,
      state_root,
      tagged_message,
      expected_victim_host_pids);
}

void HostdAppAssignmentSupport::ApplyDesiredNodeState(
    const naim::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) const {
  apply_support_.ApplyDesiredNodeState(
      desired_node_state,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root,
      compose_mode,
      source_label,
      desired_generation,
      assignment_id,
      backend,
      [&](const std::string& phase,
          const std::string& title,
          const std::string& detail,
          int percent,
          const std::string& plane_name,
          const std::string& node_name) {
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                phase,
                title,
                detail,
                percent,
                plane_name,
                node_name));
      });
}

void HostdAppAssignmentSupport::DownloadModelLibraryArtifacts(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  const std::vector<std::string> source_urls =
      payload.value("source_urls", std::vector<std::string>{});
  const std::vector<std::string> target_paths =
      payload.value("target_paths", std::vector<std::string>{});
  if (source_urls.empty()) {
    throw std::runtime_error("model-library-download payload is missing source_urls");
  }
  if (source_urls.size() != target_paths.size()) {
    throw std::runtime_error("model-library-download payload source_urls/target_paths mismatch");
  }

  std::uintmax_t aggregate_prefix = 0;
  std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
  for (const auto& source_url : source_urls) {
    const auto size = model_library_transfer_support_.ProbeContentLength(source_url);
    if (!size.has_value()) {
      aggregate_total = std::nullopt;
      break;
    }
    *aggregate_total += *size;
  }

  for (std::size_t index = 0; index < source_urls.size(); ++index) {
    model_library_transfer_support_.DownloadFileWithProgress(
        source_urls[index],
        target_paths[index],
        backend,
        assignment_id,
        payload.value("job_id", std::string("model-library")),
        node_name,
        index,
        source_urls.size(),
        aggregate_prefix,
        aggregate_total);
    if (const auto size =
            model_library_transfer_support_.FileSizeIfExists(target_paths[index]);
        size.has_value()) {
      aggregate_prefix += *size;
    }
  }
}

void HostdAppAssignmentSupport::ReadModelArtifactChunk(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("model-artifact-read-chunk requires a controller assignment");
  }
  const std::string source_node_name = payload.value("source_node_name", node_name);
  if (source_node_name != node_name) {
    throw std::runtime_error("model-artifact-read-chunk source node mismatch");
  }
  const std::string source_path =
      NormalizePathString(std::filesystem::path(payload.value("source_path", std::string{})));
  if (source_path.empty() || source_path.front() != '/') {
    throw std::runtime_error("model-artifact-read-chunk payload is missing source_path");
  }
  const std::uintmax_t offset =
      payload.contains("offset") && payload.at("offset").is_number_unsigned()
          ? payload.at("offset").get<std::uintmax_t>()
          : std::uintmax_t{0};
  std::uintmax_t max_bytes =
      payload.contains("max_bytes") && payload.at("max_bytes").is_number_unsigned()
          ? payload.at("max_bytes").get<std::uintmax_t>()
          : kMaxModelArtifactChunkBytes;
  if (max_bytes == 0 || max_bytes > kMaxModelArtifactChunkBytes) {
    max_bytes = kMaxModelArtifactChunkBytes;
  }

  std::ifstream input(source_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open model artifact chunk source: " + source_path);
  }
  input.seekg(0, std::ios::end);
  const auto end_position = input.tellg();
  if (end_position < 0) {
    throw std::runtime_error("failed to determine model artifact size: " + source_path);
  }
  const std::uintmax_t total_size = static_cast<std::uintmax_t>(end_position);
  const bool offset_past_eof = offset >= total_size;
  std::uintmax_t bytes_to_read = 0;
  if (!offset_past_eof) {
    bytes_to_read = std::min(max_bytes, total_size - offset);
  }
  std::vector<unsigned char> bytes(static_cast<std::size_t>(bytes_to_read));
  if (bytes_to_read > 0) {
    input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    input.read(
        reinterpret_cast<char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    const auto bytes_read = input.gcount();
    if (bytes_read < 0) {
      throw std::runtime_error("failed to read model artifact chunk: " + source_path);
    }
    bytes.resize(static_cast<std::size_t>(bytes_read));
  }
  const std::uintmax_t next_offset = offset + bytes.size();
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "chunk-ready"},
          {"title", "Model artifact chunk ready"},
          {"detail", "Storage node read a model artifact chunk."},
          {"percent", next_offset >= total_size ? 100 : 50},
          {"transfer_id", payload.value("transfer_id", std::string{})},
          {"requester_node_name", payload.value("requester_node_name", std::string{})},
          {"source_node_name", source_node_name},
          {"source_path", source_path},
          {"offset", offset},
          {"next_offset", next_offset},
          {"bytes_read", bytes.size()},
          {"bytes_total", total_size},
          {"eof", next_offset >= total_size},
          {"bytes_base64", naim::EncodeBytesBase64(bytes)}});
}

void HostdAppAssignmentSupport::BuildModelArtifactManifest(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("model-artifact-build-manifest requires a controller assignment");
  }
  const std::string source_node_name = payload.value("source_node_name", node_name);
  if (source_node_name != node_name) {
    throw std::runtime_error("model-artifact-build-manifest source node mismatch");
  }
  if (!payload.contains("source_paths") || !payload.at("source_paths").is_array()) {
    throw std::runtime_error("model-artifact-build-manifest payload is missing source_paths");
  }

  nlohmann::json roots = nlohmann::json::array();
  nlohmann::json files = nlohmann::json::array();
  std::uintmax_t total_size = 0;
  int root_index = 0;
  for (const auto& source_path_item : payload.at("source_paths")) {
    if (!source_path_item.is_string()) {
      continue;
    }
    const std::filesystem::path source_path(
        NormalizePathString(std::filesystem::path(source_path_item.get<std::string>())));
    const std::string source_path_text = source_path.string();
    if (source_path_text.empty() || source_path_text.front() != '/') {
      throw std::runtime_error("model-artifact-build-manifest source path must be absolute");
    }

    std::error_code error;
    if (!std::filesystem::exists(source_path, error) || error) {
      throw std::runtime_error("model artifact manifest source does not exist: " + source_path_text);
    }

    const bool source_is_directory = std::filesystem::is_directory(source_path, error) && !error;
    roots.push_back(
        nlohmann::json{
            {"source_path", source_path_text},
            {"kind", source_is_directory ? "directory" : "file"},
            {"root_index", root_index},
        });

    auto append_file =
        [&](const std::filesystem::path& current_path, const std::filesystem::path& relative_path) {
          if (!IsSafeRelativePath(relative_path)) {
            throw std::runtime_error(
                "model artifact manifest contains unsafe relative path: " +
                relative_path.generic_string());
          }
          std::error_code file_error;
          const auto size = std::filesystem::file_size(current_path, file_error);
          if (file_error) {
            throw std::runtime_error(
                "failed to read model artifact file size: " + current_path.string());
          }
          total_size += size;
          const std::string sha256 = naim::ComputeFileSha256Hex(current_path.string());
          files.push_back(
              nlohmann::json{
                  {"root_index", root_index},
                  {"source_path", NormalizePathString(current_path)},
                  {"relative_path", relative_path.generic_string()},
                  {"size_bytes", size},
                  {"sha256", sha256},
              });
        };

    if (source_is_directory) {
      for (std::filesystem::recursive_directory_iterator iterator(
               source_path,
               std::filesystem::directory_options::skip_permission_denied,
               error);
           !error && iterator != std::filesystem::recursive_directory_iterator();
           iterator.increment(error)) {
        if (error) {
          break;
        }
        if (!iterator->is_regular_file(error)) {
          error.clear();
          continue;
        }
        error.clear();
        append_file(iterator->path(), iterator->path().lexically_relative(source_path));
      }
      if (error) {
        throw std::runtime_error(
            "failed to walk model artifact directory: " + source_path_text);
      }
    } else if (std::filesystem::is_regular_file(source_path, error) && !error) {
      append_file(source_path, source_path.filename());
    } else {
      throw std::runtime_error(
          "model artifact manifest source is neither file nor directory: " + source_path_text);
    }
    ++root_index;
  }

  if (files.empty()) {
    throw std::runtime_error("model artifact manifest has no files");
  }
  const std::string manifest_sha256 = naim::ComputeSha256Hex(BuildManifestCanonicalText(files));

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "manifest-ready"},
          {"title", "Model artifact manifest ready"},
          {"detail", "Storage node built a model artifact file manifest."},
          {"percent", 100},
          {"transfer_id", payload.value("transfer_id", std::string{})},
          {"requester_node_name", payload.value("requester_node_name", std::string{})},
          {"source_node_name", source_node_name},
          {"roots", roots},
          {"files", files},
          {"file_count", files.size()},
          {"bytes_total", total_size},
          {"manifest_algorithm", "naim-model-manifest-v1"},
          {"manifest_sha256", manifest_sha256},
      });
}

void HostdAppAssignmentSupport::ExecuteRuntimeHttpProxy(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("runtime-http-proxy requires a controller assignment");
  }
  const std::string method = payload.value("method", std::string{});
  const std::string path = payload.value("path", std::string{});
  const std::string host = payload.value("target_host", std::string("127.0.0.1"));
  const int port = payload.value("target_port", 0);
  const std::string body = payload.value("body", std::string{});
  const std::map<std::string, std::string> headers =
      ParseRuntimeProxyHeaders(payload.value("headers", nlohmann::json::array()));

  const RuntimeHttpResponse response = SendRuntimeHttpRequest(
      host,
      port,
      method,
      path,
      body,
      headers,
      IsAllowedRuntimeProxyPath,
      "runtime-http-proxy");
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "response-ready"},
          {"title", "Runtime proxy response ready"},
          {"detail", "Hostd executed the runtime HTTP request locally."},
          {"percent", 100},
          {"request_id", payload.value("request_id", std::string{})},
          {"plane_name", payload.value("plane_name", std::string{})},
          {"node_name", node_name},
          {"method", method},
          {"path", path},
          {"status_code", response.status_code},
          {"content_type", response.content_type},
          {"headers", response.headers},
          {"body", response.body}});
}

void HostdAppAssignmentSupport::ApplyKnowledgeVaultService(
    const nlohmann::json& payload,
    const std::string& node_name,
    const std::string& storage_root,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("knowledge-vault-apply requires a controller assignment");
  }
  const std::string service_id = payload.value("service_id", std::string("kv_default"));
  const std::string image = payload.value("image", std::string{});
  const int port = payload.value("endpoint_port", 18200);
  if (image.empty()) {
    throw std::runtime_error("knowledge-vault-apply payload is missing image");
  }
  if (port <= 0) {
    throw std::runtime_error("knowledge-vault-apply endpoint port is invalid");
  }

  const std::string safe_service_id = SafeContainerToken(service_id);
  const std::string container_name = "naim-knowledge-vault-" + safe_service_id;
  const std::string effective_storage_root =
      storage_root.empty() ? payload.value("storage_root", std::string{}) : storage_root;
  if (effective_storage_root.empty()) {
    throw std::runtime_error("knowledge-vault-apply storage root is empty");
  }
  const std::filesystem::path service_root =
      std::filesystem::path(effective_storage_root) / "knowledge-vault" / safe_service_id;
  std::filesystem::create_directories(service_root);

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "starting"},
          {"title", "Starting knowledge vault"},
          {"detail", "Hostd is preparing the knowledge vault container."},
          {"percent", 10},
          {"service_id", service_id},
          {"node_name", node_name},
          {"storage_root", service_root.string()}});

  const std::string docker = command_support_.ResolvedDockerCommand();
  const auto quote = [&](const std::string& value) {
    return command_support_.ShellQuote(value);
  };
  command_support_.RunCommandOk(
      docker + " rm -f " + quote(container_name) + " >/dev/null 2>&1 || true");
  if (!command_support_.RunCommandOk(docker + " pull " + quote(image))) {
    throw std::runtime_error("failed to pull knowledge vault image: " + image);
  }

  std::ostringstream run_command;
  run_command
      << docker << " run -d"
      << " --name " << quote(container_name)
      << " --restart unless-stopped"
      << " -p 127.0.0.1:" << port << ":" << port
      << " -v " << quote(service_root.string()) << ":/naim/knowledge"
      << " -e " << quote("NAIM_KNOWLEDGE_SERVICE_ID=" + service_id)
      << " -e " << quote("NAIM_NODE_NAME=" + node_name)
      << " -e " << quote("NAIM_KNOWLEDGE_LISTEN_HOST=0.0.0.0")
      << " -e " << quote("NAIM_KNOWLEDGE_PORT=" + std::to_string(port))
      << " -e " << quote("NAIM_KNOWLEDGE_DB_PATH=/naim/knowledge/knowledge.sqlite")
      << " -e " << quote("NAIM_KNOWLEDGE_STATUS_PATH=/naim/knowledge/runtime-status.json")
      << " " << quote(image);
  if (!command_support_.RunCommandOk(run_command.str())) {
    throw std::runtime_error("failed to start knowledge vault container: " + container_name);
  }

  RuntimeHttpResponse health;
  bool ready = false;
  for (int attempt = 0; attempt < 40; ++attempt) {
    try {
      health = SendRuntimeHttpRequest(
          "127.0.0.1",
          port,
          "GET",
          "/health",
          "",
          {},
          IsAllowedKnowledgeVaultProxyPath,
          "knowledge-vault-http-proxy");
      if (health.status_code >= 200 && health.status_code < 300) {
        ready = true;
        break;
      }
    } catch (const std::exception&) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  if (!ready) {
    throw std::runtime_error("knowledge vault container did not become healthy");
  }

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "completed"},
          {"title", "Knowledge vault ready"},
          {"detail", "Knowledge vault container is running on this storage node."},
          {"percent", 100},
          {"service_id", service_id},
          {"node_name", node_name},
          {"container_name", container_name},
          {"endpoint_port", port},
          {"health_status_code", health.status_code}});
}

void HostdAppAssignmentSupport::StopKnowledgeVaultService(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("knowledge-vault-stop requires a controller assignment");
  }
  const std::string service_id = payload.value("service_id", std::string("kv_default"));
  const std::string container_name =
      "naim-knowledge-vault-" + SafeContainerToken(service_id);
  const std::string docker = command_support_.ResolvedDockerCommand();
  const std::string command = docker + " rm -f " +
                              command_support_.ShellQuote(container_name) +
                              " >/dev/null 2>&1 || true";
  if (!command_support_.RunCommandOk(command)) {
    throw std::runtime_error("failed to stop knowledge vault container: " + container_name);
  }
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "completed"},
          {"title", "Knowledge vault stopped"},
          {"detail", "Hostd stopped the knowledge vault container."},
          {"percent", 100},
          {"service_id", service_id},
          {"node_name", node_name},
          {"container_name", container_name}});
}

void HostdAppAssignmentSupport::ExecuteKnowledgeVaultHttpProxy(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("knowledge-vault-http-proxy requires a controller assignment");
  }
  const std::string method = payload.value("method", std::string{});
  const std::string path = payload.value("path", std::string{});
  const std::string host = payload.value("target_host", std::string("127.0.0.1"));
  const int port = payload.value("target_port", 0);
  const std::string body = payload.value("body", std::string{});
  const std::map<std::string, std::string> headers =
      ParseRuntimeProxyHeaders(payload.value("headers", nlohmann::json::array()));

  const RuntimeHttpResponse response = SendRuntimeHttpRequest(
      host,
      port,
      method,
      path,
      body,
      headers,
      IsAllowedKnowledgeVaultProxyPath,
      "knowledge-vault-http-proxy");
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "response-ready"},
          {"title", "Knowledge vault proxy response ready"},
          {"detail", "Hostd executed the knowledge vault HTTP request locally."},
          {"percent", 100},
          {"relay_id", payload.value("relay_id", std::string{})},
          {"service_id", payload.value("service_id", std::string{})},
          {"node_name", node_name},
          {"method", method},
          {"path", path},
          {"status_code", response.status_code},
          {"content_type", response.content_type},
          {"headers", response.headers},
          {"body", response.body}});
}

void HostdAppAssignmentSupport::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  display_support_.ShowDemoOps(node_name, storage_root, runtime_root);
}

void HostdAppAssignmentSupport::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) const {
  display_support_.ShowStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root);
}

void HostdAppAssignmentSupport::AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  observation_support_.AppendHostdEvent(
      backend,
      category,
      event_type,
      message,
      payload,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      severity);
}

}  // namespace naim::hostd
