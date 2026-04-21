#include "app/hostd_bootstrap_model_support.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <map>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <netdb.h>

#include "naim/core/platform_compat.h"
#include "naim/security/crypto_utils.h"

namespace naim::hostd {

namespace fs = std::filesystem;

namespace {

constexpr std::uintmax_t kControllerRelayedChunkBytes = 4ULL * 1024ULL * 1024ULL;
constexpr std::uintmax_t kPeerDirectChunkBytes = 64ULL * 1024ULL * 1024ULL;
constexpr int kControllerRelayedChunkPollAttempts = 600;
constexpr std::chrono::milliseconds kControllerRelayedChunkPollInterval(500);

struct PeerHttpResponse {
  int status_code = 0;
  std::map<std::string, std::string> headers;
  std::string body;
};

struct PeerEndpoint {
  std::string raw;
  std::string host;
  int port = 29999;
};

PeerEndpoint ParsePeerEndpoint(std::string endpoint) {
  PeerEndpoint parsed;
  parsed.raw = endpoint;
  if (endpoint.rfind("http://", 0) == 0) {
    endpoint = endpoint.substr(7);
  }
  const std::size_t slash = endpoint.find('/');
  if (slash != std::string::npos) {
    endpoint = endpoint.substr(0, slash);
  }
  const std::size_t colon = endpoint.rfind(':');
  if (colon == std::string::npos) {
    parsed.host = endpoint;
  } else {
    parsed.host = endpoint.substr(0, colon);
    parsed.port = std::stoi(endpoint.substr(colon + 1));
  }
  if (parsed.host.empty()) {
    throw std::runtime_error("invalid peer endpoint: " + parsed.raw);
  }
  return parsed;
}

PeerHttpResponse SendPeerHttpRawRequest(
    const std::string& endpoint,
    const std::string& path,
    const std::string& body,
    const std::string& content_type,
    const std::map<std::string, std::string>& extra_headers);

PeerHttpResponse SendPeerHttpRequest(
    const std::string& endpoint,
    const std::string& path,
    const nlohmann::json& payload) {
  std::map<std::string, std::string> headers;
  const std::string body = payload.dump();
  return SendPeerHttpRawRequest(endpoint, path, body, "application/json", headers);
}

PeerHttpResponse SendPeerHttpRawRequest(
    const std::string& endpoint,
    const std::string& path,
    const std::string& body,
    const std::string& content_type,
    const std::map<std::string, std::string>& extra_headers) {
  const PeerEndpoint target = ParsePeerEndpoint(endpoint);
  naim::platform::EnsureSocketsInitialized();
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(target.port);
  const int lookup = getaddrinfo(target.host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error("failed to resolve peer endpoint: " + target.raw);
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
    throw std::runtime_error("failed to connect to peer endpoint: " + target.raw);
  }
  std::ostringstream request;
  request << "POST " << path << " HTTP/1.1\r\n";
  request << "Host: " << target.host << ":" << target.port << "\r\n";
  request << "Connection: close\r\n";
  request << "Content-Type: " << content_type << "\r\n";
  for (const auto& [key, value] : extra_headers) {
    request << key << ": " << value << "\r\n";
  }
  request << "Content-Length: " << body.size() << "\r\n\r\n";
  request << body;
  const std::string request_text = request.str();
  const char* data = request_text.data();
  std::size_t remaining = request_text.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, data, remaining, 0);
    if (written <= 0) {
      naim::platform::CloseSocket(fd);
      throw std::runtime_error("failed to write peer request");
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
  std::string response_text;
  std::array<char, 8192> buffer{};
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count < 0) {
      naim::platform::CloseSocket(fd);
      throw std::runtime_error("failed to read peer response");
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  naim::platform::CloseSocket(fd);
  const std::size_t headers_end = response_text.find("\r\n\r\n");
  const std::string headers =
      headers_end == std::string::npos ? response_text : response_text.substr(0, headers_end);
  PeerHttpResponse response;
  std::istringstream status(headers.substr(0, headers.find("\r\n")));
  std::string version;
  status >> version >> response.status_code;
  std::istringstream header_lines(headers);
  std::string header_line;
  bool first_header = true;
  while (std::getline(header_lines, header_line)) {
    if (!header_line.empty() && header_line.back() == '\r') {
      header_line.pop_back();
    }
    if (first_header) {
      first_header = false;
      continue;
    }
    const std::size_t colon = header_line.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    std::string key = header_line.substr(0, colon);
    std::transform(key.begin(), key.end(), key.begin(), [](unsigned char ch) {
      return static_cast<char>(std::tolower(ch));
    });
    std::string value = header_line.substr(colon + 1);
    while (!value.empty() && value.front() == ' ') {
      value.erase(value.begin());
    }
    response.headers[key] = value;
  }
  response.body =
      headers_end == std::string::npos ? std::string{} : response_text.substr(headers_end + 4);
  if (response.status_code >= 400) {
    std::string detail = response.body;
    if (detail.size() > 512) {
      detail = detail.substr(0, 512);
    }
    throw std::runtime_error(
        "peer request failed with status " + std::to_string(response.status_code) +
        (detail.empty() ? std::string{} : ": " + detail));
  }
  return response;
}

std::optional<std::uintmax_t> JsonUintmax(const nlohmann::json& payload, const std::string& key) {
  if (!payload.contains(key) || !payload.at(key).is_number_unsigned()) {
    return std::nullopt;
  }
  return payload.at(key).get<std::uintmax_t>();
}

std::string BuildManifestCanonicalText(const std::vector<nlohmann::json>& files) {
  std::vector<nlohmann::json> sorted_files = files;
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

std::string ComputeManifestSha256Hex(const std::vector<nlohmann::json>& files) {
  return naim::ComputeSha256Hex(BuildManifestCanonicalText(files));
}

std::string NormalizeLowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string EnvString(const char* name) {
  const char* value = std::getenv(name);
  return value == nullptr ? std::string{} : std::string(value);
}

std::string SanitizePathPart(std::string value) {
  for (char& ch : value) {
    const bool ok = std::isalnum(static_cast<unsigned char>(ch)) || ch == '-' || ch == '_' ||
                    ch == '.';
    if (!ok) {
      ch = '-';
    }
  }
  while (value.find("--") != std::string::npos) {
    value.replace(value.find("--"), 2, "-");
  }
  if (value.empty() || value == "." || value == "..") {
    return "model";
  }
  return value;
}

bool EndsWithIgnoreCase(const std::string& value, const std::string& suffix) {
  if (value.size() < suffix.size()) {
    return false;
  }
  return NormalizeLowercase(value.substr(value.size() - suffix.size())) ==
         NormalizeLowercase(suffix);
}

}  // namespace

HostdBootstrapModelSupport::HostdBootstrapModelSupport(
    const HostdBootstrapModelArtifactSupport& artifact_support,
    const HostdBootstrapActiveModelSupport& active_model_support,
    const HostdBootstrapTransferSupport& transfer_support,
    const HostdCommandSupport& command_support,
    const HostdFileSupport& file_support,
    const HostdReportingSupport& reporting_support)
    : artifact_support_(artifact_support),
      active_model_support_(active_model_support),
      transfer_support_(transfer_support),
      command_support_(command_support),
      file_support_(file_support),
      reporting_support_(reporting_support) {}

void HostdBootstrapModelSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done,
    const std::optional<std::uintmax_t>& bytes_total) const {
  reporting_support_.PublishAssignmentProgress(
      backend,
      assignment_id,
      reporting_support_.BuildAssignmentProgressPayload(
          phase,
          title,
          detail,
          percent,
          plane_name,
          node_name,
          bytes_done,
          bytes_total));
}

bool HostdBootstrapModelSupport::TryUseReferenceBootstrapModel(
    const naim::DesiredState& state,
    const std::string& node_name,
    const naim::BootstrapModelSpec& bootstrap_model,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (bootstrap_model.materialization_mode != "reference" ||
      !bootstrap_model.local_path.has_value() ||
      bootstrap_model.local_path->empty()) {
    return false;
  }

  std::error_code error;
  if (!fs::exists(*bootstrap_model.local_path, error) || error) {
    if (bootstrap_model.source_node_name.has_value() &&
        !bootstrap_model.source_node_name->empty()) {
      return false;
    }
    throw std::runtime_error(
        "bootstrap model reference path does not exist: " + *bootstrap_model.local_path);
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "using-model-reference",
      "Using model reference",
      "Using the configured model path directly without copying it into the plane shared disk.",
      72,
      state.plane_name,
      node_name);
  active_model_support_.WriteBootstrapActiveModel(
      state,
      node_name,
      *bootstrap_model.local_path,
      *bootstrap_model.local_path);
  return true;
}

bool HostdBootstrapModelSupport::TryAcquireControllerRelayedBootstrapModel(
    const naim::DesiredState& state,
    const std::string& node_name,
    const naim::BootstrapModelSpec& bootstrap_model,
    const std::string& target_path,
    const bool write_active_model,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (!bootstrap_model.local_path.has_value() ||
      bootstrap_model.local_path->empty() ||
      !bootstrap_model.source_node_name.has_value() ||
      bootstrap_model.source_node_name->empty()) {
    return false;
  }

  if (backend == nullptr) {
    throw std::runtime_error(
        "bootstrap model source_node_name requires a controller backend for model relay");
  }

  std::vector<std::string> source_paths = bootstrap_model.source_paths;
  if (source_paths.empty()) {
    source_paths.push_back(*bootstrap_model.local_path);
  }
  for (auto& source_path : source_paths) {
    source_path = fs::path(source_path).lexically_normal().string();
    if (source_path.empty() || source_path.front() != '/') {
      throw std::runtime_error(
          "bootstrap model controller relay requires absolute source paths");
    }
  }
  std::sort(source_paths.begin(), source_paths.end());
  source_paths.erase(std::unique(source_paths.begin(), source_paths.end()), source_paths.end());

  bool all_sources_exist_locally = !source_paths.empty();
  std::error_code error;
  for (const auto& source_path : source_paths) {
    all_sources_exist_locally =
        all_sources_exist_locally && fs::exists(source_path, error) && !error;
    error.clear();
  }
  if (all_sources_exist_locally) {
    return false;
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "acquiring-model",
      "Acquiring model",
      "Checking direct LAN transfer from storage node " +
          *bootstrap_model.source_node_name + ".",
      20,
      state.plane_name,
      node_name);

  nlohmann::json manifest = nlohmann::json::object();
  bool manifest_ready = false;
  bool use_peer_direct = false;
  std::string peer_ticket_id;
  std::string peer_endpoint;
  try {
    const nlohmann::json ticket = backend->RequestFileTransferTicket(
        node_name,
        *bootstrap_model.source_node_name,
        source_paths);
    if (ticket.value("status", std::string{}) == "issued") {
      peer_ticket_id = ticket.value("ticket_id", std::string{});
      peer_endpoint = ticket.value("source_endpoint", std::string{});
      const PeerHttpResponse peer_manifest = SendPeerHttpRequest(
          peer_endpoint,
          "/peer/v1/files/manifest",
          nlohmann::json{
              {"ticket_id", peer_ticket_id},
              {"source_paths", source_paths},
              {"defer_sha256", true},
          });
      manifest = nlohmann::json::parse(peer_manifest.body);
      if (manifest.value("phase", std::string{}) == "manifest-ready" &&
          manifest.contains("files") &&
          manifest.at("files").is_array()) {
        manifest_ready = true;
        use_peer_direct = true;
      }
    }
  } catch (const std::exception& error) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "acquiring-model",
        "Acquiring model",
        "Direct LAN transfer is unavailable; falling back to controller relay: " +
            std::string(error.what()),
        20,
        state.plane_name,
        node_name);
  }

  if (!manifest_ready) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "acquiring-model",
        "Acquiring model",
        "Requesting bootstrap model manifest from storage node " +
            *bootstrap_model.source_node_name + ".",
        20,
        state.plane_name,
        node_name);
    const nlohmann::json manifest_request = backend->RequestModelArtifactManifest(
        node_name,
        *bootstrap_model.source_node_name,
        source_paths);
    if (manifest_request.value("status", std::string{}) != "queued") {
      throw std::runtime_error(
          "controller did not queue model artifact manifest relay: " +
          manifest_request.value("message", std::string("unknown error")));
    }
    const int manifest_assignment_id = manifest_request.value("assignment_id", 0);
    if (manifest_assignment_id <= 0) {
      throw std::runtime_error("controller returned invalid model artifact manifest assignment id");
    }

    for (int attempt = 0; attempt < kControllerRelayedChunkPollAttempts; ++attempt) {
      const nlohmann::json poll =
          backend->LoadModelArtifactManifest(node_name, manifest_assignment_id);
      const std::string status = poll.value("status", std::string{});
      if (status == "failed" || status == "superseded") {
        throw std::runtime_error(
            "model artifact manifest relay failed: " +
            poll.value("status_message", std::string("unknown error")));
      }
      if (status == "applied") {
        manifest = poll.contains("progress") && poll.at("progress").is_object()
                       ? poll.at("progress")
                       : nlohmann::json::object();
        if (manifest.value("phase", std::string{}) != "manifest-ready" ||
            !manifest.contains("files") ||
            !manifest.at("files").is_array()) {
          throw std::runtime_error("model artifact manifest relay applied without manifest payload");
        }
        manifest_ready = true;
        break;
      }
      std::this_thread::sleep_for(kControllerRelayedChunkPollInterval);
    }
  }
  if (!manifest_ready) {
    throw std::runtime_error("timed out waiting for model artifact manifest relay");
  }

  std::vector<nlohmann::json> files = manifest.at("files").get<std::vector<nlohmann::json>>();
  if (files.empty()) {
    throw std::runtime_error("model artifact manifest is empty");
  }
  std::sort(files.begin(), files.end(), [](const nlohmann::json& lhs, const nlohmann::json& rhs) {
    const int lhs_root = lhs.value("root_index", 0);
    const int rhs_root = rhs.value("root_index", 0);
    if (lhs_root != rhs_root) {
      return lhs_root < rhs_root;
    }
    return lhs.value("relative_path", std::string{}) <
           rhs.value("relative_path", std::string{});
  });
  const bool deferred_peer_sha256 =
      use_peer_direct && manifest.value("sha256_deferred", false);
  for (const auto& file : files) {
    if (!JsonUintmax(file, "size_bytes").has_value()) {
      throw std::runtime_error("model artifact manifest is missing file size metadata");
    }
    if (!deferred_peer_sha256 && file.value("sha256", std::string{}).empty()) {
      throw std::runtime_error("model artifact manifest is missing file checksum metadata");
    }
  }
  const std::string manifest_sha256 = manifest.value("manifest_sha256", std::string{});
  const std::string computed_manifest_sha256 = ComputeManifestSha256Hex(files);
  if (manifest_sha256.empty() ||
      NormalizeLowercase(manifest_sha256) != NormalizeLowercase(computed_manifest_sha256)) {
    throw std::runtime_error("model artifact manifest checksum mismatch");
  }

  bool directory_transfer = false;
  if (manifest.contains("roots") && manifest.at("roots").is_array()) {
    for (const auto& root : manifest.at("roots")) {
      directory_transfer =
          directory_transfer || root.value("kind", std::string{}) == "directory";
    }
  }
  if (bootstrap_model.sha256.has_value()) {
    const std::string expected_artifact_sha256 =
        (!directory_transfer && files.size() == 1)
            ? files.front().value("sha256", std::string{})
            : manifest_sha256;
    if (expected_artifact_sha256.empty()) {
      throw std::runtime_error(
          "bootstrap model artifact checksum cannot be verified from a deferred peer manifest");
    }
    if (NormalizeLowercase(*bootstrap_model.sha256) !=
        NormalizeLowercase(expected_artifact_sha256)) {
      throw std::runtime_error("bootstrap model artifact checksum mismatch in manifest");
    }
  }

  std::vector<std::string> target_paths;
  target_paths.reserve(files.size());
  const fs::path target_root =
      directory_transfer ? fs::path(target_path + ".partdir") : fs::path(target_path).parent_path();
  if (directory_transfer) {
    fs::remove_all(target_root, error);
    fs::create_directories(target_root);
  }

  for (std::size_t index = 0; index < files.size(); ++index) {
    const std::string relative_path =
        fs::path(files[index].value("relative_path", std::string{})).lexically_normal().generic_string();
    if (relative_path.empty() || relative_path == "." || relative_path.front() == '/' ||
        relative_path == ".." || relative_path.rfind("../", 0) == 0) {
      throw std::runtime_error("model artifact manifest contains unsafe relative path");
    }
    fs::path resolved_target;
    if (directory_transfer) {
      resolved_target = target_root / relative_path;
    } else if (files.size() == 1) {
      resolved_target = target_path;
    } else {
      resolved_target = fs::path(target_path).parent_path() / relative_path;
    }
    target_paths.push_back(resolved_target.string());
  }

  bool already_present = true;
  for (std::size_t index = 0; index < files.size(); ++index) {
    const auto expected_size = JsonUintmax(files[index], "size_bytes");
    const auto current_size = transfer_support_.FileSizeIfExists(target_paths[index]);
    already_present = already_present && expected_size.has_value() &&
                      current_size.has_value() && *expected_size == *current_size;
    const std::string expected_sha256 = files[index].value("sha256", std::string{});
    if (already_present && !expected_sha256.empty()) {
      already_present =
          NormalizeLowercase(naim::ComputeFileSha256Hex(target_paths[index])) ==
          NormalizeLowercase(expected_sha256);
    }
  }
  if (already_present) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "using-cached-model",
        "Using cached model",
        "Using the model artifact already present in the plane shared disk.",
        72,
        state.plane_name,
        node_name);
    if (write_active_model) {
      active_model_support_.WriteBootstrapActiveModel(
          state,
          node_name,
          directory_transfer ? target_path : target_paths.front());
    }
    return true;
  }

  const std::uintmax_t aggregate_total =
      JsonUintmax(manifest, "bytes_total").value_or(std::uintmax_t{0});
  std::uintmax_t aggregate_done = 0;
  for (std::size_t file_index = 0; file_index < files.size(); ++file_index) {
    const std::string source_path = files[file_index].value("source_path", std::string{});
    const std::string file_target_path = target_paths[file_index];
    file_support_.EnsureParentDirectory(file_target_path);
    const std::string temp_path = file_target_path + ".part";
    fs::remove(temp_path, error);
    std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
      throw std::runtime_error("failed to open bootstrap model relay target: " + temp_path);
    }

    const std::uintmax_t file_prefix_done = aggregate_done;
    std::uintmax_t offset = 0;
    bool eof = false;
    while (!eof) {
      if (use_peer_direct) {
        try {
          const std::uintmax_t expected_size =
              JsonUintmax(files[file_index], "size_bytes").value_or(0);
          const PeerHttpResponse peer_chunk = SendPeerHttpRequest(
              peer_endpoint,
              "/peer/v1/files/chunk",
              nlohmann::json{
                  {"ticket_id", peer_ticket_id},
                  {"source_path", source_path},
                  {"offset", offset},
                  {"max_bytes", kPeerDirectChunkBytes},
              });
          if (peer_chunk.body.empty() && offset < expected_size) {
            throw std::runtime_error("direct LAN transfer returned an empty non-final chunk");
          }
          if (!peer_chunk.body.empty()) {
            const auto chunk_sha256_it = peer_chunk.headers.find("x-naim-chunk-sha256");
            if (deferred_peer_sha256 && chunk_sha256_it == peer_chunk.headers.end()) {
              throw std::runtime_error("direct LAN transfer chunk is missing checksum header");
            }
            if (chunk_sha256_it != peer_chunk.headers.end() &&
                NormalizeLowercase(naim::ComputeSha256Hex(peer_chunk.body)) !=
                    NormalizeLowercase(chunk_sha256_it->second)) {
              throw std::runtime_error("direct LAN transfer chunk checksum mismatch");
            }
            output.write(peer_chunk.body.data(), static_cast<std::streamsize>(peer_chunk.body.size()));
            if (!output.good()) {
              throw std::runtime_error("failed to write bootstrap model direct target: " + temp_path);
            }
          }
          const std::uintmax_t next_offset = offset + peer_chunk.body.size();
          aggregate_done += next_offset - offset;
          offset = next_offset;
          eof = offset >= expected_size || peer_chunk.body.size() < kPeerDirectChunkBytes;
          int percent = 60;
          if (aggregate_total > 0) {
            percent = 20 + static_cast<int>(
                               (static_cast<double>(aggregate_done) / aggregate_total) * 40.0);
            percent = std::clamp(percent, 20, 60);
          }
          const std::optional<std::uintmax_t> aggregate_total_progress =
              aggregate_total > 0 ? std::optional<std::uintmax_t>(aggregate_total)
                                  : std::nullopt;
          PublishAssignmentProgress(
              backend,
              assignment_id,
              "acquiring-model",
              "Acquiring model",
              "Copying bootstrap model directly from storage node " +
                  *bootstrap_model.source_node_name + " over LAN.",
              percent,
              state.plane_name,
              node_name,
              aggregate_done,
              aggregate_total_progress);
          continue;
        } catch (const std::exception& direct_error) {
          if (deferred_peer_sha256) {
            throw;
          }
          output.close();
          std::error_code cleanup_error;
          fs::remove(temp_path, cleanup_error);
          output.open(temp_path, std::ios::binary | std::ios::trunc);
          if (!output.is_open()) {
            throw std::runtime_error("failed to reopen bootstrap model relay target: " + temp_path);
          }
          aggregate_done = file_prefix_done;
          offset = 0;
          eof = false;
          use_peer_direct = false;
          PublishAssignmentProgress(
              backend,
              assignment_id,
              "acquiring-model",
              "Acquiring model",
              "Direct LAN transfer failed; continuing through the controller relay: " +
                  std::string(direct_error.what()),
              20,
              state.plane_name,
              node_name,
              aggregate_done,
              aggregate_total > 0 ? std::optional<std::uintmax_t>(aggregate_total)
                                  : std::nullopt);
        }
      }
      const nlohmann::json request = backend->RequestModelArtifactChunk(
          node_name,
          *bootstrap_model.source_node_name,
          source_path,
          offset,
          kControllerRelayedChunkBytes);
      if (request.value("status", std::string{}) != "queued") {
        throw std::runtime_error(
            "controller did not queue model artifact chunk relay: " +
            request.value("message", std::string("unknown error")));
      }
      const int chunk_assignment_id = request.value("assignment_id", 0);
      if (chunk_assignment_id <= 0) {
        throw std::runtime_error("controller returned invalid model artifact chunk assignment id");
      }

      bool chunk_ready = false;
      for (int attempt = 0; attempt < kControllerRelayedChunkPollAttempts; ++attempt) {
        const nlohmann::json poll =
            backend->LoadModelArtifactChunk(node_name, chunk_assignment_id);
        const std::string status = poll.value("status", std::string{});
        if (status == "failed" || status == "superseded") {
          throw std::runtime_error(
              "model artifact chunk relay failed: " +
              poll.value("status_message", std::string("unknown error")));
        }
        if (status == "applied") {
          const nlohmann::json progress =
              poll.contains("progress") && poll.at("progress").is_object()
                  ? poll.at("progress")
                  : nlohmann::json::object();
          if (progress.value("phase", std::string{}) != "chunk-ready") {
            throw std::runtime_error("model artifact chunk relay applied without chunk payload");
          }
          const auto progress_offset = JsonUintmax(progress, "offset").value_or(offset);
          if (progress_offset != offset) {
            throw std::runtime_error("model artifact chunk relay returned an unexpected offset");
          }
          const std::vector<unsigned char> bytes =
              naim::DecodeBytesBase64(progress.value("bytes_base64", std::string{}));
          if (bytes.empty() && !progress.value("eof", false)) {
            throw std::runtime_error("model artifact chunk relay returned an empty non-final chunk");
          }
          if (!bytes.empty()) {
            output.write(
                reinterpret_cast<const char*>(bytes.data()),
                static_cast<std::streamsize>(bytes.size()));
            if (!output.good()) {
              throw std::runtime_error("failed to write bootstrap model relay target: " + temp_path);
            }
          }
          const std::uintmax_t next_offset =
              JsonUintmax(progress, "next_offset").value_or(offset + bytes.size());
          aggregate_done += next_offset - offset;
          offset = next_offset;
          eof = progress.value("eof", false);
          int percent = 60;
          if (aggregate_total > 0) {
            percent = 20 + static_cast<int>(
                               (static_cast<double>(aggregate_done) / aggregate_total) * 40.0);
            percent = std::clamp(percent, 20, 60);
          }
          const std::optional<std::uintmax_t> aggregate_total_progress =
              aggregate_total > 0 ? std::optional<std::uintmax_t>(aggregate_total)
                                  : std::nullopt;
          PublishAssignmentProgress(
              backend,
              assignment_id,
              "acquiring-model",
              "Acquiring model",
              "Copying bootstrap model from storage node " + *bootstrap_model.source_node_name +
                  " through the controller.",
              percent,
              state.plane_name,
              node_name,
              aggregate_done,
              aggregate_total_progress);
          chunk_ready = true;
          break;
        }
        std::this_thread::sleep_for(kControllerRelayedChunkPollInterval);
      }
      if (!chunk_ready) {
        throw std::runtime_error("timed out waiting for model artifact chunk relay");
      }
    }

    output.close();
    if (!output.good()) {
      throw std::runtime_error("failed to close bootstrap model relay target: " + temp_path);
    }
    fs::rename(temp_path, file_target_path);
    const std::string expected_file_sha256 =
        files[file_index].value("sha256", std::string{});
    if (!expected_file_sha256.empty() &&
        NormalizeLowercase(naim::ComputeFileSha256Hex(file_target_path)) !=
            NormalizeLowercase(expected_file_sha256)) {
      throw std::runtime_error("model artifact chunk relay file checksum mismatch: " + file_target_path);
    }
  }

  std::string active_target_path = target_paths.front();
  if (directory_transfer) {
    fs::remove_all(target_path, error);
    fs::rename(target_root, target_path);
    active_target_path = target_path;
  }
  if (write_active_model) {
    active_model_support_.WriteBootstrapActiveModel(state, node_name, active_target_path);
  }
  return true;
}

bool HostdBootstrapModelSupport::TryPrepareWorkerBootstrapModel(
    const naim::DesiredState& state,
    const std::string& node_name,
    const naim::BootstrapModelSpec& bootstrap_model,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (bootstrap_model.materialization_mode != "prepare_on_worker") {
    return false;
  }
  if (!bootstrap_model.source_node_name.has_value() ||
      bootstrap_model.source_node_name->empty()) {
    throw std::runtime_error("prepare_on_worker requires source_node_name");
  }

  const std::string target_path = artifact_support_.TargetPath(state, node_name);
  const fs::path target_base(target_path);
  const fs::path output_dir = target_base.parent_path();
  const std::string quantization =
      bootstrap_model.quantization.value_or("base").empty()
          ? std::string("base")
          : bootstrap_model.quantization.value_or("base");
  const std::string source_format =
      NormalizeLowercase(bootstrap_model.source_format.value_or(""));
  const bool source_quantization_matches =
      source_format == "gguf" && quantization != "base" &&
      NormalizeLowercase(bootstrap_model.source_quantization.value_or("")) ==
          NormalizeLowercase(quantization);
  const bool should_quantize = quantization != "base" && !source_quantization_matches;
  const std::string base_stem =
      SanitizePathPart(target_base.stem().string().empty()
                           ? bootstrap_model.model_id
                           : target_base.stem().string());
  const fs::path prepared_path =
      output_dir /
      (quantization == "base" ? base_stem + ".gguf"
                              : base_stem + "-" + SanitizePathPart(quantization) + ".gguf");
  const fs::path signature_path = prepared_path.string() + ".naim-prep.json";
  const nlohmann::json signature = {
      {"version", 1},
      {"model_id", bootstrap_model.model_id},
      {"source_node_name", *bootstrap_model.source_node_name},
      {"source_paths", bootstrap_model.source_paths},
      {"source_format", bootstrap_model.source_format.value_or("")},
      {"source_quantization", bootstrap_model.source_quantization.value_or("")},
      {"desired_output_format", bootstrap_model.desired_output_format.value_or("gguf")},
      {"quantization", quantization},
      {"served_model_name", bootstrap_model.served_model_name.value_or("")},
  };
  std::error_code error;
  if (fs::exists(prepared_path, error) && fs::exists(signature_path, error)) {
    std::ifstream input(signature_path);
    const auto cached_signature = nlohmann::json::parse(input, nullptr, false);
    if (!cached_signature.is_discarded() && cached_signature == signature) {
      PublishAssignmentProgress(
          backend,
          assignment_id,
          "using-cached-prepared-model",
          "Using prepared model",
          "Using the existing worker-local prepared model artifact.",
          82,
          state.plane_name,
          node_name);
      active_model_support_.WriteBootstrapActiveModel(state, node_name, prepared_path.string());
      TryWriteBackPreparedModel(
          state,
          node_name,
          bootstrap_model,
          prepared_path.string(),
          backend,
          assignment_id);
      return true;
    }
  }

  fs::create_directories(output_dir, error);
  const fs::path staging_root = output_dir / (base_stem + ".prepare");
  std::string source_cache_name = "source";
  if (!bootstrap_model.source_paths.empty()) {
    source_cache_name = fs::path(bootstrap_model.source_paths.front()).filename().string();
  } else if (bootstrap_model.local_path.has_value()) {
    source_cache_name = fs::path(*bootstrap_model.local_path).filename().string();
  }
  if (source_cache_name.empty() || source_cache_name == "." || source_cache_name == "..") {
    source_cache_name = "source";
  }
  const fs::path source_cache = staging_root / source_cache_name;
  fs::remove_all(staging_root, error);
  fs::create_directories(staging_root, error);

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "acquiring-model",
      "Acquiring model",
      "Copying source model to worker-local preparation cache.",
      18,
      state.plane_name,
      node_name);

  bool acquired = false;
  if (bootstrap_model.local_path.has_value() && fs::exists(*bootstrap_model.local_path, error) &&
      !error) {
    transfer_support_.CopyFileWithProgress(
        *bootstrap_model.local_path,
        source_cache.string(),
        backend,
        assignment_id,
        state.plane_name,
        node_name);
    acquired = true;
  } else {
    acquired = TryAcquireControllerRelayedBootstrapModel(
        state,
        node_name,
        bootstrap_model,
        source_cache.string(),
        false,
        backend,
        assignment_id);
  }
  if (!acquired) {
    throw std::runtime_error("failed to acquire source model for worker preparation");
  }

  fs::remove(prepared_path, error);
  PublishAssignmentProgress(
      backend,
      assignment_id,
      should_quantize ? "quantizing-model" : "preparing-model",
      should_quantize ? "Quantizing model" : "Preparing model",
      should_quantize
          ? "Generating requested GGUF quantization on worker."
          : (source_quantization_matches
                 ? "Selected GGUF already matches requested quantization; caching on worker."
                 : "Preparing GGUF model artifact on worker."),
      68,
      state.plane_name,
      node_name);

  const bool source_is_directory = fs::is_directory(source_cache, error) && !error;
  if (source_is_directory) {
    const std::string convert_script =
        !EnvString("NAIM_MODEL_LIBRARY_CONVERT_SCRIPT").empty()
            ? EnvString("NAIM_MODEL_LIBRARY_CONVERT_SCRIPT")
            : EnvString("NAIM_LLAMA_CPP_CONVERT_SCRIPT");
    if (convert_script.empty()) {
      throw std::runtime_error(
          "prepare_on_worker requires NAIM_MODEL_LIBRARY_CONVERT_SCRIPT for directory models");
    }
    const fs::path base_output = staging_root / (base_stem + ".gguf");
    const std::string command =
        "python3 " + command_support_.ShellQuote(convert_script) + " " +
        command_support_.ShellQuote(source_cache.string()) + " --outfile " +
        command_support_.ShellQuote(base_output.string());
    if (!command_support_.RunCommandOk(command)) {
      throw std::runtime_error("failed to convert source model directory to GGUF");
    }
    if (!should_quantize) {
      fs::rename(base_output, prepared_path);
    } else {
      const std::string quantize_bin =
          !EnvString("NAIM_MODEL_LIBRARY_QUANTIZE_BIN").empty()
              ? EnvString("NAIM_MODEL_LIBRARY_QUANTIZE_BIN")
              : EnvString("NAIM_LLAMA_CPP_QUANTIZE");
      if (quantize_bin.empty()) {
        throw std::runtime_error(
            "prepare_on_worker requires NAIM_MODEL_LIBRARY_QUANTIZE_BIN for quantization");
      }
      const std::string command =
          command_support_.ShellQuote(quantize_bin) + " " +
          command_support_.ShellQuote(base_output.string()) + " " +
          command_support_.ShellQuote(prepared_path.string()) + " " +
          command_support_.ShellQuote(quantization);
      if (!command_support_.RunCommandOk(command)) {
        throw std::runtime_error("failed to quantize prepared GGUF model");
      }
      fs::remove(base_output, error);
    }
  } else if (EndsWithIgnoreCase(source_cache.filename().string(), ".gguf") ||
             (source_format == "gguf" && fs::is_regular_file(source_cache, error))) {
    if (!should_quantize) {
      fs::rename(source_cache, prepared_path);
    } else {
      const std::string quantize_bin =
          !EnvString("NAIM_MODEL_LIBRARY_QUANTIZE_BIN").empty()
              ? EnvString("NAIM_MODEL_LIBRARY_QUANTIZE_BIN")
              : EnvString("NAIM_LLAMA_CPP_QUANTIZE");
      if (quantize_bin.empty()) {
        throw std::runtime_error(
            "prepare_on_worker requires NAIM_MODEL_LIBRARY_QUANTIZE_BIN for quantization");
      }
      const std::string command =
          command_support_.ShellQuote(quantize_bin) + " " +
          command_support_.ShellQuote(source_cache.string()) + " " +
          command_support_.ShellQuote(prepared_path.string()) + " " +
          command_support_.ShellQuote(quantization);
      if (!command_support_.RunCommandOk(command)) {
        throw std::runtime_error("failed to quantize prepared GGUF model");
      }
    }
  } else {
    throw std::runtime_error("prepare_on_worker source must be a GGUF file or model directory");
  }

  if (!fs::exists(prepared_path, error) || error) {
    throw std::runtime_error("prepared model artifact was not created: " + prepared_path.string());
  }
  if (!bootstrap_model.keep_source) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "cleanup-model-source",
        "Cleaning up source model",
        "Removing worker-local source staging files after preparation.",
        84,
        state.plane_name,
        node_name);
    fs::remove_all(staging_root, error);
  }
  {
    std::ofstream output(signature_path, std::ios::trunc);
    output << signature.dump(2) << "\n";
  }
  PublishAssignmentProgress(
      backend,
      assignment_id,
      "prepared-model",
      "Prepared model",
      "Worker-local model artifact is ready.",
      86,
      state.plane_name,
      node_name,
      transfer_support_.FileSizeIfExists(prepared_path.string()),
      transfer_support_.FileSizeIfExists(prepared_path.string()));
  active_model_support_.WriteBootstrapActiveModel(state, node_name, prepared_path.string());
  TryWriteBackPreparedModel(
      state,
      node_name,
      bootstrap_model,
      prepared_path.string(),
      backend,
      assignment_id);
  return true;
}

bool HostdBootstrapModelSupport::TryWriteBackPreparedModel(
    const naim::DesiredState& state,
    const std::string& node_name,
    const naim::BootstrapModelSpec& bootstrap_model,
    const std::string& prepared_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (!bootstrap_model.writeback_enabled || backend == nullptr) {
    return false;
  }
  const std::string target_node =
      bootstrap_model.writeback_target_node_name.value_or(
          bootstrap_model.source_node_name.value_or(""));
  if (target_node.empty() || target_node == node_name) {
    return false;
  }
  try {
    const std::string sha256 = naim::ComputeFileSha256Hex(prepared_path);
    const auto size = transfer_support_.FileSizeIfExists(prepared_path);
    if (!size.has_value() || *size == 0) {
      return false;
    }
    const std::string quantization = bootstrap_model.quantization.value_or("base");
    const std::string relative_path =
        "prepared/" + SanitizePathPart(bootstrap_model.model_id) + "/" +
        SanitizePathPart(quantization) + "/" +
        SanitizePathPart(fs::path(prepared_path).filename().string());
    const nlohmann::json ticket = backend->RequestFileUploadTicket(
        node_name,
        target_node,
        relative_path,
        sha256,
        *size,
        bootstrap_model.writeback_if_missing);
    if (ticket.value("status", std::string{}) != "issued") {
      PublishAssignmentProgress(
          backend,
          assignment_id,
          "writeback-pending",
          "Storage writeback pending",
          "Prepared model is running locally; storage writeback is pending: " +
              ticket.value("message", std::string("upload ticket unavailable")),
          92,
          state.plane_name,
          node_name);
      return false;
    }
    const std::string endpoint = ticket.value("target_endpoint", std::string{});
    const std::string ticket_id = ticket.value("ticket_id", std::string{});
    const PeerHttpResponse start = SendPeerHttpRequest(
        endpoint,
        "/peer/v1/files/upload-start",
        nlohmann::json{{"ticket_id", ticket_id}});
    const auto start_json = nlohmann::json::parse(start.body, nullptr, false);
    if (!start_json.is_discarded() &&
        start_json.value("status", std::string{}) == "already_exists") {
      return true;
    }
    std::ifstream input(prepared_path, std::ios::binary);
    if (!input.is_open()) {
      return false;
    }
    std::array<char, 4 * 1024 * 1024> buffer{};
    std::uintmax_t offset = 0;
    while (input.good()) {
      input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      const auto count = input.gcount();
      if (count <= 0) {
        break;
      }
      const std::string body(buffer.data(), static_cast<std::size_t>(count));
      SendPeerHttpRawRequest(
          endpoint,
          "/peer/v1/files/upload-chunk",
          body,
          "application/octet-stream",
          {
              {"X-Naim-Ticket-Id", ticket_id},
              {"X-Naim-Offset", std::to_string(offset)},
          });
      offset += static_cast<std::uintmax_t>(count);
      PublishAssignmentProgress(
          backend,
          assignment_id,
          "writeback-model",
          "Writing model back to storage",
          "Uploading prepared model variant to storage node " + target_node + ".",
          std::clamp(86 + static_cast<int>((static_cast<double>(offset) / *size) * 10.0), 86, 96),
          state.plane_name,
          node_name,
          offset,
          *size);
    }
    const PeerHttpResponse complete = SendPeerHttpRequest(
        endpoint,
        "/peer/v1/files/upload-complete",
        nlohmann::json{{"ticket_id", ticket_id}});
    const auto complete_json = nlohmann::json::parse(complete.body, nullptr, false);
    return !complete_json.is_discarded() &&
           (complete_json.value("status", std::string{}) == "completed" ||
            complete_json.value("status", std::string{}) == "already_exists");
  } catch (const std::exception& error) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "writeback-pending",
        "Storage writeback pending",
        std::string("Prepared model is running locally; storage writeback failed: ") +
            error.what(),
        92,
        state.plane_name,
        node_name);
    return false;
  }
}

bool HostdBootstrapModelSupport::TryUseSharedBootstrapFromOtherNode(
    const naim::DesiredState& state,
    const std::string& node_name,
    const std::vector<HostdBootstrapModelArtifact>& artifacts,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  const std::string bootstrap_owner_node = artifact_support_.SharedModelBootstrapOwnerNode(state);
  const bool shared_bootstrap_owned_elsewhere =
      bootstrap_owner_node != node_name &&
      std::any_of(
          artifacts.begin(),
          artifacts.end(),
          [](const HostdBootstrapModelArtifact& artifact) {
            return artifact.local_path.has_value() && !artifact.local_path->empty();
          });
  if (!shared_bootstrap_owned_elsewhere) {
    return false;
  }

  for (int attempt = 0; attempt < 300; ++attempt) {
    if (HostdBootstrapModelArtifactSupport::LooksLikeRecognizedModelDirectory(target_path) ||
        fs::exists(target_path)) {
      active_model_support_.WriteBootstrapActiveModel(state, node_name, target_path);
      return true;
    }
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "waiting-for-model",
        "Waiting for shared model",
        "Waiting for " + bootstrap_owner_node +
            " to finish copying the shared model into the plane disk.",
        18,
        state.plane_name,
        node_name);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  throw std::runtime_error(
      "timed out waiting for shared model bootstrap on node '" + bootstrap_owner_node + "'");
}

std::optional<std::uintmax_t> HostdBootstrapModelSupport::ExpectedArtifactSize(
    const HostdBootstrapModelArtifact& artifact) const {
  if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
    return transfer_support_.FileSizeIfExists(*artifact.local_path);
  }
  if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
    return transfer_support_.ProbeContentLength(*artifact.source_url);
  }
  return std::nullopt;
}

bool HostdBootstrapModelSupport::IsArtifactAlreadyPresent(
    const HostdBootstrapModelArtifact& artifact) const {
  if (!fs::exists(artifact.target_host_path)) {
    return false;
  }

  const auto target_size = transfer_support_.FileSizeIfExists(artifact.target_host_path);
  if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
    const auto source_size = transfer_support_.FileSizeIfExists(*artifact.local_path);
    const bool source_is_directory = fs::is_directory(*artifact.local_path);
    const bool target_has_model_root =
        !source_is_directory ||
        HostdBootstrapModelArtifactSupport::LooksLikeRecognizedModelDirectory(
            artifact.target_host_path);
    return source_size.has_value() && target_size.has_value() &&
           *source_size == *target_size && target_has_model_root;
  }
  if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
    const auto remote_size = transfer_support_.ProbeContentLength(*artifact.source_url);
    return remote_size.has_value() && target_size.has_value() && *remote_size == *target_size;
  }
  return fs::exists(artifact.target_host_path);
}

std::optional<std::uintmax_t> HostdBootstrapModelSupport::ComputeAggregateExpectedSize(
    const std::vector<HostdBootstrapModelArtifact>& artifacts,
    bool& already_present) const {
  already_present = !artifacts.empty();
  std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
  for (const auto& artifact : artifacts) {
    if (!IsArtifactAlreadyPresent(artifact)) {
      already_present = false;
    }
    const auto expected_size = ExpectedArtifactSize(artifact);
    if (!expected_size.has_value()) {
      aggregate_total = std::nullopt;
    } else if (aggregate_total.has_value()) {
      *aggregate_total += *expected_size;
    }
  }
  return aggregate_total;
}

void HostdBootstrapModelSupport::AcquireArtifactsIfNeeded(
    const naim::DesiredState& state,
    const std::string& node_name,
    const std::vector<HostdBootstrapModelArtifact>& artifacts,
    const std::optional<std::uintmax_t>& aggregate_total,
    bool already_present,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (already_present) {
    return;
  }

  std::uintmax_t aggregate_prefix = 0;
  for (std::size_t index = 0; index < artifacts.size(); ++index) {
    const auto& artifact = artifacts[index];
    std::optional<std::uintmax_t> artifact_size =
        transfer_support_.FileSizeIfExists(artifact.target_host_path);
    if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
      if (!IsArtifactAlreadyPresent(artifact)) {
        transfer_support_.CopyFileWithProgress(
            *artifact.local_path,
            artifact.target_host_path,
            backend,
            assignment_id,
            state.plane_name,
            node_name,
            index,
            artifacts.size(),
            aggregate_prefix,
            aggregate_total);
        artifact_size = transfer_support_.FileSizeIfExists(artifact.target_host_path);
      }
    } else if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
      if (!IsArtifactAlreadyPresent(artifact)) {
        transfer_support_.DownloadFileWithProgress(
            *artifact.source_url,
            artifact.target_host_path,
            backend,
            assignment_id,
            state.plane_name,
            node_name,
            index,
            artifacts.size(),
            aggregate_prefix,
            aggregate_total);
        artifact_size = transfer_support_.FileSizeIfExists(artifact.target_host_path);
      }
    }
    if (artifact_size.has_value()) {
      aggregate_prefix += *artifact_size;
    }
  }
}

void HostdBootstrapModelSupport::VerifyBootstrapChecksumIfNeeded(
    const naim::DesiredState& state,
    const std::string& node_name,
    const naim::BootstrapModelSpec& bootstrap_model,
    const std::vector<HostdBootstrapModelArtifact>& artifacts,
    const std::string& target_path,
    bool already_present,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (!bootstrap_model.sha256.has_value()) {
    return;
  }
  if (artifacts.size() > 1) {
    throw std::runtime_error(
        "bootstrap_model.sha256 is not supported with multipart bootstrap_model.source_urls");
  }
  if (fs::is_directory(target_path)) {
    throw std::runtime_error(
        "bootstrap_model.sha256 is not supported for directory-based bootstrap models");
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "verifying-model",
      "Verifying model",
      already_present ? "Checking the existing shared-disk model checksum."
                      : "Verifying the model checksum in the shared disk.",
      already_present ? 68 : 72,
      state.plane_name,
      node_name);
  if (!transfer_support_.CheckFileSha256Hex(target_path, *bootstrap_model.sha256)) {
    throw std::runtime_error("bootstrap model checksum mismatch for " + target_path);
  }
}

bool HostdBootstrapModelSupport::HasBootstrapSource(
    const naim::BootstrapModelSpec& bootstrap_model) {
  return (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) ||
         (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) ||
         !bootstrap_model.source_urls.empty();
}

void HostdBootstrapModelSupport::BootstrapPlaneModelIfNeeded(
    const naim::DesiredState& state,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (state.instances.empty()) {
    return;
  }

  const auto& shared_disk = artifact_support_.RequirePlaneSharedDiskForNode(state, node_name);
  if (!fs::exists(shared_disk.host_path)) {
    throw std::runtime_error(
        "plane shared disk path does not exist after ensure-disk: " + shared_disk.host_path);
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "ensuring-shared-disk",
      "Ensuring shared disk",
      "Plane shared disk is mounted and ready for model/bootstrap data.",
      12,
      state.plane_name,
      node_name);

  const std::string active_model_path =
      active_model_support_.ActiveModelPathForNode(state, node_name);
  if (!state.bootstrap_model.has_value()) {
    file_support_.RemoveFileIfExists(active_model_path);
    return;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  if (TryPrepareWorkerBootstrapModel(
          state,
          node_name,
          bootstrap_model,
          backend,
          assignment_id)) {
    return;
  }
  if (TryUseReferenceBootstrapModel(
          state,
          node_name,
          bootstrap_model,
          backend,
          assignment_id)) {
    return;
  }

  const std::string target_path = artifact_support_.TargetPath(state, node_name);
  const auto artifacts = artifact_support_.BuildArtifacts(state, node_name);
  if (TryAcquireControllerRelayedBootstrapModel(
          state,
          node_name,
          bootstrap_model,
          target_path,
          true,
          backend,
          assignment_id)) {
    return;
  }
  if (TryUseSharedBootstrapFromOtherNode(
          state,
          node_name,
          artifacts,
          target_path,
          backend,
          assignment_id)) {
    return;
  }

  bool already_present = false;
  const auto aggregate_total = ComputeAggregateExpectedSize(artifacts, already_present);
  if (already_present) {
    VerifyBootstrapChecksumIfNeeded(
        state,
        node_name,
        bootstrap_model,
        artifacts,
        target_path,
        true,
        backend,
        assignment_id);
  }

  AcquireArtifactsIfNeeded(
      state,
      node_name,
      artifacts,
      aggregate_total,
      already_present,
      backend,
      assignment_id);
  VerifyBootstrapChecksumIfNeeded(
      state,
      node_name,
      bootstrap_model,
      artifacts,
      target_path,
      false,
      backend,
      assignment_id);

  if (!HasBootstrapSource(bootstrap_model) && !fs::exists(target_path)) {
    file_support_.RemoveFileIfExists(active_model_path);
    return;
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      "activating-model",
      "Activating model",
      "Writing active-model.json for infer and worker runtime.",
      80,
      state.plane_name,
      node_name);
  active_model_support_.WriteBootstrapActiveModel(state, node_name, target_path);
}

}  // namespace naim::hostd
