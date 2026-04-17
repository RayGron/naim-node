#include "app/hostd_bootstrap_model_support.h"

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

namespace fs = std::filesystem;

namespace {

constexpr std::uintmax_t kControllerRelayedChunkBytes = 4ULL * 1024ULL * 1024ULL;
constexpr int kControllerRelayedChunkPollAttempts = 600;
constexpr std::chrono::milliseconds kControllerRelayedChunkPollInterval(500);

struct PeerHttpResponse {
  int status_code = 0;
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

PeerHttpResponse SendPeerHttpRequest(
    const std::string& endpoint,
    const std::string& path,
    const nlohmann::json& payload) {
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
  const std::string body = payload.dump();
  std::ostringstream request;
  request << "POST " << path << " HTTP/1.1\r\n";
  request << "Host: " << target.host << ":" << target.port << "\r\n";
  request << "Connection: close\r\n";
  request << "Content-Type: application/json\r\n";
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
  response.body =
      headers_end == std::string::npos ? std::string{} : response_text.substr(headers_end + 4);
  if (response.status_code >= 400) {
    throw std::runtime_error("peer request failed with status " + std::to_string(response.status_code));
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

}  // namespace

HostdBootstrapModelSupport::HostdBootstrapModelSupport(
    const HostdBootstrapModelArtifactSupport& artifact_support,
    const HostdBootstrapActiveModelSupport& active_model_support,
    const HostdBootstrapTransferSupport& transfer_support,
    const HostdFileSupport& file_support,
    const HostdReportingSupport& reporting_support)
    : artifact_support_(artifact_support),
      active_model_support_(active_model_support),
      transfer_support_(transfer_support),
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
          nlohmann::json{{"ticket_id", peer_ticket_id}, {"source_paths", source_paths}});
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
  for (const auto& file : files) {
    if (file.value("sha256", std::string{}).empty() ||
        !JsonUintmax(file, "size_bytes").has_value()) {
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
    if (already_present) {
      already_present =
          NormalizeLowercase(naim::ComputeFileSha256Hex(target_paths[index])) ==
          NormalizeLowercase(files[index].value("sha256", std::string{}));
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
    active_model_support_.WriteBootstrapActiveModel(
        state,
        node_name,
        directory_transfer ? target_path : target_paths.front());
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

    std::uintmax_t offset = 0;
    bool eof = false;
    while (!eof) {
      if (use_peer_direct) {
        const std::uintmax_t expected_size =
            JsonUintmax(files[file_index], "size_bytes").value_or(0);
        const PeerHttpResponse peer_chunk = SendPeerHttpRequest(
            peer_endpoint,
            "/peer/v1/files/chunk",
            nlohmann::json{
                {"ticket_id", peer_ticket_id},
                {"source_path", source_path},
                {"offset", offset},
                {"max_bytes", kControllerRelayedChunkBytes},
            });
        if (peer_chunk.body.empty() && offset < expected_size) {
          throw std::runtime_error("direct LAN transfer returned an empty non-final chunk");
        }
        if (!peer_chunk.body.empty()) {
          output.write(peer_chunk.body.data(), static_cast<std::streamsize>(peer_chunk.body.size()));
          if (!output.good()) {
            throw std::runtime_error("failed to write bootstrap model direct target: " + temp_path);
          }
        }
        const std::uintmax_t next_offset = offset + peer_chunk.body.size();
        aggregate_done += next_offset - offset;
        offset = next_offset;
        eof = offset >= expected_size || peer_chunk.body.size() < kControllerRelayedChunkBytes;
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
    if (NormalizeLowercase(naim::ComputeFileSha256Hex(file_target_path)) !=
        NormalizeLowercase(files[file_index].value("sha256", std::string{}))) {
      throw std::runtime_error("model artifact chunk relay file checksum mismatch: " + file_target_path);
    }
  }

  std::string active_target_path = target_paths.front();
  if (directory_transfer) {
    fs::remove_all(target_path, error);
    fs::rename(target_root, target_path);
    active_target_path = target_path;
  }
  active_model_support_.WriteBootstrapActiveModel(state, node_name, active_target_path);
  return true;
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
