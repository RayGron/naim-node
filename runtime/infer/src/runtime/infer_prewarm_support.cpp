#include "runtime/infer_prewarm_support.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "runtime/infer_control_support.h"
#include "runtime/infer_replica_support.h"

namespace comet::infer::prewarm_support {

namespace fs = std::filesystem;
using control_support::BuildControlPaths;
using control_support::LoadActiveModel;
using nlohmann::json;

namespace {

constexpr std::string_view kHttpPrefix = "http://";
constexpr int kSocketTimeoutSeconds = 120;

struct HttpEndpoint {
  std::string host;
  int port = 80;
};

std::string JsonString(const json& object, const char* key) {
  if (!object.is_object() || !object.contains(key) || !object.at(key).is_string()) {
    return {};
  }
  return object.at(key).get<std::string>();
}

json LoadJsonOrDefault(const fs::path& path, json fallback) {
  if (!fs::exists(path)) {
    return fallback;
  }
  std::ifstream input(path);
  if (!input.is_open()) {
    return fallback;
  }
  json value = json::parse(input, nullptr, false);
  return value.is_discarded() ? fallback : value;
}

void SaveJsonFile(const fs::path& path, const json& value) {
  if (!path.parent_path().empty()) {
    fs::create_directories(path.parent_path());
  }
  std::ofstream output(path);
  if (!output.is_open()) {
    throw std::runtime_error("failed to write json file: " + path.string());
  }
  output << value.dump(2) << "\n";
}

std::set<std::string> LoadPrewarmedBaseUrlSet(const RuntimeConfig& config) {
  std::set<std::string> base_urls;
  const json payload =
      LoadJsonOrDefault(BuildControlPaths(config).prewarmed_replicas_path, json::object());
  for (const auto& entry : payload.value("prewarmed_base_urls", json::array())) {
    if (entry.is_string()) {
      base_urls.insert(entry.get<std::string>());
    }
  }
  return base_urls;
}

void SavePrewarmedBaseUrls(
    const RuntimeConfig& config,
    const std::vector<std::string>& base_urls) {
  SaveJsonFile(
      BuildControlPaths(config).prewarmed_replicas_path,
      json{{"version", 1}, {"prewarmed_base_urls", base_urls}});
}

std::optional<HttpEndpoint> ParseHttpBaseUrl(const std::string& base_url) {
  if (base_url.rfind(std::string(kHttpPrefix), 0) != 0) {
    return std::nullopt;
  }
  std::string remainder = base_url.substr(kHttpPrefix.size());
  const std::size_t slash = remainder.find('/');
  if (slash != std::string::npos) {
    remainder = remainder.substr(0, slash);
  }
  if (remainder.empty()) {
    return std::nullopt;
  }

  HttpEndpoint endpoint;
  const std::size_t colon = remainder.rfind(':');
  if (colon == std::string::npos) {
    endpoint.host = remainder;
    endpoint.port = 80;
    return endpoint;
  }

  endpoint.host = remainder.substr(0, colon);
  const std::string port_text = remainder.substr(colon + 1);
  if (endpoint.host.empty() || port_text.empty()) {
    return std::nullopt;
  }
  try {
    endpoint.port = std::stoi(port_text);
  } catch (const std::exception&) {
    return std::nullopt;
  }
  return endpoint.port > 0 ? std::optional<HttpEndpoint>(endpoint) : std::nullopt;
}

int ConnectTcpHost(const std::string& host, int port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  addrinfo* results = nullptr;
  const std::string service = std::to_string(port);
  const int rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &results);
  if (rc != 0) {
    return -1;
  }

  int fd = -1;
  for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
    fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd < 0) {
      continue;
    }
    const timeval timeout{kSocketTimeoutSeconds, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }

  freeaddrinfo(results);
  return fd;
}

bool SendAll(int fd, const std::string& data) {
  const char* cursor = data.c_str();
  std::size_t remaining = data.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, cursor, remaining, 0);
    if (written <= 0) {
      return false;
    }
    cursor += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

std::optional<std::string> ReceiveAll(int fd) {
  std::string response;
  std::vector<char> buffer(4096);
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count == 0) {
      break;
    }
    if (read_count < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return std::nullopt;
      }
      return std::nullopt;
    }
    response.append(buffer.data(), static_cast<std::size_t>(read_count));
    if (response.size() > 1 << 20) {
      break;
    }
  }
  return response;
}

bool IsHttp200(const std::string& response) {
  return response.rfind("HTTP/1.1 200", 0) == 0 || response.rfind("HTTP/1.0 200", 0) == 0;
}

std::optional<json> ParseJsonBody(const std::string& response) {
  const std::size_t body_pos = response.find("\r\n\r\n");
  if (body_pos == std::string::npos) {
    return std::nullopt;
  }
  json payload = json::parse(response.substr(body_pos + 4), nullptr, false);
  if (payload.is_discarded()) {
    return std::nullopt;
  }
  return payload;
}

std::optional<json> PerformJsonRequest(
    const std::string& base_url,
    const std::string& method,
    const std::string& path,
    const std::optional<std::string>& body = std::nullopt) {
  const auto endpoint = ParseHttpBaseUrl(base_url);
  if (!endpoint.has_value()) {
    return std::nullopt;
  }
  const int fd = ConnectTcpHost(endpoint->host, endpoint->port);
  if (fd < 0) {
    return std::nullopt;
  }

  std::string request = method + " " + path + " HTTP/1.1\r\nHost: " + endpoint->host +
                        "\r\nConnection: close\r\n";
  if (body.has_value()) {
    request += "Content-Type: application/json\r\n";
    request += "Content-Length: " + std::to_string(body->size()) + "\r\n";
  }
  request += "\r\n";
  if (body.has_value()) {
    request += *body;
  }

  const bool sent = SendAll(fd, request);
  const auto response = sent ? ReceiveAll(fd) : std::nullopt;
  close(fd);
  if (!sent || !response.has_value() || !IsHttp200(*response)) {
    return std::nullopt;
  }
  return ParseJsonBody(*response);
}

std::string ResolveModelIdForPrewarm(const RuntimeConfig& config, const std::string& base_url) {
  if (const auto models_payload = PerformJsonRequest(base_url, "GET", "/v1/models")) {
    const json data = models_payload->value("data", json::array());
    if (data.is_array()) {
      for (const auto& entry : data) {
        const std::string model_id = JsonString(entry, "id");
        if (!model_id.empty()) {
          return model_id;
        }
      }
    }
  }

  const json active_model = LoadActiveModel(config);
  const std::string served_model_name =
      active_model.value("served_model_name", std::string{});
  if (!served_model_name.empty()) {
    return served_model_name;
  }
  return active_model.value("model_id", std::string{});
}

bool PrewarmBaseUrl(const RuntimeConfig& config, const std::string& base_url) {
  const std::string model_id = ResolveModelIdForPrewarm(config, base_url);
  if (model_id.empty()) {
    return false;
  }

  const json payload = {
      {"model", model_id},
      {"messages",
       json::array(
           {{{"role", "user"}, {"content", "Warm up this replica. Reply with ok."}}})},
      {"max_tokens", 8},
      {"temperature", 0},
      {"stream", false},
  };
  const auto response =
      PerformJsonRequest(base_url, "POST", "/v1/chat/completions", payload.dump());
  return response.has_value() &&
         response->contains("choices") &&
         response->at("choices").is_array() &&
         !response->at("choices").empty();
}

}  // namespace

std::vector<std::string> ObservedReadyReplicaLeaderBaseUrls(const RuntimeConfig& config) {
  if (const char* worker_vllm_upstream = std::getenv("COMET_INFER_VLLM_UPSTREAM_URL");
      worker_vllm_upstream != nullptr && std::strlen(worker_vllm_upstream) > 0) {
    return {std::string(worker_vllm_upstream)};
  }

  const auto topology = replica_support::InspectReplicaTopology(config);
  if (!topology.ready_replica_base_urls.empty()) {
    return topology.ready_replica_base_urls;
  }

  return {};
}

std::vector<std::string> FilterPrewarmedReplicaBaseUrls(
    const RuntimeConfig& config,
    const std::vector<std::string>& candidate_base_urls) {
  const std::set<std::string> prewarmed = LoadPrewarmedBaseUrlSet(config);
  std::vector<std::string> filtered;
  filtered.reserve(candidate_base_urls.size());
  for (const auto& base_url : candidate_base_urls) {
    if (prewarmed.count(base_url) > 0) {
      filtered.push_back(base_url);
    }
  }
  return filtered;
}

void ResetPrewarmState(const RuntimeConfig& config) {
  std::error_code error;
  fs::remove(BuildControlPaths(config).prewarmed_replicas_path, error);
}

PrewarmState PrewarmReadyReplicaLeaders(const RuntimeConfig& config) {
  PrewarmState state;
  state.ready_base_urls = ObservedReadyReplicaLeaderBaseUrls(config);
  state.ready_upstreams = static_cast<int>(state.ready_base_urls.size());
  if (state.ready_base_urls.empty()) {
    SavePrewarmedBaseUrls(config, {});
    return state;
  }

  std::set<std::string> prewarmed = LoadPrewarmedBaseUrlSet(config);
  std::vector<std::string> updated_base_urls;
  updated_base_urls.reserve(state.ready_base_urls.size());
  for (const auto& base_url : state.ready_base_urls) {
    if (prewarmed.count(base_url) == 0 && !PrewarmBaseUrl(config, base_url)) {
      continue;
    }
    prewarmed.insert(base_url);
    updated_base_urls.push_back(base_url);
  }

  SavePrewarmedBaseUrls(config, updated_base_urls);
  state.prewarmed_base_urls = updated_base_urls;
  state.prewarmed_upstreams = static_cast<int>(updated_base_urls.size());
  return state;
}

}  // namespace comet::infer::prewarm_support
