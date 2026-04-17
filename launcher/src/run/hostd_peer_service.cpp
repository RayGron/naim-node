#include "run/hostd_peer_service.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "app/hostd_controller_transport_support.h"
#include "backend/http_hostd_backend.h"
#include "backend/http_hostd_backend_support.h"
#include "naim/security/crypto_utils.h"

namespace naim::launcher {

namespace {

using json = nlohmann::json;

bool EnvFlagEnabled(const char* name, bool fallback) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || *raw == '\0') {
    return fallback;
  }
  std::string value(raw);
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value == "1" || value == "yes" || value == "true" || value == "on";
}

int EnvInt(const char* name, int fallback) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || *raw == '\0') {
    return fallback;
  }
  return std::stoi(raw);
}

std::string EnvString(const char* name, const std::string& fallback) {
  const char* raw = std::getenv(name);
  return raw == nullptr || *raw == '\0' ? fallback : std::string(raw);
}

bool IsPrivateIpv4(const std::string& address) {
  std::istringstream input(address);
  std::string octet_text;
  std::vector<int> octets;
  while (std::getline(input, octet_text, '.')) {
    if (octet_text.empty()) {
      return false;
    }
    octets.push_back(std::stoi(octet_text));
  }
  if (octets.size() != 4) {
    return false;
  }
  return octets[0] == 10 ||
         (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) ||
         (octets[0] == 192 && octets[1] == 168);
}

struct ParsedUrl {
  std::string host;
  int port = 80;
};

ParsedUrl ParseHttpUrl(const std::string& url) {
  std::string target = url;
  if (target.rfind("http://", 0) == 0) {
    target = target.substr(7);
  }
  const std::size_t slash = target.find('/');
  if (slash != std::string::npos) {
    target = target.substr(0, slash);
  }
  ParsedUrl parsed;
  const std::size_t colon = target.rfind(':');
  if (colon == std::string::npos) {
    parsed.host = target;
  } else {
    parsed.host = target.substr(0, colon);
    parsed.port = std::stoi(target.substr(colon + 1));
  }
  return parsed;
}

std::string HttpReason(int status_code) {
  if (status_code == 200) {
    return "OK";
  }
  if (status_code == 400) {
    return "Bad Request";
  }
  if (status_code == 403) {
    return "Forbidden";
  }
  if (status_code == 404) {
    return "Not Found";
  }
  return "Internal Server Error";
}

class LauncherHostdBackendSupport final : public naim::hostd::IHttpHostdBackendSupport {
 public:
  nlohmann::json SendControllerJsonRequest(
      const std::string& controller_url,
      const std::string& method,
      const std::string& path,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const override {
    return naim::hostd::controller_transport_support::SendControllerJsonRequest(
        controller_url,
        method,
        path,
        payload,
        headers);
  }

  naim::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) const override {
    return naim::hostd::controller_transport_support::ParseAssignmentPayload(payload);
  }

  nlohmann::json BuildHostObservationPayload(
      const naim::HostObservation& observation) const override {
    return naim::hostd::controller_transport_support::BuildHostObservationPayload(observation);
  }

  nlohmann::json BuildDiskRuntimeStatePayload(
      const naim::DiskRuntimeState& state) const override {
    return naim::hostd::controller_transport_support::BuildDiskRuntimeStatePayload(state);
  }

  naim::DiskRuntimeState ParseDiskRuntimeStatePayload(
      const nlohmann::json& payload) const override {
    return naim::hostd::controller_transport_support::ParseDiskRuntimeStatePayload(payload);
  }

  std::string Trim(const std::string& value) const override {
    return naim::hostd::controller_transport_support::Trim(value);
  }
};

std::string ReadAll(int fd) {
  std::string data;
  std::array<char, 8192> buffer{};
  while (data.find("\r\n\r\n") == std::string::npos) {
    const ssize_t count = recv(fd, buffer.data(), buffer.size(), 0);
    if (count <= 0) {
      return data;
    }
    data.append(buffer.data(), static_cast<std::size_t>(count));
    if (data.size() > 1024 * 1024) {
      return data;
    }
  }
  const std::size_t header_end = data.find("\r\n\r\n");
  std::size_t content_length = 0;
  std::istringstream headers(data.substr(0, header_end));
  std::string line;
  while (std::getline(headers, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    std::string lower = line;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char ch) {
      return static_cast<char>(std::tolower(ch));
    });
    const std::string prefix = "content-length:";
    if (lower.rfind(prefix, 0) == 0) {
      content_length = static_cast<std::size_t>(std::stoull(line.substr(prefix.size())));
    }
  }
  const std::size_t current_body = data.size() - header_end - 4;
  while (current_body < content_length && data.size() < header_end + 4 + content_length) {
    const ssize_t count = recv(fd, buffer.data(), buffer.size(), 0);
    if (count <= 0) {
      break;
    }
    data.append(buffer.data(), static_cast<std::size_t>(count));
  }
  return data;
}

}  // namespace

HostdPeerService::HostdPeerService(HostdRunOptions options)
    : options_(std::move(options)) {
  enabled_ = EnvFlagEnabled("NAIM_HOSTD_PEER_ENABLED", true);
  peer_port_ = EnvInt("NAIM_HOSTD_PEER_PORT", peer_port_);
  discovery_port_ = EnvInt("NAIM_HOSTD_DISCOVERY_PORT", discovery_port_);
  discovery_group_ = EnvString("NAIM_HOSTD_DISCOVERY_GROUP", discovery_group_);
}

HostdPeerService::~HostdPeerService() {
  Stop();
}

void HostdPeerService::Start() {
  if (!enabled_ || options_.controller_url.empty()) {
    return;
  }
  stop_requested_ = false;
  beacon_thread_ = std::thread([this]() { BeaconLoop(); });
  listen_thread_ = std::thread([this]() { ListenLoop(); });
  http_thread_ = std::thread([this]() { HttpLoop(); });
}

void HostdPeerService::Stop() {
  stop_requested_ = true;
#if !defined(_WIN32)
  {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(static_cast<uint16_t>(peer_port_));
      inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
      connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
      close(fd);
    }
  }
  {
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(static_cast<uint16_t>(discovery_port_));
      inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
      const char wake = '\n';
      sendto(fd, &wake, 1, 0, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
      close(fd);
    }
  }
#endif
  if (beacon_thread_.joinable()) {
    beacon_thread_.join();
  }
  if (listen_thread_.joinable()) {
    listen_thread_.join();
  }
  if (http_thread_.joinable()) {
    http_thread_.join();
  }
}

std::string HostdPeerService::CurrentTimestamp() const {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  localtime_r(&now, &tm);
  char buffer[32];
  if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm) == 0) {
    return {};
  }
  return buffer;
}

std::vector<std::pair<std::string, std::string>> HostdPeerService::LocalInterfaceAddresses()
    const {
  std::vector<std::pair<std::string, std::string>> result;
  FILE* pipe = popen("ip -o -4 addr show 2>/dev/null", "r");
  if (pipe == nullptr) {
    return result;
  }
  std::array<char, 512> buffer{};
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    std::istringstream line(buffer.data());
    std::string index;
    std::string iface;
    std::string family;
    std::string cidr;
    line >> index >> iface >> family >> cidr;
    if (!iface.empty() && iface.back() == ':') {
      iface.pop_back();
    }
    const std::string address = cidr.substr(0, cidr.find('/'));
    if (family == "inet" && iface != "lo" && IsPrivateIpv4(address)) {
      result.push_back({iface, address});
    }
  }
  pclose(pipe);
  return result;
}

std::string HostdPeerService::BestLanAddress() const {
  const auto addresses = LocalInterfaceAddresses();
  if (!addresses.empty()) {
    return addresses.front().second;
  }
  return "127.0.0.1";
}

std::string HostdPeerService::BuildAdvertisedEndpoint() const {
  return "http://" + BestLanAddress() + ":" + std::to_string(peer_port_);
}

std::string HostdPeerService::BuildBeaconPayload() const {
  return json{
      {"service", "naim-peer-v1"},
      {"node_name", options_.node_name},
      {"peer_endpoint", BuildAdvertisedEndpoint()},
      {"sent_at", CurrentTimestamp()},
  }.dump();
}

void HostdPeerService::BeaconLoop() {
#if defined(_WIN32)
  return;
#else
  const int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return;
  }
  int ttl = 1;
  setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(discovery_port_));
  inet_pton(AF_INET, discovery_group_.c_str(), &addr.sin_addr);
  while (!stop_requested_) {
    const std::string payload = BuildBeaconPayload();
    sendto(fd, payload.data(), payload.size(), 0, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    for (int i = 0; i < 5 && !stop_requested_; ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  close(fd);
#endif
}

void HostdPeerService::ListenLoop() {
#if defined(_WIN32)
  return;
#else
  const int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  timeval timeout{};
  timeout.tv_sec = 1;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  sockaddr_in bind_addr{};
  bind_addr.sin_family = AF_INET;
  bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  bind_addr.sin_port = htons(static_cast<uint16_t>(discovery_port_));
  if (bind(fd, reinterpret_cast<sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
    close(fd);
    return;
  }
  ip_mreq membership{};
  inet_pton(AF_INET, discovery_group_.c_str(), &membership.imr_multiaddr);
  membership.imr_interface.s_addr = htonl(INADDR_ANY);
  setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &membership, sizeof(membership));

  std::array<char, 4096> buffer{};
  while (!stop_requested_) {
    sockaddr_in remote{};
    socklen_t remote_len = sizeof(remote);
    const ssize_t count = recvfrom(
        fd,
        buffer.data(),
        buffer.size(),
        0,
        reinterpret_cast<sockaddr*>(&remote),
        &remote_len);
    if (count <= 0) {
      continue;
    }
    const auto parsed = json::parse(
        std::string(buffer.data(), static_cast<std::size_t>(count)),
        nullptr,
        false);
    if (parsed.is_discarded() || parsed.value("service", std::string{}) != "naim-peer-v1") {
      continue;
    }
    const std::string peer_node_name = parsed.value("node_name", std::string{});
    if (peer_node_name.empty() || peer_node_name == options_.node_name) {
      continue;
    }
    char remote_text[INET_ADDRSTRLEN] = {};
    inet_ntop(AF_INET, &remote.sin_addr, remote_text, sizeof(remote_text));
    PeerRecord peer;
    peer.peer_node_name = peer_node_name;
    peer.peer_endpoint = parsed.value("peer_endpoint", std::string{});
    peer.remote_address = remote_text;
    peer.local_interface = LocalInterfaceAddresses().empty()
                                ? std::string{}
                                : LocalInterfaceAddresses().front().first;
    peer.seen_udp = true;
    peer.last_seen_at = CurrentTimestamp();
    int rtt_ms = 0;
    peer.tcp_reachable = ProbePeerHealth(peer.peer_endpoint, &rtt_ms);
    peer.rtt_ms = rtt_ms;
    peer.last_probe_at = CurrentTimestamp();
    RecordPeer(std::move(peer));
  }
  close(fd);
#endif
}

bool HostdPeerService::ProbePeerHealth(const std::string& endpoint, int* rtt_ms) const {
#if defined(_WIN32)
  (void)endpoint;
  (void)rtt_ms;
  return false;
#else
  try {
    const auto parsed = ParseHttpUrl(endpoint);
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      return false;
    }
    timeval timeout{};
    timeout.tv_sec = 2;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(parsed.port));
    if (inet_pton(AF_INET, parsed.host.c_str(), &addr.sin_addr) != 1) {
      close(fd);
      return false;
    }
    const auto start = std::chrono::steady_clock::now();
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      close(fd);
      return false;
    }
    const std::string request =
        "GET /peer/v1/health HTTP/1.1\r\nHost: " + parsed.host +
        "\r\nConnection: close\r\n\r\n";
    send(fd, request.data(), request.size(), 0);
    const std::string response = ReadAll(fd);
    close(fd);
    const auto end = std::chrono::steady_clock::now();
    if (rtt_ms != nullptr) {
      *rtt_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    }
    return response.find(" 200 ") != std::string::npos &&
           response.find("\"naim-peer\"") != std::string::npos;
  } catch (const std::exception&) {
    return false;
  }
#endif
}

void HostdPeerService::RecordPeer(PeerRecord peer) {
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peers_[peer.peer_node_name] = std::move(peer);
  }
  WritePeerState();
}

void HostdPeerService::WritePeerState() {
  std::lock_guard<std::mutex> lock(peers_mutex_);
  json peers = json::array();
  for (const auto& [_, peer] : peers_) {
    peers.push_back(json{
        {"peer_node_name", peer.peer_node_name},
        {"peer_endpoint", peer.peer_endpoint},
        {"local_interface", peer.local_interface},
        {"remote_address", peer.remote_address},
        {"seen_udp", peer.seen_udp},
        {"tcp_reachable", peer.tcp_reachable},
        {"rtt_ms", peer.rtt_ms},
        {"last_seen_at", peer.last_seen_at},
        {"last_probe_at", peer.last_probe_at},
    });
  }
  std::filesystem::create_directories(options_.state_root);
  const std::filesystem::path target = options_.state_root / "peer-discovery.json";
  const std::filesystem::path temp = options_.state_root / "peer-discovery.json.tmp";
  std::ofstream output(temp, std::ios::trunc);
  output << json{{"peers", std::move(peers)}, {"updated_at", CurrentTimestamp()}}.dump(2)
         << "\n";
  output.close();
  std::filesystem::rename(temp, target);
}

void HostdPeerService::HttpLoop() {
#if defined(_WIN32)
  return;
#else
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  timeval timeout{};
  timeout.tv_sec = 1;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(peer_port_));
  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(fd, 16) != 0) {
    close(fd);
    return;
  }
  while (!stop_requested_) {
    const int client = accept(fd, nullptr, nullptr);
    if (client < 0) {
      continue;
    }
    std::thread([this, client]() { HandleHttpClient(client); }).detach();
  }
  close(fd);
#endif
}

void HostdPeerService::HandleHttpClient(int client_fd) const {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string response_body;
  try {
    const std::string request = ReadAll(client_fd);
    const std::size_t first_line_end = request.find("\r\n");
    const std::string first_line =
        first_line_end == std::string::npos ? request : request.substr(0, first_line_end);
    std::istringstream first(first_line);
    std::string method;
    std::string path;
    first >> method >> path;
    const std::size_t header_end = request.find("\r\n\r\n");
    const std::string body =
        header_end == std::string::npos ? std::string{} : request.substr(header_end + 4);
    if (method == "GET" && path == "/peer/v1/health") {
      response_body = json{
          {"service", "naim-peer"},
          {"node_name", options_.node_name},
          {"peer_endpoint", BuildAdvertisedEndpoint()},
      }.dump();
    } else if (method == "POST" && path == "/peer/v1/files/manifest") {
      response_body = HandlePeerJsonRequest(path, body, &status_code, &content_type);
    } else if (method == "POST" && path == "/peer/v1/files/chunk") {
      response_body = HandlePeerChunkRequest(body, &status_code, &content_type);
    } else {
      status_code = 404;
      response_body = json{{"status", "not_found"}}.dump();
    }
  } catch (const std::exception& error) {
    status_code = 500;
    response_body = json{{"status", "internal_error"}, {"message", error.what()}}.dump();
  }
  std::ostringstream response;
  response << "HTTP/1.1 " << status_code << " " << HttpReason(status_code) << "\r\n";
  response << "Content-Type: " << content_type << "\r\n";
  response << "Content-Length: " << response_body.size() << "\r\n";
  response << "Connection: close\r\n\r\n";
  response << response_body;
  const std::string text = response.str();
  send(client_fd, text.data(), text.size(), 0);
  close(client_fd);
}

std::string HostdPeerService::HandlePeerJsonRequest(
    const std::string&,
    const std::string& body,
    int* status_code,
    std::string* content_type) const {
  *content_type = "application/json";
  const auto request = json::parse(body, nullptr, false);
  if (request.is_discarded() || !request.is_object()) {
    *status_code = 400;
    return json{{"status", "bad_request"}}.dump();
  }
  const std::string ticket_id = request.value("ticket_id", std::string{});
  std::vector<std::string> source_paths;
  std::uintmax_t max_chunk_bytes = 0;
  for (const auto& path_value : request.value("source_paths", json::array())) {
    if (!path_value.is_string()) {
      continue;
    }
    const std::string source_path = path_value.get<std::string>();
    if (!ValidateTicketForPath(ticket_id, source_path, &source_paths, &max_chunk_bytes)) {
      *status_code = 403;
      return json{{"status", "forbidden"}}.dump();
    }
  }
  if (source_paths.empty()) {
    *status_code = 400;
    return json{{"status", "bad_request"}, {"message", "source_paths is empty"}}.dump();
  }

  json files = json::array();
  json roots = json::array();
  std::uintmax_t total = 0;
  for (std::size_t root_index = 0; root_index < source_paths.size(); ++root_index) {
    const std::filesystem::path root(source_paths[root_index]);
    const bool is_directory = std::filesystem::is_directory(root);
    roots.push_back(json{
        {"root_index", root_index},
        {"source_path", root.string()},
        {"kind", is_directory ? "directory" : "file"},
    });
    if (is_directory) {
      for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
          continue;
        }
        const std::string relative =
            std::filesystem::relative(entry.path(), root).generic_string();
        const auto size = std::filesystem::file_size(entry.path());
        total += size;
        files.push_back(json{
            {"root_index", root_index},
            {"source_path", entry.path().string()},
            {"relative_path", relative},
            {"size_bytes", size},
            {"sha256", naim::ComputeFileSha256Hex(entry.path().string())},
        });
      }
    } else {
      const auto size = std::filesystem::file_size(root);
      total += size;
      files.push_back(json{
          {"root_index", root_index},
          {"source_path", root.string()},
          {"relative_path", root.filename().string()},
          {"size_bytes", size},
          {"sha256", naim::ComputeFileSha256Hex(root.string())},
      });
    }
  }
  std::vector<json> file_vector = files.get<std::vector<json>>();
  std::sort(file_vector.begin(), file_vector.end(), [](const json& left, const json& right) {
    const int left_root = left.value("root_index", 0);
    const int right_root = right.value("root_index", 0);
    if (left_root != right_root) {
      return left_root < right_root;
    }
    return left.value("relative_path", std::string{}) <
           right.value("relative_path", std::string{});
  });
  std::string canonical = "naim-model-manifest-v1\n";
  for (const auto& file : file_vector) {
    canonical += "file " + std::to_string(file.value("root_index", 0)) + " " +
                 file.value("relative_path", std::string{}) + " " +
                 std::to_string(file.value("size_bytes", static_cast<std::uintmax_t>(0))) +
                 " " + file.value("sha256", std::string{}) + "\n";
  }
  return json{
      {"status", "manifest-ready"},
      {"phase", "manifest-ready"},
      {"roots", std::move(roots)},
      {"files", std::move(files)},
      {"bytes_total", total},
      {"manifest_sha256", naim::ComputeSha256Hex(canonical)},
      {"max_chunk_bytes", max_chunk_bytes},
  }.dump();
}

std::string HostdPeerService::HandlePeerChunkRequest(
    const std::string& body,
    int* status_code,
    std::string* content_type) const {
  const auto request = json::parse(body, nullptr, false);
  if (request.is_discarded() || !request.is_object()) {
    *status_code = 400;
    *content_type = "application/json";
    return json{{"status", "bad_request"}}.dump();
  }
  const std::string ticket_id = request.value("ticket_id", std::string{});
  const std::string source_path = request.value("source_path", std::string{});
  std::vector<std::string> allowed_paths;
  std::uintmax_t ticket_max_chunk_bytes = 0;
  if (!ValidateTicketForPath(ticket_id, source_path, &allowed_paths, &ticket_max_chunk_bytes)) {
    *status_code = 403;
    *content_type = "application/json";
    return json{{"status", "forbidden"}}.dump();
  }
  const std::uintmax_t offset =
      request.value("offset", static_cast<std::uintmax_t>(0));
  const std::uintmax_t requested_max =
      request.value("max_bytes", static_cast<std::uintmax_t>(4 * 1024 * 1024));
  const std::uintmax_t max_bytes = std::min(requested_max, ticket_max_chunk_bytes);
  std::ifstream input(source_path, std::ios::binary);
  if (!input.is_open()) {
    *status_code = 404;
    *content_type = "application/json";
    return json{{"status", "not_found"}}.dump();
  }
  input.seekg(0, std::ios::end);
  const std::uintmax_t size = static_cast<std::uintmax_t>(input.tellg());
  if (offset > size) {
    *status_code = 400;
    *content_type = "application/json";
    return json{{"status", "bad_offset"}}.dump();
  }
  input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  std::string bytes;
  bytes.resize(static_cast<std::size_t>(std::min(max_bytes, size - offset)));
  if (!bytes.empty()) {
    input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    bytes.resize(static_cast<std::size_t>(input.gcount()));
  }
  *content_type = "application/octet-stream";
  return bytes;
}

std::string HostdPeerService::ReadPrivateKey() const {
  std::ifstream input(options_.host_private_key_path);
  std::string value;
  std::getline(input, value);
  return value;
}

bool HostdPeerService::ValidateTicketForPath(
    const std::string& ticket_id,
    const std::string& source_path,
    std::vector<std::string>* allowed_paths,
    std::uintmax_t* max_chunk_bytes) const {
  if (ticket_id.empty() || source_path.empty() || !IsPathUnderStorageRoot(source_path)) {
    return false;
  }
  std::lock_guard<std::mutex> lock(backend_mutex_);
  static LauncherHostdBackendSupport support;
  naim::hostd::HttpHostdBackend backend(
      options_.controller_url,
      ReadPrivateKey(),
      options_.controller_fingerprint,
      options_.onboarding_key,
      options_.node_name,
      options_.storage_root,
      support);
  const json response = backend.ValidateFileTransferTicket(options_.node_name, ticket_id);
  if (response.value("status", std::string{}) != "valid") {
    return false;
  }
  std::vector<std::string> paths;
  for (const auto& path_value : response.value("source_paths", json::array())) {
    if (path_value.is_string()) {
      paths.push_back(path_value.get<std::string>());
    }
  }
  if (!IsPathAllowed(source_path, paths)) {
    return false;
  }
  if (allowed_paths != nullptr) {
    *allowed_paths = std::move(paths);
  }
  if (max_chunk_bytes != nullptr) {
    *max_chunk_bytes = response.value("max_chunk_bytes", static_cast<std::uintmax_t>(0));
    if (*max_chunk_bytes == 0) {
      *max_chunk_bytes = 64ULL * 1024ULL * 1024ULL;
    }
  }
  return true;
}

bool HostdPeerService::IsPathAllowed(
    const std::string& source_path,
    const std::vector<std::string>& allowed_paths) const {
  const auto source = std::filesystem::weakly_canonical(source_path);
  for (const auto& allowed_path : allowed_paths) {
    const auto allowed = std::filesystem::weakly_canonical(allowed_path);
    if (source == allowed) {
      return true;
    }
    if (std::filesystem::is_directory(allowed)) {
      const std::string source_text = source.string();
      std::string allowed_text = allowed.string();
      if (!allowed_text.empty() && allowed_text.back() != '/') {
        allowed_text.push_back('/');
      }
      if (source_text.rfind(allowed_text, 0) == 0) {
        return true;
      }
    }
  }
  return false;
}

bool HostdPeerService::IsPathUnderStorageRoot(const std::string& source_path) const {
  if (options_.storage_root.empty()) {
    return false;
  }
  std::error_code error;
  const auto storage = std::filesystem::weakly_canonical(options_.storage_root, error);
  if (error) {
    return false;
  }
  const auto source = std::filesystem::weakly_canonical(source_path, error);
  if (error) {
    return false;
  }
  std::string storage_text = storage.string();
  std::string source_text = source.string();
  if (!storage_text.empty() && storage_text.back() != '/') {
    storage_text.push_back('/');
  }
  return source == storage || source_text.rfind(storage_text, 0) == 0;
}

}  // namespace naim::launcher
