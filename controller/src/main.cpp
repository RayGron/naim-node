#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <ctime>
#include <cstddef>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "comet/compose_renderer.h"
#include "comet/demo_state.h"
#include "comet/execution_plan.h"
#include "comet/import_bundle.h"
#include "comet/infer_runtime_config.h"
#include "comet/models.h"
#include "comet/planner.h"
#include "comet/reconcile.h"
#include "comet/runtime_status.h"
#include "comet/scheduling_policy.h"
#include "comet/sqlite_store.h"
#include "comet/state_json.h"

namespace {

using nlohmann::json;

std::string DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

int DefaultStaleAfterSeconds() {
  return 300;
}

int MinimumSafeDirectRebalanceScore() {
  return 100;
}

int MaximumRebalanceIterationsPerGeneration() {
  return 1;
}

int WorkerMinimumResidencySeconds() {
  return 300;
}

int NodeCooldownAfterMoveSeconds() {
  return 60;
}

int VerificationStableSamplesRequired() {
  return 3;
}

int VerificationTimeoutSeconds() {
  return 45;
}

std::string DefaultListenHost() {
  return "127.0.0.1";
}

int DefaultListenPort() {
  return 18080;
}

std::atomic<bool> g_stop_requested{false};

struct HttpRequest {
  std::string method = "GET";
  std::string path = "/";
  std::map<std::string, std::string> headers;
  std::map<std::string, std::string> query_params;
  std::string body;
};

struct HttpResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
};

thread_local const HttpRequest* g_current_http_request = nullptr;

struct ScopedCurrentHttpRequest {
  const HttpRequest* previous = nullptr;

  explicit ScopedCurrentHttpRequest(const HttpRequest& request) : previous(g_current_http_request) {
    g_current_http_request = &request;
  }

  ~ScopedCurrentHttpRequest() {
    g_current_http_request = previous;
  }
};

struct SchedulerRuntimeView;

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name);

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds);

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state);

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  comet-controller show-demo-plan\n"
      << "  comet-controller render-demo-compose [--node <node-name>]\n"
      << "  comet-controller init-db [--db <path>]\n"
      << "  comet-controller seed-demo [--db <path>]\n"
      << "  comet-controller validate-bundle --bundle <dir>\n"
      << "  comet-controller preview-bundle --bundle <dir> [--node <node-name>]\n"
      << "  comet-controller plan-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller plan-host-ops --bundle <dir> [--db <path>] [--artifacts-root <path>] [--node <node-name>]\n"
      << "  comet-controller apply-bundle --bundle <dir> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller import-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller show-host-assignments [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-host-observations [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-host-health [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-disk-state [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-rollout-actions [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-rebalance-plan [--db <path>] [--node <node-name>]\n"
      << "  comet-controller apply-rebalance-proposal --worker <worker-name> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller reconcile-rebalance-proposals [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller scheduler-tick [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller set-rollout-action-status --id <action-id> --status <pending|acknowledged|ready-to-retry> [--message <text>] [--db <path>]\n"
      << "  comet-controller enqueue-rollout-eviction --id <action-id> [--db <path>]\n"
      << "  comet-controller reconcile-rollout-actions [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller apply-ready-rollout-action --id <action-id> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller show-node-availability [--db <path>] [--node <node-name>]\n"
      << "  comet-controller set-node-availability --node <node-name> --availability <active|draining|unavailable> [--message <text>] [--db <path>]\n"
      << "  comet-controller retry-host-assignment --id <assignment-id> [--db <path>]\n"
      << "  comet-controller show-state [--db <path>]\n"
      << "  comet-controller render-infer-runtime [--db <path>]\n"
      << "  comet-controller render-compose [--db <path>] [--node <node-name>]\n"
      << "  comet-controller serve [--db <path>] [--listen-host <host>] [--listen-port <port>]\n"
      << "\n"
      << "Remote operator CLI:\n"
      << "  most inspection and mutation commands also accept --controller <http://host:port>\n"
      << "  target resolution order: --controller, COMET_CONTROLLER, ~/.config/comet/controller\n"
      << "  explicit --db keeps the command local\n";
}

std::optional<std::string> ParseNodeArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--node" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseDbArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--db" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseBundleArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--bundle" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseArtifactsRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--artifacts-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseListenHostArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--listen-host" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<int> ParseListenPortArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--listen-port" && index + 1 < argc) {
      return std::stoi(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseControllerArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--controller" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<int> ParseIdArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--id" && index + 1 < argc) {
      return std::stoi(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<int> ParseStaleAfterArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--stale-after" && index + 1 < argc) {
      return std::stoi(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseAvailabilityArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--availability" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseMessageArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--message" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseStatusArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--status" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseWorkerArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--worker" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string Lowercase(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::map<std::string, std::string> ParseQueryParams(const std::string& query_text) {
  const auto decode_component = [](const std::string& value) {
    std::string decoded;
    decoded.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
      if (value[i] == '+' ) {
        decoded.push_back(' ');
        continue;
      }
      if (value[i] == '%' && i + 2 < value.size()) {
        const std::string hex = value.substr(i + 1, 2);
        char* end = nullptr;
        const long code = std::strtol(hex.c_str(), &end, 16);
        if (end != nullptr && *end == '\0') {
          decoded.push_back(static_cast<char>(code));
          i += 2;
          continue;
        }
      }
      decoded.push_back(value[i]);
    }
    return decoded;
  };

  std::map<std::string, std::string> params;
  std::size_t offset = 0;
  while (offset <= query_text.size()) {
    const std::size_t next = query_text.find('&', offset);
    const std::string pair = query_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    if (!pair.empty()) {
      const std::size_t equals = pair.find('=');
      if (equals == std::string::npos) {
        params.emplace(decode_component(pair), "");
      } else {
        params.emplace(
            decode_component(pair.substr(0, equals)),
            decode_component(pair.substr(equals + 1)));
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 1;
  }
  return params;
}

std::string UrlEncode(std::string_view value) {
  std::ostringstream encoded;
  for (const unsigned char ch : value) {
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~' ||
        ch == '/' || ch == ':') {
      encoded << static_cast<char>(ch);
    } else if (ch == ' ') {
      encoded << '+';
    } else {
      encoded << '%' << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
              << static_cast<int>(ch) << std::nouppercase << std::dec;
    }
  }
  return encoded.str();
}

std::string BuildQueryString(
    const std::vector<std::pair<std::string, std::string>>& params) {
  std::ostringstream query;
  bool first = true;
  for (const auto& [key, value] : params) {
    if (value.empty()) {
      continue;
    }
    query << (first ? '?' : '&') << UrlEncode(key) << '=' << UrlEncode(value);
    first = false;
  }
  return query.str();
}

HttpRequest ParseHttpRequest(const std::string& request_text) {
  HttpRequest request;
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  const std::string header_text =
      headers_end == std::string::npos ? request_text : request_text.substr(0, headers_end);
  request.body =
      headers_end == std::string::npos ? std::string{} : request_text.substr(headers_end + 4);

  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line =
      line_end == std::string::npos ? header_text : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  stream >> request.method >> request.path;
  if (request.path.empty()) {
    request.path = "/";
  }
  const std::size_t query = request.path.find('?');
  if (query != std::string::npos) {
    request.query_params = ParseQueryParams(request.path.substr(query + 1));
    request.path = request.path.substr(0, query);
  }

  std::size_t offset = line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      request.headers.emplace(
          Lowercase(Trim(line.substr(0, colon))),
          Trim(line.substr(colon + 1)));
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return request;
}

std::string ReasonPhrase(int status_code) {
  switch (status_code) {
    case 200:
      return "OK";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 500:
      return "Internal Server Error";
    default:
      return "Response";
  }
}

void SendHttpResponse(int client_fd, const HttpResponse& response) {
  std::ostringstream out;
  out << "HTTP/1.1 " << response.status_code << " " << ReasonPhrase(response.status_code) << "\r\n";
  out << "Content-Type: " << response.content_type << "\r\n";
  out << "Content-Length: " << response.body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << response.body;
  const std::string payload = out.str();
  const char* data = payload.c_str();
  std::size_t remaining = payload.size();
  while (remaining > 0) {
    const ssize_t written = send(client_fd, data, remaining, 0);
    if (written <= 0) {
      break;
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
}

struct ControllerEndpointTarget {
  std::string raw;
  std::string host;
  int port = DefaultListenPort();
  std::string base_path;
};

std::optional<std::string> LoadControllerTargetConfig() {
  const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME");
  std::filesystem::path config_path;
  if (xdg_config_home != nullptr && *xdg_config_home != '\0') {
    config_path = std::filesystem::path(xdg_config_home) / "comet" / "controller";
  } else {
    const char* home = std::getenv("HOME");
    if (home == nullptr || *home == '\0') {
      return std::nullopt;
    }
    config_path = std::filesystem::path(home) / ".config" / "comet" / "controller";
  }
  if (!std::filesystem::exists(config_path)) {
    return std::nullopt;
  }
  std::ifstream in(config_path);
  std::stringstream buffer;
  buffer << in.rdbuf();
  const std::string value = Trim(buffer.str());
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::string> ResolveControllerTarget(
    const std::optional<std::string>& explicit_target,
    const std::optional<std::string>& db_arg) {
  if (db_arg.has_value()) {
    return std::nullopt;
  }
  if (explicit_target.has_value()) {
    return explicit_target;
  }
  if (const char* env_target = std::getenv("COMET_CONTROLLER");
      env_target != nullptr && *env_target != '\0') {
    return std::string(env_target);
  }
  return LoadControllerTargetConfig();
}

ControllerEndpointTarget ParseControllerEndpointTarget(const std::string& raw_target) {
  std::string target = Trim(raw_target);
  if (target.empty()) {
    throw std::runtime_error("empty controller target");
  }

  ControllerEndpointTarget parsed;
  parsed.raw = target;
  if (target.rfind("http://", 0) == 0) {
    target = target.substr(7);
  } else if (target.rfind("https://", 0) == 0) {
    throw std::runtime_error("https controller targets are not supported yet");
  }

  const std::size_t slash = target.find('/');
  if (slash != std::string::npos) {
    parsed.base_path = target.substr(slash);
    target = target.substr(0, slash);
    if (parsed.base_path == "/") {
      parsed.base_path.clear();
    }
  }

  const std::size_t colon = target.rfind(':');
  if (colon != std::string::npos) {
    parsed.host = target.substr(0, colon);
    parsed.port = std::stoi(target.substr(colon + 1));
  } else {
    parsed.host = target;
  }
  if (parsed.host.empty()) {
    throw std::runtime_error("invalid controller target '" + raw_target + "'");
  }
  return parsed;
}

HttpResponse ParseHttpResponse(const std::string& response_text) {
  HttpResponse response;
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
      const std::string key = Lowercase(Trim(line.substr(0, colon)));
      const std::string value = Trim(line.substr(colon + 1));
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

HttpResponse SendControllerHttpRequest(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const std::string& body = "") {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(target.port);
  const int lookup = getaddrinfo(target.host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error(
        "failed to resolve controller target '" + target.raw + "': " + gai_strerror(lookup));
  }

  int fd = -1;
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    fd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(results);
  if (fd < 0) {
    throw std::runtime_error("failed to connect to controller target '" + target.raw + "'");
  }

  const std::string request_path = target.base_path + path_and_query;
  std::ostringstream request;
  request << method << " " << request_path << " HTTP/1.1\r\n";
  request << "Host: " << target.host << ":" << target.port << "\r\n";
  request << "Connection: close\r\n";
  if (!body.empty()) {
    request << "Content-Type: application/json\r\n";
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
      const std::string error = std::strerror(errno);
      close(fd);
      throw std::runtime_error("failed to write HTTP request: " + error);
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }

  std::string response_text;
  std::array<char, 8192> buffer{};
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count < 0) {
      const std::string error = std::strerror(errno);
      close(fd);
      throw std::runtime_error("failed to read HTTP response: " + error);
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  close(fd);
  return ParseHttpResponse(response_text);
}

json SendControllerJsonRequest(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::vector<std::pair<std::string, std::string>>& params = {}) {
  const HttpResponse response =
      SendControllerHttpRequest(target, method, path + BuildQueryString(params));
  json payload = response.body.empty() ? json::object() : json::parse(response.body);
  payload["_http_status"] = response.status_code;
  return payload;
}

int CreateListenSocket(const std::string& host, int port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create server socket");
  }

  int yes = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    close(fd);
    throw std::runtime_error("invalid listen host '" + host + "'");
  }

  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    const std::string error = std::strerror(errno);
    close(fd);
    throw std::runtime_error("failed to bind " + host + ":" + std::to_string(port) + ": " + error);
  }
  if (listen(fd, 64) != 0) {
    const std::string error = std::strerror(errno);
    close(fd);
    throw std::runtime_error("failed to listen on " + host + ":" + std::to_string(port) + ": " + error);
  }
  return fd;
}

json BuildControllerHealthPayload(const std::string& db_path) {
  json payload{
      {"status", "ok"},
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
  };

  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto generation = store.LoadDesiredGeneration();
    const auto desired_state = store.LoadDesiredState();
    payload["store_ready"] = true;
    payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
    payload["plane_name"] =
        desired_state.has_value() ? json(desired_state->plane_name) : json(nullptr);
  } catch (const std::exception& error) {
    payload["store_ready"] = false;
    payload["error"] = error.what();
  }

  return payload;
}

json BuildControllerStatePayload(const std::string& db_path) {
  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
  };

  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto generation = store.LoadDesiredGeneration();
  const auto desired_state = store.LoadDesiredState();
  payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
  payload["desired_state"] =
      desired_state.has_value()
          ? json::parse(comet::SerializeDesiredStateJson(*desired_state))
          : json(nullptr);
  return payload;
}

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key) {
  const auto value = FindQueryString(request, key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

struct HostAssignmentsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::vector<comet::HostAssignment> assignments;
};

struct HostObservationsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::vector<comet::HostObservation> observations;
};

struct HostHealthViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<comet::DesiredState> desired_state;
  std::vector<comet::HostObservation> observations;
  std::vector<comet::NodeAvailabilityOverride> availability_overrides;
};

struct DiskStateViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::optional<comet::DesiredState> desired_state;
  std::vector<comet::DiskRuntimeState> runtime_states;
};

HostAssignmentsViewData LoadHostAssignmentsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostAssignmentsViewData{
      db_path,
      node_name,
      store.LoadHostAssignments(node_name),
  };
}

HostObservationsViewData LoadHostObservationsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostObservationsViewData{
      db_path,
      node_name,
      stale_after_seconds,
      store.LoadHostObservations(node_name),
  };
}

HostHealthViewData LoadHostHealthViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostHealthViewData{
      db_path,
      node_name,
      stale_after_seconds,
      store.LoadDesiredState(),
      store.LoadHostObservations(node_name),
      store.LoadNodeAvailabilityOverrides(node_name),
  };
}

DiskStateViewData LoadDiskStateViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state = store.LoadDesiredState();
  return DiskStateViewData{
      db_path,
      node_name,
      desired_state,
      desired_state.has_value()
          ? store.LoadDiskRuntimeStates(desired_state->plane_name, node_name)
          : std::vector<comet::DiskRuntimeState>{},
  };
}

json BuildHostAssignmentsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadHostAssignmentsViewData(db_path, node_name);

  json assignments = json::array();
  for (const auto& assignment : view.assignments) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    assignments.push_back(json{
        {"id", assignment.id},
        {"node_name", assignment.node_name},
        {"plane_name", assignment.plane_name},
        {"desired_generation", assignment.desired_generation},
        {"attempt_count", assignment.attempt_count},
        {"max_attempts", assignment.max_attempts},
        {"assignment_type", assignment.assignment_type},
        {"status", comet::ToString(assignment.status)},
        {"status_message", assignment.status_message},
        {"artifacts_root", assignment.artifacts_root},
        {"instance_count", desired_node_state.instances.size()},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"assignments", assignments},
  };
}

json BuildHostObservationsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostObservationsViewData(db_path, node_name, stale_after_seconds);

  json observations = json::array();
  for (const auto& observation : view.observations) {
    json entry{
        {"node_name", observation.node_name},
        {"plane_name", observation.plane_name.empty() ? json(nullptr) : json(observation.plane_name)},
        {"status", comet::ToString(observation.status)},
        {"status_message", observation.status_message},
        {"heartbeat_at", observation.heartbeat_at},
    };
    if (observation.applied_generation.has_value()) {
      entry["applied_generation"] = *observation.applied_generation;
    } else {
      entry["applied_generation"] = nullptr;
    }
    if (observation.last_assignment_id.has_value()) {
      entry["last_assignment_id"] = *observation.last_assignment_id;
    } else {
      entry["last_assignment_id"] = nullptr;
    }
    const auto age_seconds = HeartbeatAgeSeconds(observation.heartbeat_at);
    entry["age_seconds"] = age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    entry["health"] = HealthFromAge(age_seconds, view.stale_after_seconds);

    if (!observation.observed_state_json.empty()) {
      entry["observed_state"] = json::parse(observation.observed_state_json);
    } else {
      entry["observed_state"] = nullptr;
    }
    if (const auto runtime_status = ParseRuntimeStatus(observation); runtime_status.has_value()) {
      entry["runtime_status"] = json::parse(comet::SerializeRuntimeStatusJson(*runtime_status));
    } else {
      entry["runtime_status"] = nullptr;
    }
    if (const auto telemetry = ParseGpuTelemetry(observation); telemetry.has_value()) {
      entry["gpu_telemetry"] = json::parse(comet::SerializeGpuTelemetryJson(*telemetry));
    } else {
      entry["gpu_telemetry"] = nullptr;
    }
    if (const auto statuses = ParseInstanceRuntimeStatuses(observation); !statuses.empty()) {
      entry["instance_runtimes"] = json::parse(comet::SerializeRuntimeStatusListJson(statuses));
    } else {
      entry["instance_runtimes"] = json::array();
    }

    observations.push_back(std::move(entry));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
      {"observations", observations},
  };
}

json BuildHostHealthPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostHealthViewData(db_path, node_name, stale_after_seconds);
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(view.availability_overrides);

  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : view.observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (view.desired_state.has_value()) {
    for (const auto& node : view.desired_state->nodes) {
      if (!view.node_name.has_value() || node.name == *view.node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!view.node_name.has_value() || observed_node_name == *view.node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;
  json items = json::array();
  for (const auto& current_node_name : nodes) {
    json item{
        {"node_name", current_node_name},
        {"availability",
         comet::ToString(
             ResolveNodeAvailability(availability_override_map, current_node_name))},
    };
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      ++unknown_count;
      items.push_back(std::move(item));
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, view.stale_after_seconds);
    item["health"] = health;
    item["status"] = comet::ToString(observation_it->second.status);
    item["age_seconds"] = age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      item["runtime_phase"] = runtime_status->runtime_phase;
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_backend"] = runtime_status->runtime_backend;
    }
    if (const auto telemetry = ParseGpuTelemetry(observation_it->second);
        telemetry.has_value()) {
      item["telemetry_degraded"] = telemetry->degraded;
      item["telemetry_source"] = telemetry->source;
      item["gpu_device_count"] = telemetry->devices.size();
    }
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }
    items.push_back(std::move(item));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
      {"summary",
       {
           {"online", online_count},
           {"stale", stale_count},
           {"unknown", unknown_count},
       }},
      {"items", items},
  };
}

json BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);

json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);

json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds);

std::string ResolveArtifactsRoot(const std::optional<std::string>& artifacts_root_arg);

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root);

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root);

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root);

int SetRolloutActionStatus(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message);

int EnqueueRolloutEviction(
    const std::string& db_path,
    int action_id);

int ApplyReadyRolloutAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root);

int ApplyRebalanceProposal(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root);

int ValidateBundle(const std::string& bundle_dir);

int PreviewBundle(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name);

int ImportBundle(const std::string& db_path, const std::string& bundle_dir);

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root);

int SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message);

int RetryHostAssignment(const std::string& db_path, int assignment_id);

struct ControllerActionResult {
  std::string action_name;
  int exit_code = 0;
  std::string output;
};

HttpResponse BuildJsonResponse(int status_code, const json& payload) {
  json enriched = payload;
  if (enriched.is_object()) {
    if (!enriched.contains("api_version")) {
      enriched["api_version"] = "v1";
    }
    if (g_current_http_request != nullptr && !enriched.contains("request")) {
      enriched["request"] = {
          {"path", g_current_http_request->path},
          {"method", g_current_http_request->method},
      };
    }
    if (status_code >= 400) {
      json error{
          {"code", enriched.value("status", "error")},
          {"message", enriched.value("message", ReasonPhrase(status_code))},
      };
      if (enriched.contains("details")) {
        error["details"] = enriched["details"];
      }
      enriched["status"] = "error";
      enriched["error"] = error;
      enriched.erase("message");
      enriched.erase("details");
      enriched.erase("path");
      enriched.erase("method");
    }
  }
  return HttpResponse{status_code, "application/json", enriched.dump()};
}

ControllerActionResult RunControllerActionResult(
    const std::string& action_name,
    const std::function<int()>& action) {
  std::ostringstream captured_stdout;
  auto* const original_stdout = std::cout.rdbuf(captured_stdout.rdbuf());
  try {
    const int exit_code = action();
    std::cout.rdbuf(original_stdout);
    return ControllerActionResult{action_name, exit_code, captured_stdout.str()};
  } catch (...) {
    std::cout.rdbuf(original_stdout);
    throw;
  }
}

json BuildControllerActionPayload(const ControllerActionResult& result) {
  return json{
      {"status", result.exit_code == 0 ? "ok" : "failed"},
      {"action", result.action_name},
      {"exit_code", result.exit_code},
      {"output", result.output},
  };
}

int EmitRemoteJsonPayload(const json& payload) {
  const int http_status = payload.value("_http_status", 200);
  json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  std::cout << sanitized.dump(2) << "\n";
  return 0;
}

int EmitRemoteControllerActionPayload(const json& payload) {
  const int http_status = payload.value("_http_status", 200);
  json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  const std::string output = sanitized.value("output", "");
  if (!output.empty()) {
    std::cout << output;
    if (output.back() != '\n') {
      std::cout << "\n";
    }
  } else {
    std::cout << sanitized.dump(2) << "\n";
  }
  return sanitized.value("exit_code", 0);
}

int EmitControllerActionResult(const ControllerActionResult& result) {
  std::cout << result.output;
  return result.exit_code;
}

ControllerActionResult ExecuteValidateBundleAction(const std::string& bundle_dir) {
  return RunControllerActionResult(
      "validate-bundle",
      [&]() { return ValidateBundle(bundle_dir); });
}

ControllerActionResult ExecutePreviewBundleAction(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name) {
  return RunControllerActionResult(
      "preview-bundle",
      [&]() { return PreviewBundle(bundle_dir, node_name); });
}

ControllerActionResult ExecuteImportBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir) {
  return RunControllerActionResult(
      "import-bundle",
      [&]() { return ImportBundle(db_path, bundle_dir); });
}

ControllerActionResult ExecuteApplyBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "apply-bundle",
      [&]() { return ApplyBundle(db_path, bundle_dir, artifacts_root); });
}

ControllerActionResult ExecuteSchedulerTickAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "scheduler-tick",
      [&]() { return SchedulerTick(db_path, artifacts_root); });
}

ControllerActionResult ExecuteReconcileRebalanceProposalsAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "reconcile-rebalance-proposals",
      [&]() { return ReconcileRebalanceProposals(db_path, artifacts_root); });
}

ControllerActionResult ExecuteReconcileRolloutActionsAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "reconcile-rollout-actions",
      [&]() { return ReconcileRolloutActions(db_path, artifacts_root); });
}

ControllerActionResult ExecuteApplyRebalanceProposalAction(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "apply-rebalance-proposal",
      [&]() { return ApplyRebalanceProposal(db_path, worker_name, artifacts_root); });
}

ControllerActionResult ExecuteSetRolloutActionStatusAction(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message) {
  return RunControllerActionResult(
      "set-rollout-action-status",
      [&]() { return SetRolloutActionStatus(db_path, action_id, status, status_message); });
}

ControllerActionResult ExecuteEnqueueRolloutEvictionAction(
    const std::string& db_path,
    int action_id) {
  return RunControllerActionResult(
      "enqueue-rollout-eviction",
      [&]() { return EnqueueRolloutEviction(db_path, action_id); });
}

ControllerActionResult ExecuteApplyReadyRolloutActionAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root) {
  return RunControllerActionResult(
      "apply-ready-rollout-action",
      [&]() { return ApplyReadyRolloutAction(db_path, action_id, artifacts_root); });
}

ControllerActionResult ExecuteSetNodeAvailabilityAction(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message) {
  return RunControllerActionResult(
      "set-node-availability",
      [&]() { return SetNodeAvailability(db_path, node_name, availability, status_message); });
}

ControllerActionResult ExecuteRetryHostAssignmentAction(
    const std::string& db_path,
    int assignment_id) {
  return RunControllerActionResult(
      "retry-host-assignment",
      [&]() { return RetryHostAssignment(db_path, assignment_id); });
}

int ExecuteRemoteControllerCommand(
    const ControllerEndpointTarget& target,
    const std::string& command,
    int argc,
    char** argv) {
  const auto node_name = ParseNodeArg(argc, argv);
  const auto stale_after = ParseStaleAfterArg(argc, argv);
  const auto bundle_dir = ParseBundleArg(argc, argv);
  const auto artifacts_root = ParseArtifactsRootArg(argc, argv);
  const auto action_id = ParseIdArg(argc, argv);
  const auto worker_name = ParseWorkerArg(argc, argv);
  const auto message = ParseMessageArg(argc, argv);
  const auto status = ParseStatusArg(argc, argv);
  const auto availability = ParseAvailabilityArg(argc, argv);

  if (command == "show-state") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(target, "GET", "/api/v1/state"));
  }
  if (command == "show-host-assignments") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-assignments",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "show-host-observations") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-observations",
            {{"node", node_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-host-health") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-health",
            {{"node", node_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-disk-state") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/disk-state",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "show-rollout-actions") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rollout-actions",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "show-rebalance-plan") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rebalance-plan",
            {{"node", node_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-node-availability") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/node-availability",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "validate-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/validate",
            {{"bundle", *bundle_dir}}));
  }
  if (command == "preview-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/preview",
            {{"bundle", *bundle_dir}, {"node", node_name.value_or("")}}));
  }
  if (command == "import-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/import",
            {{"bundle", *bundle_dir}}));
  }
  if (command == "apply-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/apply",
            {{"bundle", *bundle_dir},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "scheduler-tick") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/scheduler-tick",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rebalance-proposals") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/reconcile-rebalance-proposals",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rollout-actions") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/reconcile-rollout-actions",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "apply-rebalance-proposal") {
    if (!worker_name.has_value()) {
      std::cerr << "error: --worker is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/apply-rebalance-proposal",
            {{"worker", *worker_name},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-rollout-action-status") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    if (!status.has_value()) {
      std::cerr << "error: --status is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/set-rollout-action-status",
            {{"id", std::to_string(*action_id)},
             {"status", *status},
             {"message", message.value_or("")}}));
  }
  if (command == "enqueue-rollout-eviction") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/enqueue-rollout-eviction",
            {{"id", std::to_string(*action_id)}}));
  }
  if (command == "apply-ready-rollout-action") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/apply-ready-rollout-action",
            {{"id", std::to_string(*action_id)},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-node-availability") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    if (!availability.has_value()) {
      std::cerr << "error: --availability is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/node-availability",
            {{"node", *node_name},
             {"availability", *availability},
             {"message", message.value_or("")}}));
  }
  if (command == "retry-host-assignment") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/retry-host-assignment",
            {{"id", std::to_string(*action_id)}}));
  }

  std::cerr << "error: command '" << command
            << "' is not available through --controller yet\n";
  return 1;
}

HttpResponse HandleControllerRequest(
    const std::string& db_path,
    const HttpRequest& request) {
  const ScopedCurrentHttpRequest scoped_request(request);
  if (request.path == "/" || request.path == "/health" || request.path == "/api/v1/health") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    return BuildJsonResponse(200, BuildControllerHealthPayload(db_path));
  }
  if (request.path == "/api/v1/bundles/validate") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'bundle'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteValidateBundleAction(*bundle_dir)));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/bundles/preview") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'bundle'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecutePreviewBundleAction(*bundle_dir, FindQueryString(request, "node"))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/bundles/import") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'bundle'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteImportBundleAction(db_path, *bundle_dir)));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/bundles/apply") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'bundle'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteApplyBundleAction(
                  db_path,
                  *bundle_dir,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/state") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(200, BuildControllerStatePayload(db_path));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/host-assignments") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildHostAssignmentsPayload(db_path, FindQueryString(request, "node")));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/host-observations") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildHostObservationsPayload(
              db_path,
              FindQueryString(request, "node"),
              FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds())));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/host-health") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildHostHealthPayload(
              db_path,
              FindQueryString(request, "node"),
              FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds())));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/node-availability") {
    if (request.method == "GET") {
      try {
        return BuildJsonResponse(
            200,
            BuildNodeAvailabilityPayload(db_path, FindQueryString(request, "node")));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto node_name = FindQueryString(request, "node");
    const auto availability = FindQueryString(request, "availability");
    if (!node_name.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'node'"}});
    }
    if (!availability.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'availability'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteSetNodeAvailabilityAction(
                  db_path,
                  *node_name,
                  comet::ParseNodeAvailability(*availability),
                  FindQueryString(request, "message"))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/disk-state") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildDiskStatePayload(db_path, FindQueryString(request, "node")));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/rollout-actions") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildRolloutActionsPayload(db_path, FindQueryString(request, "node")));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/rebalance-plan") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildRebalancePlanPayload(
              db_path,
              FindQueryString(request, "node"),
              FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds())));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/scheduler-tick") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteSchedulerTickAction(
                  db_path,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/reconcile-rebalance-proposals") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteReconcileRebalanceProposalsAction(
                  db_path,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/reconcile-rollout-actions") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteReconcileRolloutActionsAction(
                  db_path,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/apply-rebalance-proposal") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto worker_name = FindQueryString(request, "worker");
    if (!worker_name.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'worker'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteApplyRebalanceProposalAction(
                  db_path,
                  *worker_name,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/set-rollout-action-status") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto action_id = FindQueryInt(request, "id");
    const auto status = FindQueryString(request, "status");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'id'"}});
    }
    if (!status.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'status'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteSetRolloutActionStatusAction(
                  db_path,
                  *action_id,
                  comet::ParseRolloutActionStatus(*status),
                  FindQueryString(request, "message"))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/enqueue-rollout-eviction") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto action_id = FindQueryInt(request, "id");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'id'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteEnqueueRolloutEvictionAction(db_path, *action_id)));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/apply-ready-rollout-action") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto action_id = FindQueryInt(request, "id");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'id'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteApplyReadyRolloutActionAction(
                  db_path,
                  *action_id,
                  ResolveArtifactsRoot(FindQueryString(request, "artifacts_root")))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/retry-host-assignment") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto assignment_id = FindQueryInt(request, "id");
    if (!assignment_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", "missing required query parameter 'id'"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildControllerActionPayload(
              ExecuteRetryHostAssignmentAction(db_path, *assignment_id)));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  return BuildJsonResponse(
      404,
      json{{"status", "not_found"}, {"path", request.path}, {"method", request.method}});
}

void ControllerSignalHandler(int) {
  g_stop_requested.store(true);
}

int ServeControllerApi(const std::string& db_path, const std::string& listen_host, int listen_port) {
  g_stop_requested.store(false);
  ::signal(SIGINT, ControllerSignalHandler);
  ::signal(SIGTERM, ControllerSignalHandler);

  comet::ControllerStore store(db_path);
  store.Initialize();

  const int listen_fd = CreateListenSocket(listen_host, listen_port);
  std::cout << "comet-controller serve\n";
  std::cout << "listen=" << listen_host << ":" << listen_port << "\n";
  std::cout << "db=" << db_path << "\n";
  std::cout << "routes=/health,/api/v1/health,/api/v1/bundles/validate,/api/v1/bundles/preview,/api/v1/bundles/import,/api/v1/bundles/apply,/api/v1/state,/api/v1/host-assignments,/api/v1/host-observations,/api/v1/host-health,/api/v1/disk-state,/api/v1/rollout-actions,/api/v1/rebalance-plan,/api/v1/scheduler-tick,/api/v1/reconcile-rebalance-proposals,/api/v1/reconcile-rollout-actions,/api/v1/apply-rebalance-proposal,/api/v1/set-rollout-action-status,/api/v1/enqueue-rollout-eviction,/api/v1/apply-ready-rollout-action,/api/v1/node-availability,/api/v1/retry-host-assignment\n";
  std::cout.flush();

  while (!g_stop_requested.load()) {
    pollfd fd_state{};
    fd_state.fd = listen_fd;
    fd_state.events = POLLIN;
    const int poll_result = poll(&fd_state, 1, 250);
    if (poll_result < 0) {
      if (g_stop_requested.load() || errno == EINTR) {
        continue;
      }
      const std::string error = std::strerror(errno);
      close(listen_fd);
      throw std::runtime_error("poll failed: " + error);
    }
    if (poll_result == 0) {
      continue;
    }
    if ((fd_state.revents & POLLIN) == 0) {
      continue;
    }

    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
      if (g_stop_requested.load() || errno == EINTR) {
        continue;
      }
      const std::string error = std::strerror(errno);
      close(listen_fd);
      throw std::runtime_error("accept failed: " + error);
    }

    std::string request_data;
    std::array<char, 8192> buffer{};
    while (true) {
      const ssize_t read_count = recv(client_fd, buffer.data(), buffer.size(), 0);
      if (read_count <= 0) {
        break;
      }
      request_data.append(buffer.data(), static_cast<std::size_t>(read_count));
      if (request_data.find("\r\n\r\n") != std::string::npos) {
        break;
      }
    }

    if (!request_data.empty()) {
      const HttpRequest request = ParseHttpRequest(request_data);
      const HttpResponse response = HandleControllerRequest(db_path, request);
      SendHttpResponse(client_fd, response);
    }
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
  }

  close(listen_fd);
  return 0;
}

std::string ResolveDbPath(const std::optional<std::string>& db_arg) {
  return db_arg.value_or(DefaultDbPath());
}

std::string ResolveArtifactsRoot(const std::optional<std::string>& artifacts_root_arg) {
  return artifacts_root_arg.value_or(DefaultArtifactsRoot());
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name);

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name);

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name);

bool IsNodeSchedulable(comet::NodeAvailability availability);

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds);

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds);

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds);
std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root);

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root);

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root);

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report);

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report);

void PrintStateSummary(const comet::DesiredState& state) {
  std::cout << "plane: " << state.plane_name << "\n";
  std::cout << "control_root: " << state.control_root << "\n";
  std::cout << "inference:\n";
  std::cout << "  primary_infer_node=" << state.inference.primary_infer_node
            << " net_if=" << state.inference.net_if
            << " llama_port=" << state.inference.llama_port << "\n";
  std::cout << "gateway:\n";
  std::cout << "  listen=" << state.gateway.listen_host << ":" << state.gateway.listen_port
            << " server_name=" << state.gateway.server_name << "\n";
  std::cout << "nodes:\n";
  for (const auto& node : state.nodes) {
    std::cout << "  - " << node.name << " (" << node.platform << "), gpus=";
    for (std::size_t index = 0; index < node.gpu_devices.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      const auto it = node.gpu_memory_mb.find(node.gpu_devices[index]);
      std::cout << node.gpu_devices[index];
      if (it != node.gpu_memory_mb.end()) {
        std::cout << "(" << it->second << "MB)";
      }
    }
    std::cout << "\n";
  }

  std::cout << "disks:\n";
  for (const auto& disk : state.disks) {
    std::cout << "  - " << disk.name
              << " kind=" << comet::ToString(disk.kind)
              << " node=" << disk.node_name
              << " host_path=" << disk.host_path
              << " container_path=" << disk.container_path
              << " size_gb=" << disk.size_gb
              << "\n";
  }

  std::cout << "instances:\n";
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name
              << " role=" << comet::ToString(instance.role)
              << " node=" << instance.node_name;
    if (instance.gpu_device.has_value()) {
      std::cout << " gpu=" << *instance.gpu_device
                << " fraction=" << instance.gpu_fraction
                << " placement_mode=" << comet::ToString(instance.placement_mode)
                << " share_mode=" << comet::ToString(instance.share_mode)
                << " priority=" << instance.priority
                << " preemptible=" << (instance.preemptible ? "true" : "false");
      if (instance.memory_cap_mb.has_value()) {
        std::cout << " memory_cap_mb=" << *instance.memory_cap_mb;
      }
      const auto placement_it = instance.labels.find("comet.placement");
      if (placement_it != instance.labels.end()) {
        std::cout << " placement=" << placement_it->second;
      }
      const auto action_it = instance.labels.find("comet.placement.action");
      if (action_it != instance.labels.end()) {
        std::cout << " placement_action=" << action_it->second;
      }
      const auto score_it = instance.labels.find("comet.placement.score");
      if (score_it != instance.labels.end()) {
        std::cout << " placement_score=" << score_it->second;
      }
      const auto decision_it = instance.labels.find("comet.placement.decision");
      if (decision_it != instance.labels.end()) {
        std::cout << " placement_decision=" << decision_it->second;
      }
      const auto next_action_it = instance.labels.find("comet.placement.next_action");
      if (next_action_it != instance.labels.end()) {
        std::cout << " next_action=" << next_action_it->second;
      }
      const auto next_target_it = instance.labels.find("comet.placement.next_target");
      if (next_target_it != instance.labels.end()) {
        std::cout << " next_target=" << next_target_it->second;
      }
      const auto victims_it = instance.labels.find("comet.preemption.victims");
      if (victims_it != instance.labels.end()) {
        std::cout << " preemption_victims=" << victims_it->second;
      }
      const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
      if (defer_reason_it != instance.labels.end()) {
        std::cout << " defer_reason=" << defer_reason_it->second;
      }
    }
    std::cout << "\n";
  }
}

void PrintDiskRuntimeStates(const std::vector<comet::DiskRuntimeState>& runtime_states) {
  std::cout << "disk-runtime-state:\n";
  if (runtime_states.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& runtime_state : runtime_states) {
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }
}

void PrintDetailedDiskState(
    const comet::DesiredState& state,
    const std::vector<comet::DiskRuntimeState>& runtime_states,
    const std::optional<std::string>& node_name = std::nullopt) {
  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }

  std::cout << "disk-state:\n";
  bool printed = false;
  for (const auto& disk : state.disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    printed = true;
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    std::cout << "  - disk=" << disk.name
              << " kind=" << comet::ToString(disk.kind)
              << " node=" << disk.node_name
              << " size_gb=" << disk.size_gb
              << " desired_host_path=" << disk.host_path
              << " desired_container_path=" << disk.container_path;
    if (runtime_it == runtime_by_key.end()) {
      std::cout << " realized_state=missing-runtime-state\n";
      continue;
    }

    const auto& runtime_state = runtime_it->second;
    std::cout << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : state.disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    printed = true;
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state)
              << " desired_state=(orphan-runtime-state)";
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }

  if (!printed) {
    std::cout << "  (empty)\n";
  }
}

void PrintSchedulerDecisionSummary(const comet::DesiredState& state) {
  bool has_decisions = false;
  for (const auto& instance : state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    if (instance.labels.find("comet.placement.decision") == instance.labels.end()) {
      continue;
    }
    if (!has_decisions) {
      std::cout << "scheduler-decisions:\n";
      has_decisions = true;
    }

    std::cout << "  - worker=" << instance.name;
    const auto decision_it = instance.labels.find("comet.placement.decision");
    if (decision_it != instance.labels.end()) {
      std::cout << " decision=" << decision_it->second;
    }
    const auto next_action_it = instance.labels.find("comet.placement.next_action");
    if (next_action_it != instance.labels.end()) {
      std::cout << " next_action=" << next_action_it->second;
    }
    const auto next_target_it = instance.labels.find("comet.placement.next_target");
    if (next_target_it != instance.labels.end()) {
      std::cout << " next_target=" << next_target_it->second;
    }
    const auto victims_it = instance.labels.find("comet.preemption.victims");
    if (victims_it != instance.labels.end()) {
      std::cout << " victims=" << victims_it->second;
    }
    const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
    if (defer_reason_it != instance.labels.end()) {
      std::cout << " defer_reason=" << defer_reason_it->second;
    }
    std::cout << "\n";
  }
}

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report) {
  std::map<std::string, std::vector<comet::SchedulerRolloutAction>> result;
  for (const auto& action : scheduling_report.rollout_actions) {
    result[action.target_node_name].push_back(action);
  }
  return result;
}

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report) {
  if (scheduling_report.rollout_actions.empty()) {
    return;
  }

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : scheduling_report.rollout_actions) {
    if (!action.worker_name.empty()) {
      worker_names.insert(action.worker_name);
    }
    if (!action.target_node_name.empty()) {
      node_names.insert(action.target_node_name);
    }
  }

  std::cout << "rollout-gates:\n";
  std::cout << "  gated_workers=" << worker_names.size()
            << " gated_nodes=" << node_names.size()
            << " deferred_actions=" << scheduling_report.rollout_actions.size() << "\n";
}

void PrintPersistedRolloutActions(
    const std::vector<comet::RolloutActionRecord>& actions) {
  std::cout << "rollout-actions:\n";
  if (actions.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& action : actions) {
    std::cout << "  - id=" << action.id
              << " generation=" << action.desired_generation
              << " step=" << action.step
              << " worker=" << action.worker_name
              << " action=" << action.action
              << " target=" << action.target_node_name << ":" << action.target_gpu_device
              << " status=" << comet::ToString(action.status);
    if (!action.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < action.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << action.victim_worker_names[index];
      }
    }
    if (!action.reason.empty()) {
      std::cout << " reason=" << action.reason;
    }
    std::cout << "\n";
    if (!action.status_message.empty()) {
      std::cout << "    message=" << action.status_message << "\n";
    }
  }
}

std::optional<comet::RolloutActionRecord> FindRolloutActionById(
    const std::vector<comet::RolloutActionRecord>& actions,
    int action_id) {
  for (const auto& action : actions) {
    if (action.id == action_id) {
      return action;
    }
  }
  return std::nullopt;
}

void RemoveWorkerFromDesiredState(
    comet::DesiredState* state,
    const std::string& worker_name) {
  if (state == nullptr) {
    return;
  }

  state->instances.erase(
      std::remove_if(
          state->instances.begin(),
          state->instances.end(),
          [&](const comet::InstanceSpec& instance) { return instance.name == worker_name; }),
      state->instances.end());
  state->runtime_gpu_nodes.erase(
      std::remove_if(
          state->runtime_gpu_nodes.begin(),
          state->runtime_gpu_nodes.end(),
          [&](const comet::RuntimeGpuNode& gpu_node) { return gpu_node.name == worker_name; }),
      state->runtime_gpu_nodes.end());
  state->disks.erase(
      std::remove_if(
          state->disks.begin(),
          state->disks.end(),
          [&](const comet::DiskSpec& disk) {
            return disk.kind == comet::DiskKind::WorkerPrivate &&
                   disk.owner_name == worker_name;
          }),
      state->disks.end());
  for (auto& instance : state->instances) {
    instance.depends_on.erase(
        std::remove(instance.depends_on.begin(), instance.depends_on.end(), worker_name),
        instance.depends_on.end());
  }
}

void MaterializeRetryPlacementAction(
    comet::DesiredState* state,
    const comet::RolloutActionRecord& action,
    const std::vector<std::string>& victim_worker_names) {
  if (state == nullptr) {
    return;
  }

  for (const auto& victim_worker_name : victim_worker_names) {
    RemoveWorkerFromDesiredState(state, victim_worker_name);
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == action.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + action.worker_name + "' not found in desired state");
  }

  instance_it->node_name = action.target_node_name;
  instance_it->gpu_device = action.target_gpu_device;
  instance_it->share_mode = comet::GpuShareMode::Exclusive;
  instance_it->gpu_fraction = 1.0;
  instance_it->labels["comet.node"] = action.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-retry-placement";
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == action.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = action.target_node_name;
    runtime_gpu_it->gpu_device = action.target_gpu_device;
    runtime_gpu_it->share_mode = comet::GpuShareMode::Exclusive;
    runtime_gpu_it->gpu_fraction = 1.0;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == action.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = action.target_node_name;
  }
}

std::string RolloutActionTag(int action_id) {
  return "rollout_action_id=" + std::to_string(action_id);
}

bool AssignmentReferencesRolloutAction(
    const comet::HostAssignment& assignment,
    int action_id) {
  return assignment.status_message.find(RolloutActionTag(action_id)) != std::string::npos;
}

std::vector<comet::HostAssignment> BuildEvictionAssignmentsForAction(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const comet::RolloutActionRecord& action,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  if (action.action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " is not an evict-best-effort action");
  }
  if (action.victim_worker_names.empty()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " has no victim workers to evict");
  }

  std::map<std::string, std::vector<std::string>> victim_workers_by_node;
  for (const auto& victim_worker_name : action.victim_worker_names) {
    bool found = false;
    for (const auto& instance : desired_state.instances) {
      if (instance.role == comet::InstanceRole::Worker &&
          instance.name == victim_worker_name) {
        victim_workers_by_node[instance.node_name].push_back(victim_worker_name);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error(
          "victim worker '" + victim_worker_name +
          "' not found in desired state for rollout action id=" +
          std::to_string(action.id));
    }
  }

  comet::DesiredState eviction_state = desired_state;
  int required_memory_cap_mb = 0;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Worker &&
        instance.name == action.worker_name) {
      required_memory_cap_mb = instance.memory_cap_mb.value_or(0);
      break;
    }
  }
  for (const auto& victim_worker_name : action.victim_worker_names) {
    RemoveWorkerFromDesiredState(&eviction_state, victim_worker_name);
  }

  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  std::vector<comet::HostAssignment> assignments;
  for (const auto& [node_name, victim_workers] : victim_workers_by_node) {
    comet::HostAssignment assignment;
    assignment.node_name = node_name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "evict-workers";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(eviction_state, node_name));
    const auto latest_assignment =
        FindLatestHostAssignmentForNode(existing_assignments, node_name);
    assignment.artifacts_root = latest_assignment.has_value()
                                    ? latest_assignment->artifacts_root
                                    : (plane_assignment.has_value()
                                           ? plane_assignment->artifacts_root
                                           : DefaultArtifactsRoot());
    assignment.status = comet::HostAssignmentStatus::Pending;
    std::ostringstream message;
    message << RolloutActionTag(action.id)
            << " evict workers for rollout worker=" << action.worker_name
            << " target_gpu=" << action.target_gpu_device
            << " required_memory_cap_mb=" << required_memory_cap_mb
            << " victims=";
    for (std::size_t index = 0; index < victim_workers.size(); ++index) {
      if (index > 0) {
        message << ",";
      }
      message << victim_workers[index];
    }
    assignment.status_message = message.str();
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::optional<comet::RolloutActionRecord> FindPriorRolloutActionForWorker(
    const std::vector<comet::RolloutActionRecord>& actions,
    const comet::RolloutActionRecord& action,
    const std::string& requested_action_name) {
  std::optional<comet::RolloutActionRecord> result;
  for (const auto& candidate_action : actions) {
    if (candidate_action.desired_generation != action.desired_generation ||
        candidate_action.worker_name != action.worker_name ||
        candidate_action.step >= action.step ||
        candidate_action.action != requested_action_name) {
      continue;
    }
    result = candidate_action;
  }
  return result;
}

bool AreRolloutEvictionAssignmentsApplied(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  bool found = false;
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type != "evict-workers" ||
        !AssignmentReferencesRolloutAction(assignment, action_id)) {
      continue;
    }
    found = true;
    if (assignment.status != comet::HostAssignmentStatus::Applied) {
      return false;
    }
  }
  return found;
}

enum class SchedulerRolloutPhase {
  Planned,
  EvictionEnqueued,
  EvictionApplied,
  RetryReady,
  RetryMaterialized,
  HostFailed,
  HostStale,
  RuntimeFailed,
  RolloutApplied,
};

struct RolloutLifecycleEntry {
  std::string worker_name;
  int desired_generation = 0;
  SchedulerRolloutPhase phase = SchedulerRolloutPhase::Planned;
  std::optional<int> action_id;
  std::string target_node_name;
  std::string target_gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string detail;
};

struct RebalancePlanEntry {
  std::string worker_name;
  comet::PlacementMode placement_mode = comet::PlacementMode::Manual;
  std::string current_node_name;
  std::string current_gpu_device;
  std::string target_node_name;
  std::string target_gpu_device;
  std::string rebalance_class;
  std::string decision;
  std::string state;
  std::string action;
  int score = 0;
  bool preemption_required = false;
  std::vector<std::string> victim_worker_names;
  std::string gate_reason;
};

struct RebalancePolicySummary {
  int actionable_count = 0;
  int safe_direct_count = 0;
  int rollout_class_count = 0;
  int gated_count = 0;
  int blocked_active_rollout_count = 0;
  int assignment_busy_count = 0;
  int observation_gated_count = 0;
  int stable_hold_count = 0;
  int below_threshold_count = 0;
  int propose_count = 0;
  int hold_count = 0;
  int defer_count = 0;
  int no_candidate_count = 0;
  std::vector<std::string> actionable_workers;
  std::vector<std::string> safe_direct_workers;
  std::vector<std::string> rollout_class_workers;
  std::vector<std::string> gated_workers;
  std::vector<std::string> blocked_active_rollout_workers;
  std::vector<std::string> assignment_busy_workers;
  std::vector<std::string> observation_gated_workers;
  std::vector<std::string> stable_hold_workers;
  std::vector<std::string> below_threshold_workers;
  std::vector<std::string> proposed_workers;
  std::vector<std::string> held_workers;
  std::vector<std::string> deferred_workers;
  std::vector<std::string> no_candidate_workers;
};

struct RebalanceControllerGateSummary {
  bool cluster_ready = true;
  int active_rollout_count = 0;
  int blocking_assignment_count = 0;
  int unconverged_node_count = 0;
  std::vector<std::string> active_rollout_workers;
  std::vector<std::string> blocking_assignment_nodes;
  std::vector<std::string> unconverged_nodes;
};

struct RebalanceIterationBudgetSummary {
  int current_iteration = 0;
  int max_iterations = 0;
  bool exhausted = false;
};

struct RebalanceLoopStatusSummary {
  std::string state;
  std::string reason;
};

struct SchedulerRuntimeView {
  std::optional<comet::SchedulerPlaneRuntime> plane_runtime;
  std::map<std::string, comet::SchedulerWorkerRuntime> worker_runtime_by_name;
  std::map<std::string, comet::SchedulerNodeRuntime> node_runtime_by_name;
};

void MaterializeRebalancePlanEntry(
    comet::DesiredState* state,
    const RebalancePlanEntry& entry) {
  if (state == nullptr) {
    return;
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == entry.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + entry.worker_name + "' not found in desired state");
  }

  instance_it->node_name = entry.target_node_name;
  instance_it->gpu_device = entry.target_gpu_device;
  instance_it->environment["COMET_NODE_NAME"] = entry.target_node_name;
  if (!entry.target_gpu_device.empty()) {
    instance_it->environment["COMET_GPU_DEVICE"] = entry.target_gpu_device;
  } else {
    instance_it->environment.erase("COMET_GPU_DEVICE");
  }
  if (entry.action == "upgrade-to-exclusive") {
    instance_it->share_mode = comet::GpuShareMode::Exclusive;
    instance_it->gpu_fraction = 1.0;
  }
  instance_it->labels["comet.node"] = entry.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-rebalance-" + entry.action;
  instance_it->labels["comet.placement.score"] = std::to_string(entry.score);
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == entry.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = entry.target_node_name;
    runtime_gpu_it->gpu_device = entry.target_gpu_device;
    runtime_gpu_it->share_mode = instance_it->share_mode;
    runtime_gpu_it->gpu_fraction = instance_it->gpu_fraction;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == entry.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = entry.target_node_name;
  }
}

std::string ToString(SchedulerRolloutPhase phase) {
  switch (phase) {
    case SchedulerRolloutPhase::Planned:
      return "planned";
    case SchedulerRolloutPhase::EvictionEnqueued:
      return "eviction-enqueued";
    case SchedulerRolloutPhase::EvictionApplied:
      return "eviction-applied";
    case SchedulerRolloutPhase::RetryReady:
      return "retry-ready";
    case SchedulerRolloutPhase::RetryMaterialized:
      return "retry-materialized";
    case SchedulerRolloutPhase::HostFailed:
      return "host-failed";
    case SchedulerRolloutPhase::HostStale:
      return "host-stale";
    case SchedulerRolloutPhase::RuntimeFailed:
      return "runtime-failed";
    case SchedulerRolloutPhase::RolloutApplied:
      return "rollout-applied";
  }
  return "unknown";
}

bool HasRolloutEvictionAssignments(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type == "evict-workers" &&
        AssignmentReferencesRolloutAction(assignment, action_id)) {
      return true;
    }
  }
  return false;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNodeGeneration(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name,
    int desired_generation) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name ||
        assignment.desired_generation != desired_generation) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostObservation> FindHostObservationForNode(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name) {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

std::vector<RolloutLifecycleEntry> BuildRolloutLifecycleEntries(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::RolloutActionRecord>& rollout_actions,
    const std::vector<comet::HostAssignment>& assignments,
    const std::vector<comet::HostObservation>& observations) {
  std::map<std::string, std::vector<comet::RolloutActionRecord>> actions_by_worker;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == desired_generation) {
      actions_by_worker[action.worker_name].push_back(action);
    }
  }

  std::vector<RolloutLifecycleEntry> entries;
  for (auto& [worker_name, actions] : actions_by_worker) {
    std::sort(
        actions.begin(),
        actions.end(),
        [](const comet::RolloutActionRecord& left, const comet::RolloutActionRecord& right) {
          if (left.step != right.step) {
            return left.step < right.step;
          }
          return left.id < right.id;
        });

    const comet::RolloutActionRecord* evict_action = nullptr;
    const comet::RolloutActionRecord* retry_action = nullptr;
    for (const auto& action : actions) {
      if (action.action == "evict-best-effort" && evict_action == nullptr) {
        evict_action = &action;
      } else if (action.action == "retry-placement" && retry_action == nullptr) {
        retry_action = &action;
      }
    }
    if (evict_action == nullptr && retry_action == nullptr) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = worker_name;
    entry.desired_generation = desired_generation;
    const auto* target_action = retry_action != nullptr ? retry_action : evict_action;
    entry.target_node_name = target_action->target_node_name;
    entry.target_gpu_device = target_action->target_gpu_device;
    if (evict_action != nullptr) {
      entry.victim_worker_names = evict_action->victim_worker_names;
    }

    if (evict_action != nullptr) {
      entry.action_id = evict_action->id;
      if (evict_action->status == comet::RolloutActionStatus::Pending) {
        entry.phase = SchedulerRolloutPhase::Planned;
        entry.detail = "awaiting eviction enqueue";
      } else if (evict_action->status == comet::RolloutActionStatus::Acknowledged) {
        if (AreRolloutEvictionAssignmentsApplied(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionApplied;
          entry.detail = "eviction assignments applied";
        } else if (HasRolloutEvictionAssignments(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = "eviction assignments enqueued";
        } else {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = evict_action->status_message.empty()
                             ? "eviction acknowledged"
                             : evict_action->status_message;
        }
      } else if (evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
        entry.phase = SchedulerRolloutPhase::EvictionApplied;
        entry.detail = "eviction completed";
      }
    }

    if (retry_action != nullptr &&
        retry_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      entry.phase = SchedulerRolloutPhase::RetryReady;
      entry.action_id = retry_action->id;
      entry.detail = "retry placement can be materialized";
    }

    entries.push_back(std::move(entry));
  }

  for (const auto& instance : desired_state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    const auto placement_action_it = instance.labels.find("comet.placement.action");
    const auto placement_decision_it = instance.labels.find("comet.placement.decision");
    if (placement_action_it == instance.labels.end() ||
        placement_decision_it == instance.labels.end() ||
        placement_action_it->second != "materialized-retry-placement" ||
        placement_decision_it->second != "applied") {
      continue;
    }
    if (actions_by_worker.find(instance.name) != actions_by_worker.end()) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = instance.name;
    entry.desired_generation = desired_generation;
    entry.phase = SchedulerRolloutPhase::RetryMaterialized;
    entry.target_node_name = instance.node_name;
    entry.target_gpu_device = instance.gpu_device.value_or("");

    const auto target_assignment =
        FindLatestHostAssignmentForNodeGeneration(
            assignments,
            instance.node_name,
            desired_generation);
    const auto target_observation =
        FindHostObservationForNode(observations, instance.node_name);
    if (target_observation.has_value() &&
        target_observation->status == comet::HostObservationStatus::Failed) {
      entry.phase = SchedulerRolloutPhase::HostFailed;
      entry.detail = "target node observation failed";
    } else if (target_observation.has_value() &&
               HealthFromAge(
                   HeartbeatAgeSeconds(target_observation->heartbeat_at),
                   DefaultStaleAfterSeconds()) == "stale") {
      entry.phase = SchedulerRolloutPhase::HostStale;
      entry.detail = "target node observation stale";
    } else if (target_observation.has_value() &&
               ParseRuntimeStatus(*target_observation).has_value() &&
               ParseRuntimeStatus(*target_observation)->runtime_phase == "failed") {
      entry.phase = SchedulerRolloutPhase::RuntimeFailed;
      entry.detail = "target runtime reported failed phase";
    } else if (target_observation.has_value() &&
               target_observation->status == comet::HostObservationStatus::Applied &&
               target_observation->applied_generation.has_value() &&
               *target_observation->applied_generation >= desired_generation) {
      entry.phase = SchedulerRolloutPhase::RolloutApplied;
      entry.detail = "target node observed desired generation applied";
    } else if (target_assignment.has_value()) {
      entry.detail =
          "target node assignment status=" + comet::ToString(target_assignment->status);
    } else {
      entry.detail = "materialized in desired state";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RolloutLifecycleEntry& left, const RolloutLifecycleEntry& right) {
        return left.worker_name < right.worker_name;
      });
  return entries;
}

void PrintRolloutLifecycleEntries(const std::vector<RolloutLifecycleEntry>& entries) {
  std::cout << "rollout-lifecycle:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " generation=" << entry.desired_generation
              << " phase=" << ToString(entry.phase);
    if (entry.action_id.has_value()) {
      std::cout << " action_id=" << *entry.action_id;
    }
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.detail.empty()) {
      std::cout << " detail=" << entry.detail;
    }
    std::cout << "\n";
  }
}

std::optional<RolloutLifecycleEntry> FindRolloutLifecycleEntry(
    const std::vector<RolloutLifecycleEntry>& entries,
    const std::string& worker_name) {
  for (const auto& entry : entries) {
    if (entry.worker_name == worker_name) {
      return entry;
    }
  }
  return std::nullopt;
}

bool RolloutPhaseBlocksRebalance(SchedulerRolloutPhase phase) {
  return phase != SchedulerRolloutPhase::RolloutApplied;
}

bool HostAssignmentBlocksRebalance(const comet::HostAssignment& assignment) {
  return assignment.status == comet::HostAssignmentStatus::Pending ||
         assignment.status == comet::HostAssignmentStatus::Claimed;
}

bool NodeHasBlockingHostAssignment(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  for (const auto& assignment : assignments) {
    if (assignment.node_name == node_name &&
        HostAssignmentBlocksRebalance(assignment)) {
      return true;
    }
  }
  return false;
}

RebalanceControllerGateSummary BuildRebalanceControllerGateSummary(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  RebalanceControllerGateSummary summary;
  std::set<std::string> active_rollout_workers;
  for (const auto& entry : rollout_lifecycle_entries) {
    if (RolloutPhaseBlocksRebalance(entry.phase)) {
      active_rollout_workers.insert(entry.worker_name);
    }
  }
  if (scheduler_runtime.plane_runtime.has_value() &&
      !scheduler_runtime.plane_runtime->active_action.empty() &&
      !scheduler_runtime.plane_runtime->active_worker_name.empty()) {
    active_rollout_workers.insert(scheduler_runtime.plane_runtime->active_worker_name);
  }

  std::set<std::string> blocking_assignment_nodes;
  for (const auto& assignment : assignments) {
    if (HostAssignmentBlocksRebalance(assignment)) {
      blocking_assignment_nodes.insert(assignment.node_name);
    }
  }

  summary.active_rollout_workers.assign(
      active_rollout_workers.begin(), active_rollout_workers.end());
  summary.blocking_assignment_nodes.assign(
      blocking_assignment_nodes.begin(), blocking_assignment_nodes.end());
  summary.active_rollout_count =
      static_cast<int>(summary.active_rollout_workers.size());
  summary.blocking_assignment_count =
      static_cast<int>(summary.blocking_assignment_nodes.size());

  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  std::set<std::string> unconverged_nodes;
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (observation->status == comet::HostObservationStatus::Failed) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
    if (HealthFromAge(age_seconds, stale_after_seconds) != "online") {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation != desired_generation) {
      unconverged_nodes.insert(node.name);
      continue;
    }
  }

  summary.unconverged_nodes.assign(
      unconverged_nodes.begin(), unconverged_nodes.end());
  summary.unconverged_node_count =
      static_cast<int>(summary.unconverged_nodes.size());
  summary.cluster_ready =
      summary.active_rollout_count == 0 &&
      summary.blocking_assignment_count == 0 &&
      summary.unconverged_node_count == 0;
  return summary;
}

const comet::InstanceSpec* FindWorkerInstance(
    const comet::DesiredState& state,
    const std::string& worker_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == comet::InstanceRole::Worker && instance.name == worker_name) {
      return &instance;
    }
  }
  return nullptr;
}

constexpr int ComputePressureUtilizationThresholdPct() {
  return 85;
}

constexpr int ObservedMoveVramReserveMb() {
  return 1024;
}

std::optional<comet::GpuDeviceTelemetry> FindObservedGpuDeviceTelemetry(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  const auto telemetry = ParseGpuTelemetry(*observation);
  if (!telemetry.has_value()) {
    return std::nullopt;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device == gpu_device) {
      return device;
    }
  }
  return std::nullopt;
}

bool ObservedGpuDeviceHasForeignProcess(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device,
    const std::string& worker_name) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, node_name, gpu_device);
  if (!device.has_value()) {
    return false;
  }
  for (const auto& process : device->processes) {
    if (process.instance_name != worker_name && process.instance_name != "unknown") {
      return true;
    }
  }
  return false;
}

std::optional<std::string> ObservedGpuPlacementGateReason(
    const std::vector<comet::HostObservation>& observations,
    const comet::InstanceSpec& worker,
    const std::string& target_node_name,
    const std::string& target_gpu_device,
    bool moving_to_different_gpu) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, target_node_name, target_gpu_device);
  if (!device.has_value()) {
    return std::nullopt;
  }

  if (worker.memory_cap_mb.has_value() &&
      device->free_vram_mb < (*worker.memory_cap_mb + ObservedMoveVramReserveMb())) {
    return std::string("observed-insufficient-vram");
  }

  if (moving_to_different_gpu &&
      device->gpu_utilization_pct >= ComputePressureUtilizationThresholdPct() &&
      ObservedGpuDeviceHasForeignProcess(observations, target_node_name, target_gpu_device, worker.name)) {
    return std::string("compute-pressure");
  }

  return std::nullopt;
}

std::vector<RebalancePlanEntry> BuildRebalancePlanEntries(
    const comet::DesiredState& state,
    const comet::SchedulingPolicyReport& scheduling_report,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds,
    const std::optional<std::string>& node_name_filter = std::nullopt) {
  std::vector<RebalancePlanEntry> entries;
  for (const auto& recommendation : scheduling_report.placement_recommendations) {
    const auto* worker = FindWorkerInstance(state, recommendation.worker_name);
    if (worker == nullptr) {
      continue;
    }
    if (worker->placement_mode == comet::PlacementMode::Manual) {
      continue;
    }
    if (node_name_filter.has_value() && worker->node_name != *node_name_filter) {
      bool candidate_matches = false;
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.node_name == *node_name_filter) {
          candidate_matches = true;
          break;
        }
      }
      if (!candidate_matches) {
        continue;
      }
    }

    RebalancePlanEntry entry;
    entry.worker_name = recommendation.worker_name;
    entry.placement_mode = worker->placement_mode;
    entry.current_node_name = recommendation.current_node_name;
    entry.current_gpu_device = recommendation.current_gpu_device;
    const auto availability_override_map =
        BuildAvailabilityOverrideMap(availability_overrides);
    const auto source_availability =
        ResolveNodeAvailability(availability_override_map, recommendation.current_node_name);
    const bool source_requires_exit = source_availability != comet::NodeAvailability::Active;

    const auto worker_runtime_it =
        scheduler_runtime.worker_runtime_by_name.find(recommendation.worker_name);
    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end() &&
        worker_runtime_it->second.manual_intervention_required) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "manual-intervention-required";
      entry.gate_reason = "manual-intervention-required";
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name == recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = scheduler_runtime.plane_runtime->phase.empty()
                        ? "active-scheduler-action"
                        : scheduler_runtime.plane_runtime->phase;
      entry.target_node_name = scheduler_runtime.plane_runtime->target_node_name;
      entry.target_gpu_device = scheduler_runtime.plane_runtime->target_gpu_device;
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name != recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "active-scheduler-action";
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    const auto rollout_lifecycle =
        FindRolloutLifecycleEntry(rollout_lifecycle_entries, recommendation.worker_name);
    if (rollout_lifecycle.has_value() &&
        RolloutPhaseBlocksRebalance(rollout_lifecycle->phase)) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "hold";
      entry.state = "active-rollout";
      entry.target_node_name = rollout_lifecycle->target_node_name;
      entry.target_gpu_device = rollout_lifecycle->target_gpu_device;
      entry.gate_reason = ToString(rollout_lifecycle->phase);
      entries.push_back(std::move(entry));
      continue;
    }

    const comet::PlacementCandidate* selected_candidate = nullptr;
    if (source_requires_exit) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        const auto target_availability =
            ResolveNodeAvailability(availability_override_map, candidate.node_name);
        if (candidate.node_name != recommendation.current_node_name &&
            IsNodeSchedulable(target_availability)) {
          selected_candidate = &candidate;
          break;
        }
      }
    }
    if (selected_candidate == nullptr) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        selected_candidate = &candidate;
        break;
      }
    }
    if (selected_candidate == nullptr && !recommendation.candidates.empty()) {
      selected_candidate = &recommendation.candidates.front();
    }
    if (selected_candidate == nullptr) {
      entry.rebalance_class = source_requires_exit ? "gated" : "no-candidate";
      entry.decision = "hold";
      entry.state = source_requires_exit ? "draining-source" : "no-candidate";
      entry.gate_reason =
          source_requires_exit ? "no-active-drain-target" : std::string{};
      entries.push_back(std::move(entry));
      continue;
    }

    entry.target_node_name = selected_candidate->node_name;
    entry.target_gpu_device = selected_candidate->gpu_device;
    entry.action = selected_candidate->action;
    entry.score = selected_candidate->score;
    entry.preemption_required = selected_candidate->preemption_required;
    entry.victim_worker_names = selected_candidate->preemption_victims;
    const auto target_availability =
        ResolveNodeAvailability(availability_override_map, selected_candidate->node_name);

    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end()) {
      const auto last_move_age = TimestampAgeSeconds(worker_runtime_it->second.last_move_at);
      if (last_move_age.has_value() &&
          *last_move_age < WorkerMinimumResidencySeconds()) {
        entry.rebalance_class = "stable";
        entry.decision = "hold";
        entry.state = "min-residency";
        entry.gate_reason =
            "min-residency(" + std::to_string(*last_move_age) + "<" +
            std::to_string(WorkerMinimumResidencySeconds()) + ")";
        entries.push_back(std::move(entry));
        continue;
      }
    }

    auto source_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(recommendation.current_node_name);
    auto target_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(selected_candidate->node_name);
    const auto source_move_age =
        source_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(source_node_runtime_it->second.last_move_at);
    const auto target_move_age =
        target_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(target_node_runtime_it->second.last_move_at);
    if ((source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) ||
        (target_move_age.has_value() && *target_move_age < NodeCooldownAfterMoveSeconds())) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "cooldown";
      if (source_move_age.has_value() && target_move_age.has_value() &&
          *source_move_age < NodeCooldownAfterMoveSeconds() &&
          *target_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source-and-target";
      } else if (source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source";
      } else {
        entry.gate_reason = "cooldown-target";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    if (source_requires_exit &&
        selected_candidate->node_name == recommendation.current_node_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "draining-source";
      entry.gate_reason = "no-active-drain-target";
      entries.push_back(std::move(entry));
      continue;
    }

    if (!IsNodeSchedulable(target_availability)) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason =
          target_availability == comet::NodeAvailability::Draining
              ? "draining-target"
              : "unavailable-target";
      entries.push_back(std::move(entry));
      continue;
    }

    const bool source_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, recommendation.current_node_name);
    const bool target_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, selected_candidate->node_name);
    if (source_assignment_busy || target_assignment_busy) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "assignment-in-flight";
      if (source_assignment_busy && target_assignment_busy) {
        entry.gate_reason = "source-and-target-node-busy";
      } else if (source_assignment_busy) {
        entry.gate_reason = "source-node-busy";
      } else {
        entry.gate_reason = "target-node-busy";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    const auto gate_reason =
        ObservedSchedulingGateReason(
            observations, selected_candidate->node_name, stale_after_seconds);
    if (gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gate_reason;
    } else if (const auto gpu_gate_reason =
                   ObservedGpuPlacementGateReason(
                       observations,
                       *worker,
                       selected_candidate->node_name,
                       selected_candidate->gpu_device,
                       selected_candidate->node_name != recommendation.current_node_name ||
                           selected_candidate->gpu_device != recommendation.current_gpu_device);
               gpu_gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gpu_gate_reason;
    } else if (selected_candidate->preemption_required) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "defer";
      entry.state = source_requires_exit ? "drain-preemption" : "deferred-preemption";
    } else if (selected_candidate->score < MinimumSafeDirectRebalanceScore()) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "below-threshold";
      entry.gate_reason =
          "score-below-threshold(" + std::to_string(selected_candidate->score) + "<" +
          std::to_string(MinimumSafeDirectRebalanceScore()) + ")";
    } else if (selected_candidate->same_node &&
               selected_candidate->action == "upgrade-to-exclusive") {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = "ready-in-place-upgrade";
    } else if (selected_candidate->same_node) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "stay";
    } else {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = source_requires_exit ? "ready-drain-move" : "ready-move";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.worker_name != right.worker_name) {
          return left.worker_name < right.worker_name;
        }
        return left.target_node_name < right.target_node_name;
      });
  return entries;
}

void PrintRebalancePlanEntries(const std::vector<RebalancePlanEntry>& entries) {
  std::cout << "rebalance-plan:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " placement_mode=" << comet::ToString(entry.placement_mode)
              << " current=" << entry.current_node_name << ":" << entry.current_gpu_device
              << " class=" << (entry.rebalance_class.empty() ? "(empty)" : entry.rebalance_class)
              << " decision=" << entry.decision
              << " state=" << entry.state;
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.action.empty()) {
      std::cout << " action=" << entry.action;
    }
    std::cout << " score=" << entry.score
              << " preemption_required=" << (entry.preemption_required ? "yes" : "no");
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.gate_reason.empty()) {
      std::cout << " gate_reason=" << entry.gate_reason;
    }
    std::cout << "\n";
  }
}

RebalancePolicySummary BuildRebalancePolicySummary(
    const std::vector<RebalancePlanEntry>& entries) {
  RebalancePolicySummary summary;
  for (const auto& entry : entries) {
    if (entry.state == "no-candidate") {
      ++summary.gated_count;
      ++summary.no_candidate_count;
      summary.gated_workers.push_back(entry.worker_name);
      summary.no_candidate_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "propose") {
      ++summary.actionable_count;
      ++summary.safe_direct_count;
      ++summary.propose_count;
      summary.actionable_workers.push_back(entry.worker_name);
      summary.safe_direct_workers.push_back(entry.worker_name);
      summary.proposed_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "defer") {
      ++summary.rollout_class_count;
      ++summary.defer_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.deferred_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.state == "active-rollout") {
      ++summary.rollout_class_count;
      ++summary.blocked_active_rollout_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.blocked_active_rollout_workers.push_back(entry.worker_name);
    } else if (entry.state == "assignment-in-flight" ||
               entry.state == "gated-target" ||
               entry.state == "draining-source" ||
               entry.state == "manual-intervention-required" ||
               entry.state == "active-scheduler-action" ||
               entry.state == "cooldown" ||
               entry.state == "min-residency") {
      ++summary.gated_count;
      summary.gated_workers.push_back(entry.worker_name);
      if (entry.state == "assignment-in-flight") {
        ++summary.assignment_busy_count;
        summary.assignment_busy_workers.push_back(entry.worker_name);
      } else if (entry.state == "gated-target") {
        ++summary.observation_gated_count;
        summary.observation_gated_workers.push_back(entry.worker_name);
      }
    } else {
      if (entry.state == "below-threshold") {
        ++summary.below_threshold_count;
        summary.below_threshold_workers.push_back(entry.worker_name);
      }
      ++summary.stable_hold_count;
      summary.stable_hold_workers.push_back(entry.worker_name);
    }
    ++summary.hold_count;
    summary.held_workers.push_back(entry.worker_name);
  }
  return summary;
}

void PrintWorkerListLine(
    const std::string& label,
    const std::vector<std::string>& workers) {
  if (workers.empty()) {
    return;
  }
  std::cout << "  " << label << "=";
  for (std::size_t index = 0; index < workers.size(); ++index) {
    if (index > 0) {
      std::cout << ",";
    }
    std::cout << workers[index];
  }
  std::cout << "\n";
}

void PrintRebalancePolicySummary(const RebalancePolicySummary& summary) {
  std::cout << "rebalance-policy:\n";
  std::cout << "  actionable=" << summary.actionable_count
            << " safe_direct=" << summary.safe_direct_count
            << " rollout_class=" << summary.rollout_class_count
            << " gated=" << summary.gated_count
            << " blocked_active_rollouts=" << summary.blocked_active_rollout_count
            << " assignment_busy=" << summary.assignment_busy_count
            << " observation_gated=" << summary.observation_gated_count
            << " stable_holds=" << summary.stable_hold_count
            << " below_threshold=" << summary.below_threshold_count
            << " deferred=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  std::cout << "  propose=" << summary.propose_count
            << " hold=" << summary.hold_count
            << " defer=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  PrintWorkerListLine("actionable_workers", summary.actionable_workers);
  PrintWorkerListLine("safe_direct_workers", summary.safe_direct_workers);
  PrintWorkerListLine("rollout_class_workers", summary.rollout_class_workers);
  PrintWorkerListLine("gated_workers", summary.gated_workers);
  PrintWorkerListLine(
      "blocked_active_rollout_workers", summary.blocked_active_rollout_workers);
  PrintWorkerListLine("assignment_busy_workers", summary.assignment_busy_workers);
  PrintWorkerListLine("observation_gated_workers", summary.observation_gated_workers);
  PrintWorkerListLine("stable_hold_workers", summary.stable_hold_workers);
  PrintWorkerListLine("below_threshold_workers", summary.below_threshold_workers);
  PrintWorkerListLine("proposed_workers", summary.proposed_workers);
  PrintWorkerListLine("held_workers", summary.held_workers);
  PrintWorkerListLine("deferred_workers", summary.deferred_workers);
  PrintWorkerListLine("no_candidate_workers", summary.no_candidate_workers);
}

void PrintRebalanceControllerGateSummary(
    const RebalanceControllerGateSummary& summary) {
  std::cout << "rebalance-controller-gate:\n";
  std::cout << "  cluster_ready=" << (summary.cluster_ready ? "yes" : "no")
            << " active_rollouts=" << summary.active_rollout_count
            << " blocking_assignment_nodes=" << summary.blocking_assignment_count
            << " unconverged_nodes=" << summary.unconverged_node_count << "\n";
  PrintWorkerListLine("active_rollout_workers", summary.active_rollout_workers);
  PrintWorkerListLine("blocking_assignment_nodes", summary.blocking_assignment_nodes);
  PrintWorkerListLine("unconverged_nodes", summary.unconverged_nodes);
}

RebalanceIterationBudgetSummary BuildRebalanceIterationBudgetSummary(int current_iteration) {
  RebalanceIterationBudgetSummary summary;
  summary.current_iteration = current_iteration;
  summary.max_iterations = MaximumRebalanceIterationsPerGeneration();
  summary.exhausted = summary.current_iteration >= summary.max_iterations;
  return summary;
}

void PrintRebalanceIterationBudgetSummary(
    const RebalanceIterationBudgetSummary& summary) {
  std::cout << "rebalance-iteration-budget:\n";
  std::cout << "  iteration=" << summary.current_iteration << "/" << summary.max_iterations
            << " exhausted=" << (summary.exhausted ? "yes" : "no") << "\n";
}

RebalanceLoopStatusSummary BuildRebalanceLoopStatusSummary(
    const RebalanceControllerGateSummary& controller_gate_summary,
    const RebalanceIterationBudgetSummary& iteration_budget_summary,
    const RebalancePolicySummary& policy_summary) {
  RebalanceLoopStatusSummary summary;
  if (!controller_gate_summary.cluster_ready) {
    summary.state = "waiting-for-convergence";
    if (controller_gate_summary.unconverged_node_count > 0) {
      summary.reason =
          "unconverged-nodes=" + std::to_string(controller_gate_summary.unconverged_node_count);
    } else if (controller_gate_summary.blocking_assignment_count > 0) {
      summary.reason =
          "blocking-assignments=" + std::to_string(controller_gate_summary.blocking_assignment_count);
    } else {
      summary.reason =
          "active-rollouts=" + std::to_string(controller_gate_summary.active_rollout_count);
    }
    return summary;
  }
  if (iteration_budget_summary.exhausted && policy_summary.actionable_count > 0) {
    summary.state = "complete";
    summary.reason =
        "rebalance-iteration-limit=" + std::to_string(iteration_budget_summary.current_iteration) +
        "/" + std::to_string(iteration_budget_summary.max_iterations);
    return summary;
  }
  if (policy_summary.rollout_class_count > 0) {
    summary.state = "waiting-for-rollout";
    summary.reason = "rollout-class-workers=" + std::to_string(policy_summary.rollout_class_count);
    return summary;
  }
  if (policy_summary.actionable_count > 0) {
    summary.state = "actionable";
    summary.reason = "safe-direct-workers=" + std::to_string(policy_summary.actionable_count);
    return summary;
  }
  summary.state = "complete";
  if (policy_summary.below_threshold_count > 0) {
    summary.reason =
        "remaining-moves-below-threshold=" + std::to_string(policy_summary.below_threshold_count);
  } else if (policy_summary.no_candidate_count > 0) {
    summary.reason =
        "no-candidate-workers=" + std::to_string(policy_summary.no_candidate_count);
  } else {
    summary.reason = "no-actionable-rebalance";
  }
  return summary;
}

void PrintRebalanceLoopStatusSummary(const RebalanceLoopStatusSummary& summary) {
  std::cout << "rebalance-loop-status:\n";
  std::cout << "  state=" << summary.state;
  if (!summary.reason.empty()) {
    std::cout << " reason=" << summary.reason;
  }
  std::cout << "\n";
}

struct RolloutActionsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::RolloutActionRecord> actions;
  std::optional<SchedulerRuntimeView> scheduler_runtime;
  std::vector<RolloutLifecycleEntry> lifecycle;
  std::size_t gated_worker_count = 0;
  std::size_t gated_node_count = 0;
};

struct RebalancePlanViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<comet::DesiredState> desired_state;
  int desired_generation = 0;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary policy_summary;
  RebalanceLoopStatusSummary loop_status;
  SchedulerRuntimeView scheduler_runtime;
};

struct StateAggregateViewData {
  std::string db_path;
  int stale_after_seconds = 0;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::DiskRuntimeState> disk_runtime_states;
  comet::SchedulingPolicyReport scheduling_report;
  std::vector<comet::HostObservation> observations;
  std::vector<comet::HostAssignment> assignments;
  std::vector<comet::NodeAvailabilityOverride> availability_overrides;
  SchedulerRuntimeView scheduler_runtime;
  std::vector<RolloutLifecycleEntry> rollout_lifecycle;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary rebalance_policy_summary;
  RebalanceLoopStatusSummary loop_status;
};

RolloutActionsViewData LoadRolloutActionsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RolloutActionsViewData view;
  view.db_path = db_path;
  view.node_name = node_name;
  view.desired_state = store.LoadDesiredState();
  view.desired_generation = store.LoadDesiredGeneration();
  view.actions = store.LoadRolloutActions(node_name);

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : view.actions) {
    worker_names.insert(action.worker_name);
    node_names.insert(action.target_node_name);
  }
  view.gated_worker_count = worker_names.size();
  view.gated_node_count = node_names.size();

  if (view.desired_state.has_value()) {
    view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
    if (view.desired_generation.has_value()) {
      view.lifecycle = BuildRolloutLifecycleEntries(
          *view.desired_state,
          *view.desired_generation,
          view.actions,
          store.LoadHostAssignments(),
          store.LoadHostObservations());
    }
  }
  return view;
}

RebalancePlanViewData LoadRebalancePlanViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RebalancePlanViewData view;
  view.db_path = db_path;
  view.node_name = node_name;
  view.stale_after_seconds = stale_after_seconds;
  view.desired_state = store.LoadDesiredState();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.desired_generation = store.LoadDesiredGeneration().value_or(0);
  const auto observations = store.LoadHostObservations();
  const auto assignments = store.LoadHostAssignments();
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto rollout_actions = store.LoadRolloutActions();
  const auto rollout_lifecycle = BuildRolloutLifecycleEntries(
      *view.desired_state,
      view.desired_generation,
      rollout_actions,
      assignments,
      observations);
  view.rebalance_entries = BuildRebalancePlanEntries(
      *view.desired_state,
      scheduling_report,
      availability_overrides,
      rollout_lifecycle,
      assignments,
      view.scheduler_runtime,
      observations,
      stale_after_seconds,
      node_name);
  view.controller_gate_summary = BuildRebalanceControllerGateSummary(
      *view.desired_state,
      view.desired_generation,
      availability_overrides,
      rollout_lifecycle,
      assignments,
      view.scheduler_runtime,
      observations,
      stale_after_seconds);
  view.iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  view.policy_summary = BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status = BuildRebalanceLoopStatusSummary(
      view.controller_gate_summary,
      view.iteration_budget_summary,
      view.policy_summary);
  return view;
}

StateAggregateViewData LoadStateAggregateViewData(
    const std::string& db_path,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  StateAggregateViewData view;
  view.db_path = db_path;
  view.stale_after_seconds = stale_after_seconds;
  view.desired_state = store.LoadDesiredState();
  view.desired_generation = store.LoadDesiredGeneration();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.disk_runtime_states = store.LoadDiskRuntimeStates(view.desired_state->plane_name);
  view.scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.observations = store.LoadHostObservations();
  view.assignments = store.LoadHostAssignments();
  view.availability_overrides = store.LoadNodeAvailabilityOverrides();
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  view.rollout_lifecycle =
      view.desired_generation.has_value()
          ? BuildRolloutLifecycleEntries(
                *view.desired_state,
                *view.desired_generation,
                store.LoadRolloutActions(),
                view.assignments,
                view.observations)
          : std::vector<RolloutLifecycleEntry>{};
  view.rebalance_entries =
      BuildRebalancePlanEntries(
          *view.desired_state,
          view.scheduling_report,
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *view.desired_state,
          view.desired_generation.value_or(0),
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  view.rebalance_policy_summary =
      BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status =
      BuildRebalanceLoopStatusSummary(
          view.controller_gate_summary,
          view.iteration_budget_summary,
          view.rebalance_policy_summary);
  return view;
}

json BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
  };
  if (!state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["items"] = json::array();
    return payload;
  }

  const auto runtime_states = store.LoadDiskRuntimeStates(state->plane_name, node_name);
  payload["plane_name"] = state->plane_name;
  payload["desired_generation"] =
      store.LoadDesiredGeneration().has_value()
          ? json(*store.LoadDesiredGeneration())
          : json(nullptr);

  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }

  json items = json::array();
  for (const auto& disk : state->disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    json item{
        {"disk_name", disk.name},
        {"kind", comet::ToString(disk.kind)},
        {"plane_name", disk.plane_name},
        {"owner_name", disk.owner_name},
        {"node_name", disk.node_name},
        {"size_gb", disk.size_gb},
        {"desired_host_path", disk.host_path},
        {"desired_container_path", disk.container_path},
    };
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    if (runtime_it == runtime_by_key.end()) {
      item["realized_state"] = "missing-runtime-state";
    } else {
      const auto& runtime_state = runtime_it->second;
      item["realized_state"] =
          runtime_state.runtime_state.empty() ? json("(empty)") : json(runtime_state.runtime_state);
      item["mount_point"] = runtime_state.mount_point.empty() ? json(nullptr) : json(runtime_state.mount_point);
      item["filesystem_type"] =
          runtime_state.filesystem_type.empty() ? json(nullptr) : json(runtime_state.filesystem_type);
      item["image_path"] = runtime_state.image_path.empty() ? json(nullptr) : json(runtime_state.image_path);
      item["loop_device"] = runtime_state.loop_device.empty() ? json(nullptr) : json(runtime_state.loop_device);
      item["last_verified_at"] =
          runtime_state.last_verified_at.empty() ? json(nullptr) : json(runtime_state.last_verified_at);
      item["status_message"] =
          runtime_state.status_message.empty() ? json(nullptr) : json(runtime_state.status_message);
    }
    items.push_back(std::move(item));
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : state->disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    items.push_back(json{
        {"disk_name", runtime_state.disk_name},
        {"plane_name", runtime_state.plane_name},
        {"node_name", runtime_state.node_name},
        {"realized_state",
         runtime_state.runtime_state.empty() ? json("(empty)") : json(runtime_state.runtime_state)},
        {"desired_state", "orphan-runtime-state"},
        {"mount_point", runtime_state.mount_point.empty() ? json(nullptr) : json(runtime_state.mount_point)},
        {"image_path", runtime_state.image_path.empty() ? json(nullptr) : json(runtime_state.image_path)},
        {"loop_device", runtime_state.loop_device.empty() ? json(nullptr) : json(runtime_state.loop_device)},
        {"status_message",
         runtime_state.status_message.empty() ? json(nullptr) : json(runtime_state.status_message)},
    });
  }

  payload["desired_state"] = "present";
  payload["items"] = std::move(items);
  return payload;
}

json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto overrides = store.LoadNodeAvailabilityOverrides(node_name);

  json items = json::array();
  for (const auto& override_record : overrides) {
    items.push_back(json{
        {"node_name", override_record.node_name},
        {"availability", comet::ToString(override_record.availability)},
        {"status_message",
         override_record.status_message.empty() ? json(nullptr) : json(override_record.status_message)},
        {"updated_at",
         override_record.updated_at.empty() ? json(nullptr) : json(override_record.updated_at)},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"items", items},
  };
}

json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
  };
  payload["desired_generation"] =
      view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr);

  json action_items = json::array();
  for (const auto& action : view.actions) {
    action_items.push_back(json{
        {"id", action.id},
        {"desired_generation", action.desired_generation},
        {"step", action.step},
        {"worker_name", action.worker_name},
        {"action", action.action},
        {"target_node_name", action.target_node_name},
        {"target_gpu_device", action.target_gpu_device},
        {"victim_worker_names", action.victim_worker_names},
        {"reason", action.reason},
        {"status", comet::ToString(action.status)},
        {"status_message", action.status_message},
    });
  }
  payload["rollout_gates"] = json{
      {"gated_workers", view.gated_worker_count},
      {"gated_nodes", view.gated_node_count},
      {"deferred_actions", view.actions.size()},
  };
  payload["actions"] = std::move(action_items);

  if (view.desired_state.has_value() && view.desired_generation.has_value()) {
    json lifecycle_items = json::array();
    for (const auto& entry : view.lifecycle) {
      lifecycle_items.push_back(json{
          {"worker_name", entry.worker_name},
          {"desired_generation", entry.desired_generation},
          {"phase", ToString(entry.phase)},
          {"action_id", entry.action_id.has_value() ? json(*entry.action_id) : json(nullptr)},
          {"target_node_name", entry.target_node_name.empty() ? json(nullptr) : json(entry.target_node_name)},
          {"target_gpu_device", entry.target_gpu_device.empty() ? json(nullptr) : json(entry.target_gpu_device)},
          {"victim_worker_names", entry.victim_worker_names},
          {"detail", entry.detail.empty() ? json(nullptr) : json(entry.detail)},
      });
    }
    payload["rollout_lifecycle"] = std::move(lifecycle_items);
  } else {
    payload["rollout_lifecycle"] = json::array();
  }

  return payload;
}

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadRebalancePlanViewData(db_path, node_name, stale_after_seconds);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
  };
  if (!view.desired_state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["rebalance_plan"] = json::array();
    return payload;
  }

  json plan_items = json::array();
  for (const auto& entry : view.rebalance_entries) {
    json item;
    item["worker_name"] = entry.worker_name;
    item["placement_mode"] = comet::ToString(entry.placement_mode);
    item["current_node_name"] = entry.current_node_name;
    item["current_gpu_device"] = entry.current_gpu_device;
    item["target_node_name"] =
        entry.target_node_name.empty() ? json(nullptr) : json(entry.target_node_name);
    item["target_gpu_device"] =
        entry.target_gpu_device.empty() ? json(nullptr) : json(entry.target_gpu_device);
    item["rebalance_class"] = entry.rebalance_class;
    item["decision"] = entry.decision;
    item["state"] = entry.state;
    item["action"] = entry.action.empty() ? json(nullptr) : json(entry.action);
    item["score"] = entry.score;
    item["preemption_required"] = entry.preemption_required;
    item["victim_worker_names"] = entry.victim_worker_names;
    item["gate_reason"] = entry.gate_reason.empty() ? json(nullptr) : json(entry.gate_reason);
    plan_items.push_back(std::move(item));
  }

  json worker_runtime_items = json::array();
  for (const auto& [worker_name, runtime] : view.scheduler_runtime.worker_runtime_by_name) {
    json item;
    item["worker_name"] = worker_name;
    item["last_move_at"] = runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_eviction_at"] =
        runtime.last_eviction_at.empty() ? json(nullptr) : json(runtime.last_eviction_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value() ? json(*runtime.last_verified_generation)
                                                     : json(nullptr);
    item["last_scheduler_phase"] =
        runtime.last_scheduler_phase.empty() ? json(nullptr) : json(runtime.last_scheduler_phase);
    item["last_status_message"] =
        runtime.last_status_message.empty() ? json(nullptr) : json(runtime.last_status_message);
    item["manual_intervention_required"] = runtime.manual_intervention_required;
    worker_runtime_items.push_back(std::move(item));
  }
  json node_runtime_items = json::array();
  for (const auto& [runtime_node_name, runtime] : view.scheduler_runtime.node_runtime_by_name) {
    json item;
    item["node_name"] = runtime_node_name;
    item["last_move_at"] = runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value() ? json(*runtime.last_verified_generation)
                                                     : json(nullptr);
    node_runtime_items.push_back(std::move(item));
  }

  payload["plane_name"] = view.desired_state->plane_name;
  payload["desired_generation"] = view.desired_generation;
  payload["rebalance_plan"] = std::move(plan_items);
  payload["controller_gate"] = json{
      {"cluster_ready", view.controller_gate_summary.cluster_ready},
      {"active_rollouts", view.controller_gate_summary.active_rollout_count},
      {"blocking_assignment_nodes", view.controller_gate_summary.blocking_assignment_count},
      {"unconverged_nodes", view.controller_gate_summary.unconverged_node_count},
      {"active_rollout_workers", view.controller_gate_summary.active_rollout_workers},
      {"blocking_assignment_node_names", view.controller_gate_summary.blocking_assignment_nodes},
      {"unconverged_node_names", view.controller_gate_summary.unconverged_nodes},
  };
  payload["iteration_budget"] = json{
      {"current_iteration", view.iteration_budget_summary.current_iteration},
      {"max_iterations", view.iteration_budget_summary.max_iterations},
      {"exhausted", view.iteration_budget_summary.exhausted},
  };
  payload["loop_status"] = json{
      {"state", view.loop_status.state},
      {"reason", view.loop_status.reason},
  };
  payload["policy_summary"] = json{
      {"actionable", view.policy_summary.actionable_count},
      {"safe_direct", view.policy_summary.safe_direct_count},
      {"rollout_class", view.policy_summary.rollout_class_count},
      {"gated", view.policy_summary.gated_count},
      {"blocked_active_rollouts", view.policy_summary.blocked_active_rollout_count},
      {"assignment_busy", view.policy_summary.assignment_busy_count},
      {"observation_gated", view.policy_summary.observation_gated_count},
      {"stable_holds", view.policy_summary.stable_hold_count},
      {"below_threshold", view.policy_summary.below_threshold_count},
      {"deferred", view.policy_summary.defer_count},
      {"no_candidate", view.policy_summary.no_candidate_count},
      {"actionable_workers", view.policy_summary.actionable_workers},
      {"safe_direct_workers", view.policy_summary.safe_direct_workers},
      {"rollout_class_workers", view.policy_summary.rollout_class_workers},
      {"gated_workers", view.policy_summary.gated_workers},
      {"stable_hold_workers", view.policy_summary.stable_hold_workers},
  };
  json scheduler_runtime_json;
  if (view.scheduler_runtime.plane_runtime.has_value()) {
    json plane_runtime_json;
    plane_runtime_json["active_action"] = view.scheduler_runtime.plane_runtime->active_action;
    plane_runtime_json["active_worker_name"] = view.scheduler_runtime.plane_runtime->active_worker_name;
    plane_runtime_json["phase"] = view.scheduler_runtime.plane_runtime->phase;
    plane_runtime_json["action_generation"] = view.scheduler_runtime.plane_runtime->action_generation;
    plane_runtime_json["stable_samples"] = view.scheduler_runtime.plane_runtime->stable_samples;
    plane_runtime_json["rollback_attempt_count"] =
        view.scheduler_runtime.plane_runtime->rollback_attempt_count;
    plane_runtime_json["source_node_name"] = view.scheduler_runtime.plane_runtime->source_node_name;
    plane_runtime_json["source_gpu_device"] = view.scheduler_runtime.plane_runtime->source_gpu_device;
    plane_runtime_json["target_node_name"] = view.scheduler_runtime.plane_runtime->target_node_name;
    plane_runtime_json["target_gpu_device"] = view.scheduler_runtime.plane_runtime->target_gpu_device;
    plane_runtime_json["status_message"] = view.scheduler_runtime.plane_runtime->status_message;
    plane_runtime_json["started_at"] = view.scheduler_runtime.plane_runtime->started_at;
    scheduler_runtime_json["plane_runtime"] = std::move(plane_runtime_json);
  } else {
    scheduler_runtime_json["plane_runtime"] = nullptr;
  }
  scheduler_runtime_json["worker_runtime"] = std::move(worker_runtime_items);
  scheduler_runtime_json["node_runtime"] = std::move(node_runtime_items);
  payload["scheduler_runtime"] = std::move(scheduler_runtime_json);
  return payload;
}

void ShowDemoPlan() {
  PrintStateSummary(comet::BuildDemoState());
}

void PrintPreviewSummary(const comet::DesiredState& state) {
  std::cout << "preview:\n";
  std::cout << "  plane=" << state.plane_name << "\n";
  std::cout << "  nodes=" << state.nodes.size() << "\n";
  std::cout << "  disks=" << state.disks.size() << "\n";
  std::cout << "  instances=" << state.instances.size() << "\n";

  const auto node_plans = comet::BuildNodeComposePlans(state);
  for (const auto& plan : node_plans) {
    std::cout << "  node " << plan.node_name
              << ": services=" << plan.services.size()
              << " disks=" << plan.disks.size() << "\n";
  }
}

int RenderComposeForState(
    const comet::DesiredState& state,
    const std::optional<std::string>& node_name) {
  if (node_name.has_value()) {
    const auto plan = comet::FindNodeComposePlan(state, *node_name);
    if (!plan.has_value()) {
      std::cerr << "error: node '" << *node_name << "' not found in state\n";
      return 1;
    }
    std::cout << comet::RenderComposeYaml(*plan);
    return 0;
  }

  const auto plans = comet::BuildNodeComposePlans(state);
  for (std::size_t index = 0; index < plans.size(); ++index) {
    if (index > 0) {
      std::cout << "\n---\n";
    }
    std::cout << comet::RenderComposeYaml(plans[index]);
  }
  return 0;
}

int RenderDemoCompose(const std::optional<std::string>& node_name) {
  return RenderComposeForState(comet::BuildDemoState(), node_name);
}

int ValidateBundle(const std::string& bundle_dir) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  std::cout << "bundle validation: OK\n";
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int PreviewBundle(const std::string& bundle_dir, const std::optional<std::string>& node_name) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << "\ncompose-preview:\n";
  return RenderComposeForState(state, node_name);
}

int PlanBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);

  std::cout << "bundle-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

std::vector<comet::NodeExecutionPlan> FilterNodeExecutionPlans(
    const std::vector<comet::NodeExecutionPlan>& plans,
    const std::optional<std::string>& node_name) {
  if (!node_name.has_value()) {
    return plans;
  }

  std::vector<comet::NodeExecutionPlan> filtered;
  for (const auto& plan : plans) {
    if (plan.node_name == *node_name) {
      filtered.push_back(plan);
    }
  }

  if (filtered.empty()) {
    throw std::runtime_error("node '" + *node_name + "' not found in execution plan");
  }

  return filtered;
}

std::map<std::string, comet::NodeComposePlan> BuildComposePlanMap(const comet::DesiredState& state) {
  std::map<std::string, comet::NodeComposePlan> result;
  for (const auto& plan : comet::BuildNodeComposePlans(state)) {
    result.emplace(plan.node_name, plan);
  }
  return result;
}

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::map<std::string, comet::NodeAvailabilityOverride> result;
  for (const auto& availability_override : availability_overrides) {
    result.emplace(availability_override.node_name, availability_override);
  }
  return result;
}

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name) {
  const auto it = availability_overrides.find(node_name);
  if (it == availability_overrides.end()) {
    return comet::NodeAvailability::Active;
  }
  return it->second.availability;
}

bool IsNodeSchedulable(comet::NodeAvailability availability) {
  return availability == comet::NodeAvailability::Active;
}

void PrintNodeAvailabilityOverrides(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::cout << "node-availability:\n";
  if (availability_overrides.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& availability_override : availability_overrides) {
    std::cout << "  - node=" << availability_override.node_name
              << " availability=" << comet::ToString(availability_override.availability)
              << " updated_at=" << availability_override.updated_at << "\n";
    if (!availability_override.status_message.empty()) {
      std::cout << "    message=" << availability_override.status_message << "\n";
    }
  }
}

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  if (observation->status == comet::HostObservationStatus::Failed) {
    return std::string("failed");
  }
  const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
  if (HealthFromAge(age_seconds, stale_after_seconds) == "stale") {
    return std::string("stale");
  }
  const auto runtime_status = ParseRuntimeStatus(*observation);
  if (runtime_status.has_value() && runtime_status->runtime_phase == "failed") {
    return std::string("runtime-failed");
  }
  const auto gpu_telemetry = ParseGpuTelemetry(*observation);
  if (gpu_telemetry.has_value() && gpu_telemetry->degraded) {
    return std::string("telemetry-degraded");
  }
  return std::nullopt;
}

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::size_t schedulable_nodes = 0;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state.nodes) {
    const auto availability = ResolveNodeAvailability(availability_overrides, node.name);
    if (!IsNodeSchedulable(availability)) {
      skipped_nodes.push_back(
          node.name + "(" + comet::ToString(availability) + ")");
      continue;
    }
    const auto observed_gate_reason =
        ObservedSchedulingGateReason(observations, node.name, stale_after_seconds);
    if (observed_gate_reason.has_value()) {
      skipped_nodes.push_back(node.name + "(" + *observed_gate_reason + ")");
      continue;
    }
    ++schedulable_nodes;
  }

  std::cout << "assignment-dispatch:\n";
  std::cout << "  schedulable_nodes=" << schedulable_nodes << "/" << desired_state.nodes.size()
            << "\n";
  if (!skipped_nodes.empty()) {
    std::cout << "  skipped_nodes=";
    for (std::size_t index = 0; index < skipped_nodes.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      std::cout << skipped_nodes[index];
    }
    std::cout << "\n";
  }
}

void WriteTextFile(const std::string& path, const std::string& contents) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

void MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans) {
  const auto desired_compose_plans = BuildComposePlanMap(desired_state);

  for (const auto& host_plan : host_plans) {
    for (const auto& operation : host_plan.operations) {
      if (operation.kind == comet::HostOperationKind::WriteComposeFile) {
        const auto compose_it = desired_compose_plans.find(host_plan.node_name);
        if (compose_it == desired_compose_plans.end()) {
          throw std::runtime_error(
              "missing compose plan for node '" + host_plan.node_name + "'");
        }
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_it->second));
      }

      if (operation.kind == comet::HostOperationKind::RemoveComposeFile) {
        RemoveFileIfExists(operation.target);
      }
    }
  }
}

std::string InferRuntimeArtifactPath(
    const std::string& artifacts_root,
    const std::string& plane_name) {
  return (
      std::filesystem::path(artifacts_root) / plane_name / "infer-runtime.json").string();
}

void MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root) {
  WriteTextFile(
      InferRuntimeArtifactPath(artifacts_root, desired_state.plane_name),
      comet::RenderInferRuntimeConfigJson(desired_state));
}

std::vector<comet::HostAssignment> BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report = std::nullopt) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  const auto rollout_actions_by_target_node =
      scheduling_report.has_value()
          ? BuildRolloutActionsByTargetNode(*scheduling_report)
          : std::map<std::string, std::vector<comet::SchedulerRolloutAction>>{};

  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    if (ObservedSchedulingGateReason(
            observations, node.name, DefaultStaleAfterSeconds()).has_value()) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "apply-node-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    const auto rollout_it = rollout_actions_by_target_node.find(node.name);
    if (rollout_it != rollout_actions_by_target_node.end() &&
        !rollout_it->second.empty()) {
      std::set<std::string> gated_workers;
      for (const auto& action : rollout_it->second) {
        if (!action.worker_name.empty()) {
          gated_workers.insert(action.worker_name);
        }
      }
      std::ostringstream message;
      message << "scheduler rollout actions pending on target node " << node.name << " for workers ";
      bool first = true;
      for (const auto& worker_name : gated_workers) {
        if (!first) {
          message << ",";
        }
        first = false;
        message << worker_name;
      }
      assignment.status_message = message.str();
    }
    assignments.push_back(std::move(assignment));
  }

  return assignments;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

bool AssignmentRepresentsDrainedNode(const comet::HostAssignment& assignment) {
  return assignment.assignment_type == "drain-node-state" &&
         (assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed ||
          assignment.status == comet::HostAssignmentStatus::Applied);
}

bool ObservedNodeStateNeedsResync(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const comet::HostObservation& observation) {
  if (observation.observed_state_json.empty()) {
    return true;
  }

  const comet::DesiredState observed_node_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(desired_state, node_name);

  if (desired_node_state.disks.empty() && desired_node_state.instances.empty()) {
    return false;
  }

  return observed_node_state.disks.empty() || observed_node_state.instances.empty();
}

std::optional<comet::HostAssignment> BuildResyncAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments,
    const std::optional<comet::HostObservation>& observation) {
  bool node_exists = false;
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      node_exists = true;
      break;
    }
  }
  if (!node_exists) {
    return std::nullopt;
  }

  const auto latest_assignment = FindLatestHostAssignmentForNode(existing_assignments, node_name);
  const bool latest_assignment_is_drain =
      latest_assignment.has_value() && latest_assignment->desired_generation == desired_generation &&
      AssignmentRepresentsDrainedNode(*latest_assignment);

  if (observation.has_value() &&
      observation->applied_generation.has_value() &&
      *observation->applied_generation == desired_generation &&
      observation->status != comet::HostObservationStatus::Failed &&
      !latest_assignment_is_drain &&
      !ObservedNodeStateNeedsResync(desired_state, node_name, *observation)) {
    return std::nullopt;
  }

  if (latest_assignment.has_value() &&
      latest_assignment->desired_generation == desired_generation &&
      latest_assignment->assignment_type == "apply-node-state" &&
      (latest_assignment->status == comet::HostAssignmentStatus::Pending ||
       latest_assignment->status == comet::HostAssignmentStatus::Claimed ||
       latest_assignment->status == comet::HostAssignmentStatus::Applied)) {
    return std::nullopt;
  }

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "apply-node-state";
  assignment.desired_state_json =
      comet::SerializeDesiredStateJson(
          comet::SliceDesiredStateForNode(desired_state, node_name));
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "resync after node returned to active";
  return assignment;
}

std::optional<comet::NodeInventory> FindNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return node;
    }
  }
  return std::nullopt;
}

std::optional<comet::HostAssignment> BuildDrainAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  const auto node = FindNodeInventory(desired_state, node_name);
  if (!node.has_value()) {
    return std::nullopt;
  }

  comet::DesiredState drain_state;
  drain_state.plane_name = desired_state.plane_name;
  drain_state.plane_shared_disk_name = desired_state.plane_shared_disk_name;
  drain_state.control_root = desired_state.control_root;
  drain_state.inference = desired_state.inference;
  drain_state.gateway = desired_state.gateway;
  drain_state.runtime_gpu_nodes = desired_state.runtime_gpu_nodes;
  drain_state.nodes.push_back(*node);

  const auto latest_assignment = FindLatestHostAssignmentForNode(existing_assignments, node_name);
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "drain-node-state";
  assignment.desired_state_json = comet::SerializeDesiredStateJson(drain_state);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "drain after node availability changed";
  return assignment;
}

void PrintHostAssignments(const std::vector<comet::HostAssignment>& assignments) {
  std::cout << "host-assignments:\n";
  if (assignments.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& assignment : assignments) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    std::cout << "  - id=" << assignment.id
              << " node=" << assignment.node_name
              << " plane=" << assignment.plane_name
              << " generation=" << assignment.desired_generation
              << " attempts=" << assignment.attempt_count << "/" << assignment.max_attempts
              << " type=" << assignment.assignment_type
              << " status=" << comet::ToString(assignment.status)
              << " instances=" << desired_node_state.instances.size()
              << " artifacts_root=" << assignment.artifacts_root << "\n";
    if (!assignment.status_message.empty()) {
      std::cout << "    message=" << assignment.status_message << "\n";
    }
  }
}

std::time_t ToUtcTime(std::tm* timestamp) {
#if defined(_WIN32)
  return _mkgmtime(timestamp);
#else
  return timegm(timestamp);
#endif
}

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at) {
  if (heartbeat_at.empty()) {
    return std::nullopt;
  }

  std::tm heartbeat_tm{};
  std::istringstream input(heartbeat_at);
  input >> std::get_time(&heartbeat_tm, "%Y-%m-%d %H:%M:%S");
  if (input.fail()) {
    return std::nullopt;
  }

  const std::time_t heartbeat_time = ToUtcTime(&heartbeat_tm);
  if (heartbeat_time < 0) {
    return std::nullopt;
  }

  const std::time_t now = std::time(nullptr);
  return static_cast<long long>(now - heartbeat_time);
}

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text) {
  return HeartbeatAgeSeconds(timestamp_text);
}

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state) {
  SchedulerRuntimeView view;
  if (!desired_state.has_value()) {
    return view;
  }
  view.plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  for (const auto& runtime : store.LoadSchedulerWorkerRuntimes(desired_state->plane_name)) {
    view.worker_runtime_by_name.emplace(runtime.worker_name, runtime);
  }
  for (const auto& runtime : store.LoadSchedulerNodeRuntimes(desired_state->plane_name)) {
    view.node_runtime_by_name.emplace(runtime.node_name, runtime);
  }
  return view;
}

void PrintSchedulerRuntimeView(const SchedulerRuntimeView& view) {
  std::cout << "scheduler-runtime:\n";
  if (!view.plane_runtime.has_value()) {
    std::cout << "  plane_action=(none)\n";
  } else {
    const auto& runtime = *view.plane_runtime;
    std::cout << "  plane_action="
              << (runtime.active_action.empty() ? "(none)" : runtime.active_action)
              << " phase=" << (runtime.phase.empty() ? "(empty)" : runtime.phase)
              << " worker="
              << (runtime.active_worker_name.empty() ? "(empty)" : runtime.active_worker_name)
              << " generation=" << runtime.action_generation
              << " stable_samples=" << runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " rollback_attempts=" << runtime.rollback_attempt_count << "\n";
    if (!runtime.status_message.empty()) {
      std::cout << "  status_message=" << runtime.status_message << "\n";
    }
  }
  if (!view.worker_runtime_by_name.empty()) {
    std::cout << "  worker_runtime:\n";
    for (const auto& [worker_name, runtime] : view.worker_runtime_by_name) {
      std::cout << "    - worker=" << worker_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at)
                << " last_eviction_at="
                << (runtime.last_eviction_at.empty() ? "(empty)" : runtime.last_eviction_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      if (!runtime.last_scheduler_phase.empty()) {
        std::cout << " last_phase=" << runtime.last_scheduler_phase;
      }
      std::cout << " manual_intervention_required="
                << (runtime.manual_intervention_required ? "yes" : "no") << "\n";
      if (!runtime.last_status_message.empty()) {
        std::cout << "      status_message=" << runtime.last_status_message << "\n";
      }
    }
  }
  if (!view.node_runtime_by_name.empty()) {
    std::cout << "  node_runtime:\n";
    for (const auto& [node_name, runtime] : view.node_runtime_by_name) {
      std::cout << "    - node=" << node_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      std::cout << "\n";
    }
  }
}

const comet::RuntimeProcessStatus* FindInstanceRuntimeStatus(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::string& instance_name,
    const std::string& gpu_device) {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name &&
        status.gpu_device == gpu_device) {
      return &status;
    }
  }
  return nullptr;
}

bool TelemetryShowsOwnedProcess(
    const std::optional<comet::GpuTelemetrySnapshot>& telemetry,
    const std::string& gpu_device,
    const std::string& instance_name) {
  if (!telemetry.has_value()) {
    return false;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device != gpu_device) {
      continue;
    }
    for (const auto& process : device.processes) {
      if (process.instance_name == instance_name) {
        return true;
      }
    }
  }
  return false;
}

struct SchedulerVerificationResult {
  bool converged = false;
  bool stable = false;
  bool timed_out = false;
  int next_stable_samples = 0;
  std::string detail;
};

SchedulerVerificationResult EvaluateSchedulerActionVerification(
    const comet::SchedulerPlaneRuntime& plane_runtime,
    const std::vector<comet::HostObservation>& observations) {
  SchedulerVerificationResult result;
  const bool rollback_mode = plane_runtime.phase == "rollback-applied" ||
                             plane_runtime.phase == "rollback-planned";
  const std::string expected_node =
      rollback_mode ? plane_runtime.source_node_name : plane_runtime.target_node_name;
  const std::string expected_gpu =
      rollback_mode ? plane_runtime.source_gpu_device : plane_runtime.target_gpu_device;
  const std::string cleared_node =
      rollback_mode ? plane_runtime.target_node_name : plane_runtime.source_node_name;
  const std::string cleared_gpu =
      rollback_mode ? plane_runtime.target_gpu_device : plane_runtime.source_gpu_device;

  const auto target_observation = FindHostObservationForNode(observations, expected_node);
  const auto source_observation = FindHostObservationForNode(observations, cleared_node);
  if (!target_observation.has_value()) {
    result.detail = "missing-target-observation";
  } else {
    const auto target_runtimes = ParseInstanceRuntimeStatuses(*target_observation);
    const auto target_runtime =
        FindInstanceRuntimeStatus(target_runtimes, plane_runtime.active_worker_name, expected_gpu);
    const auto target_telemetry = ParseGpuTelemetry(*target_observation);
    const bool target_generation_applied =
        target_observation->applied_generation.has_value() &&
        *target_observation->applied_generation >= plane_runtime.action_generation;
    const bool target_runtime_ready =
        target_runtime != nullptr &&
        target_runtime->ready &&
        (target_runtime->runtime_phase == "running" ||
         target_runtime->runtime_phase == "ready" ||
         target_runtime->runtime_phase == "loaded");
    const bool target_gpu_owned =
        TelemetryShowsOwnedProcess(
            target_telemetry, expected_gpu, plane_runtime.active_worker_name);

    bool source_cleared = true;
    if (source_observation.has_value()) {
      const auto source_runtimes = ParseInstanceRuntimeStatuses(*source_observation);
      const auto source_runtime =
          FindInstanceRuntimeStatus(
              source_runtimes,
              plane_runtime.active_worker_name,
              cleared_gpu);
      const auto source_telemetry = ParseGpuTelemetry(*source_observation);
      source_cleared =
          source_runtime == nullptr &&
          !TelemetryShowsOwnedProcess(
              source_telemetry, cleared_gpu, plane_runtime.active_worker_name);
    }

    result.converged =
        target_generation_applied && target_runtime_ready && target_gpu_owned && source_cleared;
    if (result.converged) {
      result.next_stable_samples = plane_runtime.stable_samples + 1;
      result.stable = result.next_stable_samples >= VerificationStableSamplesRequired();
      result.detail = "verified-sample";
    } else {
      result.next_stable_samples = 0;
      std::ostringstream detail;
      detail << "target_generation_applied=" << (target_generation_applied ? "yes" : "no")
             << " target_runtime_ready=" << (target_runtime_ready ? "yes" : "no")
             << " target_gpu_owned=" << (target_gpu_owned ? "yes" : "no")
             << " source_cleared=" << (source_cleared ? "yes" : "no");
      result.detail = detail.str();
    }
  }

  const auto action_age = TimestampAgeSeconds(plane_runtime.started_at);
  result.timed_out =
      action_age.has_value() && *action_age >= VerificationTimeoutSeconds();
  return result;
}

std::string UtcNowSqlTimestamp() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  gmtime_r(&now, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

void MarkWorkerMoveVerified(
    comet::ControllerStore* store,
    const comet::SchedulerPlaneRuntime& plane_runtime) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store->LoadSchedulerWorkerRuntime(plane_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = plane_runtime.plane_name;
  worker_runtime.worker_name = plane_runtime.active_worker_name;
  worker_runtime.last_move_at = now;
  worker_runtime.last_verified_generation = plane_runtime.action_generation;
  worker_runtime.last_scheduler_phase = "verified";
  worker_runtime.last_status_message =
      plane_runtime.phase == "rollback-applied"
          ? "rollback verification succeeded"
          : "move verification succeeded";
  worker_runtime.manual_intervention_required = false;
  store->UpsertSchedulerWorkerRuntime(worker_runtime);

  for (const auto& node_name : {plane_runtime.source_node_name, plane_runtime.target_node_name}) {
    if (node_name.empty()) {
      continue;
    }
    comet::SchedulerNodeRuntime node_runtime;
    if (const auto current = store->LoadSchedulerNodeRuntime(node_name); current.has_value()) {
      node_runtime = *current;
    }
    node_runtime.plane_name = plane_runtime.plane_name;
    node_runtime.node_name = node_name;
    node_runtime.last_move_at = now;
    node_runtime.last_verified_generation = plane_runtime.action_generation;
    store->UpsertSchedulerNodeRuntime(node_runtime);
  }
}

void MarkWorkersEvicted(
    comet::ControllerStore* store,
    const std::string& plane_name,
    const std::vector<std::string>& worker_names) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  for (const auto& worker_name : worker_names) {
    if (worker_name.empty()) {
      continue;
    }
    comet::SchedulerWorkerRuntime runtime;
    if (const auto current = store->LoadSchedulerWorkerRuntime(worker_name); current.has_value()) {
      runtime = *current;
    }
    runtime.plane_name = plane_name;
    runtime.worker_name = worker_name;
    runtime.last_eviction_at = now;
    runtime.last_scheduler_phase = "evicted";
    runtime.last_status_message = "eviction verified";
    store->UpsertSchedulerWorkerRuntime(runtime);
  }
}

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds) {
  if (!age_seconds.has_value()) {
    return "unknown";
  }
  return *age_seconds > stale_after_seconds ? "stale" : "online";
}

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation) {
  if (observation.runtime_status_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeRuntimeStatusJson(observation.runtime_status_json);
}

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation) {
  if (observation.instance_runtime_json.empty()) {
    return {};
  }
  return comet::DeserializeRuntimeStatusListJson(observation.instance_runtime_json);
}

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation) {
  if (observation.gpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeGpuTelemetryJson(observation.gpu_telemetry_json);
}

void PrintHostObservations(
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::cout << "host-observations:\n";
  if (observations.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& observation : observations) {
    std::size_t disk_count = 0;
    std::size_t instance_count = 0;
    if (!observation.observed_state_json.empty()) {
      const comet::DesiredState observed_state =
          comet::DeserializeDesiredStateJson(observation.observed_state_json);
      disk_count = observed_state.disks.size();
      instance_count = observed_state.instances.size();
    }
    const auto runtime_status = ParseRuntimeStatus(observation);
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    const auto gpu_telemetry = ParseGpuTelemetry(observation);
    const auto age_seconds = HeartbeatAgeSeconds(observation.heartbeat_at);

    std::cout << "  - node=" << observation.node_name
              << " plane=" << (observation.plane_name.empty() ? "(none)" : observation.plane_name)
              << " status=" << comet::ToString(observation.status);
    if (observation.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation.applied_generation;
    }
    if (observation.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation.last_assignment_id;
    }
    std::cout << " disks=" << disk_count
              << " instances=" << instance_count
              << " heartbeat_at=" << observation.heartbeat_at;
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds
                << " health=" << HealthFromAge(age_seconds, stale_after_seconds);
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id)
                << " gateway="
                << (runtime_status->gateway_listen.empty()
                        ? "(empty)"
                        : runtime_status->gateway_listen);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry_source="
                << (gpu_telemetry->source.empty() ? "(empty)" : gpu_telemetry->source)
                << " telemetry_degraded=" << (gpu_telemetry->degraded ? "yes" : "no")
                << " gpu_devices=" << gpu_telemetry->devices.size();
    }
    if (!instance_statuses.empty()) {
      std::cout << " instance_runtimes=" << instance_statuses.size();
    }
    std::cout << "\n";
    if (!observation.status_message.empty()) {
      std::cout << "    message=" << observation.status_message << "\n";
    }
    if (runtime_status.has_value()) {
      std::cout << "    runtime aliases=";
      if (runtime_status->aliases.empty()) {
        std::cout << "(empty)";
      } else {
        for (std::size_t index = 0; index < runtime_status->aliases.size(); ++index) {
          if (index > 0) {
            std::cout << ",";
          }
          std::cout << runtime_status->aliases[index];
        }
      }
      std::cout << " runtime_profile="
                << (runtime_status->active_runtime_profile.empty()
                        ? "(empty)"
                        : runtime_status->active_runtime_profile)
                << " inference_ready=" << (runtime_status->inference_ready ? "yes" : "no")
                << " gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no")
                << "\n";
    }
    if (gpu_telemetry.has_value()) {
      for (const auto& device : gpu_telemetry->devices) {
        std::cout << "    gpu device=" << device.gpu_device
                  << " used_vram_mb=" << device.used_vram_mb
                  << "/" << device.total_vram_mb
                  << " free_vram_mb=" << device.free_vram_mb
                  << " util_pct=" << device.gpu_utilization_pct;
        if (!device.processes.empty()) {
          std::cout << " processes=";
          for (std::size_t index = 0; index < device.processes.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << device.processes[index].instance_name
                      << ":" << device.processes[index].pid
                      << ":" << device.processes[index].used_vram_mb << "MB";
          }
        }
        std::cout << "\n";
      }
    }
    if (!instance_statuses.empty()) {
      for (const auto& instance_status : instance_statuses) {
        std::cout << "    instance name=" << instance_status.instance_name
                  << " role=" << instance_status.instance_role
                  << " phase=" << instance_status.runtime_phase
                  << " ready=" << (instance_status.ready ? "yes" : "no")
                  << " pid=" << instance_status.runtime_pid
                  << " gpu=" << (instance_status.gpu_device.empty() ? "(empty)" : instance_status.gpu_device);
        if (!instance_status.model_path.empty()) {
          std::cout << " model_path=" << instance_status.model_path;
        }
        std::cout << "\n";
      }
    }
  }
}

void PrintHostHealth(
    const std::optional<comet::DesiredState>& desired_state,
    const std::vector<comet::HostObservation>& observations,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (desired_state.has_value()) {
    for (const auto& node : desired_state->nodes) {
      if (!node_name.has_value() || node.name == *node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!node_name.has_value() || observed_node_name == *node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  std::cout << "host-health:\n";
  if (nodes.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;

  for (const auto& current_node_name : nodes) {
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      std::cout << "  - node=" << current_node_name
                << " availability="
                << comet::ToString(
                       ResolveNodeAvailability(availability_override_map, current_node_name))
                << " health=unknown status=(none)\n";
      ++unknown_count;
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, stale_after_seconds);
    const auto runtime_status = ParseRuntimeStatus(observation_it->second);
    const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }

    std::cout << "  - node=" << current_node_name
              << " availability="
              << comet::ToString(
                     ResolveNodeAvailability(availability_override_map, current_node_name))
              << " health=" << health
              << " status=" << comet::ToString(observation_it->second.status);
    if (observation_it->second.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation_it->second.applied_generation;
    }
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds;
    }
    if (observation_it->second.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation_it->second.last_assignment_id;
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry="
                << (gpu_telemetry->degraded ? "degraded" : "ok")
                << ":" << (gpu_telemetry->source.empty() ? "unknown" : gpu_telemetry->source);
    }
    std::cout << "\n";
    if (!observation_it->second.status_message.empty()) {
      std::cout << "    message=" << observation_it->second.status_message << "\n";
    }
  }

  std::cout << "summary: online=" << online_count
            << " stale=" << stale_count
            << " unknown=" << unknown_count << "\n";
}

int PlanHostOps(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto host_plans =
      FilterNodeExecutionPlans(
          comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root),
          node_name);

  std::cout << "host-op-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << "  artifacts_root=" << artifacts_root << "\n";
  std::cout << comet::RenderNodeExecutionPlans(host_plans);
  return 0;
}

int InitDb(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  std::cout << "initialized db: " << db_path << "\n";
  return 0;
}

int SeedDemo(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::BuildDemoState();
  comet::RequireSchedulingPolicy(desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          DefaultArtifactsRoot(),
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "seeded demo state into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ImportBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          DefaultArtifactsRoot(),
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "imported bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto host_plans =
      comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << comet::RenderNodeExecutionPlans(host_plans);

  MaterializeComposeArtifacts(desired_state, host_plans);
  MaterializeInferRuntimeArtifact(desired_state, artifacts_root);
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          artifacts_root,
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "applied bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ShowHostAssignments(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadHostAssignmentsViewData(db_path, node_name);
  std::cout << "db: " << view.db_path << "\n";
  PrintHostAssignments(view.assignments);
  return 0;
}

int ShowHostObservations(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostObservationsViewData(db_path, node_name, stale_after_seconds);
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  PrintHostObservations(view.observations, view.stale_after_seconds);
  return 0;
}

int ShowNodeAvailability(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));
  return 0;
}

int ShowHostHealth(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostHealthViewData(db_path, node_name, stale_after_seconds);
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      view.node_name,
      view.stale_after_seconds);
  return 0;
}

int ShowRolloutActions(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name);

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  if (!view.actions.empty()) {
    std::cout << "rollout-gates:\n";
    std::cout << "  gated_workers=" << view.gated_worker_count
              << " gated_nodes=" << view.gated_node_count
              << " deferred_actions=" << view.actions.size() << "\n";
  }
  PrintPersistedRolloutActions(view.actions);
  if (view.scheduler_runtime.has_value()) {
    PrintSchedulerRuntimeView(*view.scheduler_runtime);
  }
  if (!view.lifecycle.empty()) {
    PrintRolloutLifecycleEntries(view.lifecycle);
  }
  return 0;
}

int ShowRebalancePlan(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadRebalancePlanViewData(
      db_path,
      node_name,
      DefaultStaleAfterSeconds());
  if (!view.desired_state.has_value()) {
    std::cout << "rebalance-plan:\n  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  std::cout << "desired generation: " << view.desired_generation << "\n";
  PrintRebalanceControllerGateSummary(view.controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(view.iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(view.loop_status);
  PrintRebalancePlanEntries(view.rebalance_entries);
  PrintRebalancePolicySummary(view.policy_summary);
  PrintSchedulerRuntimeView(view.scheduler_runtime);
  return 0;
}

int SetRolloutActionStatus(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  if (!store.UpdateRolloutActionStatus(action_id, status, status_message.value_or(""))) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  std::cout << "updated rollout action id=" << action_id
            << " status=" << comet::ToString(status) << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  return 0;
}

int EnqueueRolloutEviction(
    const std::string& db_path,
    int action_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions();
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->desired_generation != *desired_generation) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " does not belong to current desired generation " +
        std::to_string(*desired_generation));
  }
  if (action->action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not an evict-best-effort action");
  }
  if (action->status != comet::RolloutActionStatus::Pending &&
      action->status != comet::RolloutActionStatus::Acknowledged) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " cannot enqueue eviction from status=" +
        comet::ToString(action->status));
  }

  const auto existing_assignments = store.LoadHostAssignments();
  const auto eviction_assignments = BuildEvictionAssignmentsForAction(
      *desired_state,
      *desired_generation,
      *action,
      existing_assignments);
  store.EnqueueHostAssignments(
      eviction_assignments,
      "superseded by rollout eviction action id=" + std::to_string(action_id));

  std::set<std::string> node_names;
  for (const auto& assignment : eviction_assignments) {
    node_names.insert(assignment.node_name);
  }
  std::ostringstream message;
  message << "eviction assignments enqueued on nodes ";
  bool first = true;
  for (const auto& node_name : node_names) {
    if (!first) {
      message << ",";
    }
    first = false;
    message << node_name;
  }
  store.UpdateRolloutActionStatus(
      action_id,
      comet::RolloutActionStatus::Acknowledged,
      message.str());

  std::cout << "enqueued rollout eviction action id=" << action_id << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  for (const auto& node_name : node_names) {
    PrintHostAssignments(store.LoadHostAssignments(node_name));
  }
  return 0;
}

int ApplyReadyRolloutAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions();
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->status != comet::RolloutActionStatus::ReadyToRetry) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not ready-to-retry; current status=" +
        comet::ToString(action->status));
  }
  if (action->action != "retry-placement") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not a retry-placement action");
  }

  std::vector<std::string> victim_worker_names;
  for (const auto& candidate_action : rollout_actions) {
    if (candidate_action.desired_generation != action->desired_generation ||
        candidate_action.worker_name != action->worker_name ||
        candidate_action.step >= action->step) {
      continue;
    }
    if (candidate_action.status != comet::RolloutActionStatus::ReadyToRetry) {
      throw std::runtime_error(
          "prior rollout step id=" + std::to_string(candidate_action.id) +
          " is not ready-to-retry");
    }
    if (candidate_action.action == "evict-best-effort") {
      victim_worker_names = candidate_action.victim_worker_names;
    }
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRetryPlacementAction(&updated_state, *action, victim_worker_names);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();

  store.ReplaceDesiredState(updated_state, next_generation, 0);
  store.ClearSchedulerPlaneRuntime(updated_state.plane_name);
  store.ReplaceRolloutActions(next_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          scheduling_report));

  std::cout << "applied ready rollout action id=" << action_id << "\n";
  std::cout << "desired generation: " << next_generation << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int ApplyRebalanceProposal(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(),
          assignments,
          observations);
  const auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());

  const auto rebalance_it = std::find_if(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [&](const RebalancePlanEntry& entry) { return entry.worker_name == worker_name; });
  if (rebalance_it == rebalance_entries.end()) {
    throw std::runtime_error(
        "no rebalance plan entry found for worker '" + worker_name + "'");
  }
  if (rebalance_it->decision != "propose") {
    throw std::runtime_error(
        "worker '" + worker_name + "' is not actionable for rebalance; current decision=" +
        rebalance_it->decision + " state=" + rebalance_it->state);
  }
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  if (iteration_budget_summary.exhausted) {
    throw std::runtime_error(
        "rebalance iteration budget exhausted (" +
        std::to_string(iteration_budget_summary.current_iteration) + "/" +
        std::to_string(iteration_budget_summary.max_iterations) +
        "); apply a fresh bundle or rollout generation before materializing another direct rebalance");
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRebalancePlanEntry(&updated_state, *rebalance_it);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport updated_scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto host_plans =
      comet::BuildNodeExecutionPlans(desired_state, updated_state, artifacts_root);

  MaterializeComposeArtifacts(updated_state, host_plans);
  MaterializeInferRuntimeArtifact(updated_state, artifacts_root);
  store.ReplaceDesiredState(
      updated_state,
      next_generation,
      rebalance_iteration.value_or(0) + 1);
  store.ReplaceRolloutActions(next_generation, updated_scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          updated_scheduling_report));
  comet::SchedulerPlaneRuntime plane_runtime;
  plane_runtime.plane_name = updated_state.plane_name;
  plane_runtime.active_action = "rebalance";
  plane_runtime.active_worker_name = rebalance_it->worker_name;
  plane_runtime.phase = "verifying-move";
  plane_runtime.action_generation = next_generation;
  plane_runtime.stable_samples = 0;
  plane_runtime.rollback_attempt_count = 0;
  plane_runtime.source_node_name = rebalance_it->current_node_name;
  plane_runtime.source_gpu_device = rebalance_it->current_gpu_device;
  plane_runtime.target_node_name = rebalance_it->target_node_name;
  plane_runtime.target_gpu_device = rebalance_it->target_gpu_device;
  plane_runtime.previous_state_json = comet::SerializeDesiredStateJson(*desired_state);
  plane_runtime.status_message = "awaiting post-move verification";
  store.UpsertSchedulerPlaneRuntime(plane_runtime);

  std::cout << "applied rebalance proposal for worker '" << worker_name << "'\n";
  std::cout << "desired generation: " << next_generation << "\n";
  std::cout << "target=" << rebalance_it->target_node_name << ":"
            << rebalance_it->target_gpu_device << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(updated_scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(updated_scheduling_report);
  PrintAssignmentDispatchSummary(
      updated_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(),
          assignments,
          observations);
  auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *desired_state,
          *desired_generation,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  const auto rebalance_policy_summary =
      BuildRebalancePolicySummary(rebalance_entries);
  PrintRebalanceControllerGateSummary(controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(
      BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));

  if (!controller_gate_summary.cluster_ready) {
    std::cout << "rebalance proposals: blocked by controller gate\n";
    return 0;
  }

  rebalance_entries.erase(
      std::remove_if(
          rebalance_entries.begin(),
          rebalance_entries.end(),
          [](const RebalancePlanEntry& entry) {
            return entry.decision != "propose" || entry.rebalance_class != "safe-direct";
          }),
      rebalance_entries.end());

  if (rebalance_entries.empty()) {
    std::cout << "rebalance proposals: none actionable\n";
    return 0;
  }
  if (iteration_budget_summary.exhausted) {
    std::cout << "rebalance proposals: blocked by iteration budget\n";
    return 0;
  }

  std::sort(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.worker_name < right.worker_name;
      });

  std::cout << "selected rebalance proposal: worker=" << rebalance_entries.front().worker_name
            << " target=" << rebalance_entries.front().target_node_name << ":"
            << rebalance_entries.front().target_gpu_device
            << " score=" << rebalance_entries.front().score << "\n";
  return ApplyRebalanceProposal(
      db_path, rebalance_entries.front().worker_name, artifacts_root);
}

int AdvanceActiveSchedulerAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler active-action: no desired state\n";
    return 0;
  }

  const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  if (!plane_runtime.has_value() || plane_runtime->active_action.empty()) {
    std::cout << "scheduler active-action: none\n";
    return 0;
  }

  if (plane_runtime->phase == "rollback-planned") {
    if (plane_runtime->previous_state_json.empty()) {
      throw std::runtime_error(
          "rollback-planned action has no previous desired state payload");
    }
    const comet::DesiredState rollback_state =
        comet::DeserializeDesiredStateJson(plane_runtime->previous_state_json);
    comet::RequireSchedulingPolicy(rollback_state);
    const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
    const auto observations = store.LoadHostObservations();
    const auto rollback_report = comet::EvaluateSchedulingPolicy(rollback_state);
    const int rollback_generation = *desired_generation + 1;
    store.ReplaceDesiredState(rollback_state, rollback_generation, 0);
    store.ReplaceRolloutActions(rollback_generation, rollback_report.rollout_actions);
    store.ReplaceHostAssignments(
        BuildHostAssignments(
            rollback_state,
            artifacts_root,
            rollback_generation,
            availability_overrides,
            observations,
            rollback_report));
    comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
    updated_runtime.phase = "rollback-applied";
    updated_runtime.action_generation = rollback_generation;
    updated_runtime.stable_samples = 0;
    updated_runtime.rollback_attempt_count = 1;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "rollback materialized after verification timeout";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: rollback-applied worker="
              << updated_runtime.active_worker_name
              << " generation=" << rollback_generation << "\n";
    return 0;
  }

  const auto observations = store.LoadHostObservations();
  const auto verification = EvaluateSchedulerActionVerification(*plane_runtime, observations);
  comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
  updated_runtime.stable_samples = verification.next_stable_samples;
  updated_runtime.status_message = verification.detail;

  if (verification.stable) {
    MarkWorkerMoveVerified(&store, updated_runtime);
    store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
    std::cout << "scheduler active-action: verified worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase << "\n";
    return 0;
  }

  if (!verification.timed_out) {
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: waiting worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase
              << " stable_samples=" << updated_runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " detail=" << verification.detail << "\n";
    return 0;
  }

  if (updated_runtime.rollback_attempt_count == 0 &&
      !updated_runtime.previous_state_json.empty()) {
    comet::SchedulerWorkerRuntime worker_runtime;
    if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
        current.has_value()) {
      worker_runtime = *current;
    }
    worker_runtime.plane_name = updated_runtime.plane_name;
    worker_runtime.worker_name = updated_runtime.active_worker_name;
    worker_runtime.last_scheduler_phase = "failed-verification";
    worker_runtime.last_status_message = verification.detail;
    worker_runtime.manual_intervention_required = false;
    store.UpsertSchedulerWorkerRuntime(worker_runtime);
    updated_runtime.phase = "rollback-planned";
    updated_runtime.stable_samples = 0;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "verification timed out; rollback planned";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: rollback-planned worker="
              << updated_runtime.active_worker_name
              << " generation=" << updated_runtime.action_generation << "\n";
    return 0;
  }

  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = updated_runtime.plane_name;
  worker_runtime.worker_name = updated_runtime.active_worker_name;
  worker_runtime.last_scheduler_phase = "manual-intervention-required";
  worker_runtime.last_status_message = verification.detail;
  worker_runtime.manual_intervention_required = true;
  store.UpsertSchedulerWorkerRuntime(worker_runtime);
  store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
  std::cout << "scheduler active-action: manual-intervention-required worker="
            << updated_runtime.active_worker_name
            << " detail=" << verification.detail << "\n";
  return 0;
}

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler-tick: no desired state\n";
    return 0;
  }

  if (const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
      plane_runtime.has_value() && !plane_runtime->active_action.empty()) {
    std::cout << "scheduler-tick: step=active-scheduler-action\n";
    return AdvanceActiveSchedulerAction(db_path, artifacts_root);
  }

  const auto rollout_actions = store.LoadRolloutActions();
  bool has_active_rollout = false;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == *desired_generation &&
        action.status != comet::RolloutActionStatus::ReadyToRetry) {
      has_active_rollout = true;
      break;
    }
  }
  if (!rollout_actions.empty()) {
    std::cout << "scheduler-tick: step=rollout-reconcile\n";
    return ReconcileRolloutActions(db_path, artifacts_root);
  }

  std::cout << "scheduler-tick: step=rebalance-reconcile\n";
  if (has_active_rollout) {
    std::cout << "scheduler-tick: rollout still active\n";
    return 0;
  }
  return ReconcileRebalanceProposals(db_path, artifacts_root);
}

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto all_rollout_actions = store.LoadRolloutActions();
  std::vector<comet::RolloutActionRecord> rollout_actions;
  for (const auto& action : all_rollout_actions) {
    if (action.desired_generation == *desired_generation) {
      rollout_actions.push_back(action);
    }
  }

  std::cout << "db: " << db_path << "\n";
  std::cout << "desired generation: " << *desired_generation << "\n";
  if (rollout_actions.empty()) {
    std::cout << "rollout reconcile: no rollout actions for current generation\n";
    return 0;
  }

  bool changed = false;
  for (const auto& action : rollout_actions) {
    if (action.action == "evict-best-effort") {
      if (action.status == comet::RolloutActionStatus::Pending) {
        const auto existing_assignments = store.LoadHostAssignments();
        const auto eviction_assignments = BuildEvictionAssignmentsForAction(
            *desired_state,
            *desired_generation,
            action,
            existing_assignments);
        store.EnqueueHostAssignments(
            eviction_assignments,
            "superseded by rollout eviction action id=" + std::to_string(action.id));
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::Acknowledged,
            "controller-managed eviction assignments enqueued");
        std::cout << "rollout reconcile: enqueued eviction action id=" << action.id << "\n";
        changed = true;
        continue;
      }

      if (action.status == comet::RolloutActionStatus::Acknowledged &&
          AreRolloutEvictionAssignmentsApplied(store.LoadHostAssignments(), action.id)) {
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::ReadyToRetry,
            "eviction assignments applied");
        MarkWorkersEvicted(&store, desired_state->plane_name, action.victim_worker_names);
        std::cout << "rollout reconcile: eviction action id=" << action.id
                  << " is ready-to-retry\n";
        changed = true;
      }
      continue;
    }

    if (action.action != "retry-placement") {
      continue;
    }

    auto current_action = FindRolloutActionById(store.LoadRolloutActions(), action.id);
    if (!current_action.has_value()) {
      continue;
    }

    const auto prior_evict_action = FindPriorRolloutActionForWorker(
        store.LoadRolloutActions(),
        *current_action,
        "evict-best-effort");
    if (current_action->status == comet::RolloutActionStatus::Pending &&
        prior_evict_action.has_value() &&
        prior_evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      store.UpdateRolloutActionStatus(
          current_action->id,
          comet::RolloutActionStatus::ReadyToRetry,
          "preceding eviction completed");
      std::cout << "rollout reconcile: retry action id=" << current_action->id
                << " is ready-to-retry\n";
      changed = true;
      current_action = FindRolloutActionById(store.LoadRolloutActions(), action.id);
    }

    if (current_action.has_value() &&
        current_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      std::cout << "rollout reconcile: materializing retry action id="
                << current_action->id << "\n";
      return ApplyReadyRolloutAction(db_path, current_action->id, artifacts_root);
    }
  }

  if (!changed) {
    std::cout << "rollout reconcile: no state changes\n";
  }
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
      PrintRolloutLifecycleEntries(
          BuildRolloutLifecycleEntries(
              *state,
              *generation,
              store.LoadRolloutActions(),
              store.LoadHostAssignments(),
              store.LoadHostObservations()));
    }
  }
  return 0;
}

int SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto previous_override = store.LoadNodeAvailabilityOverride(node_name);
  const auto previous_availability =
      previous_override.has_value() ? previous_override->availability
                                    : comet::NodeAvailability::Active;

  comet::NodeAvailabilityOverride availability_override;
  availability_override.node_name = node_name;
  availability_override.availability = availability;
  availability_override.status_message = status_message.value_or("");
  store.UpsertNodeAvailabilityOverride(availability_override);

  std::cout << "updated node availability for " << node_name << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (desired_state.has_value() && desired_generation.has_value()) {
    if (previous_availability == comet::NodeAvailability::Active &&
        availability != comet::NodeAvailability::Active) {
      const auto drain_assignment = BuildDrainAssignmentForNode(
          *desired_state,
          *desired_generation,
          node_name,
          store.LoadHostAssignments());
      if (drain_assignment.has_value()) {
        store.EnqueueHostAssignments(
            {*drain_assignment},
            "superseded by node drain for desired generation " +
                std::to_string(*desired_generation));
        std::cout << "queued drain assignment for " << node_name
                  << " at desired generation " << *desired_generation << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      }
    }

    if (previous_availability != comet::NodeAvailability::Active &&
        availability == comet::NodeAvailability::Active) {
      const auto resync_assignment = BuildResyncAssignmentForNode(
          *desired_state,
          *desired_generation,
          node_name,
          store.LoadHostAssignments(),
          store.LoadHostObservation(node_name));
      if (resync_assignment.has_value()) {
        store.EnqueueHostAssignments(
            {*resync_assignment},
            "superseded by node reactivation for desired generation " +
                std::to_string(*desired_generation));
        std::cout << "queued resync assignment for " << node_name
                  << " at desired generation " << *desired_generation << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      } else {
        std::cout << "no resync assignment needed for " << node_name << "\n";
      }
    }
  }
  return 0;
}

int RetryHostAssignment(const std::string& db_path, int assignment_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto assignment = store.LoadHostAssignment(assignment_id);
  if (!assignment.has_value()) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) + " not found");
  }
  if (assignment->status != comet::HostAssignmentStatus::Failed) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " is not failed; current status=" + comet::ToString(assignment->status));
  }

  const auto latest_generation = store.LoadDesiredGeneration();
  if (latest_generation.has_value() &&
      assignment->desired_generation != *latest_generation) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " belongs to stale desired generation " +
        std::to_string(assignment->desired_generation) +
        "; latest generation is " + std::to_string(*latest_generation));
  }

  if (!store.RetryFailedHostAssignment(
          assignment_id,
          "requeued by operator for desired generation " +
              std::to_string(assignment->desired_generation))) {
    throw std::runtime_error(
        "failed to requeue host assignment id=" + std::to_string(assignment_id));
  }

  const auto updated_assignment = store.LoadHostAssignment(assignment_id);
  std::cout << "requeued host assignment id=" << assignment_id << "\n";
  if (updated_assignment.has_value()) {
    PrintHostAssignments({*updated_assignment});
  }
  return 0;
}

int ShowState(const std::string& db_path) {
  const auto view = LoadStateAggregateViewData(db_path, DefaultStaleAfterSeconds());
  if (!view.desired_state.has_value()) {
    std::cout << "state: empty\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  PrintStateSummary(*view.desired_state);
  PrintDiskRuntimeStates(view.disk_runtime_states);
  PrintDetailedDiskState(*view.desired_state, view.disk_runtime_states);
  std::cout << comet::RenderSchedulingPolicyReport(view.scheduling_report);
  PrintSchedulerDecisionSummary(*view.desired_state);
  PrintRolloutGateSummary(view.scheduling_report);
  PrintRebalanceControllerGateSummary(view.controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(view.iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(view.loop_status);
  PrintRebalancePlanEntries(view.rebalance_entries);
  PrintRebalancePolicySummary(view.rebalance_policy_summary);
  PrintSchedulerRuntimeView(view.scheduler_runtime);
  if (view.desired_generation.has_value()) {
    PrintRolloutLifecycleEntries(view.rollout_lifecycle);
  }
  std::cout << "\n";
  PrintNodeAvailabilityOverrides(view.availability_overrides);
  std::cout << "\n";
  PrintHostObservations(view.observations, view.stale_after_seconds);
  std::cout << "\n";
  PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      std::nullopt,
      view.stale_after_seconds);
  return 0;
}

int ShowDiskState(const std::string& db_path, const std::optional<std::string>& node_name) {
  const auto view = LoadDiskStateViewData(db_path, node_name);
  if (!view.desired_state.has_value()) {
    std::cout << "disk-state:\n";
    std::cout << "  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.node_name.has_value()) {
    std::cout << "node_filter: " << *view.node_name << "\n";
  }
  PrintDetailedDiskState(*view.desired_state, view.runtime_states, view.node_name);
  return 0;
}

int RenderCompose(const std::string& db_path, const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  return RenderComposeForState(*state, node_name);
}

int RenderInferRuntime(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  std::cout << comet::RenderInferRuntimeConfigJson(*state) << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  const std::string command = argv[1];
  if (command == "show-demo-plan") {
    ShowDemoPlan();
    return 0;
  }

  if (command == "render-demo-compose") {
    return RenderDemoCompose(ParseNodeArg(argc, argv));
  }

  try {
    const auto db_arg = ParseDbArg(argc, argv);
    const auto controller_target = ResolveControllerTarget(ParseControllerArg(argc, argv), db_arg);
    if (controller_target.has_value()) {
      return ExecuteRemoteControllerCommand(
          ParseControllerEndpointTarget(*controller_target),
          command,
          argc,
          argv);
    }

    const std::string db_path = ResolveDbPath(db_arg);

    if (command == "init-db") {
      return InitDb(db_path);
    }

    if (command == "seed-demo") {
      return SeedDemo(db_path);
    }

    if (command == "validate-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return EmitControllerActionResult(ExecuteValidateBundleAction(*bundle_dir));
    }

    if (command == "preview-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecutePreviewBundleAction(*bundle_dir, ParseNodeArg(argc, argv)));
    }

    if (command == "plan-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return PlanBundle(db_path, *bundle_dir);
    }

    if (command == "apply-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteApplyBundleAction(
              db_path,
              *bundle_dir,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "plan-host-ops") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return PlanHostOps(
          db_path,
          *bundle_dir,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          ParseNodeArg(argc, argv));
    }

    if (command == "show-state") {
      return ShowState(db_path);
    }

    if (command == "show-host-assignments") {
      return ShowHostAssignments(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "show-host-observations") {
      return ShowHostObservations(
          db_path,
          ParseNodeArg(argc, argv),
          ParseStaleAfterArg(argc, argv).value_or(DefaultStaleAfterSeconds()));
    }

    if (command == "show-host-health") {
      return ShowHostHealth(
          db_path,
          ParseNodeArg(argc, argv),
          ParseStaleAfterArg(argc, argv).value_or(DefaultStaleAfterSeconds()));
    }

    if (command == "show-disk-state") {
      return ShowDiskState(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "show-rollout-actions") {
      return ShowRolloutActions(
          db_path,
          ParseNodeArg(argc, argv));
    }

    if (command == "show-rebalance-plan") {
      return ShowRebalancePlan(
          db_path,
          ParseNodeArg(argc, argv));
    }

    if (command == "apply-rebalance-proposal") {
      const auto worker_name = ParseWorkerArg(argc, argv);
      if (!worker_name.has_value()) {
        std::cerr << "error: --worker is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteApplyRebalanceProposalAction(
              db_path,
              *worker_name,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "reconcile-rebalance-proposals") {
      return EmitControllerActionResult(
          ExecuteReconcileRebalanceProposalsAction(
              db_path,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "scheduler-tick") {
      return EmitControllerActionResult(
          ExecuteSchedulerTickAction(
              db_path,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "set-rollout-action-status") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      const auto requested_status = ParseStatusArg(argc, argv);
      if (!requested_status.has_value()) {
        std::cerr << "error: --status is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteSetRolloutActionStatusAction(
              db_path,
              *action_id,
              comet::ParseRolloutActionStatus(*requested_status),
              ParseMessageArg(argc, argv)));
    }

    if (command == "enqueue-rollout-eviction") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteEnqueueRolloutEvictionAction(db_path, *action_id));
    }

    if (command == "reconcile-rollout-actions") {
      return EmitControllerActionResult(
          ExecuteReconcileRolloutActionsAction(
              db_path,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "apply-ready-rollout-action") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteApplyReadyRolloutActionAction(
              db_path,
              *action_id,
              ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv))));
    }

    if (command == "show-node-availability") {
      return ShowNodeAvailability(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "set-node-availability") {
      const auto requested_node_name = ParseNodeArg(argc, argv);
      if (!requested_node_name.has_value()) {
        std::cerr << "error: --node is required\n";
        return 1;
      }
      const auto requested_availability = ParseAvailabilityArg(argc, argv);
      if (!requested_availability.has_value()) {
        std::cerr << "error: --availability is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteSetNodeAvailabilityAction(
              db_path,
              *requested_node_name,
              comet::ParseNodeAvailability(*requested_availability),
              ParseMessageArg(argc, argv)));
    }

    if (command == "retry-host-assignment") {
      const auto assignment_id = ParseIdArg(argc, argv);
      if (!assignment_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteRetryHostAssignmentAction(db_path, *assignment_id));
    }

    if (command == "import-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteImportBundleAction(db_path, *bundle_dir));
    }

    if (command == "render-compose") {
      return RenderCompose(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "render-infer-runtime") {
      return RenderInferRuntime(db_path);
    }

    if (command == "serve") {
      return ServeControllerApi(
          db_path,
          ParseListenHostArg(argc, argv).value_or(DefaultListenHost()),
          ParseListenPortArg(argc, argv).value_or(DefaultListenPort()));
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  PrintUsage();
  return 1;
}
