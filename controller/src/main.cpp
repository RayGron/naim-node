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
#include <chrono>
#include <cstdlib>
#include <cctype>
#include <cstdint>
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
#include "comet/crypto_utils.h"
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

int DefaultWebUiPort() {
  return 18081;
}

std::string DefaultUiRoot() {
  return (std::filesystem::path("var") / "ui").string();
}

std::string DefaultWebUiRoot() {
  return (std::filesystem::path("var") / "web-ui").string();
}

std::string DefaultWebUiImage() {
  return "comet/web-ui:dev";
}

std::string DefaultControllerUpstream() {
  return "http://host.docker.internal:18080";
}

std::atomic<bool> g_stop_requested{false};

enum class WebUiComposeMode {
  Skip,
  Exec,
};

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

struct SseStreamRequest {
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<std::string> worker_name;
  std::optional<std::string> category;
  int limit = 100;
  std::optional<int> last_event_id;
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

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name);

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name);

std::map<std::string, comet::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<comet::HostAssignment>& assignments);

int ComputeEffectivePlaneAppliedGeneration(
    const comet::PlaneRecord& plane,
    const std::optional<comet::DesiredState>& desired_state,
    const std::optional<int>& desired_generation,
    const std::vector<comet::HostObservation>& observations);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);
std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation);

std::optional<comet::DiskTelemetrySnapshot> ParseDiskTelemetry(
    const comet::HostObservation& observation);

std::optional<comet::NetworkTelemetrySnapshot> ParseNetworkTelemetry(
    const comet::HostObservation& observation);

std::string SerializeEventPayload(const json& payload);

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload = json::object(),
    const std::string& plane_name = "",
    const std::string& node_name = "",
    const std::string& worker_name = "",
    const std::optional<int>& assignment_id = std::nullopt,
    const std::optional<int>& rollout_action_id = std::nullopt,
    const std::string& severity = "info");

std::string UtcNowSqlTimestamp();

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);

std::string Trim(const std::string& value);

std::string NormalizeLanguageCode(const std::string& value);

HttpResponse BuildJsonResponse(int status_code, const json& payload);

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state);

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds);

void PrintSchedulerDecisionSummary(const comet::DesiredState& state);

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report);

void MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans);

void MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root);

std::vector<comet::HostAssignment> BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report);

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
      << "  comet-controller show-host-observations [--db <path>] [--plane <plane-name>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-host-health [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-disk-state [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-rollout-actions [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-rebalance-plan [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-events [--db <path>] [--plane <plane-name>] [--node <node-name>] [--worker <worker-name>] [--category <category>] [--limit <count>]\n"
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
      << "  comet-controller list-planes [--db <path>]\n"
      << "  comet-controller show-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller start-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller stop-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller delete-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller show-hostd-hosts [--db <path>] [--node <node-name>]\n"
      << "  comet-controller revoke-hostd --node <node-name> [--db <path>] [--message <text>]\n"
      << "  comet-controller rotate-hostd-key --node <node-name> --public-key <base64-or-file> [--db <path>] [--message <text>]\n"
      << "  comet-controller ensure-web-ui [--db <path>] [--web-ui-root <path>] [--listen-port <port>] [--controller-upstream <url>] [--compose-mode skip|exec]\n"
      << "  comet-controller show-web-ui-status [--db <path>] [--web-ui-root <path>]\n"
      << "  comet-controller stop-web-ui [--db <path>] [--web-ui-root <path>] [--compose-mode skip|exec]\n"
      << "  comet-controller show-state [--db <path>]\n"
      << "  comet-controller render-infer-runtime [--db <path>]\n"
      << "  comet-controller render-compose [--db <path>] [--node <node-name>]\n"
      << "  comet-controller serve [--db <path>] [--listen-host <host>] [--listen-port <port>] [--ui-root <path>]\n"
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

std::optional<std::string> ParsePlaneArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--plane" && index + 1 < argc) {
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

std::optional<std::string> ParseUiRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--ui-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseWebUiRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--web-ui-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseControllerUpstreamArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--controller-upstream" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseComposeModeArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--compose-mode" && index + 1 < argc) {
      return std::string(argv[index + 1]);
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

std::optional<int> ParseLimitArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--limit" && index + 1 < argc) {
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

std::optional<std::string> ParseCategoryArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--category" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParsePublicKeyArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--public-key" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::string ReadTextFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to read file '" + path.string() + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string ReadPublicKeyBase64Argument(const std::string& value) {
  const std::filesystem::path candidate(value);
  if (std::filesystem::exists(candidate)) {
    return Trim(ReadTextFile(candidate));
  }
  return value;
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

std::size_t ExpectedRequestBytes(const std::string& request_text) {
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  if (headers_end == std::string::npos) {
    return 0;
  }
  const std::string header_text = request_text.substr(0, headers_end);
  std::size_t offset = 0;
  std::size_t content_length = 0;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = Lowercase(Trim(line.substr(0, colon)));
      const std::string value = Trim(line.substr(colon + 1));
      if (key == "content-length") {
        content_length = static_cast<std::size_t>(std::stoul(value));
        break;
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return headers_end + 4 + content_length;
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

bool SendAll(int fd, const std::string& payload) {
  const char* data = payload.c_str();
  std::size_t remaining = payload.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, data, remaining, 0);
    if (written <= 0) {
      return false;
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

bool SendSseHeaders(int client_fd) {
  std::ostringstream out;
  out << "HTTP/1.1 200 OK\r\n";
  out << "Content-Type: text/event-stream\r\n";
  out << "Cache-Control: no-cache\r\n";
  out << "Connection: keep-alive\r\n";
  out << "X-Accel-Buffering: no\r\n\r\n";
  return SendAll(client_fd, out.str());
}

bool SendSseEventFrame(
    int client_fd,
    int event_id,
    const std::string& event_name,
    const std::string& payload) {
  std::ostringstream frame;
  frame << "id: " << event_id << "\n";
  frame << "event: " << event_name << "\n";
  std::stringstream lines(payload);
  std::string line;
  while (std::getline(lines, line)) {
    frame << "data: " << line << "\n";
  }
  if (!payload.empty() && payload.back() == '\n') {
    frame << "data:\n";
  }
  frame << "\n";
  return SendAll(client_fd, frame.str());
}

bool SendSseCommentFrame(int client_fd, const std::string& message) {
  return SendAll(client_fd, ":" + message + "\n\n");
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

std::optional<std::string> FindHttpHeaderValue(
    const std::string& header_text,
    const std::string& header_name) {
  const std::size_t line_end = header_text.find("\r\n");
  std::size_t offset = line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = Lowercase(Trim(line.substr(0, colon)));
      if (key == Lowercase(header_name)) {
        return Trim(line.substr(colon + 1));
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return std::nullopt;
}

bool DecodeAvailableChunkedHttpBody(
    std::string& encoded,
    std::string* decoded,
    bool* stream_finished) {
  bool progressed = false;
  while (true) {
    const std::size_t line_end = encoded.find("\r\n");
    if (line_end == std::string::npos) {
      return progressed;
    }
    std::string chunk_size_text = encoded.substr(0, line_end);
    const std::size_t extensions = chunk_size_text.find(';');
    if (extensions != std::string::npos) {
      chunk_size_text = chunk_size_text.substr(0, extensions);
    }
    chunk_size_text = Trim(chunk_size_text);
    std::size_t chunk_size = 0;
    try {
      chunk_size = static_cast<std::size_t>(std::stoull(chunk_size_text, nullptr, 16));
    } catch (const std::exception&) {
      throw std::runtime_error("invalid HTTP chunk size '" + chunk_size_text + "'");
    }

    const std::size_t chunk_data_begin = line_end + 2;
    if (chunk_size == 0) {
      if (encoded.size() < chunk_data_begin + 2) {
        return progressed;
      }
      encoded.erase(0, chunk_data_begin + 2);
      *stream_finished = true;
      return true;
    }

    if (encoded.size() < chunk_data_begin + chunk_size + 2) {
      return progressed;
    }
    decoded->append(encoded, chunk_data_begin, chunk_size);
    encoded.erase(0, chunk_data_begin + chunk_size + 2);
    progressed = true;
  }
}

HttpResponse SendControllerHttpRequest(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const std::string& body = "",
    const std::vector<std::pair<std::string, std::string>>& headers = {}) {
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
  for (const auto& [key, value] : headers) {
    request << key << ": " << value << "\r\n";
  }
  if (!body.empty()) {
    if (std::none_of(
            headers.begin(),
            headers.end(),
            [](const auto& header) { return Lowercase(header.first) == "content-type"; })) {
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

struct PlaneInteractionResolution {
  comet::DesiredState desired_state;
  std::optional<comet::PlaneRecord> plane_record;
  std::optional<comet::HostObservation> observation;
  std::optional<comet::RuntimeStatus> runtime_status;
  std::optional<ControllerEndpointTarget> target;
  json status_payload;
};

struct InteractionCompletionPolicy {
  std::string response_mode = "normal";
  int max_tokens = 512;
  std::optional<int> target_completion_tokens;
  int max_continuations = 3;
  int max_total_completion_tokens = 1536;
  int max_elapsed_time_ms = 180000;
  std::string semantic_goal;
  std::string completion_marker = "[[TASK_COMPLETE]]";
  bool require_completion_marker = false;
};

struct ResolvedInteractionPolicy {
  InteractionCompletionPolicy policy;
  std::string mode = "default";
};

struct InteractionSegmentSummary {
  int index = 0;
  int continuation_index = 0;
  std::string text;
  std::string finish_reason = "stop";
  int prompt_tokens = 0;
  int completion_tokens = 0;
  int total_tokens = 0;
  int latency_ms = 0;
  bool marker_seen = false;
};

struct InteractionSessionResult {
  std::string session_id;
  std::string model;
  std::string content;
  std::vector<InteractionSegmentSummary> segments;
  int total_prompt_tokens = 0;
  int total_completion_tokens = 0;
  int total_tokens = 0;
  int total_latency_ms = 0;
  int continuation_count = 0;
  std::string completion_status = "in_progress";
  std::string stop_reason;
  std::string final_finish_reason = "stop";
  bool marker_seen = false;
};

struct CompletionMarkerFilterState {
  std::string pending;
  bool marker_seen = false;
};

struct InteractionSseFrame {
  std::string event_name = "message";
  std::string data;
};

struct StreamedInteractionSegmentResult {
  InteractionSegmentSummary summary;
  std::string cleaned_text;
};

int ClampInteractionPolicyValue(int value, int minimum_value, int maximum_value) {
  return std::max(minimum_value, std::min(value, maximum_value));
}

InteractionCompletionPolicy NormalizeConfiguredInteractionCompletionPolicy(
    const comet::InteractionSettings::CompletionPolicy& configured_policy) {
  InteractionCompletionPolicy policy;
  const std::string normalized_mode =
      NormalizeLanguageCode(configured_policy.response_mode);
  if (!normalized_mode.empty()) {
    policy.response_mode = normalized_mode;
  }
  policy.max_tokens =
      ClampInteractionPolicyValue(configured_policy.max_tokens, 1, 1024);
  if (configured_policy.target_completion_tokens.has_value()) {
    policy.target_completion_tokens = ClampInteractionPolicyValue(
        *configured_policy.target_completion_tokens, 1, 8192);
  }
  policy.max_continuations =
      ClampInteractionPolicyValue(configured_policy.max_continuations, 0, 8);
  policy.max_total_completion_tokens = ClampInteractionPolicyValue(
      configured_policy.max_total_completion_tokens,
      policy.max_tokens,
      8192);
  policy.max_elapsed_time_ms = ClampInteractionPolicyValue(
      configured_policy.max_elapsed_time_ms, 1000, 600000);
  if (configured_policy.semantic_goal.has_value()) {
    policy.semantic_goal = Trim(*configured_policy.semantic_goal);
  }
  if (policy.target_completion_tokens.has_value()) {
    policy.max_total_completion_tokens = std::max(
        policy.max_total_completion_tokens,
        *policy.target_completion_tokens);
  }
  policy.require_completion_marker =
      !policy.semantic_goal.empty() ||
      policy.target_completion_tokens.has_value() ||
      policy.response_mode == "long" ||
      policy.response_mode == "very_long";
  return policy;
}

InteractionCompletionPolicy DefaultChatInteractionCompletionPolicy() {
  comet::InteractionSettings::CompletionPolicy configured_policy;
  configured_policy.response_mode = "normal";
  configured_policy.max_tokens = 512;
  configured_policy.max_continuations = 0;
  configured_policy.max_total_completion_tokens = 512;
  configured_policy.max_elapsed_time_ms = 30000;
  return NormalizeConfiguredInteractionCompletionPolicy(configured_policy);
}

std::string LastUserMessageContent(const json& payload) {
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    return "";
  }
  const auto& messages = payload.at("messages");
  for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
    if ((*it).is_object() &&
        (*it).value("role", std::string{}) == "user" &&
        (*it).contains("content") &&
        (*it).at("content").is_string()) {
      return (*it).at("content").get<std::string>();
    }
  }
  return "";
}

bool ContainsAnySubstring(const std::string& haystack, const std::vector<std::string>& needles) {
  for (const auto& needle : needles) {
    if (!needle.empty() && haystack.find(needle) != std::string::npos) {
      return true;
    }
  }
  return false;
}

bool LooksLikeLongFormTaskRequest(const std::string& text) {
  const std::string normalized = NormalizeLanguageCode(text);
  const std::size_t text_length = normalized.size();
  if (text_length >= 280) {
    return true;
  }
  static const std::vector<std::string> explicit_long_markers = {
      "несколько сообщений", "разбивай на несколько сообщений", "разбей на несколько сообщений",
      "продолжай в нескольких сообщениях", "in several messages", "multiple messages",
      "split across multiple messages", "continue in multiple messages",
      "2048 слов", "1024 слов", "1536 слов", "2048 words", "1024 words", "1536 words",
      "2048 токен", "1024 токен", "2048 token", "1024 token",
  };
  if (ContainsAnySubstring(normalized, explicit_long_markers)) {
    return true;
  }
  static const std::vector<std::string> long_form_keywords = {
      "напиши историю", "напиши рассказ", "напиши эссе", "напиши статью",
      "подробный план", "подробно опиши", "развернуто опиши", "детально опиши",
      "историю", "рассказ", "эссе", "статью", "гайд", "руководство",
      "write a story", "write an essay", "write an article", "write a guide",
      "detailed plan", "detailed analysis", "long-form", "long form",
  };
  return ContainsAnySubstring(normalized, long_form_keywords);
}

ResolvedInteractionPolicy ResolveInteractionCompletionPolicy(
    const comet::DesiredState& desired_state,
    const json& payload) {
  ResolvedInteractionPolicy resolved;
  const std::string last_user_message = LastUserMessageContent(payload);
  const bool long_form_task = LooksLikeLongFormTaskRequest(last_user_message);
  if (desired_state.interaction.has_value()) {
    const auto& interaction = *desired_state.interaction;
    if (long_form_task && interaction.long_completion_policy.has_value()) {
      resolved.policy =
          NormalizeConfiguredInteractionCompletionPolicy(*interaction.long_completion_policy);
      resolved.mode = "long";
      return resolved;
    }
    if (!long_form_task &&
        interaction.completion_policy.has_value() &&
        NormalizeLanguageCode(interaction.completion_policy->response_mode) != "long" &&
        NormalizeLanguageCode(interaction.completion_policy->response_mode) != "very_long") {
      resolved.policy =
          NormalizeConfiguredInteractionCompletionPolicy(*interaction.completion_policy);
      resolved.mode = "default";
      return resolved;
    }
    if (long_form_task &&
        interaction.completion_policy.has_value() &&
        (NormalizeLanguageCode(interaction.completion_policy->response_mode) == "long" ||
         NormalizeLanguageCode(interaction.completion_policy->response_mode) == "very_long")) {
      resolved.policy =
          NormalizeConfiguredInteractionCompletionPolicy(*interaction.completion_policy);
      resolved.mode = "long";
      return resolved;
    }
  }
  resolved.policy = DefaultChatInteractionCompletionPolicy();
  resolved.mode = long_form_task ? "long-fallback" : "default-fallback";
  return resolved;
}

std::string GenerateInteractionSessionId() {
  static std::atomic<unsigned long long> counter{0};
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return "sess-" + std::to_string(now) + "-" + std::to_string(++counter);
}

std::string BuildSemanticCompletionInstruction(const InteractionCompletionPolicy& policy) {
  std::ostringstream instruction;
  instruction << "Semantic completion protocol:\n"
              << "- You may need multiple assistant segments to finish this task.\n"
              << "- End the final segment with the exact marker " << policy.completion_marker
              << " on its own line only when the task is fully complete.\n"
              << "- If the task is not complete in this segment, do not output the marker.\n"
              << "- For continuation segments, continue exactly where you stopped without repeating prior text unless recap is explicitly requested.\n"
              << "- Do not emit tool calls, tool requests, or waiting states in this phase.\n";
  if (!policy.semantic_goal.empty()) {
    instruction << "- Task completion goal: " << policy.semantic_goal << "\n";
  }
  if (policy.target_completion_tokens.has_value()) {
    instruction << "- Aim for at least " << *policy.target_completion_tokens
                << " completion tokens before marking the task complete.\n";
  }
  return instruction.str();
}

std::string BuildContinuationPrompt(
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    const std::string& trailing_excerpt = "") {
  std::ostringstream prompt;
  if (natural_stop_without_marker) {
    prompt << "Your previous segment stopped before you proved the task was complete. "
           << "If the task is already complete, reply with only " << policy.completion_marker
           << ". Otherwise continue exactly where you stopped.";
  } else {
    prompt << "Continue exactly where you stopped.";
  }
  if (!trailing_excerpt.empty()) {
    prompt << " The last visible excerpt from your previous segment was:\n"
           << trailing_excerpt
           << "\nContinue immediately after that excerpt.";
  }
  prompt << " Do not repeat prior text. Emit " << policy.completion_marker
         << " on its own line only in the final segment when the task is fully complete.";
  return prompt.str();
}

bool IsUtf8ContinuationByte(unsigned char value);

std::string Utf8SafeSuffix(const std::string& value, std::size_t max_bytes) {
  if (value.size() <= max_bytes) {
    return value;
  }
  std::size_t start = value.size() - max_bytes;
  while (start < value.size() &&
         IsUtf8ContinuationByte(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  return value.substr(start);
}

bool SessionReachedTargetLength(
    const InteractionCompletionPolicy& policy,
    int total_completion_tokens) {
  return !policy.target_completion_tokens.has_value() ||
         total_completion_tokens >= *policy.target_completion_tokens;
}

bool CanCompleteOnNaturalStop(
    const InteractionCompletionPolicy& policy,
    const InteractionSegmentSummary& summary) {
  if (policy.require_completion_marker) {
    return false;
  }
  return summary.finish_reason != "length" && !Trim(summary.text).empty();
}

std::string RemoveCompletionMarkers(
    const std::string& input,
    const std::string& marker,
    bool* marker_seen) {
  std::string output = input;
  std::size_t position = std::string::npos;
  while ((position = output.find(marker)) != std::string::npos) {
    if (marker_seen != nullptr) {
      *marker_seen = true;
    }
    output.erase(position, marker.size());
  }
  return output;
}

bool IsUtf8ContinuationByte(unsigned char value) {
  return (value & 0xC0) == 0x80;
}

std::size_t Utf8SequenceLength(unsigned char lead) {
  if ((lead & 0x80) == 0) {
    return 1;
  }
  if ((lead & 0xE0) == 0xC0) {
    return 2;
  }
  if ((lead & 0xF0) == 0xE0) {
    return 3;
  }
  if ((lead & 0xF8) == 0xF0) {
    return 4;
  }
  return 0;
}

std::size_t ValidUtf8PrefixLength(const std::string& value) {
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0) {
      break;
    }
    if (index + sequence_length > value.size()) {
      break;
    }
    bool valid = true;
    for (std::size_t offset = 1; offset < sequence_length; ++offset) {
      if (!IsUtf8ContinuationByte(static_cast<unsigned char>(value[index + offset]))) {
        valid = false;
        break;
      }
    }
    if (!valid) {
      break;
    }
    index += sequence_length;
  }
  return index;
}

std::string ConsumeCompletionMarkerFilteredChunk(
    CompletionMarkerFilterState& state,
    const std::string& chunk,
    const std::string& marker,
    bool final_flush) {
  state.pending += chunk;
  std::string emitted;
  while (true) {
    const std::size_t marker_pos = state.pending.find(marker);
    if (marker_pos != std::string::npos) {
      emitted += state.pending.substr(0, marker_pos);
      state.pending.erase(0, marker_pos + marker.size());
      state.marker_seen = true;
      continue;
    }
    if (final_flush) {
      emitted += state.pending;
      state.pending.clear();
      break;
    }
    if (state.pending.size() > marker.size()) {
      const std::size_t safe_prefix = state.pending.size() - marker.size() + 1;
      const std::string candidate = state.pending.substr(0, safe_prefix);
      const std::size_t valid_prefix = ValidUtf8PrefixLength(candidate);
      if (valid_prefix == 0) {
        break;
      }
      emitted += state.pending.substr(0, valid_prefix);
      state.pending.erase(0, valid_prefix);
    }
    break;
  }
  return emitted;
}

bool TryConsumeSseFrame(std::string& buffer, InteractionSseFrame* frame) {
  const std::size_t separator = buffer.find("\n\n");
  if (separator == std::string::npos) {
    return false;
  }
  const std::string raw_frame = buffer.substr(0, separator);
  buffer.erase(0, separator + 2);
  frame->event_name = "message";
  frame->data.clear();
  std::stringstream stream(raw_frame);
  std::string line;
  std::vector<std::string> data_lines;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const std::string trimmed_line = Trim(line);
    if (line.empty() || line[0] == ':') {
      continue;
    }
    if (!trimmed_line.empty()) {
      const std::size_t extensions = trimmed_line.find(';');
      const std::string chunk_size_candidate =
          Trim(trimmed_line.substr(0, extensions == std::string::npos ? trimmed_line.size()
                                                                      : extensions));
      const bool looks_like_chunk_size =
          !chunk_size_candidate.empty() &&
          std::all_of(
              chunk_size_candidate.begin(),
              chunk_size_candidate.end(),
              [](unsigned char ch) { return std::isxdigit(ch) != 0; });
      if (looks_like_chunk_size) {
        continue;
      }
    }
    if (line.rfind("event:", 0) == 0) {
      frame->event_name = Trim(line.substr(6));
      continue;
    }
    if (line.rfind("data:", 0) == 0) {
      const std::size_t offset = line.size() > 5 && line[5] == ' ' ? 6 : 5;
      data_lines.push_back(line.substr(offset));
    }
  }
  for (std::size_t index = 0; index < data_lines.size(); ++index) {
    if (index > 0) {
      frame->data.push_back('\n');
    }
    frame->data += data_lines[index];
  }
  return true;
}

std::string NormalizeLanguageCode(const std::string& value) {
  std::string normalized;
  normalized.reserve(value.size());
  for (unsigned char ch : value) {
    if (ch == '-') {
      normalized.push_back('_');
    } else {
      normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
  }
  return normalized;
}

std::string LanguageLabel(const std::string& code) {
  const std::string normalized = NormalizeLanguageCode(code);
  if (normalized == "ru") {
    return "Russian";
  }
  if (normalized == "en") {
    return "English";
  }
  if (normalized == "uk" || normalized == "uk_ua") {
    return "Ukrainian";
  }
  if (normalized == "de" || normalized == "de_de") {
    return "German";
  }
  return code.empty() ? std::string("English") : code;
}

std::optional<std::string> ResolveInteractionPreferredLanguage(
    const comet::DesiredState& desired_state,
    const json& payload) {
  if (payload.contains("preferred_language") &&
      payload.at("preferred_language").is_string()) {
    const std::string preferred = payload.at("preferred_language").get<std::string>();
    if (!preferred.empty()) {
      return NormalizeLanguageCode(preferred);
    }
  }
  if (desired_state.interaction.has_value() &&
      !desired_state.interaction->default_response_language.empty()) {
    return NormalizeLanguageCode(desired_state.interaction->default_response_language);
  }
  return std::nullopt;
}

std::string BuildLanguageInstruction(
    const comet::DesiredState& desired_state,
    const std::optional<std::string>& preferred_language) {
  const std::string no_reasoning_instruction =
      " Do not output chain-of-thought, hidden reasoning, analysis traces, or <think> blocks. Output only the final user-facing answer.";
  if (preferred_language.has_value() && !preferred_language->empty()) {
    return "Response language requirement: Reply in " + LanguageLabel(*preferred_language) +
           ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
           no_reasoning_instruction;
  }
  if (desired_state.interaction.has_value()) {
    if (desired_state.interaction->follow_user_language) {
      return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
    if (!desired_state.interaction->default_response_language.empty()) {
      return "Response language requirement: Reply in " +
             LanguageLabel(desired_state.interaction->default_response_language) +
             ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
  }
  return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
         no_reasoning_instruction;
}

std::string BuildInteractionUpstreamBody(
    const PlaneInteractionResolution& resolution,
    json payload,
    bool force_stream,
    const InteractionCompletionPolicy& policy) {
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    payload["messages"] = json::array();
  }
  const auto preferred_language =
      ResolveInteractionPreferredLanguage(resolution.desired_state, payload);

  std::vector<std::string> system_instruction_parts;
  if (resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->system_prompt.has_value() &&
      !resolution.desired_state.interaction->system_prompt->empty()) {
    system_instruction_parts.push_back(*resolution.desired_state.interaction->system_prompt);
  }
  system_instruction_parts.push_back(
      BuildLanguageInstruction(resolution.desired_state, preferred_language));
  if (policy.require_completion_marker || policy.max_continuations > 0) {
    system_instruction_parts.push_back(BuildSemanticCompletionInstruction(policy));
  }

  json merged_messages = json::array();
  std::string combined_system_instruction;
  for (const auto& part : system_instruction_parts) {
    if (part.empty()) {
      continue;
    }
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction += part;
  }
  if (!combined_system_instruction.empty()) {
    merged_messages.push_back(json{
        {"role", "system"},
        {"content", combined_system_instruction},
    });
  }
  for (const auto& message : payload.at("messages")) {
    merged_messages.push_back(message);
  }
  payload["messages"] = merged_messages;

  if (preferred_language.has_value()) {
    payload["preferred_language"] = *preferred_language;
  }
  payload.erase("max_completion_tokens");
  payload.erase("target_completion_tokens");
  payload.erase("max_continuations");
  payload.erase("max_total_completion_tokens");
  payload.erase("max_elapsed_time_ms");
  payload.erase("semantic_goal");
  if (force_stream) {
    payload["stream"] = true;
  }
  payload["max_tokens"] = policy.max_tokens;
  if (!payload.contains("temperature")) {
    payload["temperature"] = 0.2;
  }
  if (!payload.contains("top_p")) {
    payload["top_p"] = 0.8;
  }
  payload["response_mode"] = policy.response_mode;
  return payload.dump();
}

std::string NormalizeInteractionHost(const std::string& host) {
  if (host.empty() || host == "0.0.0.0" || host == "::" || host == "[::]") {
    return "127.0.0.1";
  }
  return host;
}

std::optional<ControllerEndpointTarget> ParseInteractionTarget(
    const std::string& gateway_listen,
    int fallback_port) {
  std::string host = "127.0.0.1";
  int port = fallback_port;
  if (!gateway_listen.empty()) {
    const std::size_t colon = gateway_listen.rfind(':');
    if (colon != std::string::npos) {
      host = NormalizeInteractionHost(gateway_listen.substr(0, colon));
      port = std::stoi(gateway_listen.substr(colon + 1));
    }
  }
  if (port <= 0) {
    return std::nullopt;
  }
  return ParseControllerEndpointTarget(host + ":" + std::to_string(port));
}

std::string CurrentControllerPlatform() {
#if defined(_WIN32)
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

const comet::NodeInventory* FindPlaneNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return &node;
    }
  }
  return nullptr;
}

bool PlaneNodeUsesGpuRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& runtime_gpu_node : desired_state.runtime_gpu_nodes) {
    if (runtime_gpu_node.enabled && runtime_gpu_node.node_name == node_name) {
      return true;
    }
  }
  for (const auto& instance : desired_state.instances) {
    if (instance.node_name == node_name && instance.gpu_device.has_value() &&
        !instance.gpu_device->empty()) {
      return true;
    }
  }
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->gpu_devices.empty()) {
    return true;
  }
  return false;
}

std::optional<std::string> DescribeUnsupportedControllerLocalRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  if (node_name != "local-hostd" && node_name != "controller-local") {
    return std::nullopt;
  }

  const std::string controller_platform = CurrentControllerPlatform();
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->platform.empty() && node->platform != controller_platform) {
    return "Local host '" + node_name + "' is running on '" + controller_platform +
           "', but the plane targets platform '" + node->platform + "'";
  }

  if (controller_platform == "macos" && PlaneNodeUsesGpuRuntime(desired_state, node_name)) {
    return "Local host '" + node_name +
           "' is running on macOS, but this plane requires Linux/NVIDIA GPU runtime";
  }

  return std::nullopt;
}

void ValidateDesiredStateForControllerAdmission(const comet::DesiredState& desired_state) {
  for (const auto& node : desired_state.nodes) {
    if (const auto detail =
            DescribeUnsupportedControllerLocalRuntime(desired_state, node.name);
        detail.has_value()) {
      throw std::invalid_argument(*detail);
    }
  }
}

PlaneInteractionResolution ResolvePlaneInteraction(
    const std::string& db_path,
    const std::string& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  PlaneInteractionResolution resolution;
  resolution.desired_state = *desired_state;
  resolution.plane_record = store.LoadPlane(plane_name);
  const std::string primary_node = desired_state->inference.primary_infer_node;
  bool observation_matches_plane = false;
  if (!primary_node.empty()) {
    resolution.observation = store.LoadHostObservation(primary_node);
    observation_matches_plane =
        resolution.observation.has_value() &&
        ObservationMatchesPlane(*resolution.observation, plane_name);
    if (observation_matches_plane &&
        !resolution.observation->runtime_status_json.empty()) {
      resolution.runtime_status =
          comet::DeserializeRuntimeStatusJson(resolution.observation->runtime_status_json);
      resolution.target = ParseInteractionTarget(
          resolution.runtime_status->gateway_listen,
          desired_state->gateway.listen_port);
    }
  }

  const bool llm_plane = desired_state->plane_mode == comet::PlaneMode::Llm;
  const bool running_plane =
      resolution.plane_record.has_value() && resolution.plane_record->state == "running";
  const bool observation_ready = observation_matches_plane;
  const auto local_runtime_blocker =
      DescribeUnsupportedControllerLocalRuntime(*desired_state, primary_node);
  const bool runtime_ready =
      resolution.runtime_status.has_value() &&
      resolution.runtime_status->active_model_ready &&
      resolution.runtime_status->inference_ready &&
      resolution.runtime_status->gateway_ready &&
      resolution.runtime_status->launch_ready;
  std::string reason = "ready";
  if (!llm_plane) {
    reason = "plane_mode_compute";
  } else if (!running_plane) {
    reason = "plane_not_running";
  } else if (local_runtime_blocker.has_value()) {
    reason = "unsupported_local_runtime";
  } else if (!observation_ready) {
    reason = "no_observation";
  } else if (resolution.observation->status == comet::HostObservationStatus::Failed) {
    reason = "runtime_start_failed";
  } else if (!resolution.runtime_status.has_value()) {
    reason = "runtime_status_missing";
  } else if (!resolution.runtime_status->active_model_ready) {
    reason = "active_model_missing";
  } else if (!resolution.runtime_status->gateway_ready) {
    reason = "gateway_not_ready";
  } else if (!resolution.runtime_status->inference_ready) {
    reason = "inference_not_ready";
  } else if (!resolution.target.has_value()) {
    reason = "gateway_target_missing";
  }

  resolution.status_payload = json{
      {"plane_name", plane_name},
      {"plane_mode", comet::ToString(desired_state->plane_mode)},
      {"interaction_enabled", llm_plane},
      {"ready", llm_plane && running_plane && observation_ready && runtime_ready &&
                    resolution.target.has_value()},
      {"reason", reason},
      {"plane_state",
       resolution.plane_record.has_value() ? json(resolution.plane_record->state) : json(nullptr)},
      {"primary_infer_node",
       primary_node.empty() ? json(nullptr) : json(primary_node)},
      {"active_model_id",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->active_model_id.empty()
           ? json(resolution.runtime_status->active_model_id)
           : json(nullptr)},
      {"served_model_name",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->active_served_model_name.empty()
           ? json(resolution.runtime_status->active_served_model_name)
           : json(nullptr)},
      {"default_response_language",
       resolution.desired_state.interaction.has_value()
           ? json(resolution.desired_state.interaction->default_response_language)
           : json(nullptr)},
      {"supported_response_languages",
       resolution.desired_state.interaction.has_value()
           ? json(resolution.desired_state.interaction->supported_response_languages)
           : json(json::array())},
      {"follow_user_language",
       resolution.desired_state.interaction.has_value()
           ? json(resolution.desired_state.interaction->follow_user_language)
           : json(true)},
      {"completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->completion_policy.has_value()
           ? json{
                 {"response_mode",
                  resolution.desired_state.interaction->completion_policy->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction->completion_policy->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction->completion_policy
                          ->target_completion_tokens.has_value()
                      ? json(*resolution.desired_state.interaction->completion_policy
                                  ->target_completion_tokens)
                      : json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction->completion_policy->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction->completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction->completion_policy
                      ->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction->completion_policy->semantic_goal
                          .has_value()
                      ? json(*resolution.desired_state.interaction->completion_policy
                                  ->semantic_goal)
                      : json(nullptr)},
             }
           : json(nullptr)},
      {"long_completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->long_completion_policy.has_value()
           ? json{
                 {"response_mode",
                  resolution.desired_state.interaction->long_completion_policy->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction->long_completion_policy->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction->long_completion_policy
                          ->target_completion_tokens.has_value()
                      ? json(*resolution.desired_state.interaction->long_completion_policy
                                  ->target_completion_tokens)
                      : json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction->long_completion_policy->semantic_goal
                          .has_value()
                      ? json(*resolution.desired_state.interaction->long_completion_policy
                                  ->semantic_goal)
                      : json(nullptr)},
             }
           : json(nullptr)},
      {"gateway_listen",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->gateway_listen.empty()
           ? json(resolution.runtime_status->gateway_listen)
           : json(nullptr)},
      {"gateway_target",
       resolution.target.has_value()
           ? json(resolution.target->host + ":" + std::to_string(resolution.target->port))
           : json(nullptr)},
      {"runtime_status",
       resolution.runtime_status.has_value()
           ? json::parse(comet::SerializeRuntimeStatusJson(*resolution.runtime_status))
           : json(nullptr)},
      {"failure_detail",
       reason == "unsupported_local_runtime"
           ? json(*local_runtime_blocker)
           : (reason == "runtime_start_failed" && resolution.observation.has_value() &&
                      !resolution.observation->status_message.empty()
                  ? json(resolution.observation->status_message)
                  : json(nullptr))},
  };
  return resolution;
}

HttpResponse BuildPlaneInteractionError(
    int status_code,
    const json& status_payload,
    const std::string& message) {
  json payload = status_payload;
  payload["status"] = "error";
  payload["message"] = message;
  return HttpResponse{status_code, "application/json", payload.dump()};
}

json ParseInteractionPayload(const std::string& body) {
  return body.empty() ? json::object() : json::parse(body);
}

std::string RemoveThinkBlocks(std::string value) {
  while (true) {
    const std::size_t begin = value.find("<think>");
    if (begin == std::string::npos) {
      return value;
    }
    const std::size_t end = value.find("</think>", begin);
    if (end == std::string::npos) {
      return value.substr(0, begin);
    }
    value.erase(begin, end + std::string("</think>").size() - begin);
  }
}

std::vector<std::string> SplitParagraphs(const std::string& value) {
  std::vector<std::string> paragraphs;
  std::string current;
  bool last_blank = false;
  std::istringstream input(value);
  std::string line;
  while (std::getline(input, line)) {
    const bool blank = Trim(line).empty();
    if (blank) {
      if (!current.empty()) {
        paragraphs.push_back(Trim(current));
        current.clear();
      }
      last_blank = true;
      continue;
    }
    if (!current.empty()) {
      current += last_blank ? "\n" : "\n";
    }
    current += line;
    last_blank = false;
  }
  if (!current.empty()) {
    paragraphs.push_back(Trim(current));
  }
  return paragraphs;
}

bool StartsWithReasoningPreamble(const std::string& text) {
  const std::string lowered = Lowercase(Trim(text));
  return lowered.rfind("thinking process:", 0) == 0 || lowered.rfind("reasoning:", 0) == 0 ||
         lowered.rfind("analysis:", 0) == 0 || lowered.rfind("chain of thought:", 0) == 0;
}

std::string SanitizeInteractionText(std::string text) {
  text = RemoveThinkBlocks(std::move(text));
  text = Trim(text);
  if (StartsWithReasoningPreamble(text)) {
    const auto paragraphs = SplitParagraphs(text);
    for (auto it = paragraphs.rbegin(); it != paragraphs.rend(); ++it) {
      const std::string candidate = Trim(*it);
      if (candidate.empty()) {
        continue;
      }
      const std::string lowered = Lowercase(candidate);
      if (StartsWithReasoningPreamble(candidate) || lowered.rfind("1.", 0) == 0 ||
          lowered.rfind("2.", 0) == 0 || lowered.rfind("3.", 0) == 0 ||
          lowered.rfind("* ", 0) == 0) {
        continue;
      }
      return candidate;
    }
  }
  return text;
}

std::string ExtractInteractionText(const json& payload) {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    throw std::runtime_error("upstream interaction response did not include choices");
  }
  const json& choice = payload.at("choices").at(0);
  if (choice.contains("message") && choice.at("message").is_object()) {
    return SanitizeInteractionText(choice.at("message").value("content", std::string{}));
  }
  return SanitizeInteractionText(choice.value("text", std::string{}));
}

std::string ExtractInteractionFinishReason(const json& payload) {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    return "stop";
  }
  const json& choice = payload.at("choices").at(0);
  return choice.value("finish_reason", std::string{"stop"});
}

json ExtractInteractionUsage(const json& payload) {
  if (!payload.contains("usage") || !payload.at("usage").is_object()) {
    return json{
        {"prompt_tokens", 0},
        {"completion_tokens", 0},
        {"total_tokens", 0},
    };
  }
  const json& usage = payload.at("usage");
  return json{
      {"prompt_tokens", usage.value("prompt_tokens", 0)},
      {"completion_tokens", usage.value("completion_tokens", 0)},
      {"total_tokens", usage.value("total_tokens", 0)},
  };
}

json BuildContinuationPayload(
    const json& original_payload,
    const std::string& accumulated_text,
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker) {
  json payload = original_payload;
  json messages = json::array();
  if (payload.contains("messages") && payload.at("messages").is_array()) {
    for (const auto& message : payload.at("messages")) {
      messages.push_back(message);
    }
  }
  const std::string trailing_excerpt =
      accumulated_text.empty() ? std::string{} : Utf8SafeSuffix(accumulated_text, 256);
  messages.push_back(json{
      {"role", "user"},
      {"content", BuildContinuationPrompt(policy, natural_stop_without_marker, trailing_excerpt)},
  });
  payload["messages"] = messages;
  return payload;
}

json BuildInteractionSessionPayload(const InteractionSessionResult& result) {
  json segments = json::array();
  for (const auto& segment : result.segments) {
    segments.push_back(json{
        {"index", segment.index},
        {"continuation_index", segment.continuation_index},
        {"finish_reason", segment.finish_reason},
        {"usage",
         json{
             {"prompt_tokens", segment.prompt_tokens},
             {"completion_tokens", segment.completion_tokens},
             {"total_tokens", segment.total_tokens},
         }},
        {"latency_ms", segment.latency_ms},
        {"marker_seen", segment.marker_seen},
    });
  }
  return json{
      {"id", result.session_id},
      {"status", result.completion_status},
      {"stop_reason", result.stop_reason},
      {"segment_count", static_cast<int>(result.segments.size())},
      {"continuation_count", result.continuation_count},
      {"finish_reason", result.final_finish_reason},
      {"usage",
       json{
           {"prompt_tokens", result.total_prompt_tokens},
           {"completion_tokens", result.total_completion_tokens},
           {"total_tokens", result.total_tokens},
       }},
      {"latency_ms", result.total_latency_ms},
      {"marker_seen", result.marker_seen},
      {"segments", std::move(segments)},
  };
}

HttpResponse BuildInteractionSessionResponse(const InteractionSessionResult& result) {
  const json session_payload = BuildInteractionSessionPayload(result);
  return BuildJsonResponse(
      200,
      json{
          {"id", "chatcmpl-comet-session"},
          {"object", "chat.completion"},
          {"model", result.model},
          {"choices",
           json::array({json{
               {"index", 0},
               {"message", json{{"role", "assistant"}, {"content", result.content}}},
               {"finish_reason", result.completion_status == "completed" ? "stop" : "length"},
           }})},
          {"usage", session_payload.at("usage")},
          {"session", session_payload},
      });
}

InteractionSessionResult ExecuteInteractionSession(
    const PlaneInteractionResolution& resolution,
    const std::string& body) {
  const json original_payload = ParseInteractionPayload(body);
  const InteractionCompletionPolicy policy =
      ResolveInteractionCompletionPolicy(resolution.desired_state, original_payload).policy;
  InteractionSessionResult result;
  result.session_id = GenerateInteractionSessionId();
  const auto session_started_at = std::chrono::steady_clock::now();

  json current_payload = original_payload;
  for (int segment_index = 0;; ++segment_index) {
    const auto segment_started_at = std::chrono::steady_clock::now();
    const HttpResponse upstream = SendControllerHttpRequest(
        *resolution.target,
        "POST",
        "/v1/chat/completions",
        BuildInteractionUpstreamBody(resolution, current_payload, false, policy),
        {{"Accept", "application/json"}});
    if (upstream.status_code != 200) {
      throw std::runtime_error(
          "upstream interaction request failed with status " + std::to_string(upstream.status_code));
    }
    const json upstream_payload = upstream.body.empty() ? json::object() : json::parse(upstream.body);
    const auto segment_finished_at = std::chrono::steady_clock::now();
    const json usage = ExtractInteractionUsage(upstream_payload);
    bool marker_seen_in_segment = false;
    const std::string clean_text = RemoveCompletionMarkers(
        ExtractInteractionText(upstream_payload),
        policy.completion_marker,
        &marker_seen_in_segment);
    InteractionSegmentSummary summary;
    summary.index = segment_index;
    summary.continuation_index = segment_index;
    summary.text = clean_text;
    summary.finish_reason = ExtractInteractionFinishReason(upstream_payload);
    summary.prompt_tokens = usage.value("prompt_tokens", 0);
    summary.completion_tokens = usage.value("completion_tokens", 0);
    summary.total_tokens = usage.value("total_tokens", 0);
    summary.latency_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            segment_finished_at - segment_started_at)
            .count());
    summary.marker_seen = marker_seen_in_segment;
    result.model = upstream_payload.value("model", result.model);
    result.content += clean_text;
    result.segments.push_back(summary);
    result.total_prompt_tokens += summary.prompt_tokens;
    result.total_completion_tokens += summary.completion_tokens;
    result.total_tokens += summary.total_tokens;
    result.total_latency_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            segment_finished_at - session_started_at)
            .count());
    result.final_finish_reason = summary.finish_reason;
    result.marker_seen = result.marker_seen || marker_seen_in_segment;

    if (result.marker_seen &&
        SessionReachedTargetLength(policy, result.total_completion_tokens)) {
      result.completion_status = "completed";
      result.stop_reason = "semantic_completion_marker";
      break;
    }
    if (CanCompleteOnNaturalStop(policy, summary) &&
        SessionReachedTargetLength(policy, result.total_completion_tokens)) {
      result.completion_status = "completed";
      result.stop_reason = "natural_stop";
      break;
    }

    if (result.total_completion_tokens >= policy.max_total_completion_tokens) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_total_completion_tokens_reached";
      break;
    }
    if (result.total_latency_ms >= policy.max_elapsed_time_ms) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_elapsed_time_ms_reached";
      break;
    }
    if (segment_index >= policy.max_continuations) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_continuations_reached";
      break;
    }

    result.continuation_count = segment_index + 1;
    current_payload = BuildContinuationPayload(
        original_payload,
        result.content,
        policy,
        summary.finish_reason != "length");
  }

  if (result.completion_status == "in_progress") {
    result.completion_status = "failed";
    result.stop_reason = "session_state_unresolved";
  }
  return result;
}

bool SendInteractionSseEvent(
    int client_fd,
    const std::string& event_name,
    const json& payload) {
  std::ostringstream frame;
  frame << "event: " << event_name << "\n";
  std::stringstream lines(payload.dump().append("\n"));
  std::string line;
  while (std::getline(lines, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    frame << "data: " << line << "\n";
  }
  frame << "\n";
  return SendAll(client_fd, frame.str());
}

bool SendInteractionSseDone(int client_fd) {
  return SendAll(client_fd, "data: [DONE]\n\n");
}

HttpResponse ProxyInteractionJson(
    const PlaneInteractionResolution& resolution,
    const std::string& method,
    const std::string& path,
    const std::string& body = "") {
  if (!resolution.status_payload.value("interaction_enabled", false)) {
    return BuildPlaneInteractionError(
        409,
        resolution.status_payload,
        "interaction is available only for plane_mode=llm");
  }
  if (!resolution.status_payload.value("ready", false) || !resolution.target.has_value()) {
    return BuildPlaneInteractionError(
        409,
        resolution.status_payload,
        "plane interaction target is not ready");
  }
  try {
    const std::string upstream_body =
        method == "POST"
            ? BuildInteractionUpstreamBody(
                  resolution,
                  ParseInteractionPayload(body),
                  false,
                  ResolveInteractionCompletionPolicy(
                      resolution.desired_state,
                      ParseInteractionPayload(body))
                      .policy)
            : body;
    const HttpResponse upstream = SendControllerHttpRequest(
        *resolution.target,
        method,
        path,
        upstream_body,
        {{"Accept", "application/json"}});
    return upstream;
  } catch (const std::exception& error) {
    return BuildPlaneInteractionError(502, resolution.status_payload, error.what());
  }
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
    const auto planes = store.LoadPlanes();
    payload["store_ready"] = true;
    payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
    payload["plane_name"] =
        desired_state.has_value() ? json(desired_state->plane_name) : json(nullptr);
    payload["plane_count"] = planes.size();
  } catch (const std::exception& error) {
    payload["store_ready"] = false;
    payload["error"] = error.what();
  }

  return payload;
}

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name);

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name);

json BuildControllerStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name = std::nullopt) {
  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
  };

  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto planes = store.LoadPlanes();
  const auto generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  const auto desired_states =
      plane_name.has_value()
          ? std::vector<comet::DesiredState>{}
          : store.LoadDesiredStates();
  json plane_items = json::array();
  for (const auto& plane : planes) {
    plane_items.push_back(json{
        {"name", plane.name},
        {"plane_mode", plane.plane_mode},
        {"generation", plane.generation},
        {"applied_generation", plane.applied_generation},
        {"staged_update", plane.generation > plane.applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"state", plane.state},
        {"created_at", plane.created_at},
    });
  }
  payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
  payload["planes"] = std::move(plane_items);
  if (plane_name.has_value()) {
    payload["desired_states"] = json::array();
  } else {
    json desired_state_items = json::array();
    for (const auto& state : desired_states) {
      desired_state_items.push_back(json::parse(comet::SerializeDesiredStateJson(state)));
    }
    payload["desired_states"] = std::move(desired_state_items);
  }
  payload["desired_state"] =
      desired_state.has_value()
          ? json::parse(comet::SerializeDesiredStateJson(*desired_state))
          : json(nullptr);
  return payload;
}

json BuildPlanesPayload(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& plane : store.LoadPlanes()) {
    if (plane.state != "deleting") {
      continue;
    }
    if (!CanFinalizeDeletedPlane(store, plane.name)) {
      continue;
    }
    store.DeletePlane(plane.name);
    AppendControllerEvent(
        store,
        "plane",
        "deleted",
        "plane deleted from controller registry after cleanup convergence",
        json{
            {"plane_name", plane.name},
            {"deleted_generation", plane.generation},
        },
        "");
  }

  json items = json::array();
  for (const auto& plane : store.LoadPlanes()) {
    const auto desired_state = store.LoadDesiredState(plane.name);
    const auto desired_generation = store.LoadDesiredGeneration(plane.name);
    const auto observations =
        FilterHostObservationsForPlane(store.LoadHostObservations(), plane.name);
    const auto assignments = store.LoadHostAssignments(std::nullopt, std::nullopt, plane.name);
    const int effective_applied_generation = ComputeEffectivePlaneAppliedGeneration(
        plane,
        desired_state,
        desired_generation,
        observations);
    if (effective_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_applied_generation);
    }
    const auto latest_assignments_by_node = BuildLatestPlaneAssignmentsByNode(assignments);
    int failed_assignments = 0;
    int in_flight_assignments = 0;
    for (const auto& [node_name, assignment] : latest_assignments_by_node) {
      (void)node_name;
      if (assignment.status == comet::HostAssignmentStatus::Failed) {
        ++failed_assignments;
      } else if (
          assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed) {
        ++in_flight_assignments;
      }
    }
    items.push_back(json{
        {"name", plane.name},
        {"state", plane.state},
        {"plane_mode", plane.plane_mode},
        {"generation", plane.generation},
        {"applied_generation", effective_applied_generation},
        {"staged_update", plane.generation > effective_applied_generation},
        {"failed_assignments", failed_assignments},
        {"in_flight_assignments", in_flight_assignments},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"shared_disk_name", plane.shared_disk_name},
        {"control_root", plane.control_root},
        {"created_at", plane.created_at},
        {"node_count", desired_state.has_value() ? json(desired_state->nodes.size()) : json(nullptr)},
        {"instance_count",
         desired_state.has_value() ? json(desired_state->instances.size()) : json(nullptr)},
        {"disk_count", desired_state.has_value() ? json(desired_state->disks.size()) : json(nullptr)},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"items", std::move(items)},
  };
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

std::optional<std::string> FindHeaderString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(Lowercase(key));
  if (it == request.headers.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

bool StartsWithPath(const std::string& value, const std::string& prefix) {
  return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
}

struct HostAssignmentsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::vector<comet::HostAssignment> assignments;
};

struct HostObservationsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
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
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::DiskRuntimeState> runtime_states;
  std::vector<comet::HostObservation> observations;
};

struct EventsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<std::string> worker_name;
  std::optional<std::string> category;
  int limit = 100;
  std::vector<comet::EventRecord> events;
};

struct RegisteredHostsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::vector<comet::RegisteredHostRecord> hosts;
};

std::string SerializeEventPayload(const json& payload) {
  return payload.dump();
}

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) {
  store.AppendEvent(comet::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializeEventPayload(payload),
      "",
  });
}

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

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (observation.plane_name == plane_name) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }

  const auto observed_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  if (observed_state.plane_name == plane_name) {
    return true;
  }
  for (const auto& disk : observed_state.disks) {
    if (disk.plane_name == plane_name) {
      return true;
    }
  }
  for (const auto& instance : observed_state.instances) {
    if (instance.plane_name == plane_name) {
      return true;
    }
  }
  return false;
}

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  std::vector<comet::HostObservation> result;
  for (const auto& observation : observations) {
    if (ObservationMatchesPlane(observation, plane_name)) {
      result.push_back(observation);
    }
  }
  return result;
}

bool ObservationBlocksPlaneDeletion(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (!ObservationMatchesPlane(observation, plane_name)) {
    return false;
  }
  if (observation.status != comet::HostObservationStatus::Idle) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }
  try {
    const auto observed_state =
        comet::DeserializeDesiredStateJson(observation.observed_state_json);
    for (const auto& disk : observed_state.disks) {
      if (disk.plane_name == plane_name) {
        return true;
      }
    }
    for (const auto& instance : observed_state.instances) {
      if (instance.plane_name == plane_name) {
        return true;
      }
    }
    return false;
  } catch (const std::exception&) {
    return true;
  }
}

bool HasBlockingPlaneObservations(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  return std::any_of(
      observations.begin(),
      observations.end(),
      [&](const auto& observation) {
        return ObservationBlocksPlaneDeletion(observation, plane_name);
      });
}

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name) {
  const auto pending_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Pending, plane_name);
  const auto claimed_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Claimed, plane_name);
  if (!pending_assignments.empty() || !claimed_assignments.empty()) {
    return false;
  }
  return !HasBlockingPlaneObservations(store.LoadHostObservations(), plane_name);
}

struct ObservedPlaneRuntimeSummary {
  bool available = false;
  int instance_count = 0;
  int disk_count = 0;
};

ObservedPlaneRuntimeSummary SummarizeObservedPlaneRuntime(
    const comet::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  if (observation.observed_state_json.empty()) {
    return {};
  }
  try {
    const auto observed_state =
        comet::DeserializeDesiredStateJson(observation.observed_state_json);
    const std::string target_plane =
        plane_name.has_value() ? *plane_name : observed_state.plane_name;
    if (target_plane.empty()) {
      return {};
    }

    ObservedPlaneRuntimeSummary summary;
    for (const auto& instance : observed_state.instances) {
      if (instance.node_name == node_name && instance.plane_name == target_plane) {
        ++summary.instance_count;
      }
    }
    for (const auto& disk : observed_state.disks) {
      if (disk.node_name == node_name && disk.plane_name == target_plane) {
        ++summary.disk_count;
      }
    }
    summary.available =
        observed_state.plane_name == target_plane || summary.instance_count > 0 ||
        summary.disk_count > 0;
    return summary;
  } catch (...) {
    return {};
  }
}

struct DashboardRuntimeFallback {
  bool available = false;
  bool launch_ready = false;
  std::string runtime_phase;
};

DashboardRuntimeFallback DetermineDashboardRuntimeFallback(
    const comet::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& plane_state,
    int desired_generation,
    int desired_instance_count,
    int desired_disk_count,
    const std::string& health) {
  DashboardRuntimeFallback fallback;
  if (health == "stale" || health == "failed") {
    return fallback;
  }

  const auto observed_runtime =
      SummarizeObservedPlaneRuntime(observation, node_name, plane_name);
  const bool has_applied_generation =
      observation.applied_generation.has_value() &&
      *observation.applied_generation >= desired_generation;
  const bool has_observed_runtime =
      observed_runtime.available || has_applied_generation;
  if (!has_observed_runtime) {
    return fallback;
  }

  fallback.available = true;
  const std::string state = plane_state.value_or("");
  if (state == "stopped") {
    const bool stop_converged =
        has_applied_generation &&
        observed_runtime.instance_count == 0 &&
        (desired_disk_count == 0 || observed_runtime.disk_count >= desired_disk_count);
    fallback.launch_ready = stop_converged;
    fallback.runtime_phase = stop_converged ? "stopped" : "stopping";
    return fallback;
  }

  if (state == "running") {
    const bool start_converged =
        has_applied_generation &&
        (desired_instance_count == 0 ||
         observed_runtime.instance_count >= desired_instance_count);
    fallback.launch_ready = start_converged;
    fallback.runtime_phase = start_converged ? "applied" : "starting";
    return fallback;
  }

  fallback.launch_ready =
      has_applied_generation && observed_runtime.instance_count >= desired_instance_count;
  fallback.runtime_phase = fallback.launch_ready ? "applied" : "pending";
  return fallback;
}

HostObservationsViewData LoadHostObservationsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostObservationsViewData{
      db_path,
      plane_name,
      node_name,
      stale_after_seconds,
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name),
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  return DiskStateViewData{
      db_path,
      plane_name,
      node_name,
      desired_state,
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration(),
      desired_state.has_value()
          ? store.LoadDiskRuntimeStates(desired_state->plane_name, node_name)
          : std::vector<comet::DiskRuntimeState>{},
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name),
  };
}

EventsViewData LoadEventsViewData(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return EventsViewData{
      db_path,
      plane_name,
      node_name,
      worker_name,
      category,
      limit,
      store.LoadEvents(
          plane_name,
          node_name,
          worker_name,
          category,
          limit),
  };
}

RegisteredHostsViewData LoadRegisteredHostsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return RegisteredHostsViewData{
      db_path,
      node_name,
      store.LoadRegisteredHosts(node_name),
  };
}

json BuildEventPayloadItem(const comet::EventRecord& event) {
  json payload = json::object();
  if (!event.payload_json.empty()) {
    try {
      payload = json::parse(event.payload_json);
    } catch (...) {
      payload = json{
          {"raw", event.payload_json},
      };
    }
  }
  return json{
      {"id", event.id},
      {"plane_name", event.plane_name.empty() ? json(nullptr) : json(event.plane_name)},
      {"node_name", event.node_name.empty() ? json(nullptr) : json(event.node_name)},
      {"worker_name", event.worker_name.empty() ? json(nullptr) : json(event.worker_name)},
      {"assignment_id", event.assignment_id.has_value() ? json(*event.assignment_id) : json(nullptr)},
      {"rollout_action_id",
       event.rollout_action_id.has_value() ? json(*event.rollout_action_id) : json(nullptr)},
      {"category", event.category},
      {"event_type", event.event_type},
      {"severity", event.severity},
      {"message", event.message},
      {"payload", payload},
      {"created_at", event.created_at},
  };
}

json BuildAssignmentPayloadItem(const comet::HostAssignment& assignment) {
  json progress = nullptr;
  if (!assignment.progress_json.empty() && assignment.progress_json != "{}") {
    progress = json::parse(assignment.progress_json);
  }
  return json{
      {"id", assignment.id},
      {"node_name", assignment.node_name},
      {"plane_name", assignment.plane_name},
      {"desired_generation", assignment.desired_generation},
      {"attempt_count", assignment.attempt_count},
      {"max_attempts", assignment.max_attempts},
      {"assignment_type", assignment.assignment_type},
      {"desired_state_json", assignment.desired_state_json},
      {"artifacts_root", assignment.artifacts_root},
      {"status", comet::ToString(assignment.status)},
      {"status_message", assignment.status_message},
      {"progress", progress},
  };
}

json BuildBootstrapModelPayloadItem(
    const std::optional<comet::BootstrapModelSpec>& bootstrap_model) {
  if (!bootstrap_model.has_value()) {
    return nullptr;
  }
  json item{
      {"model_id", bootstrap_model->model_id},
      {"served_model_name",
       bootstrap_model->served_model_name.has_value() ? json(*bootstrap_model->served_model_name)
                                                      : json(nullptr)},
      {"local_path",
       bootstrap_model->local_path.has_value() ? json(*bootstrap_model->local_path)
                                               : json(nullptr)},
      {"source_url",
       bootstrap_model->source_url.has_value() ? json(*bootstrap_model->source_url)
                                               : json(nullptr)},
      {"source_urls", bootstrap_model->source_urls},
      {"target_filename",
       bootstrap_model->target_filename.has_value()
           ? json(*bootstrap_model->target_filename)
           : json(nullptr)},
      {"sha256",
       bootstrap_model->sha256.has_value() ? json(*bootstrap_model->sha256) : json(nullptr)},
  };
  return item;
}

comet::HostObservation ParseHostObservationPayload(const json& payload) {
  comet::HostObservation observation;
  observation.node_name = payload.value("node_name", std::string{});
  observation.plane_name = payload.value("plane_name", std::string{});
  if (payload.contains("applied_generation") && !payload.at("applied_generation").is_null()) {
    observation.applied_generation = payload.at("applied_generation").get<int>();
  }
  if (payload.contains("last_assignment_id") && !payload.at("last_assignment_id").is_null()) {
    observation.last_assignment_id = payload.at("last_assignment_id").get<int>();
  }
  observation.status =
      comet::ParseHostObservationStatus(payload.value("status", std::string("idle")));
  observation.status_message = payload.value("status_message", std::string{});
  observation.observed_state_json = payload.value("observed_state_json", std::string{});
  observation.runtime_status_json = payload.value("runtime_status_json", std::string{});
  observation.instance_runtime_json = payload.value("instance_runtime_json", std::string{});
  observation.gpu_telemetry_json = payload.value("gpu_telemetry_json", std::string{});
  observation.disk_telemetry_json = payload.value("disk_telemetry_json", std::string{});
  observation.network_telemetry_json = payload.value("network_telemetry_json", std::string{});
  observation.cpu_telemetry_json = payload.value("cpu_telemetry_json", std::string{});
  observation.heartbeat_at = payload.value("heartbeat_at", std::string{});
  return observation;
}

json BuildDiskRuntimeStatePayloadItem(const comet::DiskRuntimeState& state) {
  return json{
      {"disk_name", state.disk_name},
      {"plane_name", state.plane_name},
      {"node_name", state.node_name},
      {"image_path", state.image_path},
      {"filesystem_type", state.filesystem_type},
      {"loop_device", state.loop_device},
      {"mount_point", state.mount_point},
      {"runtime_state", state.runtime_state},
      {"attached_at", state.attached_at},
      {"mounted_at", state.mounted_at},
      {"last_verified_at", state.last_verified_at},
      {"status_message", state.status_message},
      {"updated_at", state.updated_at},
  };
}

comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const json& payload) {
  comet::DiskRuntimeState state;
  state.disk_name = payload.value("disk_name", std::string{});
  state.plane_name = payload.value("plane_name", std::string{});
  state.node_name = payload.value("node_name", std::string{});
  state.image_path = payload.value("image_path", std::string{});
  state.filesystem_type = payload.value("filesystem_type", std::string{});
  state.loop_device = payload.value("loop_device", std::string{});
  state.mount_point = payload.value("mount_point", std::string{});
  state.runtime_state = payload.value("runtime_state", std::string{});
  state.attached_at = payload.value("attached_at", std::string{});
  state.mounted_at = payload.value("mounted_at", std::string{});
  state.last_verified_at = payload.value("last_verified_at", std::string{});
  state.status_message = payload.value("status_message", std::string{});
  state.updated_at = payload.value("updated_at", std::string{});
  return state;
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
        {"progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
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

json BuildRegisteredHostsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadRegisteredHostsViewData(db_path, node_name);
  json items = json::array();
  for (const auto& host : view.hosts) {
    items.push_back(json{
        {"node_name", host.node_name},
        {"advertised_address", host.advertised_address.empty() ? json(nullptr) : json(host.advertised_address)},
        {"transport_mode", host.transport_mode},
        {"registration_state", host.registration_state},
        {"session_state", host.session_state},
        {"controller_public_key_fingerprint",
         host.controller_public_key_fingerprint.empty()
             ? json(nullptr)
             : json(host.controller_public_key_fingerprint)},
        {"host_public_key_fingerprint",
         host.public_key_base64.empty()
             ? json(nullptr)
             : json(comet::ComputeKeyFingerprintHex(host.public_key_base64))},
        {"status_message", host.status_message.empty() ? json(nullptr) : json(host.status_message)},
        {"last_session_at", host.last_session_at.empty() ? json(nullptr) : json(host.last_session_at)},
        {"session_expires_at",
         host.session_expires_at.empty() ? json(nullptr) : json(host.session_expires_at)},
        {"last_heartbeat_at",
         host.last_heartbeat_at.empty() ? json(nullptr) : json(host.last_heartbeat_at)},
        {"updated_at", host.updated_at},
    });
  }
  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"items", items},
  };
}

json BuildHostObservationsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds) {
  const auto view =
      LoadHostObservationsViewData(db_path, node_name, plane_name, stale_after_seconds);

  json observations = json::array();
  for (const auto& observation : view.observations) {
    const auto runtime_status = ParseRuntimeStatus(observation);
    const auto telemetry = ParseGpuTelemetry(observation);
    const auto cpu_telemetry = ParseCpuTelemetry(observation);
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    const auto network_telemetry = ParseNetworkTelemetry(observation);

    const auto build_runtime_status_payload =
        [&](const std::optional<comet::RuntimeStatus>& status) -> json {
      if (!status.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"runtime", nullptr},
        };
      }
      return json{
          {"contract_version", 1},
          {"available", true},
          {"runtime", json::parse(comet::SerializeRuntimeStatusJson(*status))},
      };
    };

    const auto build_instance_runtime_payload =
        [&](const std::vector<comet::RuntimeProcessStatus>& statuses) -> json {
      int ready_count = 0;
      int gpu_bound_count = 0;
      int running_count = 0;
      for (const auto& status : statuses) {
        if (status.ready) {
          ++ready_count;
        }
        if (!status.gpu_device.empty()) {
          ++gpu_bound_count;
        }
        if (!status.runtime_phase.empty() && status.runtime_phase != "stopped") {
          ++running_count;
        }
      }
      return json{
          {"contract_version", 1},
          {"available", !statuses.empty()},
          {"summary",
           {
               {"count", statuses.size()},
               {"ready_count", ready_count},
               {"running_count", running_count},
               {"gpu_bound_count", gpu_bound_count},
           }},
          {"items",
           statuses.empty() ? json::array()
                            : json::parse(comet::SerializeRuntimeStatusListJson(statuses))},
      };
    };

    const auto build_gpu_telemetry_payload =
        [&](const std::optional<comet::GpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"device_count", 0},
                 {"owned_process_count", 0},
                 {"unknown_process_count", 0},
                 {"total_vram_mb", 0},
                 {"used_vram_mb", 0},
                 {"free_vram_mb", 0},
             }},
            {"devices", json::array()},
        };
      }

      int owned_process_count = 0;
      int unknown_process_count = 0;
      int total_vram_mb = 0;
      int used_vram_mb = 0;
      int free_vram_mb = 0;
      for (const auto& device : snapshot->devices) {
        total_vram_mb += device.total_vram_mb;
        used_vram_mb += device.used_vram_mb;
        free_vram_mb += device.free_vram_mb;
        for (const auto& process : device.processes) {
          if (process.instance_name == "unknown") {
            ++unknown_process_count;
          } else {
            ++owned_process_count;
          }
        }
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"device_count", snapshot->devices.size()},
               {"owned_process_count", owned_process_count},
               {"unknown_process_count", unknown_process_count},
               {"total_vram_mb", total_vram_mb},
               {"used_vram_mb", used_vram_mb},
               {"free_vram_mb", free_vram_mb},
           }},
          {"devices", json::parse(comet::SerializeGpuTelemetryJson(*snapshot)).at("devices")},
      };
    };

    const auto build_disk_telemetry_payload =
        [&](const std::optional<comet::DiskTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"disk_count", 0},
                 {"mounted_count", 0},
                 {"healthy_count", 0},
                 {"total_bytes", 0},
                 {"used_bytes", 0},
                 {"free_bytes", 0},
             }},
            {"items", json::array()},
        };
      }

      int mounted_count = 0;
      int healthy_count = 0;
      std::uint64_t total_bytes = 0;
      std::uint64_t used_bytes = 0;
      std::uint64_t free_bytes = 0;
      std::uint64_t read_ios = 0;
      std::uint64_t write_ios = 0;
      std::uint64_t read_bytes = 0;
      std::uint64_t write_bytes = 0;
      std::uint64_t io_time_ms = 0;
      std::uint64_t weighted_io_time_ms = 0;
      int io_in_progress = 0;
      int warning_count = 0;
      int fault_count = 0;
      int read_only_count = 0;
      int perf_counters_count = 0;
      int io_error_counter_count = 0;
      for (const auto& item : snapshot->items) {
        if (item.mounted) {
          ++mounted_count;
        }
        if (item.health == "ok") {
          ++healthy_count;
        }
        total_bytes += item.total_bytes;
        used_bytes += item.used_bytes;
        free_bytes += item.free_bytes;
        read_ios += item.read_ios;
        write_ios += item.write_ios;
        read_bytes += item.read_bytes;
        write_bytes += item.write_bytes;
        io_time_ms += item.io_time_ms;
        weighted_io_time_ms += item.weighted_io_time_ms;
        io_in_progress += item.io_in_progress;
        warning_count += item.warning_count;
        fault_count += item.fault_count;
        if (item.read_only) {
          ++read_only_count;
        }
        if (item.perf_counters_available) {
          ++perf_counters_count;
        }
        if (item.io_error_counters_available) {
          ++io_error_counter_count;
        }
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"disk_count", snapshot->items.size()},
               {"mounted_count", mounted_count},
               {"healthy_count", healthy_count},
               {"total_bytes", total_bytes},
               {"used_bytes", used_bytes},
               {"free_bytes", free_bytes},
               {"read_ios", read_ios},
               {"write_ios", write_ios},
               {"read_bytes", read_bytes},
               {"write_bytes", write_bytes},
               {"io_time_ms", io_time_ms},
               {"weighted_io_time_ms", weighted_io_time_ms},
               {"io_in_progress", io_in_progress},
               {"warning_count", warning_count},
               {"fault_count", fault_count},
               {"read_only_count", read_only_count},
               {"perf_counters_count", perf_counters_count},
               {"io_error_counter_count", io_error_counter_count},
           }},
          {"items", json::parse(comet::SerializeDiskTelemetryJson(*snapshot)).at("items")},
      };
    };

    const auto build_network_telemetry_payload =
        [&](const std::optional<comet::NetworkTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"interface_count", 0},
                 {"up_count", 0},
                 {"loopback_count", 0},
                 {"rx_bytes", 0},
                 {"tx_bytes", 0},
             }},
            {"interfaces", json::array()},
        };
      }

      int up_count = 0;
      int loopback_count = 0;
      std::uint64_t rx_bytes = 0;
      std::uint64_t tx_bytes = 0;
      for (const auto& interface : snapshot->interfaces) {
        if (interface.link_state == "up" || interface.oper_state == "up") {
          ++up_count;
        }
        if (interface.loopback) {
          ++loopback_count;
        }
        rx_bytes += interface.rx_bytes;
        tx_bytes += interface.tx_bytes;
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"interface_count", snapshot->interfaces.size()},
               {"up_count", up_count},
               {"loopback_count", loopback_count},
               {"rx_bytes", rx_bytes},
               {"tx_bytes", tx_bytes},
           }},
          {"interfaces", json::parse(comet::SerializeNetworkTelemetryJson(*snapshot)).at("interfaces")},
      };
    };

    const auto build_cpu_telemetry_payload =
        [&](const std::optional<comet::CpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"core_count", 0},
                 {"utilization_pct", 0.0},
                 {"loadavg_1m", 0.0},
                 {"loadavg_5m", 0.0},
                 {"loadavg_15m", 0.0},
                 {"total_memory_bytes", 0},
                 {"available_memory_bytes", 0},
                 {"used_memory_bytes", 0},
             }},
            {"snapshot", nullptr},
        };
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"core_count", snapshot->core_count},
               {"utilization_pct", snapshot->utilization_pct},
               {"loadavg_1m", snapshot->loadavg_1m},
               {"loadavg_5m", snapshot->loadavg_5m},
               {"loadavg_15m", snapshot->loadavg_15m},
               {"total_memory_bytes", snapshot->total_memory_bytes},
               {"available_memory_bytes", snapshot->available_memory_bytes},
               {"used_memory_bytes", snapshot->used_memory_bytes},
           }},
          {"snapshot", json::parse(comet::SerializeCpuTelemetryJson(*snapshot))},
      };
    };

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
    entry["runtime_status"] = build_runtime_status_payload(runtime_status);
    entry["gpu_telemetry"] = build_gpu_telemetry_payload(telemetry);
    entry["disk_telemetry"] = build_disk_telemetry_payload(disk_telemetry);
    entry["network_telemetry"] = build_network_telemetry_payload(network_telemetry);
    entry["cpu_telemetry"] = build_cpu_telemetry_payload(cpu_telemetry);
    entry["instance_runtimes"] = build_instance_runtime_payload(instance_statuses);

    observations.push_back(std::move(entry));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildDashboardPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);

json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit);

std::string ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root = DefaultArtifactsRoot());

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

int StartPlane(const std::string& db_path, const std::string& plane_name);

int StopPlane(const std::string& db_path, const std::string& plane_name);

int RevokeHostd(
    const std::string& db_path,
    const std::string& node_name,
    const std::optional<std::string>& status_message);

int RotateHostdKey(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& public_key_base64,
    const std::optional<std::string>& status_message);

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

json ParseJsonRequestBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return json::object();
  }
  return json::parse(request.body);
}

std::string BuildHostRequestAad(
    const std::string& message_type,
    const std::string& node_name,
    std::int64_t sequence_number) {
  return "request\n" + message_type + "\n" + node_name + "\n" + std::to_string(sequence_number);
}

std::string BuildHostResponseAad(
    const std::string& message_type,
    const std::string& node_name,
    std::int64_t sequence_number) {
  return "response\n" + message_type + "\n" + node_name + "\n" + std::to_string(sequence_number);
}

std::optional<comet::RegisteredHostRecord> AuthenticateHostSession(
    comet::ControllerStore& store,
    const HttpRequest& request,
    const std::optional<std::string>& expected_node_name = std::nullopt) {
  const auto token_it = request.headers.find("x-comet-host-session");
  if (token_it == request.headers.end() || token_it->second.empty()) {
    return std::nullopt;
  }
  const auto node_name_it = request.headers.find("x-comet-host-node");
  if (node_name_it == request.headers.end() || node_name_it->second.empty()) {
    return std::nullopt;
  }
  const auto host = store.LoadRegisteredHost(node_name_it->second);
  if (!host.has_value()) {
    return std::nullopt;
  }
  if (expected_node_name.has_value() && *expected_node_name != host->node_name) {
    return std::nullopt;
  }
  if (host->registration_state != "registered") {
    return std::nullopt;
  }
  if (!host->session_expires_at.empty()) {
    const auto expires_age = TimestampAgeSeconds(host->session_expires_at);
    if (expires_age.has_value() && *expires_age >= 0) {
      return std::nullopt;
    }
  }
  if (host->session_token.empty() || host->session_token != token_it->second) {
    return std::nullopt;
  }
  return host;
}

constexpr int HostSessionLifetimeSeconds() {
  return 600;
}

std::string SqlTimestampAfterSeconds(int seconds) {
  const std::time_t future = std::time(nullptr) + seconds;
  std::tm tm{};
  gmtime_r(&future, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::optional<std::tm> ParseDisplayTimestamp(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

std::string FormatDisplayTimestamp(const std::string& value) {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

json ParseEncryptedHostRequestBody(
    comet::ControllerStore& store,
    const HttpRequest& request,
    comet::RegisteredHostRecord* host,
    const std::string& message_type) {
  const json body = ParseJsonRequestBody(request);
  if (!body.value("encrypted", false)) {
    return body;
  }
  const std::int64_t sequence_number = body.value("sequence_number", static_cast<std::int64_t>(0));
  if (sequence_number <= host->session_host_sequence) {
    throw std::runtime_error("stale or replayed host session request");
  }
  const comet::EncryptedEnvelope envelope{
      body.value("nonce", std::string{}),
      body.value("ciphertext", std::string{}),
  };
  const std::string decrypted = comet::DecryptEnvelopeBase64(
      envelope,
      host->session_token,
      BuildHostRequestAad(message_type, host->node_name, sequence_number));
  host->session_host_sequence = sequence_number;
  host->session_expires_at = SqlTimestampAfterSeconds(HostSessionLifetimeSeconds());
  store.UpsertRegisteredHost(*host);
  if (decrypted.empty()) {
    return json::object();
  }
  return json::parse(decrypted);
}

HttpResponse BuildEncryptedHostResponse(
    comet::ControllerStore& store,
    comet::RegisteredHostRecord* host,
    const std::string& message_type,
    const json& payload) {
  host->session_controller_sequence += 1;
  store.UpsertRegisteredHost(*host);
  const comet::EncryptedEnvelope envelope = comet::EncryptEnvelopeBase64(
      payload.dump(),
      host->session_token,
      BuildHostResponseAad(message_type, host->node_name, host->session_controller_sequence));
  return BuildJsonResponse(
      200,
      json{
          {"encrypted", true},
          {"sequence_number", host->session_controller_sequence},
          {"nonce", envelope.nonce_base64},
          {"ciphertext", envelope.ciphertext_base64},
      });
}

std::string GuessContentType(const std::filesystem::path& file_path) {
  const std::string extension = file_path.extension().string();
  if (extension == ".html") {
    return "text/html; charset=utf-8";
  }
  if (extension == ".js" || extension == ".mjs") {
    return "application/javascript; charset=utf-8";
  }
  if (extension == ".css") {
    return "text/css; charset=utf-8";
  }
  if (extension == ".json") {
    return "application/json; charset=utf-8";
  }
  if (extension == ".svg") {
    return "image/svg+xml";
  }
  if (extension == ".png") {
    return "image/png";
  }
  if (extension == ".jpg" || extension == ".jpeg") {
    return "image/jpeg";
  }
  if (extension == ".ico") {
    return "image/x-icon";
  }
  if (extension == ".txt") {
    return "text/plain; charset=utf-8";
  }
  return "application/octet-stream";
}

std::optional<std::filesystem::path> ResolveUiRequestPath(
    const std::filesystem::path& ui_root,
    const std::string& request_path) {
  if (request_path.empty() || request_path[0] != '/') {
    return std::nullopt;
  }
  if (request_path.rfind("/api/", 0) == 0 || request_path == "/health") {
    return std::nullopt;
  }

  std::filesystem::path relative_path;
  if (request_path == "/") {
    relative_path = "index.html";
  } else {
    relative_path = std::filesystem::path(request_path.substr(1)).lexically_normal();
  }
  if (relative_path.empty()) {
    relative_path = "index.html";
  }
  if (relative_path.is_absolute()) {
    return std::nullopt;
  }
  for (const auto& part : relative_path) {
    if (part == "..") {
      return std::nullopt;
    }
  }

  const auto candidate = ui_root / relative_path;
  if (std::filesystem::is_regular_file(candidate)) {
    return candidate;
  }

  const auto fallback = ui_root / "index.html";
  if (std::filesystem::is_regular_file(fallback)) {
    return fallback;
  }
  return std::nullopt;
}

HttpResponse BuildStaticFileResponse(const std::filesystem::path& file_path) {
  std::ifstream input(file_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open static asset: " + file_path.string());
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return HttpResponse{200, GuessContentType(file_path), buffer.str()};
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

int DeletePlane(const std::string& db_path, const std::string& plane_name);

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

int ApplyDesiredState(
    const std::string& db_path,
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    const std::string& source_label) {
  ValidateDesiredStateForControllerAdmission(desired_state);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState(desired_state.plane_name);
  comet::RequireSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
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
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});

  const bool existed = current_state.has_value();
  AppendControllerEvent(
      store,
      "plane",
      existed ? "staged-update" : "created",
      existed ? "updated plane desired state; rollout is staged until explicit restart"
             : "created plane desired state in stopped lifecycle state",
      json{
          {"source", source_label},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"applied_generation",
           current_state.has_value()
               ? json(store.LoadPlane(desired_state.plane_name)->applied_generation)
               : json(0)},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name);
  std::cout << (existed ? "staged update for" : "created") << " plane '" << desired_state.plane_name
            << "' in: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  const auto plane = store.LoadPlane(desired_state.plane_name);
  if (plane.has_value()) {
    std::cout << "applied generation: " << plane->applied_generation << "\n";
    std::cout << "plane state: " << plane->state << "\n";
  }
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

ControllerActionResult ExecuteUpsertPlaneStateAction(
    const std::string& db_path,
    const std::string& desired_state_json,
    const std::string& artifacts_root,
    const std::optional<std::string>& expected_plane_name,
    const std::string& source_label) {
  return RunControllerActionResult(
      "upsert-plane-state",
      [&]() {
        const auto desired_state = comet::DeserializeDesiredStateJson(desired_state_json);
        if (expected_plane_name.has_value() &&
            desired_state.plane_name != *expected_plane_name) {
          throw std::runtime_error(
              "plane name mismatch: expected '" + *expected_plane_name + "' but payload contains '" +
              desired_state.plane_name + "'");
        }
        return ApplyDesiredState(db_path, desired_state, artifacts_root, source_label);
      });
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

ControllerActionResult ExecuteStartPlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  return RunControllerActionResult(
      "start-plane",
      [&]() { return StartPlane(db_path, plane_name); });
}

ControllerActionResult ExecuteStopPlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  return RunControllerActionResult(
      "stop-plane",
      [&]() { return StopPlane(db_path, plane_name); });
}

ControllerActionResult ExecuteDeletePlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  return RunControllerActionResult(
      "delete-plane",
      [&]() { return DeletePlane(db_path, plane_name); });
}

ControllerActionResult ExecuteRevokeHostdAction(
    const std::string& db_path,
    const std::string& node_name,
    const std::optional<std::string>& status_message) {
  return RunControllerActionResult(
      "revoke-hostd",
      [&]() { return RevokeHostd(db_path, node_name, status_message); });
}

ControllerActionResult ExecuteRotateHostdKeyAction(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& public_key_base64,
    const std::optional<std::string>& status_message) {
  return RunControllerActionResult(
      "rotate-hostd-key",
      [&]() { return RotateHostdKey(db_path, node_name, public_key_base64, status_message); });
}

int ExecuteRemoteControllerCommand(
    const ControllerEndpointTarget& target,
    const std::string& command,
    int argc,
    char** argv) {
  const auto plane_name = ParsePlaneArg(argc, argv);
  const auto node_name = ParseNodeArg(argc, argv);
  const auto stale_after = ParseStaleAfterArg(argc, argv);
  const auto bundle_dir = ParseBundleArg(argc, argv);
  const auto artifacts_root = ParseArtifactsRootArg(argc, argv);
  const auto action_id = ParseIdArg(argc, argv);
  const auto worker_name = ParseWorkerArg(argc, argv);
  const auto limit = ParseLimitArg(argc, argv);
  const auto category = ParseCategoryArg(argc, argv);
  const auto message = ParseMessageArg(argc, argv);
  const auto status = ParseStatusArg(argc, argv);
  const auto availability = ParseAvailabilityArg(argc, argv);

  if (command == "list-planes") {
    std::cout << SendControllerJsonRequest(target, "GET", "/api/v1/planes").dump(2) << "\n";
    return 0;
  }
  if (command == "show-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote show-plane");
    }
    std::cout << SendControllerJsonRequest(target, "GET", "/api/v1/planes/" + UrlEncode(*plane_name))
                     .dump(2)
              << "\n";
    return 0;
  }
  if (command == "start-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote start-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/planes/" + UrlEncode(*plane_name) + "/start"));
  }
  if (command == "stop-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote stop-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/planes/" + UrlEncode(*plane_name) + "/stop"));
  }
  if (command == "delete-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote delete-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "DELETE",
            "/api/v1/planes/" + UrlEncode(*plane_name)));
  }
  if (command == "show-state") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(target, "GET", "/api/v1/state"));
  }
  if (command == "show-hostd-hosts") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/hostd/hosts",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "revoke-hostd") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/hostd/hosts/" + UrlEncode(*node_name) + "/revoke",
            {{"message", message.value_or("")}}));
  }
  if (command == "rotate-hostd-key") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    const auto public_key = ParsePublicKeyArg(argc, argv);
    if (!public_key.has_value()) {
      std::cerr << "error: --public-key is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/hostd/hosts/" + UrlEncode(*node_name) + "/rotate-key",
            json{
                {"public_key_base64", ReadPublicKeyBase64Argument(*public_key)},
                {"message", message.value_or("")},
            }));
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
            {{"plane", plane_name.value_or("")},
             {"node", node_name.value_or("")},
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
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rollout-actions") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rollout-actions",
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rebalance-plan") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rebalance-plan",
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-events") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/events",
            {{"plane", plane_name.value_or("")},
             {"node", node_name.value_or("")},
             {"worker", worker_name.value_or("")},
             {"category", category.value_or("")},
             {"limit", limit.has_value() ? std::to_string(*limit) : ""}}));
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

std::string BuildSseEventName(const comet::EventRecord& event) {
  if (!event.category.empty() && !event.event_type.empty()) {
    return event.category + "." + event.event_type;
  }
  if (!event.category.empty()) {
    return event.category;
  }
  if (!event.event_type.empty()) {
    return event.event_type;
  }
  return "event";
}

SseStreamRequest ParseSseStreamRequest(const HttpRequest& request) {
  SseStreamRequest stream_request;
  stream_request.plane_name = FindQueryString(request, "plane");
  stream_request.node_name = FindQueryString(request, "node");
  stream_request.worker_name = FindQueryString(request, "worker");
  stream_request.category = FindQueryString(request, "category");
  stream_request.limit = FindQueryInt(request, "limit").value_or(100);
  stream_request.last_event_id = FindQueryInt(request, "since_id");
  if (!stream_request.last_event_id.has_value()) {
    const auto header_value = FindHeaderString(request, "last-event-id");
    if (header_value.has_value()) {
      stream_request.last_event_id = std::stoi(*header_value);
    }
  }
  return stream_request;
}

void StreamEventsSse(
    int client_fd,
    const std::string& db_path,
    const HttpRequest& request) {
  const SseStreamRequest stream_request = ParseSseStreamRequest(request);
  int last_event_id = stream_request.last_event_id.value_or(0);
  if (!SendSseHeaders(client_fd)) {
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }
  if (!SendSseCommentFrame(client_fd, " connected")) {
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }

  auto last_keepalive = std::chrono::steady_clock::now();
  while (!g_stop_requested.load()) {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto events = store.LoadEvents(
        stream_request.plane_name,
        stream_request.node_name,
        stream_request.worker_name,
        stream_request.category,
        stream_request.limit,
        last_event_id > 0 ? std::optional<int>(last_event_id) : std::nullopt,
        true);
    for (const auto& event : events) {
      const std::string payload = BuildEventPayloadItem(event).dump();
      if (!SendSseEventFrame(client_fd, event.id, BuildSseEventName(event), payload)) {
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
        return;
      }
      last_event_id = std::max(last_event_id, event.id);
      last_keepalive = std::chrono::steady_clock::now();
    }

    const auto now = std::chrono::steady_clock::now();
    if (now - last_keepalive >= std::chrono::seconds(5)) {
      if (!SendSseCommentFrame(client_fd, " keepalive")) {
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
        return;
      }
      last_keepalive = now;
    }

    pollfd fd_state{};
    fd_state.fd = client_fd;
    fd_state.events = POLLIN | POLLERR | POLLHUP;
    const int poll_result = poll(&fd_state, 1, 1000);
    if (poll_result < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (poll_result == 0) {
      continue;
    }
    if ((fd_state.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      break;
    }
    if ((fd_state.revents & POLLIN) != 0) {
      char scratch[1];
      const ssize_t read_count = recv(client_fd, scratch, sizeof(scratch), MSG_PEEK);
      if (read_count <= 0) {
        break;
      }
    }
  }

  shutdown(client_fd, SHUT_RDWR);
  close(client_fd);
}

std::optional<std::string> ParseInteractionStreamPlaneName(const HttpRequest& request) {
  if (request.method != "POST") {
    return std::nullopt;
  }
  constexpr std::string_view kPrefix = "/api/v1/planes/";
  constexpr std::string_view kSuffix = "/interaction/chat/completions/stream";
  if (request.path.rfind(std::string(kPrefix), 0) != 0) {
    return std::nullopt;
  }
  if (request.path.size() <= kPrefix.size() + kSuffix.size()) {
    return std::nullopt;
  }
  if (request.path.substr(request.path.size() - kSuffix.size()) != kSuffix) {
    return std::nullopt;
  }
  return request.path.substr(
      kPrefix.size(),
      request.path.size() - kPrefix.size() - kSuffix.size());
}

int ConnectHttpTarget(const ControllerEndpointTarget& target) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(target.port);
  const int lookup = getaddrinfo(target.host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error(
        "failed to resolve proxy target '" + target.raw + "': " + gai_strerror(lookup));
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
    throw std::runtime_error("failed to connect to proxy target '" + target.raw + "'");
  }
  return fd;
}

void StreamPlaneInteractionSse(
    int client_fd,
    const std::string& db_path,
    const HttpRequest& request) {
  const auto plane_name = ParseInteractionStreamPlaneName(request);
  if (!plane_name.has_value()) {
    SendHttpResponse(
        client_fd,
        BuildJsonResponse(
            404,
            json{{"status", "not_found"}, {"path", request.path}, {"method", request.method}}));
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }

  PlaneInteractionResolution resolution = ResolvePlaneInteraction(db_path, *plane_name);
  if (!resolution.status_payload.value("interaction_enabled", false)) {
    SendHttpResponse(
        client_fd,
        BuildPlaneInteractionError(
            409,
            resolution.status_payload,
            "interaction is available only for plane_mode=llm"));
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }
  if (!resolution.status_payload.value("ready", false) || !resolution.target.has_value()) {
    SendHttpResponse(
        client_fd,
        BuildPlaneInteractionError(
            409,
            resolution.status_payload,
            "plane interaction target is not ready"));
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }

  const json original_payload = ParseInteractionPayload(request.body);
  const InteractionCompletionPolicy policy =
      ResolveInteractionCompletionPolicy(resolution.desired_state, original_payload).policy;
  InteractionSessionResult session;
  session.session_id = GenerateInteractionSessionId();
  const auto session_started_at = std::chrono::steady_clock::now();

  auto stream_single_segment =
      [&](const json& payload, int segment_index) -> StreamedInteractionSegmentResult {
    StreamedInteractionSegmentResult result;
    result.summary.index = segment_index;
    result.summary.continuation_index = segment_index;
    int upstream_fd = -1;
    const auto segment_started_at = std::chrono::steady_clock::now();
    try {
      upstream_fd = ConnectHttpTarget(*resolution.target);
      const std::string body = BuildInteractionUpstreamBody(resolution, payload, true, policy);
      std::ostringstream upstream_request;
      upstream_request << "POST /v1/chat/completions HTTP/1.1\r\n";
      upstream_request << "Host: " << resolution.target->host << ":" << resolution.target->port
                       << "\r\n";
      upstream_request << "Connection: close\r\n";
      upstream_request << "Accept: text/event-stream\r\n";
      upstream_request << "Content-Type: application/json\r\n";
      upstream_request << "Content-Length: " << body.size() << "\r\n\r\n";
      upstream_request << body;
      if (!SendAll(upstream_fd, upstream_request.str())) {
        throw std::runtime_error("failed to write upstream interaction request");
      }

      std::string response_text;
      std::array<char, 8192> buffer{};
      std::size_t header_end = std::string::npos;
      while (header_end == std::string::npos) {
        const ssize_t read_count = recv(upstream_fd, buffer.data(), buffer.size(), 0);
        if (read_count <= 0) {
          break;
        }
        response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
        header_end = response_text.find("\r\n\r\n");
      }
      if (header_end == std::string::npos) {
        throw std::runtime_error("upstream interaction response ended before headers");
      }

      const std::string header_text = response_text.substr(0, header_end);
      HttpResponse upstream = ParseHttpResponse(response_text);
      const bool chunked_transfer =
          FindHttpHeaderValue(header_text, "transfer-encoding").has_value() &&
          Lowercase(*FindHttpHeaderValue(header_text, "transfer-encoding")).find("chunked") !=
              std::string::npos;
      if (upstream.status_code != 200 ||
          upstream.content_type.find("text/event-stream") == std::string::npos) {
        while (true) {
          const ssize_t read_count = recv(upstream_fd, buffer.data(), buffer.size(), 0);
          if (read_count <= 0) {
            break;
          }
          response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
        }
        upstream = ParseHttpResponse(response_text);
        throw std::runtime_error(
            "upstream interaction request failed: " +
            (upstream.body.empty() ? std::to_string(upstream.status_code) : upstream.body));
      }

      CompletionMarkerFilterState filter_state;
      json complete_payload = json::object();
      bool saw_complete = false;
      bool upstream_stream_finished = false;
      std::string openai_stream_raw_text;
      std::string openai_stream_emitted_text;
      std::string transport_buffer = upstream.body;
      std::string sse_buffer;
      if (chunked_transfer) {
        DecodeAvailableChunkedHttpBody(
            transport_buffer, &sse_buffer, &upstream_stream_finished);
      } else {
        sse_buffer = std::move(transport_buffer);
      }
      const auto emit_openai_stream_progress =
          [&](const std::string& model_name, bool final_flush) {
            std::string visible_text;
            const std::size_t think_close = openai_stream_raw_text.rfind("</think>");
            if (think_close != std::string::npos) {
              visible_text = openai_stream_raw_text.substr(think_close + std::string("</think>").size());
            } else {
              const std::string trimmed = Trim(openai_stream_raw_text);
              if (!final_flush &&
                  (openai_stream_raw_text.find("<think>") != std::string::npos ||
                   StartsWithReasoningPreamble(trimmed))) {
                return;
              }
              visible_text = openai_stream_raw_text;
            }
            bool marker_seen = false;
            visible_text = RemoveCompletionMarkers(
                visible_text, policy.completion_marker, &marker_seen);
            filter_state.marker_seen = filter_state.marker_seen || marker_seen;
            visible_text = SanitizeInteractionText(visible_text);
            if (visible_text.empty()) {
              return;
            }
            if (visible_text.size() < openai_stream_emitted_text.size() ||
                visible_text.compare(0, openai_stream_emitted_text.size(), openai_stream_emitted_text) !=
                    0) {
              if (!final_flush || !openai_stream_emitted_text.empty()) {
                return;
              }
            }
            const std::string delta = visible_text.substr(openai_stream_emitted_text.size());
            if (delta.empty()) {
              return;
            }
            openai_stream_emitted_text = visible_text;
            result.cleaned_text += delta;
            if (!SendInteractionSseEvent(
                    client_fd,
                    "delta",
                    json{
                        {"session_id", session.session_id},
                        {"segment_index", segment_index},
                        {"continuation_index", segment_index},
                        {"model", model_name},
                        {"delta", delta},
                    })) {
              throw std::runtime_error("failed to write downstream delta");
            }
          };
      const auto handle_frame = [&](const InteractionSseFrame& frame) {
        if (frame.data == "[DONE]") {
          emit_openai_stream_progress(session.model, true);
          if (!saw_complete) {
            complete_payload = json{
                {"model", session.model},
                {"finish_reason", "stop"},
                {"usage",
                 json{
                     {"prompt_tokens", 0},
                     {"completion_tokens", 0},
                     {"total_tokens", 0},
                 }},
            };
            saw_complete = true;
          }
          return false;
        }
        if (frame.event_name == "message") {
          if (frame.data.empty()) {
            return true;
          }
          const json chunk_payload = json::parse(frame.data);
          if (session.model.empty()) {
            session.model = chunk_payload.value("model", std::string{});
          }
          if (chunk_payload.contains("choices") && chunk_payload.at("choices").is_array() &&
              !chunk_payload.at("choices").empty()) {
            const json& choice = chunk_payload.at("choices").at(0);
            std::string delta_text;
            if (choice.contains("delta") && choice.at("delta").is_object()) {
              delta_text = choice.at("delta").value("content", std::string{});
            }
            openai_stream_raw_text += delta_text;
            emit_openai_stream_progress(chunk_payload.value("model", std::string{}), false);
            if (choice.contains("finish_reason") && !choice.at("finish_reason").is_null()) {
              emit_openai_stream_progress(chunk_payload.value("model", std::string{}), true);
              complete_payload = json{
                  {"model", chunk_payload.value("model", std::string{})},
                  {"finish_reason", choice.value("finish_reason", std::string{"stop"})},
                  {"usage", ExtractInteractionUsage(chunk_payload)},
              };
              saw_complete = true;
            }
          }
          return true;
        }
        if (frame.event_name == "delta") {
          if (frame.data.empty()) {
            return true;
          }
          const json delta_payload = json::parse(frame.data);
          const std::string filtered = ConsumeCompletionMarkerFilteredChunk(
              filter_state,
              delta_payload.value("delta", std::string{}),
              policy.completion_marker,
              false);
          if (!filtered.empty()) {
            result.cleaned_text += filtered;
            if (!SendInteractionSseEvent(
                    client_fd,
                    "delta",
                    json{
                        {"session_id", session.session_id},
                        {"segment_index", segment_index},
                        {"continuation_index", segment_index},
                        {"model", delta_payload.value("model", std::string{})},
                        {"delta", filtered},
                    })) {
              throw std::runtime_error("failed to write downstream delta");
            }
          }
          return true;
        }
        if (frame.event_name == "complete") {
          complete_payload = json::parse(frame.data);
          saw_complete = true;
          return true;
        }
        if (frame.event_name == "error") {
          const json error_payload = json::parse(frame.data);
          throw std::runtime_error(
              error_payload.value("message", std::string{"upstream stream error"}));
        }
        return true;
      };

      const auto drain_frames = [&](bool final_chunk) {
        if (chunked_transfer) {
          DecodeAvailableChunkedHttpBody(
              transport_buffer, &sse_buffer, &upstream_stream_finished);
        }
        if (final_chunk && !sse_buffer.empty() &&
            sse_buffer.find("\n\n") == std::string::npos) {
          sse_buffer.append("\n\n");
        }
        InteractionSseFrame frame;
        while (TryConsumeSseFrame(sse_buffer, &frame)) {
          if (!handle_frame(frame)) {
            break;
          }
        }
      };

      while (true) {
        drain_frames(false);
        if (saw_complete && sse_buffer.find("[DONE]") != std::string::npos) {
          break;
        }
        if (chunked_transfer && upstream_stream_finished) {
          drain_frames(true);
          break;
        }
        const ssize_t read_count = recv(upstream_fd, buffer.data(), buffer.size(), 0);
        if (read_count <= 0) {
          drain_frames(true);
          break;
        }
        if (chunked_transfer) {
          transport_buffer.append(buffer.data(), static_cast<std::size_t>(read_count));
        } else {
          sse_buffer.append(buffer.data(), static_cast<std::size_t>(read_count));
        }
      }

      const std::string final_filtered = ConsumeCompletionMarkerFilteredChunk(
          filter_state,
          std::string{},
          policy.completion_marker,
          true);
      if (!final_filtered.empty()) {
        result.cleaned_text += final_filtered;
        if (!SendInteractionSseEvent(
                client_fd,
                "delta",
                json{
                    {"session_id", session.session_id},
                    {"segment_index", segment_index},
                    {"continuation_index", segment_index},
                    {"delta", final_filtered},
                })) {
          throw std::runtime_error("failed to flush downstream delta");
        }
      }
      if (!saw_complete) {
        if (Trim(result.cleaned_text).empty()) {
          const HttpResponse fallback = SendControllerHttpRequest(
              *resolution.target,
              "POST",
              "/v1/chat/completions",
              BuildInteractionUpstreamBody(resolution, payload, false, policy),
              {{"Accept", "application/json"}});
          if (fallback.status_code == 200 && !fallback.body.empty()) {
            const json fallback_payload = json::parse(fallback.body);
            bool marker_seen_in_fallback = false;
            const std::string fallback_text = RemoveCompletionMarkers(
                ExtractInteractionText(fallback_payload),
                policy.completion_marker,
                &marker_seen_in_fallback);
            if (!fallback_text.empty()) {
              result.cleaned_text += fallback_text;
              if (!SendInteractionSseEvent(
                      client_fd,
                      "delta",
                      json{
                          {"session_id", session.session_id},
                          {"segment_index", segment_index},
                          {"continuation_index", segment_index},
                          {"model", fallback_payload.value("model", std::string{})},
                          {"delta", fallback_text},
                      })) {
                throw std::runtime_error("failed to write downstream fallback delta");
              }
            }
            filter_state.marker_seen = filter_state.marker_seen || marker_seen_in_fallback;
            complete_payload = fallback_payload;
            saw_complete = true;
          }
        }
        if (!saw_complete && !Trim(result.cleaned_text).empty()) {
          complete_payload = json{
              {"model", session.model},
              {"finish_reason", "length"},
              {"usage",
               json{
                   {"prompt_tokens", 0},
                   {"completion_tokens", 0},
                   {"total_tokens", 0},
               }},
          };
          saw_complete = true;
        }
        if (!saw_complete) {
          throw std::runtime_error("upstream interaction stream ended without completion event");
        }
      }

      const auto segment_finished_at = std::chrono::steady_clock::now();
      const json usage = complete_payload.value("usage", json::object());
      result.summary.text = result.cleaned_text;
      result.summary.finish_reason = complete_payload.value("finish_reason", std::string{"stop"});
      result.summary.prompt_tokens = usage.value("prompt_tokens", 0);
      result.summary.completion_tokens = usage.value("completion_tokens", 0);
      result.summary.total_tokens = usage.value("total_tokens", 0);
      result.summary.latency_ms = complete_payload.value(
          "latency_ms",
          static_cast<int>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  segment_finished_at - segment_started_at)
                  .count()));
      result.summary.marker_seen = filter_state.marker_seen;
      if (session.model.empty()) {
        session.model = complete_payload.value("model", std::string{});
      }
      shutdown(upstream_fd, SHUT_RDWR);
      close(upstream_fd);
      return result;
    } catch (...) {
      if (upstream_fd >= 0) {
        shutdown(upstream_fd, SHUT_RDWR);
        close(upstream_fd);
      }
      if (Trim(result.cleaned_text).empty()) {
        try {
          const HttpResponse fallback = SendControllerHttpRequest(
              *resolution.target,
              "POST",
              "/v1/chat/completions",
              BuildInteractionUpstreamBody(resolution, payload, false, policy),
              {{"Accept", "application/json"}});
          if (fallback.status_code == 200 && !fallback.body.empty()) {
            const json fallback_payload = json::parse(fallback.body);
            bool marker_seen_in_fallback = false;
            const std::string fallback_text = RemoveCompletionMarkers(
                ExtractInteractionText(fallback_payload),
                policy.completion_marker,
                &marker_seen_in_fallback);
            if (!fallback_text.empty()) {
              result.cleaned_text = fallback_text;
              if (!SendInteractionSseEvent(
                      client_fd,
                      "delta",
                      json{
                          {"session_id", session.session_id},
                          {"segment_index", segment_index},
                          {"continuation_index", segment_index},
                          {"model", fallback_payload.value("model", std::string{})},
                          {"delta", fallback_text},
                      })) {
                throw std::runtime_error("failed to write downstream fallback delta");
              }
            }
            const json usage = ExtractInteractionUsage(fallback_payload);
            const auto segment_finished_at = std::chrono::steady_clock::now();
            result.summary.text = result.cleaned_text;
            result.summary.finish_reason = ExtractInteractionFinishReason(fallback_payload);
            result.summary.prompt_tokens = usage.value("prompt_tokens", 0);
            result.summary.completion_tokens = usage.value("completion_tokens", 0);
            result.summary.total_tokens = usage.value("total_tokens", 0);
            result.summary.latency_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    segment_finished_at - segment_started_at)
                    .count());
            result.summary.marker_seen = marker_seen_in_fallback;
            if (session.model.empty()) {
              session.model = fallback_payload.value("model", std::string{});
            }
            return result;
          }
        } catch (...) {
        }
      }
      throw;
    }
  };

  if (!SendSseHeaders(client_fd)) {
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
    return;
  }

  try {
    json current_payload = original_payload;
    for (int segment_index = 0;; ++segment_index) {
      if (!SendInteractionSseEvent(
              client_fd,
              "segment_started",
              json{
                  {"session_id", session.session_id},
                  {"segment_index", segment_index},
                  {"continuation_index", segment_index},
              })) {
        throw std::runtime_error("failed to write segment_started");
      }

      const StreamedInteractionSegmentResult segment =
          stream_single_segment(current_payload, segment_index);
      session.content += segment.cleaned_text;
      session.segments.push_back(segment.summary);
      session.total_prompt_tokens += segment.summary.prompt_tokens;
      session.total_completion_tokens += segment.summary.completion_tokens;
      session.total_tokens += segment.summary.total_tokens;
      session.total_latency_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - session_started_at)
              .count());
      session.final_finish_reason = segment.summary.finish_reason;
      session.marker_seen = session.marker_seen || segment.summary.marker_seen;

      if (!SendInteractionSseEvent(
              client_fd,
              "segment_complete",
              json{
                  {"session_id", session.session_id},
                  {"segment_index", segment_index},
                  {"continuation_index", segment_index},
                  {"finish_reason", segment.summary.finish_reason},
                  {"usage",
                   json{
                       {"prompt_tokens", segment.summary.prompt_tokens},
                       {"completion_tokens", segment.summary.completion_tokens},
                       {"total_tokens", segment.summary.total_tokens},
                   }},
                  {"latency_ms", segment.summary.latency_ms},
                  {"marker_seen", segment.summary.marker_seen},
              })) {
        throw std::runtime_error("failed to write segment_complete");
      }

      if (session.marker_seen &&
          SessionReachedTargetLength(policy, session.total_completion_tokens)) {
        session.completion_status = "completed";
        session.stop_reason = "semantic_completion_marker";
        break;
      }
      if (CanCompleteOnNaturalStop(policy, segment.summary) &&
          SessionReachedTargetLength(policy, session.total_completion_tokens)) {
        session.completion_status = "completed";
        session.stop_reason = "natural_stop";
        break;
      }
      if (session.total_completion_tokens >= policy.max_total_completion_tokens) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_total_completion_tokens_reached";
        break;
      }
      if (session.total_latency_ms >= policy.max_elapsed_time_ms) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_elapsed_time_ms_reached";
        break;
      }
      if (segment_index >= policy.max_continuations) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_continuations_reached";
        break;
      }

      session.continuation_count = segment_index + 1;
      if (!SendInteractionSseEvent(
              client_fd,
              "continuation_started",
              json{
                  {"session_id", session.session_id},
                  {"continuation_index", session.continuation_count},
                  {"reason",
                   segment.summary.finish_reason == "length"
                       ? "segment_hit_token_limit"
                       : "semantic_completion_not_confirmed"},
              })) {
        throw std::runtime_error("failed to write continuation_started");
      }
      current_payload = BuildContinuationPayload(
          original_payload,
          session.content,
          policy,
          segment.summary.finish_reason != "length");
    }

    if (session.completion_status == "in_progress") {
      session.completion_status = "failed";
      session.stop_reason = "session_state_unresolved";
    }

    const json session_payload = BuildInteractionSessionPayload(session);
    SendInteractionSseEvent(client_fd, "session_complete", session_payload);
    SendInteractionSseEvent(
        client_fd,
        "complete",
        json{
            {"model", session.model},
            {"finish_reason", session.completion_status == "completed" ? "stop" : "length"},
            {"latency_ms", session.total_latency_ms},
            {"usage", session_payload.at("usage")},
            {"session", session_payload},
            {"completion_status", session.completion_status},
            {"stop_reason", session.stop_reason},
            {"continuation_count", session.continuation_count},
            {"segment_count", static_cast<int>(session.segments.size())},
        });
    SendInteractionSseDone(client_fd);
  } catch (const std::exception& error) {
    SendInteractionSseEvent(
        client_fd,
        "session_failed",
        json{
            {"session_id", session.session_id},
            {"status", "failed"},
            {"message", error.what()},
            {"segment_count", static_cast<int>(session.segments.size())},
            {"continuation_count", session.continuation_count},
        });
    SendInteractionSseEvent(
        client_fd,
        "error",
        json{
            {"message", error.what()},
            {"plane_name", plane_name.value_or(std::string{})},
        });
    SendInteractionSseDone(client_fd);
  }

  shutdown(client_fd, SHUT_RDWR);
  close(client_fd);
}

HttpResponse HandleControllerRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request,
    const std::optional<std::filesystem::path>& ui_root) {
  const ScopedCurrentHttpRequest scoped_request(request);
  if (request.path == "/health" || request.path == "/api/v1/health") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    return BuildJsonResponse(200, BuildControllerHealthPayload(db_path));
  }
  if (request.path == "/api/v1/hostd/register") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      const json body = ParseJsonRequestBody(request);
      const std::string node_name = body.value("node_name", std::string{});
      if (node_name.empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing required field 'node_name'"}});
      }
      comet::ControllerStore store(db_path);
      store.Initialize();
      comet::RegisteredHostRecord host;
      if (const auto current = store.LoadRegisteredHost(node_name); current.has_value()) {
        host = *current;
      }
      host.node_name = node_name;
      host.advertised_address = body.value("advertised_address", host.advertised_address);
      host.public_key_base64 = body.value("public_key_base64", host.public_key_base64);
      host.controller_public_key_fingerprint = body.value(
          "controller_public_key_fingerprint",
          host.controller_public_key_fingerprint);
      host.transport_mode = body.value("transport_mode", host.transport_mode.empty() ? "out" : host.transport_mode);
      host.registration_state = body.value(
          "registration_state",
          host.registration_state.empty() ? "registered" : host.registration_state);
      host.session_state = body.value(
          "session_state",
          host.session_state.empty() ? "disconnected" : host.session_state);
      host.session_token.clear();
      host.session_expires_at.clear();
      host.capabilities_json = body.value("capabilities_json", std::string("{}"));
      host.status_message = body.value("status_message", std::string("registered via host-agent API"));
      store.UpsertRegisteredHost(host);
      AppendControllerEvent(
          store,
          "host-registry",
          "registered",
          "registered host node",
          json{{"transport_mode", host.transport_mode}},
          "",
          node_name);
      return BuildJsonResponse(
          200,
          json{
              {"service", "comet-controller"},
              {"node_name", node_name},
              {"registration_state", host.registration_state},
          });
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/hosts") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildRegisteredHostsPayload(db_path, FindQueryString(request, "node")));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (StartsWithPath(request.path, "/api/v1/hostd/hosts/")) {
    const std::string remainder =
        request.path.substr(std::string("/api/v1/hostd/hosts/").size());
    if (remainder.empty()) {
      return BuildJsonResponse(404, json{{"status", "not_found"}});
    }
    const auto revoke_pos = remainder.find("/revoke");
    if (revoke_pos != std::string::npos &&
        revoke_pos + std::string("/revoke").size() == remainder.size()) {
      if (request.method != "POST") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string node_name = remainder.substr(0, revoke_pos);
      try {
        const json body = ParseJsonRequestBody(request);
        const std::optional<std::string> message =
            body.contains("message") && body["message"].is_string()
                ? std::make_optional(body["message"].get<std::string>())
                : std::nullopt;
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteRevokeHostdAction(db_path, node_name, message)));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    const auto rotate_pos = remainder.find("/rotate-key");
    if (rotate_pos != std::string::npos &&
        rotate_pos + std::string("/rotate-key").size() == remainder.size()) {
      if (request.method != "POST") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string node_name = remainder.substr(0, rotate_pos);
      try {
        const json body = ParseJsonRequestBody(request);
        const std::string public_key_base64 = body.value("public_key_base64", std::string{});
        if (public_key_base64.empty()) {
          return BuildJsonResponse(
              400,
              json{{"status", "bad_request"}, {"message", "missing required field 'public_key_base64'"}}); 
        }
        const std::optional<std::string> message =
            body.contains("message") && body["message"].is_string()
                ? std::make_optional(body["message"].get<std::string>())
                : std::nullopt;
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteRotateHostdKeyAction(db_path, node_name, public_key_base64, message)));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    return BuildJsonResponse(404, json{{"status", "not_found"}});
  }
  if (request.path == "/api/v1/hostd/session/open") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      const json body = ParseJsonRequestBody(request);
      const std::string node_name = body.value("node_name", std::string{});
      if (node_name.empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing required field 'node_name'"}});
      }
      comet::ControllerStore store(db_path);
      store.Initialize();
      auto current = store.LoadRegisteredHost(node_name);
      if (!current.has_value()) {
        return BuildJsonResponse(
            404,
            json{{"status", "not_found"}, {"message", "host node is not registered"}});
      }
      const std::string timestamp = body.value("timestamp", std::string{});
      const std::string nonce = body.value("nonce", std::string{});
      const std::string signature = body.value("signature", std::string{});
      if (timestamp.empty() || nonce.empty() || signature.empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing required session handshake fields"}});
      }
      if (current->public_key_base64.empty()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "registered host is missing public key"}});
      }
      const std::string signed_message =
          "hostd-session-open\n" + node_name + "\n" + timestamp + "\n" + nonce;
      if (!comet::VerifyDetachedBase64(signed_message, signature, current->public_key_base64)) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid host session signature"}});
      }
      current->session_state = "connected";
      current->last_session_at = UtcNowSqlTimestamp();
      current->session_token = comet::RandomTokenBase64(32);
      current->session_expires_at = SqlTimestampAfterSeconds(HostSessionLifetimeSeconds());
      current->session_host_sequence = 0;
      current->session_controller_sequence = 0;
      current->status_message = body.value("status_message", std::string("session opened"));
      store.UpsertRegisteredHost(*current);
      AppendControllerEvent(
          store,
          "host-registry",
          "session-opened",
          "opened host-agent session",
          json::object(),
          "",
          node_name);
      return BuildJsonResponse(
          200,
          json{
              {"service", "comet-controller"},
              {"node_name", node_name},
              {"session_state", current->session_state},
              {"last_session_at", current->last_session_at},
              {"session_token", current->session_token},
              {"controller_public_key_fingerprint",
               current->controller_public_key_fingerprint},
              {"controller_sequence", current->session_controller_sequence},
          });
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/session/heartbeat") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto authenticated = AuthenticateHostSession(store, request);
      if (!authenticated.has_value()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
      }
      auto current = *authenticated;
      const json decrypted = ParseEncryptedHostRequestBody(store, request, &current, "session/heartbeat");
      const std::string node_name =
          decrypted.value("node_name", current.node_name);
      if (node_name.empty() || node_name != current.node_name) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "node mismatch for host heartbeat"}});
      }
      current.session_state = decrypted.value("session_state", std::string("connected"));
      current.last_heartbeat_at = UtcNowSqlTimestamp();
      current.last_session_at = current.last_heartbeat_at;
      current.status_message = decrypted.value("status_message", std::string("heartbeat"));
      store.UpsertRegisteredHost(current);
      return BuildEncryptedHostResponse(
          store,
          &current,
          "session/heartbeat",
          json{
              {"service", "comet-controller"},
              {"node_name", node_name},
              {"session_state", current.session_state},
              {"last_heartbeat_at", current.last_heartbeat_at},
          });
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/assignments/next") {
    if (request.method != "GET" && request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      std::optional<std::string> node_name = FindQueryString(request, "node");
      std::optional<comet::RegisteredHostRecord> authenticated;
      bool encrypted_request = false;
      if (request.method == "POST") {
        authenticated = AuthenticateHostSession(store, request);
        if (!authenticated.has_value()) {
          return BuildJsonResponse(
              403,
              json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
        }
        auto host = *authenticated;
        const json decrypted = ParseEncryptedHostRequestBody(store, request, &host, "assignments/next");
        node_name = decrypted.contains("node_name")
                        ? std::optional<std::string>(decrypted.value("node_name", std::string{}))
                        : node_name;
        authenticated = host;
        encrypted_request = true;
      }
      if (!node_name.has_value() || node_name->empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing required query parameter 'node'"}});
      }
      if (!authenticated.has_value()) {
        authenticated = AuthenticateHostSession(store, request, *node_name);
        if (!authenticated.has_value()) {
          return BuildJsonResponse(
              403,
              json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
        }
      }
      const auto assignment = store.ClaimNextHostAssignment(*node_name);
      const json payload{
          {"service", "comet-controller"},
          {"node_name", *node_name},
          {"assignment", assignment.has_value() ? BuildAssignmentPayloadItem(*assignment) : json(nullptr)},
      };
      if (encrypted_request) {
        auto host = *authenticated;
        return BuildEncryptedHostResponse(store, &host, "assignments/next", payload);
      }
      return BuildJsonResponse(200, payload);
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (StartsWithPath(request.path, "/api/v1/hostd/assignments/")) {
    const std::string remainder =
        request.path.substr(std::string("/api/v1/hostd/assignments/").size());
    const auto slash = remainder.find('/');
    if (slash == std::string::npos) {
      return BuildJsonResponse(404, json{{"status", "not_found"}});
    }
    const int assignment_id = std::stoi(remainder.substr(0, slash));
    const std::string action = remainder.substr(slash + 1);
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto assignment = store.LoadHostAssignment(assignment_id);
      if (!assignment.has_value()) {
        return BuildJsonResponse(404, json{{"status", "not_found"}, {"message", "assignment not found"}});
      }
      const auto authenticated =
          AuthenticateHostSession(store, request, assignment->node_name);
      if (!authenticated.has_value()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json body = ParseEncryptedHostRequestBody(
          store,
          request,
          &host,
          "assignments/" + std::to_string(assignment_id) + "/" + action);
      const std::string status_message = body.value("status_message", std::string{});
      if (action == "progress") {
        const bool updated = store.UpdateHostAssignmentProgress(assignment_id, body.dump());
        return BuildEncryptedHostResponse(
            store,
            &host,
            "assignments/" + std::to_string(assignment_id) + "/progress",
            json{{"service", "comet-controller"}, {"updated", updated}, {"assignment_id", assignment_id}});
      }
      if (action == "applied") {
        const bool updated = store.TransitionClaimedHostAssignment(
            assignment_id,
            comet::HostAssignmentStatus::Applied,
            status_message);
        if (updated && assignment->assignment_type == "apply-node-state") {
          const auto plane_assignments =
              store.LoadHostAssignments(std::nullopt, std::nullopt, assignment->plane_name);
          const auto latest_assignments_by_node =
              BuildLatestPlaneAssignmentsByNode(plane_assignments);
          const bool converged_generation =
              std::all_of(
                  latest_assignments_by_node.begin(),
                  latest_assignments_by_node.end(),
                  [&](const auto& entry) {
                    const auto& candidate = entry.second;
                    if (candidate.assignment_type != "apply-node-state" ||
                        candidate.desired_generation != assignment->desired_generation) {
                      return true;
                    }
                    return candidate.status == comet::HostAssignmentStatus::Applied ||
                           candidate.status == comet::HostAssignmentStatus::Superseded;
                  });
          if (converged_generation) {
            store.UpdatePlaneAppliedGeneration(
                assignment->plane_name,
                assignment->desired_generation);
          }
        }
        return BuildEncryptedHostResponse(
            store,
            &host,
            "assignments/" + std::to_string(assignment_id) + "/applied",
            json{{"service", "comet-controller"}, {"updated", updated}, {"assignment_id", assignment_id}});
      }
      if (action == "failed") {
        const bool retry = body.value("retry", false);
        const bool updated = retry
                                 ? store.TransitionClaimedHostAssignment(
                                       assignment_id,
                                       comet::HostAssignmentStatus::Pending,
                                       status_message)
                                 : store.TransitionClaimedHostAssignment(
                                       assignment_id,
                                       comet::HostAssignmentStatus::Failed,
                                       status_message);
        return BuildEncryptedHostResponse(
            store,
            &host,
            "assignments/" + std::to_string(assignment_id) + "/failed",
            json{
                {"service", "comet-controller"},
                {"updated", updated},
                {"assignment_id", assignment_id},
                {"retry", retry},
            });
      }
      return BuildJsonResponse(404, json{{"status", "not_found"}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/observations") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto authenticated = AuthenticateHostSession(store, request);
      if (!authenticated.has_value()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json body = ParseEncryptedHostRequestBody(store, request, &host, "observations/upsert");
      const auto observation = ParseHostObservationPayload(body);
      if (observation.node_name.empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing required field 'node_name'"}});
      }
      if (host.node_name != observation.node_name) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "node mismatch for host observation"}});
      }
      store.UpsertHostObservation(observation);
      return BuildEncryptedHostResponse(
          store,
          &host,
          "observations/upsert",
          json{{"service", "comet-controller"}, {"node_name", observation.node_name}, {"updated", true}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/events") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto authenticated = AuthenticateHostSession(store, request);
      if (!authenticated.has_value()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json body = ParseEncryptedHostRequestBody(store, request, &host, "events/append");
      const std::string node_name = body.value("node_name", std::string{});
      if (!node_name.empty() && node_name != host.node_name) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "node mismatch for event append"}});
      }
      store.AppendEvent(comet::EventRecord{
          0,
          body.value("plane_name", std::string{}),
          body.value("node_name", std::string{}),
          body.value("worker_name", std::string{}),
          body.contains("assignment_id") && !body.at("assignment_id").is_null()
              ? std::optional<int>(body.at("assignment_id").get<int>())
              : std::nullopt,
          body.contains("rollout_action_id") && !body.at("rollout_action_id").is_null()
              ? std::optional<int>(body.at("rollout_action_id").get<int>())
              : std::nullopt,
          body.value("category", std::string{}),
          body.value("event_type", std::string{}),
          body.value("severity", std::string("info")),
          body.value("message", std::string{}),
          body.value("payload_json", std::string("{}")),
          "",
      });
      return BuildEncryptedHostResponse(
          store,
          &host,
          "events/append",
          json{{"service", "comet-controller"}, {"appended", true}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/disk-runtime-state") {
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      if (request.method == "GET") {
        const auto disk_name = FindQueryString(request, "disk_name");
        const auto node_name = FindQueryString(request, "node");
        if (!disk_name.has_value() || !node_name.has_value()) {
          return BuildJsonResponse(
              400,
              json{{"status", "bad_request"}, {"message", "missing required query parameters 'disk_name' and 'node'"}});
        }
        const auto authenticated = AuthenticateHostSession(store, request, *node_name);
        if (!authenticated.has_value()) {
          return BuildJsonResponse(
              403,
              json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
        }
        const auto runtime_state = store.LoadDiskRuntimeState(*disk_name, *node_name);
        return BuildJsonResponse(
            200,
            json{
                {"service", "comet-controller"},
                {"runtime_state", runtime_state.has_value() ? BuildDiskRuntimeStatePayloadItem(*runtime_state) : json(nullptr)},
            });
      }
      if (request.method == "POST") {
        const auto authenticated = AuthenticateHostSession(store, request);
        if (!authenticated.has_value()) {
          return BuildJsonResponse(
              403,
              json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
        }
        auto host = *authenticated;
        const json body = ParseEncryptedHostRequestBody(store, request, &host, "disk-runtime-state/upsert");
        const auto runtime_state = ParseDiskRuntimeStatePayload(body);
        if (runtime_state.disk_name.empty() || runtime_state.node_name.empty()) {
          return BuildJsonResponse(
              400,
              json{{"status", "bad_request"}, {"message", "missing required fields 'disk_name' and 'node_name'"}});
        }
        if (host.node_name != runtime_state.node_name) {
          return BuildJsonResponse(
              403,
              json{{"status", "forbidden"}, {"message", "node mismatch for disk runtime state"}});
        }
        store.UpsertDiskRuntimeState(runtime_state);
        return BuildEncryptedHostResponse(
            store,
            &host,
            "disk-runtime-state/upsert",
            json{{"service", "comet-controller"}, {"updated", true}, {"disk_name", runtime_state.disk_name}});
      }
      if (request.method == "POST" && request.path == "/api/v1/hostd/disk-runtime-state/load") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/hostd/disk-runtime-state/load") {
    if (request.method != "POST") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto authenticated = AuthenticateHostSession(store, request);
      if (!authenticated.has_value()) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json body = ParseEncryptedHostRequestBody(store, request, &host, "disk-runtime-state/load");
      const std::string disk_name = body.value("disk_name", std::string{});
      const std::string node_name = body.value("node_name", std::string{});
      if (disk_name.empty() || node_name.empty()) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", "missing disk_name or node_name"}});
      }
      if (host.node_name != node_name) {
        return BuildJsonResponse(
            403,
            json{{"status", "forbidden"}, {"message", "node mismatch for disk runtime load"}});
      }
      const auto runtime_state = store.LoadDiskRuntimeState(disk_name, node_name);
      return BuildEncryptedHostResponse(
          store,
          &host,
          "disk-runtime-state/load",
          json{
              {"service", "comet-controller"},
              {"runtime_state", runtime_state.has_value() ? BuildDiskRuntimeStatePayloadItem(*runtime_state)
                                                          : json(nullptr)},
          });
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.method == "GET" && ui_root.has_value()) {
    if (const auto static_path = ResolveUiRequestPath(*ui_root, request.path);
        static_path.has_value()) {
      try {
        return BuildStaticFileResponse(*static_path);
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/planes") {
    if (request.method == "GET") {
      try {
        return BuildJsonResponse(200, BuildPlanesPayload(db_path));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (request.method == "POST") {
      try {
        const json body = ParseJsonRequestBody(request);
        const json desired_state_payload =
            body.contains("desired_state") ? body.at("desired_state") : body;
        if (!desired_state_payload.is_object()) {
          return BuildJsonResponse(
              400,
              json{{"status", "bad_request"},
                   {"message", "request body must contain desired_state object"}});
        }
        const std::string artifacts_root = ResolveArtifactsRoot(
            body.contains("artifacts_root") && body["artifacts_root"].is_string()
                ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
                : FindQueryString(request, "artifacts_root"),
            default_artifacts_root);
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteUpsertPlaneStateAction(
                    db_path,
                    desired_state_payload.dump(2),
                    artifacts_root,
                    std::nullopt,
                    "api")));
      } catch (const std::invalid_argument& error) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
  }
  if (StartsWithPath(request.path, "/api/v1/planes/")) {
    const std::string remainder = request.path.substr(std::string("/api/v1/planes/").size());
    if (remainder.empty()) {
      return BuildJsonResponse(404, json{{"status", "not_found"}});
    }
    const auto interaction_status_pos = remainder.find("/interaction/status");
    if (interaction_status_pos != std::string::npos &&
        interaction_status_pos + std::string("/interaction/status").size() == remainder.size()) {
      if (request.method != "GET") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string plane_name = remainder.substr(0, interaction_status_pos);
      try {
        return BuildJsonResponse(200, ResolvePlaneInteraction(db_path, plane_name).status_payload);
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            404,
            json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    const auto interaction_models_pos = remainder.find("/interaction/models");
    if (interaction_models_pos != std::string::npos &&
        interaction_models_pos + std::string("/interaction/models").size() == remainder.size()) {
      if (request.method != "GET") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string plane_name = remainder.substr(0, interaction_models_pos);
      try {
        return ProxyInteractionJson(
            ResolvePlaneInteraction(db_path, plane_name),
            "GET",
            "/v1/models");
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            404,
            json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    const auto interaction_chat_pos = remainder.find("/interaction/chat/completions");
    if (interaction_chat_pos != std::string::npos &&
        interaction_chat_pos + std::string("/interaction/chat/completions").size() ==
            remainder.size()) {
      if (request.method != "POST") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string plane_name = remainder.substr(0, interaction_chat_pos);
      try {
        const PlaneInteractionResolution resolution = ResolvePlaneInteraction(db_path, plane_name);
        if (!resolution.status_payload.value("interaction_enabled", false)) {
          return BuildPlaneInteractionError(
              409,
              resolution.status_payload,
              "interaction is available only for plane_mode=llm");
        }
        if (!resolution.status_payload.value("ready", false) || !resolution.target.has_value()) {
          return BuildPlaneInteractionError(
              409,
              resolution.status_payload,
              "plane interaction target is not ready");
        }
        return BuildInteractionSessionResponse(ExecuteInteractionSession(resolution, request.body));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            502,
            json{{"status", "error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    const auto interaction_stream_pos = remainder.find("/interaction/chat/completions/stream");
    if (interaction_stream_pos != std::string::npos &&
        interaction_stream_pos + std::string("/interaction/chat/completions/stream").size() ==
            remainder.size()) {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto start_pos = remainder.find("/start");
    if (start_pos != std::string::npos &&
        start_pos + std::string("/start").size() == remainder.size()) {
      if (request.method != "POST") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string plane_name = remainder.substr(0, start_pos);
      try {
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteStartPlaneAction(db_path, plane_name)));
      } catch (const std::invalid_argument& error) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    const auto stop_pos = remainder.find("/stop");
    if (stop_pos != std::string::npos &&
        stop_pos + std::string("/stop").size() == remainder.size()) {
      if (request.method != "POST") {
        return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
      }
      const std::string plane_name = remainder.substr(0, stop_pos);
      try {
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteStopPlaneAction(db_path, plane_name)));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (request.method == "DELETE" && remainder.find('/') == std::string::npos) {
      try {
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteDeletePlaneAction(db_path, remainder)));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (request.method == "PUT" && remainder.find('/') == std::string::npos) {
      try {
        const json body = ParseJsonRequestBody(request);
        const json desired_state_payload =
            body.contains("desired_state") ? body.at("desired_state") : body;
        if (!desired_state_payload.is_object()) {
          return BuildJsonResponse(
              400,
              json{{"status", "bad_request"},
                   {"message", "request body must contain desired_state object"}});
        }
        const std::string artifacts_root = ResolveArtifactsRoot(
            body.contains("artifacts_root") && body["artifacts_root"].is_string()
                ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
                : FindQueryString(request, "artifacts_root"),
            default_artifacts_root);
        return BuildJsonResponse(
            200,
            BuildControllerActionPayload(
                ExecuteUpsertPlaneStateAction(
                    db_path,
                    desired_state_payload.dump(2),
                    artifacts_root,
                    remainder,
                    "api")));
      } catch (const std::invalid_argument& error) {
        return BuildJsonResponse(
            400,
            json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    const auto dashboard_pos = remainder.find("/dashboard");
    if (dashboard_pos != std::string::npos &&
        dashboard_pos + std::string("/dashboard").size() == remainder.size()) {
      const std::string plane_name = remainder.substr(0, dashboard_pos);
      try {
        return BuildJsonResponse(
            200,
            BuildDashboardPayload(
                db_path,
                FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds()),
                plane_name));
      } catch (const std::exception& error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
      }
    }
    if (remainder.find('/') != std::string::npos) {
      return BuildJsonResponse(404, json{{"status", "not_found"}});
    }
    try {
      return BuildJsonResponse(200, BuildControllerStatePayload(db_path, remainder));
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
  if (request.path == "/api/v1/dashboard") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildDashboardPayload(
              db_path,
              FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds()),
              FindQueryString(request, "plane")));
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
              FindQueryString(request, "plane"),
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
          BuildDiskStatePayload(
              db_path,
              FindQueryString(request, "node"),
              FindQueryString(request, "plane")));
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
          BuildRolloutActionsPayload(
              db_path,
              FindQueryString(request, "node"),
              FindQueryString(request, "plane")));
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
              FindQueryInt(request, "stale_after").value_or(DefaultStaleAfterSeconds()),
              FindQueryString(request, "plane")));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/events") {
    if (request.method != "GET") {
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    }
    try {
      return BuildJsonResponse(
          200,
          BuildEventsPayload(
              db_path,
              FindQueryString(request, "plane"),
              FindQueryString(request, "node"),
              FindQueryString(request, "worker"),
              FindQueryString(request, "category"),
              FindQueryInt(request, "limit").value_or(100)));
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }
  if (request.path == "/api/v1/events/stream") {
    return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
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
                  ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))));
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

int ServeControllerApi(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const std::string& listen_host,
    int listen_port,
    const std::optional<std::filesystem::path>& ui_root) {
  g_stop_requested.store(false);
  ::signal(SIGINT, ControllerSignalHandler);
  ::signal(SIGTERM, ControllerSignalHandler);

  comet::ControllerStore store(db_path);
  store.Initialize();

  const int listen_fd = CreateListenSocket(listen_host, listen_port);
  std::cout << "comet-controller serve\n";
  std::cout << "listen=" << listen_host << ":" << listen_port << "\n";
  std::cout << "db=" << db_path << "\n";
  std::cout << "artifacts_root=" << default_artifacts_root << "\n";
  if (ui_root.has_value()) {
    std::cout << "ui_root=" << ui_root->string() << "\n";
  }
  std::cout << "routes=/health,/api/v1/health,/api/v1/bundles/validate,/api/v1/bundles/preview,/api/v1/bundles/import,/api/v1/bundles/apply,/api/v1/planes,/api/v1/planes/<plane>,/api/v1/planes/<plane>/dashboard,/api/v1/planes/<plane>/start,/api/v1/planes/<plane>/stop,/api/v1/planes/<plane>[DELETE],/api/v1/planes/<plane>/interaction/status,/api/v1/planes/<plane>/interaction/models,/api/v1/planes/<plane>/interaction/chat/completions,/api/v1/planes/<plane>/interaction/chat/completions/stream,/api/v1/state,/api/v1/dashboard,/api/v1/host-assignments,/api/v1/host-observations,/api/v1/host-health,/api/v1/disk-state,/api/v1/rollout-actions,/api/v1/rebalance-plan,/api/v1/events,/api/v1/events/stream,/api/v1/scheduler-tick,/api/v1/reconcile-rebalance-proposals,/api/v1/reconcile-rollout-actions,/api/v1/apply-rebalance-proposal,/api/v1/set-rollout-action-status,/api/v1/enqueue-rollout-eviction,/api/v1/apply-ready-rollout-action,/api/v1/node-availability,/api/v1/retry-host-assignment,/api/v1/hostd/hosts,/api/v1/hostd/hosts/<node>/revoke,/api/v1/hostd/hosts/<node>/rotate-key\n";
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
    std::size_t expected_request_bytes = 0;
    while (true) {
      const ssize_t read_count = recv(client_fd, buffer.data(), buffer.size(), 0);
      if (read_count <= 0) {
        break;
      }
      request_data.append(buffer.data(), static_cast<std::size_t>(read_count));
      if (expected_request_bytes == 0) {
        expected_request_bytes = ExpectedRequestBytes(request_data);
      }
      if (expected_request_bytes != 0 && request_data.size() >= expected_request_bytes) {
        break;
      }
    }

    if (!request_data.empty()) {
      const HttpRequest request = ParseHttpRequest(request_data);
      if (request.method == "GET" && request.path == "/api/v1/events/stream") {
        std::thread(
            [client_fd, db_path, request]() {
              StreamEventsSse(client_fd, db_path, request);
            })
            .detach();
        continue;
      }
      if (ParseInteractionStreamPlaneName(request).has_value()) {
        std::thread(
            [client_fd, db_path, request]() {
              StreamPlaneInteractionSse(client_fd, db_path, request);
            })
            .detach();
        continue;
      }
      const HttpResponse response =
          HandleControllerRequest(db_path, default_artifacts_root, request, ui_root);
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

std::string ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root) {
  return artifacts_root_arg.value_or(fallback_artifacts_root);
}

std::string ResolveWebUiRoot(const std::optional<std::string>& web_ui_root_arg) {
  return web_ui_root_arg.value_or(DefaultWebUiRoot());
}

WebUiComposeMode ResolveComposeMode(const std::optional<std::string>& compose_mode_arg) {
  if (!compose_mode_arg.has_value() || *compose_mode_arg == "exec") {
    return WebUiComposeMode::Exec;
  }
  if (*compose_mode_arg == "skip") {
    return WebUiComposeMode::Skip;
  }
  throw std::runtime_error("unsupported compose mode '" + *compose_mode_arg + "'");
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
std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation);

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds);

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
    const std::vector<comet::HostObservation>& observations,
    const std::optional<std::string>& node_name = std::nullopt) {
  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }
  std::map<std::string, comet::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
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
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters=" << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
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
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters=" << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
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

std::map<std::string, comet::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<comet::HostAssignment>& assignments) {
  std::map<std::string, comet::HostAssignment> latest_by_node;
  for (const auto& assignment : assignments) {
    auto it = latest_by_node.find(assignment.node_name);
    if (it == latest_by_node.end() || assignment.id >= it->second.id) {
      latest_by_node[assignment.node_name] = assignment;
    }
  }
  return latest_by_node;
}

int ComputeEffectivePlaneAppliedGeneration(
    const comet::PlaneRecord& plane,
    const std::optional<comet::DesiredState>& desired_state,
    const std::optional<int>& desired_generation,
    const std::vector<comet::HostObservation>& observations) {
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    return plane.applied_generation;
  }
  if (*desired_generation <= plane.applied_generation) {
    return plane.applied_generation;
  }
  for (const auto& node : desired_state->nodes) {
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      return plane.applied_generation;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation < *desired_generation ||
        observation->status == comet::HostObservationStatus::Failed) {
      return plane.applied_generation;
    }
  }
  return *desired_generation;
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
  std::optional<std::string> plane_name;
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
  std::optional<std::string> plane_name;
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
  std::vector<comet::PlaneRecord> planes;
  std::vector<comet::DesiredState> desired_states;
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RolloutActionsViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  view.actions =
      view.desired_state.has_value()
          ? store.LoadRolloutActions(view.desired_state->plane_name, node_name)
          : store.LoadRolloutActions(plane_name, node_name);

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
      const auto plane_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
      const auto plane_observations =
          FilterHostObservationsForPlane(
              store.LoadHostObservations(),
              view.desired_state->plane_name);
      view.lifecycle = BuildRolloutLifecycleEntries(
          *view.desired_state,
          *view.desired_generation,
          view.actions,
          plane_assignments,
          plane_observations);
    }
  }
  return view;
}

RebalancePlanViewData LoadRebalancePlanViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RebalancePlanViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.stale_after_seconds = stale_after_seconds;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.desired_generation =
      (plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                              : store.LoadDesiredGeneration())
          .value_or(0);
  const auto observations =
      FilterHostObservationsForPlane(
          store.LoadHostObservations(),
          view.desired_state->plane_name);
  const auto assignments =
      store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
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
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  StateAggregateViewData view;
  view.db_path = db_path;
  view.stale_after_seconds = stale_after_seconds;
  view.planes = store.LoadPlanes();
  view.desired_states = store.LoadDesiredStates();
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name) : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.disk_runtime_states = store.LoadDiskRuntimeStates(view.desired_state->plane_name);
  view.scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.observations =
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(), *plane_name)
                             : store.LoadHostObservations();
  view.assignments =
      plane_name.has_value()
          ? store.LoadHostAssignments(std::nullopt, std::nullopt, *plane_name)
          : store.LoadHostAssignments();
  view.availability_overrides = store.LoadNodeAvailabilityOverrides();
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto plane_rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
  view.rollout_lifecycle =
      view.desired_generation.has_value()
          ? BuildRolloutLifecycleEntries(
                *view.desired_state,
                *view.desired_generation,
                plane_rollout_actions,
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadDiskStateViewData(db_path, node_name, plane_name);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = view.desired_state;

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
  };
  if (!state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["items"] = json::array();
    return payload;
  }

  const auto runtime_states = view.runtime_states;
  const auto observations = view.observations;
  payload["plane_name"] = state->plane_name;
  payload["desired_generation"] =
      view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr);

  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }
  std::map<std::string, comet::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
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
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      const auto& telemetry = telemetry_it->second;
      item["usage_bytes"] = {
          {"total_bytes", telemetry.total_bytes},
          {"used_bytes", telemetry.used_bytes},
          {"free_bytes", telemetry.free_bytes},
      };
      item["io_bytes"] = {
          {"read_bytes", telemetry.read_bytes},
          {"write_bytes", telemetry.write_bytes},
      };
      item["io_ops"] = {
          {"read_ios", telemetry.read_ios},
          {"write_ios", telemetry.write_ios},
      };
      item["io_time_ms"] = telemetry.io_time_ms;
      item["weighted_io_time_ms"] = telemetry.weighted_io_time_ms;
      item["io_error_count"] = telemetry.io_error_count;
      item["io_in_progress"] = telemetry.io_in_progress;
      item["warning_count"] = telemetry.warning_count;
      item["fault_count"] = telemetry.fault_count;
      item["read_only"] = telemetry.read_only;
      item["perf_counters_available"] = telemetry.perf_counters_available;
      item["io_error_counters_available"] = telemetry.io_error_counters_available;
      item["mount_source"] =
          telemetry.mount_source.empty() ? json(nullptr) : json(telemetry.mount_source);
      item["fault_reasons"] = telemetry.fault_reasons;
      item["mount_health"] = telemetry.health.empty() ? json(nullptr) : json(telemetry.health);
      item["mounted"] = telemetry.mounted;
      item["telemetry_status_message"] =
          telemetry.status_message.empty() ? json(nullptr) : json(telemetry.status_message);
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

json BuildDashboardPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadStateAggregateViewData(db_path, stale_after_seconds, plane_name);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto recent_events =
      store.LoadEvents(plane_name, std::nullopt, std::nullopt, std::nullopt, 10);
  const auto rollout_actions =
      plane_name.has_value() ? store.LoadRolloutActions(*plane_name)
                             : store.LoadRolloutActions();

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"stale_after_seconds", stale_after_seconds},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"desired_generation", view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr)},
  };
  if (!view.desired_state.has_value()) {
    payload["plane"] = nullptr;
    payload["planes"] = json::array();
    payload["nodes"] = json::array();
    payload["runtime"] = {
        {"observed_nodes", 0},
        {"ready_nodes", 0},
        {"not_ready_nodes", 0},
        {"degraded_gpu_telemetry_nodes", 0},
    };
    payload["assignments"] = {
        {"total", 0},
        {"pending", 0},
        {"claimed", 0},
        {"applied", 0},
        {"failed", 0},
        {"by_node", json::array()},
    };
    payload["rollout"] = {
        {"total_actions", 0},
        {"pending", 0},
        {"acknowledged", 0},
        {"ready_to_retry", 0},
        {"workers", json::array()},
    };
    payload["alerts"] = {
        {"critical", 0},
        {"warning", 0},
        {"booting", 0},
        {"total", 0},
        {"items", json::array()},
    };
    payload["recent_events"] = json::array();
    return payload;
  }

  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : view.observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  int effective_plane_applied_generation = 0;
  std::optional<comet::PlaneRecord> dashboard_plane_record;
  for (const auto& plane : view.planes) {
    if (plane.name != view.desired_state->plane_name) {
      continue;
    }
    dashboard_plane_record = plane;
    effective_plane_applied_generation = ComputeEffectivePlaneAppliedGeneration(
        plane,
        view.desired_state,
        view.desired_generation,
        view.observations);
    if (effective_plane_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_plane_applied_generation);
    }
    break;
  }
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(view.availability_overrides);

  payload["plane"] = {
      {"plane_name", view.desired_state->plane_name},
      {"plane_mode", comet::ToString(view.desired_state->plane_mode)},
      {"state",
       [&]() -> json {
         return dashboard_plane_record.has_value() ? json(dashboard_plane_record->state)
                                                   : json(nullptr);
       }()},
      {"desired_generation", view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr)},
      {"applied_generation", dashboard_plane_record.has_value() ? json(effective_plane_applied_generation)
                                                                 : json(nullptr)},
      {"staged_update",
       dashboard_plane_record.has_value()
           ? dashboard_plane_record->generation > effective_plane_applied_generation
           : false},
      {"node_count", view.desired_state->nodes.size()},
      {"instance_count", view.desired_state->instances.size()},
      {"disk_count", view.desired_state->disks.size()},
      {"shared_disk_name", view.desired_state->plane_shared_disk_name},
      {"control_root", view.desired_state->control_root},
      {"bootstrap_model", BuildBootstrapModelPayloadItem(view.desired_state->bootstrap_model)},
  };
  json plane_items = json::array();
  for (const auto& plane : view.planes) {
    const auto desired_state_it =
        std::find_if(
            view.desired_states.begin(),
            view.desired_states.end(),
            [&](const comet::DesiredState& candidate) {
              return candidate.plane_name == plane.name;
            });
    const int plane_applied_generation =
        plane.name == view.desired_state->plane_name ? effective_plane_applied_generation
                                                     : plane.applied_generation;
    plane_items.push_back(json{
        {"plane_name", plane.name},
        {"plane_mode",
         desired_state_it != view.desired_states.end()
             ? json(comet::ToString(desired_state_it->plane_mode))
             : json(plane.plane_mode)},
        {"state", plane.state},
        {"generation", plane.generation},
        {"applied_generation", plane_applied_generation},
        {"staged_update", plane.generation > plane_applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"node_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->nodes.size())
                                                       : json(nullptr)},
        {"instance_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->instances.size())
                                                       : json(nullptr)},
        {"disk_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->disks.size())
                                                       : json(nullptr)},
    });
  }
  payload["planes"] = std::move(plane_items);

  json nodes = json::array();
  int observed_nodes = 0;
  int ready_nodes = 0;
  int not_ready_nodes = 0;
  int degraded_gpu_nodes = 0;
  const std::optional<std::string> selected_plane_state =
      plane_name.has_value()
          ? [&]() -> std::optional<std::string> {
              for (const auto& plane : view.planes) {
                if (plane.name == *plane_name) {
                  return plane.state;
                }
              }
              return std::nullopt;
            }()
          : std::nullopt;
  std::map<std::string, comet::NodeInventory> dashboard_nodes;
  if (plane_name.has_value()) {
    for (const auto& node : view.desired_state->nodes) {
      dashboard_nodes.emplace(node.name, node);
    }
  } else {
    for (const auto& state : view.desired_states) {
      for (const auto& node : state.nodes) {
        dashboard_nodes.emplace(node.name, node);
      }
    }
  }
  for (const auto& [dashboard_node_name, node] : dashboard_nodes) {
    (void)dashboard_node_name;
    int desired_instance_count = 0;
    int desired_disk_count = 0;
    int desired_plane_count = 0;
    std::set<std::string> node_planes;
    if (plane_name.has_value()) {
      for (const auto& instance : view.desired_state->instances) {
        if (instance.node_name == node.name) {
          ++desired_instance_count;
          node_planes.insert(view.desired_state->plane_name);
        }
      }
      for (const auto& disk : view.desired_state->disks) {
        if (disk.node_name == node.name) {
          ++desired_disk_count;
          node_planes.insert(view.desired_state->plane_name);
        }
      }
    } else {
      for (const auto& state : view.desired_states) {
        for (const auto& instance : state.instances) {
          if (instance.node_name == node.name) {
            ++desired_instance_count;
            node_planes.insert(state.plane_name);
          }
        }
        for (const auto& disk : state.disks) {
          if (disk.node_name == node.name) {
            ++desired_disk_count;
            node_planes.insert(state.plane_name);
          }
        }
      }
    }
    desired_plane_count = static_cast<int>(node_planes.size());

    json item{
        {"node_name", node.name},
        {"availability", comet::ToString(ResolveNodeAvailability(availability_override_map, node.name))},
        {"plane_count", desired_plane_count},
        {"planes", json(node_planes)},
        {"desired_instance_count", desired_instance_count},
        {"desired_disk_count", desired_disk_count},
        {"gpu_count", node.gpu_devices.size()},
    };
    const auto observation_it = observation_by_node.find(node.name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      item["runtime_launch_ready"] = nullptr;
      item["runtime_phase"] = nullptr;
      nodes.push_back(std::move(item));
      continue;
    }

    ++observed_nodes;
    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    item["health"] = HealthFromAge(age_seconds, stale_after_seconds);
    item["status"] = comet::ToString(observation_it->second.status);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_phase"] =
          runtime_status->runtime_phase.empty() ? json(nullptr) : json(runtime_status->runtime_phase);
      item["runtime_backend"] =
          runtime_status->runtime_backend.empty() ? json(nullptr) : json(runtime_status->runtime_backend);
      if (runtime_status->launch_ready) {
        ++ready_nodes;
      } else {
        ++not_ready_nodes;
      }
    } else {
      const auto fallback = DetermineDashboardRuntimeFallback(
          observation_it->second,
          node.name,
          plane_name,
          selected_plane_state,
          view.desired_generation.value_or(0),
          desired_instance_count,
          desired_disk_count,
          item.value("health", std::string("unknown")));
      if (fallback.available) {
        item["runtime_launch_ready"] = fallback.launch_ready;
        item["runtime_phase"] =
            fallback.runtime_phase.empty() ? json(nullptr) : json(fallback.runtime_phase);
        if (fallback.launch_ready) {
          ++ready_nodes;
        } else {
          ++not_ready_nodes;
        }
      } else {
        item["runtime_launch_ready"] = nullptr;
        item["runtime_phase"] = nullptr;
        ++not_ready_nodes;
      }
    }
    if (const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
        gpu_telemetry.has_value()) {
      item["telemetry_degraded"] = gpu_telemetry->degraded;
      if (gpu_telemetry->degraded) {
        ++degraded_gpu_nodes;
      }
    }
    nodes.push_back(std::move(item));
  }
  payload["nodes"] = std::move(nodes);
  payload["runtime"] = {
      {"observed_nodes", observed_nodes},
      {"ready_nodes", ready_nodes},
      {"not_ready_nodes", not_ready_nodes},
      {"degraded_gpu_telemetry_nodes", degraded_gpu_nodes},
  };

  int pending_assignments = 0;
  int claimed_assignments = 0;
  int applied_assignments = 0;
  int failed_assignments = 0;
  const auto latest_assignments_by_node = BuildLatestPlaneAssignmentsByNode(view.assignments);
  json latest_progress = nullptr;
  int latest_progress_assignment_id = -1;
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    switch (assignment.status) {
      case comet::HostAssignmentStatus::Pending:
        ++pending_assignments;
        break;
      case comet::HostAssignmentStatus::Claimed:
        ++claimed_assignments;
        break;
      case comet::HostAssignmentStatus::Applied:
        ++applied_assignments;
        break;
      case comet::HostAssignmentStatus::Failed:
        ++failed_assignments;
        break;
      default:
        break;
    }
    if ((assignment.status == comet::HostAssignmentStatus::Pending ||
         assignment.status == comet::HostAssignmentStatus::Claimed) &&
        !assignment.progress_json.empty() &&
        assignment.progress_json != "{}" &&
        assignment.id > latest_progress_assignment_id) {
      latest_progress = json::parse(assignment.progress_json);
      latest_progress_assignment_id = assignment.id;
    }
  }
  json assignment_nodes = json::array();
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    assignment_nodes.push_back(json{
        {"node_name", assignment.node_name},
        {"latest_assignment_id", assignment.id},
        {"latest_status", comet::ToString(assignment.status)},
        {"latest_progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
        {"pending", assignment.status == comet::HostAssignmentStatus::Pending ? 1 : 0},
        {"claimed", assignment.status == comet::HostAssignmentStatus::Claimed ? 1 : 0},
        {"failed", assignment.status == comet::HostAssignmentStatus::Failed ? 1 : 0},
    });
  }
  payload["assignments"] = {
      {"total", latest_assignments_by_node.size()},
      {"pending", pending_assignments},
      {"claimed", claimed_assignments},
      {"applied", applied_assignments},
      {"failed", failed_assignments},
      {"latest_progress", latest_progress},
      {"by_node", std::move(assignment_nodes)},
  };

  int pending_rollout = 0;
  int acknowledged_rollout = 0;
  int ready_rollout = 0;
  std::set<std::string> rollout_workers;
  for (const auto& action : rollout_actions) {
    rollout_workers.insert(action.worker_name);
    if (action.status == comet::RolloutActionStatus::Pending) {
      ++pending_rollout;
    } else if (action.status == comet::RolloutActionStatus::Acknowledged) {
      ++acknowledged_rollout;
    } else if (action.status == comet::RolloutActionStatus::ReadyToRetry) {
      ++ready_rollout;
    }
  }
  payload["rollout"] = {
      {"total_actions", rollout_actions.size()},
      {"pending", pending_rollout},
      {"acknowledged", acknowledged_rollout},
      {"ready_to_retry", ready_rollout},
      {"workers", json(rollout_workers)},
      {"loop_status", view.loop_status.state},
      {"loop_reason", view.loop_status.reason},
  };

  int critical_alerts = 0;
  int warning_alerts = 0;
  int booting_alerts = 0;
  json alert_items = json::array();
  const auto push_alert =
      [&](const std::string& severity,
          const std::string& kind,
          const std::string& title,
          const std::string& detail,
          const std::optional<std::string>& node_name = std::nullopt,
          const std::optional<std::string>& worker_name = std::nullopt,
          const std::optional<int>& assignment_id = std::nullopt,
          const std::optional<int>& event_id = std::nullopt) {
        if (severity == "critical") {
          ++critical_alerts;
        } else if (severity == "warning") {
          ++warning_alerts;
        } else if (severity == "booting") {
          ++booting_alerts;
        }
        json item{
            {"severity", severity},
            {"kind", kind},
            {"title", title},
            {"detail", detail},
            {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
            {"worker_name", worker_name.has_value() ? json(*worker_name) : json(nullptr)},
            {"assignment_id", assignment_id.has_value() ? json(*assignment_id) : json(nullptr)},
            {"event_id", event_id.has_value() ? json(*event_id) : json(nullptr)},
        };
        alert_items.push_back(std::move(item));
      };

  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    if (assignment.status == comet::HostAssignmentStatus::Failed) {
      push_alert(
          "critical",
          "failed-assignment",
          "Assignment failed",
          "Host assignment failed and requires retry or operator action.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    } else if (
        assignment.status == comet::HostAssignmentStatus::Pending ||
        assignment.status == comet::HostAssignmentStatus::Claimed) {
      push_alert(
          "booting",
          "assignment-in-flight",
          "Assignment in progress",
          "Host assignment is still pending or claimed.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    }
  }

  for (const auto& [dashboard_node_name, item] : dashboard_nodes) {
    (void)dashboard_node_name;
    const auto observation_it = observation_by_node.find(item.name);
    if (observation_it == observation_by_node.end()) {
      push_alert(
          "warning",
          "missing-observation",
          "Node has no observation",
          "Controller does not have a recent observation for this node.",
          item.name);
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, stale_after_seconds);
    if (health == "failed" || health == "stale") {
      push_alert(
          "critical",
          "node-health",
          "Node heartbeat is stale",
          "Observed state for this node is stale or failed.",
          item.name);
    }

    const auto availability =
        ResolveNodeAvailability(availability_override_map, item.name);
    if (availability != comet::NodeAvailability::Active) {
      push_alert(
          "warning",
          "node-availability",
          "Node is not active",
          "Node availability override is blocking normal scheduling.",
          item.name);
    }

    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      if (!runtime_status->launch_ready) {
        push_alert(
            "booting",
            "runtime-not-ready",
            "Runtime still starting",
            "Node runtime is not launch-ready yet.",
            item.name);
      }
    } else {
      const auto fallback = DetermineDashboardRuntimeFallback(
          observation_it->second,
          item.name,
          plane_name,
          selected_plane_state,
          view.desired_generation.value_or(0),
          std::count_if(
              view.desired_state->instances.begin(),
              view.desired_state->instances.end(),
              [&](const auto& instance) { return instance.node_name == item.name; }),
          std::count_if(
              view.desired_state->disks.begin(),
              view.desired_state->disks.end(),
              [&](const auto& disk) { return disk.node_name == item.name; }),
          health);
      if (!fallback.available) {
        push_alert(
            "booting",
            "runtime-missing",
            "Runtime status missing",
            "No runtime status has been reported yet for this node.",
            item.name);
      } else if (!fallback.launch_ready) {
        push_alert(
            "booting",
            "runtime-transition",
            "Runtime transition in progress",
            "Observed runtime state is converging even though low-level runtime status is not available yet.",
            item.name);
      }
    }

    if (const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
        gpu_telemetry.has_value() && gpu_telemetry->degraded) {
      push_alert(
          "warning",
          "gpu-telemetry-degraded",
          "GPU telemetry degraded",
          "GPU telemetry is running in degraded mode on this node.",
          item.name);
    }
  }

  for (const auto& action : rollout_actions) {
    push_alert(
        "warning",
        "rollout-action",
        "Deferred rollout requires follow-up",
        action.action + " for worker " + action.worker_name,
        action.target_node_name.empty() ? std::nullopt
                                        : std::optional<std::string>(action.target_node_name),
        action.worker_name);
  }

  json recent_items = json::array();
  int surfaced_event_alerts = 0;
  for (const auto& event : recent_events) {
    if ((event.severity == "error" || event.severity == "warning") &&
        surfaced_event_alerts < 5) {
      push_alert(
          event.severity == "error" ? "critical" : "warning",
          "event-log",
          event.category + "." + event.event_type,
          event.message,
          event.node_name.empty() ? std::nullopt
                                  : std::optional<std::string>(event.node_name),
          event.worker_name.empty() ? std::nullopt
                                    : std::optional<std::string>(event.worker_name),
          std::nullopt,
          event.id);
      ++surfaced_event_alerts;
    }
    recent_items.push_back(BuildEventPayloadItem(event));
  }
  payload["alerts"] = {
      {"critical", critical_alerts},
      {"warning", warning_alerts},
      {"booting", booting_alerts},
      {"total", critical_alerts + warning_alerts + booting_alerts},
      {"items", std::move(alert_items)},
  };
  payload["recent_events"] = std::move(recent_items);
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name, plane_name);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
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

json BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  const auto view =
      LoadEventsViewData(db_path, plane_name, node_name, worker_name, category, limit);
  json items = json::array();
  for (const auto& event : view.events) {
    items.push_back(BuildEventPayloadItem(event));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"worker_name", view.worker_name.has_value() ? json(*view.worker_name) : json(nullptr)},
      {"category", view.category.has_value() ? json(*view.category) : json(nullptr)},
      {"limit", view.limit},
      {"events", std::move(items)},
  };
}

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  const auto view =
      LoadRebalancePlanViewData(db_path, node_name, stale_after_seconds, plane_name);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
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
              << " updated_at=" << FormatDisplayTimestamp(availability_override.updated_at)
              << "\n";
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

std::string ResolvedDockerCommand() {
  if (std::system("docker version >/dev/null 2>&1") == 0) {
    return "docker";
  }
  const std::string windows_docker =
      "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe";
  if (std::filesystem::exists(windows_docker) &&
      std::system(("\"" + windows_docker + "\" version >/dev/null 2>&1").c_str()) == 0) {
    return "\"" + windows_docker + "\"";
  }
  throw std::runtime_error("no working Docker CLI found for web-ui lifecycle");
}

void RunCommand(const std::string& command) {
  if (std::system(command.c_str()) != 0) {
    throw std::runtime_error("command failed: " + command);
  }
}

std::string WebUiComposePath(const std::string& web_ui_root) {
  return (std::filesystem::path(web_ui_root) / "docker-compose.yml").string();
}

std::string WebUiStatePath(const std::string& web_ui_root) {
  return (std::filesystem::path(web_ui_root) / "web-ui-state.json").string();
}

json LoadWebUiStateJson(const std::string& web_ui_root) {
  const std::string path = WebUiStatePath(web_ui_root);
  if (!std::filesystem::exists(path)) {
    return json::object();
  }
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open web-ui state file: " + path);
  }
  return json::parse(input, nullptr, true, true);
}

void SaveWebUiStateJson(const std::string& web_ui_root, const json& state) {
  WriteTextFile(WebUiStatePath(web_ui_root), state.dump(2) + "\n");
}

std::string RenderWebUiComposeYaml(
    const std::string& image,
    int listen_port,
    const std::string& controller_upstream) {
  std::ostringstream out;
  out << "services:\n";
  out << "  comet-web-ui:\n";
  out << "    image: " << image << "\n";
  out << "    container_name: comet-web-ui\n";
  out << "    restart: unless-stopped\n";
  out << "    environment:\n";
  out << "      COMET_CONTROLLER_UPSTREAM: " << controller_upstream << "\n";
  out << "    security_opt:\n";
  out << "      - apparmor=unconfined\n";
  out << "    extra_hosts:\n";
  out << "      - \"host.docker.internal:host-gateway\"\n";
  out << "    ports:\n";
  out << "      - \"" << listen_port << ":8080\"\n";
  return out.str();
}

bool WebUiComposeRunning(const std::string& web_ui_root) {
  const std::string compose_path = WebUiComposePath(web_ui_root);
  if (!std::filesystem::exists(compose_path)) {
    return false;
  }
  try {
    const std::string command =
        ResolvedDockerCommand() + " compose -f '" + compose_path +
        "' ps --services --status running | grep -Fx 'comet-web-ui' >/dev/null 2>&1";
    return std::system(command.c_str()) == 0;
  } catch (...) {
    return false;
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

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNodePlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name,
    const std::string& plane_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name || assignment.plane_name != plane_name) {
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

  std::size_t observed_disk_count = 0;
  std::size_t observed_instance_count = 0;
  for (const auto& disk : observed_node_state.disks) {
    if (disk.node_name == node_name && disk.plane_name == desired_state.plane_name) {
      ++observed_disk_count;
    }
  }
  for (const auto& instance : observed_node_state.instances) {
    if (instance.node_name == node_name && instance.plane_name == desired_state.plane_name) {
      ++observed_instance_count;
    }
  }

  if (!desired_node_state.disks.empty() && observed_disk_count == 0) {
    return true;
  }
  if (!desired_node_state.instances.empty() && observed_instance_count == 0) {
    return true;
  }
  return false;
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

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
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

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
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

comet::DesiredState BuildStoppedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState stopped_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  stopped_state.instances.clear();
  return stopped_state;
}

comet::DesiredState BuildDeletedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState deleted_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  deleted_state.instances.clear();
  deleted_state.disks.clear();
  deleted_state.plane_shared_disk_name.clear();
  deleted_state.control_root.clear();
  return deleted_state;
}

std::vector<comet::HostAssignment> BuildStopPlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "stop-plane-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            BuildStoppedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane stop lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::vector<comet::HostAssignment> BuildDeletePlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  for (const auto& node : desired_state.nodes) {
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "delete-plane-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            BuildDeletedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane delete lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
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

std::optional<comet::DiskTelemetrySnapshot> ParseDiskTelemetry(
    const comet::HostObservation& observation) {
  if (observation.disk_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeDiskTelemetryJson(observation.disk_telemetry_json);
}

std::optional<comet::NetworkTelemetrySnapshot> ParseNetworkTelemetry(
    const comet::HostObservation& observation) {
  if (observation.network_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeNetworkTelemetryJson(observation.network_telemetry_json);
}

std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation) {
  if (observation.cpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeCpuTelemetryJson(observation.cpu_telemetry_json);
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
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    const auto network_telemetry = ParseNetworkTelemetry(observation);
    const auto cpu_telemetry = ParseCpuTelemetry(observation);
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
              << " heartbeat_at=" << FormatDisplayTimestamp(observation.heartbeat_at);
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
    if (disk_telemetry.has_value()) {
      std::cout << " disk_telemetry_source="
                << (disk_telemetry->source.empty() ? "(empty)" : disk_telemetry->source)
                << " disk_count=" << disk_telemetry->items.size();
      std::uint64_t total_read_bytes = 0;
      std::uint64_t total_write_bytes = 0;
      int total_fault_count = 0;
      int total_warning_count = 0;
      for (const auto& disk : disk_telemetry->items) {
        total_read_bytes += disk.read_bytes;
        total_write_bytes += disk.write_bytes;
        total_fault_count += disk.fault_count;
        total_warning_count += disk.warning_count;
      }
      std::cout << " disk_read_bytes=" << total_read_bytes
                << " disk_write_bytes=" << total_write_bytes
                << " disk_faults=" << total_fault_count
                << " disk_warnings=" << total_warning_count;
    }
    if (network_telemetry.has_value()) {
      std::cout << " network_telemetry_source="
                << (network_telemetry->source.empty() ? "(empty)" : network_telemetry->source)
                << " net_ifaces=" << network_telemetry->interfaces.size();
    }
    if (cpu_telemetry.has_value()) {
      std::cout << " cpu_telemetry_source="
                << (cpu_telemetry->source.empty() ? "(empty)" : cpu_telemetry->source)
                << " cpu_utilization_pct=" << static_cast<int>(cpu_telemetry->utilization_pct)
                << " cpu_cores=" << cpu_telemetry->core_count;
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
    if (disk_telemetry.has_value()) {
      for (const auto& disk : disk_telemetry->items) {
        std::cout << "    disk name=" << disk.disk_name
                  << " phase=" << (disk.runtime_state.empty() ? "(empty)" : disk.runtime_state)
                  << " mounted=" << (disk.mounted ? "yes" : "no")
                  << " health=" << (disk.health.empty() ? "(empty)" : disk.health)
                  << " used_bytes=" << disk.used_bytes
                  << " free_bytes=" << disk.free_bytes
                  << " read_bytes=" << disk.read_bytes
                  << " write_bytes=" << disk.write_bytes
                  << " read_ios=" << disk.read_ios
                  << " write_ios=" << disk.write_ios
                  << " io_time_ms=" << disk.io_time_ms
                  << " io_in_progress=" << disk.io_in_progress
                  << " fault_count=" << disk.fault_count
                  << " warning_count=" << disk.warning_count
                  << " perf_counters=" << (disk.perf_counters_available ? "yes" : "no")
                  << " io_error_counters=" << (disk.io_error_counters_available ? "yes" : "no")
                  << " read_only=" << (disk.read_only ? "yes" : "no");
        if (!disk.mount_point.empty()) {
          std::cout << " mount_point=" << disk.mount_point;
        }
        if (!disk.mount_source.empty()) {
          std::cout << " mount_source=" << disk.mount_source;
        }
        if (!disk.filesystem_type.empty()) {
          std::cout << " filesystem=" << disk.filesystem_type;
        }
        if (!disk.fault_reasons.empty()) {
          std::cout << " faults=";
          for (std::size_t index = 0; index < disk.fault_reasons.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << disk.fault_reasons[index];
          }
        }
        std::cout << "\n";
      }
    }
    if (network_telemetry.has_value()) {
      for (const auto& interface : network_telemetry->interfaces) {
        std::cout << "    net iface=" << interface.interface_name
                  << " oper_state=" << (interface.oper_state.empty() ? "(empty)" : interface.oper_state)
                  << " link_state=" << (interface.link_state.empty() ? "(empty)" : interface.link_state)
                  << " rx_bytes=" << interface.rx_bytes
                  << " tx_bytes=" << interface.tx_bytes
                  << " loopback=" << (interface.loopback ? "yes" : "no")
                  << "\n";
      }
    }
    if (cpu_telemetry.has_value()) {
      std::cout << "    cpu loadavg="
                << cpu_telemetry->loadavg_1m << ","
                << cpu_telemetry->loadavg_5m << ","
                << cpu_telemetry->loadavg_15m
                << " mem_used_bytes=" << cpu_telemetry->used_memory_bytes
                << " mem_total_bytes=" << cpu_telemetry->total_memory_bytes
                << " degraded=" << (cpu_telemetry->degraded ? "yes" : "no")
                << "\n";
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
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(
      desired_state.plane_name, desired_generation, scheduling_report.rollout_actions);
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
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, DefaultArtifactsRoot());
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendControllerEvent(
      store,
      "bundle",
      "imported",
      "imported bundle into desired state; rollout is staged until explicit start",
      json{
          {"bundle_dir", bundle_dir},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name);
  std::cout << "imported bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto current_state = store.LoadDesiredState(desired_state.plane_name);
  comet::RequireSchedulingPolicy(desired_state);
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
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
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendControllerEvent(
      store,
      "bundle",
      "applied",
      "applied bundle into desired state; rollout is staged until explicit start",
      json{
          {"bundle_dir", bundle_dir},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name);
  std::cout << "applied bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
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

int ShowRegisteredHosts(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const json payload = BuildRegisteredHostsPayload(db_path, node_name);
  std::cout << payload.dump(2) << "\n";
  return 0;
}

int ShowHostObservations(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view =
      LoadHostObservationsViewData(db_path, node_name, plane_name, stale_after_seconds);
  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name, plane_name);

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
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRebalancePlanViewData(
      db_path,
      node_name,
      DefaultStaleAfterSeconds(),
      plane_name);
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

void PrintEvents(const std::vector<comet::EventRecord>& events) {
  std::cout << "events:\n";
  if (events.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& event : events) {
    std::cout << "  - id=" << event.id
              << " category=" << event.category
              << " type=" << event.event_type
              << " severity=" << event.severity;
    if (!event.plane_name.empty()) {
      std::cout << " plane=" << event.plane_name;
    }
    if (!event.node_name.empty()) {
      std::cout << " node=" << event.node_name;
    }
    if (!event.worker_name.empty()) {
      std::cout << " worker=" << event.worker_name;
    }
    if (event.assignment_id.has_value()) {
      std::cout << " assignment_id=" << *event.assignment_id;
    }
    if (event.rollout_action_id.has_value()) {
      std::cout << " rollout_action_id=" << *event.rollout_action_id;
    }
    std::cout << " at=" << FormatDisplayTimestamp(event.created_at)
              << " message="
              << (event.message.empty() ? "(empty)" : event.message)
              << "\n";
  }
}

int ShowEvents(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  const auto view =
      LoadEventsViewData(db_path, plane_name, node_name, worker_name, category, limit);
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "limit: " << view.limit << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node: " << *view.node_name << "\n";
  }
  if (view.worker_name.has_value()) {
    std::cout << "worker: " << *view.worker_name << "\n";
  }
  if (view.category.has_value()) {
    std::cout << "category: " << *view.category << "\n";
  }
  PrintEvents(view.events);
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
  const auto updated_action = FindRolloutActionById(store.LoadRolloutActions(), action_id);
  if (updated_action.has_value()) {
    AppendControllerEvent(
        store,
        "rollout-action",
        "status-updated",
        "updated rollout action status",
        json{
            {"status", comet::ToString(status)},
            {"status_message", status_message.value_or("")},
            {"action", updated_action->action},
            {"step", updated_action->step},
        },
        updated_action->plane_name,
        updated_action->target_node_name,
        updated_action->worker_name,
        std::nullopt,
        action_id);
  }
  std::cout << "updated rollout action id=" << action_id
            << " status=" << comet::ToString(status) << "\n";
  if (updated_action.has_value()) {
    PrintPersistedRolloutActions(store.LoadRolloutActions(updated_action->plane_name));
  } else {
    PrintPersistedRolloutActions(store.LoadRolloutActions());
  }
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

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
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
  AppendControllerEvent(
      store,
      "rollout-action",
      "eviction-enqueued",
      message.str(),
      json{
          {"victims", action->victim_worker_names},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
      },
      desired_state->plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id);

  std::cout << "enqueued rollout eviction action id=" << action_id << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions(desired_state->plane_name));
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

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
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
  store.ReplaceRolloutActions(
      updated_state.plane_name, next_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          scheduling_report));
  AppendControllerEvent(
      store,
      "rollout-action",
      "retry-placement-applied",
      "materialized ready rollout action",
      json{
          {"desired_generation", next_generation},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
          {"victims", victim_worker_names},
      },
      updated_state.plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id);

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
          store.LoadRolloutActions(desired_state->plane_name),
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
  store.ReplaceRolloutActions(
      updated_state.plane_name,
      next_generation,
      updated_scheduling_report.rollout_actions);
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
  AppendControllerEvent(
      store,
      "scheduler",
      "rebalance-materialized",
      "materialized safe-direct rebalance proposal",
      json{
          {"desired_generation", next_generation},
          {"source_node", rebalance_it->current_node_name},
          {"source_gpu", rebalance_it->current_gpu_device},
          {"target_node", rebalance_it->target_node_name},
          {"target_gpu", rebalance_it->target_gpu_device},
          {"action", rebalance_it->action},
          {"score", rebalance_it->score},
      },
      updated_state.plane_name,
      rebalance_it->target_node_name,
      rebalance_it->worker_name);

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
          store.LoadRolloutActions(desired_state->plane_name),
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
    store.ReplaceRolloutActions(
        rollback_state.plane_name,
        rollback_generation,
        rollback_report.rollout_actions);
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
    AppendControllerEvent(
        store,
        "scheduler",
        "rollback-applied",
        updated_runtime.status_message,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", rollback_generation},
            {"phase", updated_runtime.phase},
        },
        rollback_state.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name);
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
    AppendControllerEvent(
        store,
        "scheduler",
        "move-verified",
        verification.detail,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
            {"stable_samples", updated_runtime.stable_samples},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name);
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
    AppendControllerEvent(
        store,
        "scheduler",
        "rollback-planned",
        verification.detail,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name,
        std::nullopt,
        std::nullopt,
        "warning");
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
  AppendControllerEvent(
      store,
      "scheduler",
      "manual-intervention-required",
      verification.detail,
      json{
          {"worker", updated_runtime.active_worker_name},
          {"generation", updated_runtime.action_generation},
          {"phase", updated_runtime.phase},
      },
      updated_runtime.plane_name,
      updated_runtime.target_node_name,
      updated_runtime.active_worker_name,
      std::nullopt,
      std::nullopt,
      "error");
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

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
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

  const auto all_rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
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

    auto current_action = FindRolloutActionById(
        store.LoadRolloutActions(desired_state->plane_name), action.id);
    if (!current_action.has_value()) {
      continue;
    }

    const auto prior_evict_action = FindPriorRolloutActionForWorker(
        store.LoadRolloutActions(desired_state->plane_name),
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
      current_action = FindRolloutActionById(
          store.LoadRolloutActions(desired_state->plane_name), action.id);
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
  PrintPersistedRolloutActions(store.LoadRolloutActions(desired_state->plane_name));
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
      PrintRolloutLifecycleEntries(
          BuildRolloutLifecycleEntries(
              *state,
              *generation,
              store.LoadRolloutActions(state->plane_name),
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
  AppendControllerEvent(
      store,
      "node-availability",
      "updated",
      "updated node availability override",
      json{
          {"availability", comet::ToString(availability)},
          {"previous_availability", comet::ToString(previous_availability)},
          {"status_message", status_message.value_or("")},
      },
      "",
      node_name);

  std::cout << "updated node availability for " << node_name << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));

  const auto desired_states = store.LoadDesiredStates();
  if (!desired_states.empty()) {
    const auto existing_assignments = store.LoadHostAssignments();
    const auto node_observation = store.LoadHostObservation(node_name);
    if (previous_availability == comet::NodeAvailability::Active &&
        availability != comet::NodeAvailability::Active) {
      std::vector<comet::HostAssignment> drain_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto drain_assignment = BuildDrainAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments);
        if (drain_assignment.has_value()) {
          drain_assignments.push_back(*drain_assignment);
        }
      }
      if (!drain_assignments.empty()) {
        store.EnqueueHostAssignments(
            drain_assignments,
            "superseded by node drain for availability transition");
        std::cout << "queued drain assignment for " << node_name
                  << " planes=" << drain_assignments.size() << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      }
    }

    if (previous_availability != comet::NodeAvailability::Active &&
        availability == comet::NodeAvailability::Active) {
      std::vector<comet::HostAssignment> resync_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto resync_assignment = BuildResyncAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments,
            node_observation);
        if (resync_assignment.has_value()) {
          resync_assignments.push_back(*resync_assignment);
        }
      }
      if (!resync_assignments.empty()) {
        store.EnqueueHostAssignments(
            resync_assignments,
            "superseded by node reactivation for availability transition");
        std::cout << "queued resync assignment for " << node_name
                  << " planes=" << resync_assignments.size() << "\n";
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
  AppendControllerEvent(
      store,
      "host-assignment",
      "retried",
      "requeued failed host assignment",
      json{
          {"desired_generation", assignment->desired_generation},
          {"assignment_type", assignment->assignment_type},
          {"attempt_count", assignment->attempt_count},
      },
      assignment->plane_name,
      assignment->node_name,
      "",
      assignment_id);

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
  PrintDetailedDiskState(*view.desired_state, view.disk_runtime_states, view.observations);
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

int ListPlanes(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto planes = store.LoadPlanes();
  if (planes.empty()) {
    std::cout << "planes: empty\n";
    return 0;
  }
  std::cout << "planes:\n";
  for (const auto& plane : planes) {
    std::cout << "  - name=" << plane.name << " state=" << plane.state
              << " generation=" << plane.generation
              << " rebalance_iteration=" << plane.rebalance_iteration << "\n";
  }
  return 0;
}

int ShowPlane(const std::string& db_path, const std::string& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState(plane_name);
  const auto plane = store.LoadPlane(plane_name);
  if (!state.has_value() || !plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  std::cout << "plane:\n";
  std::cout << "  name=" << plane->name << "\n";
  std::cout << "  state=" << plane->state << "\n";
  std::cout << "  generation=" << plane->generation << "\n";
  std::cout << "  rebalance_iteration=" << plane->rebalance_iteration << "\n";
  std::cout << "  created_at=" << FormatDisplayTimestamp(plane->created_at) << "\n";
  PrintStateSummary(*state);
  return 0;
}

int StartPlane(const std::string& db_path, const std::string& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }
  ValidateDesiredStateForControllerAdmission(*desired_state);
  if (plane->state == "running") {
    std::cout << "plane already running: " << plane_name << "\n";
    return 0;
  }
  if (!store.UpdatePlaneState(plane_name, "running")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments();
    const auto plane_assignment = FindLatestHostAssignmentForPlane(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root : DefaultArtifactsRoot();
  }();
  store.ReplaceRolloutActions(
      desired_state->plane_name, plane->generation, scheduling_report.rollout_actions);
  store.EnqueueHostAssignments(
      BuildHostAssignments(
          *desired_state,
          artifacts_root,
          plane->generation,
          availability_overrides,
          observations,
          scheduling_report),
      "superseded by start-plane lifecycle transition");
  AppendControllerEvent(
      store,
      "plane",
      "started",
      "plane lifecycle moved to running and apply assignments were queued",
      json{
          {"previous_state", plane->state},
          {"next_state", "running"},
          {"desired_generation", plane->generation},
      },
      plane_name);
  std::cout << "plane started: " << plane_name
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

int StopPlane(const std::string& db_path, const std::string& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }
  if (plane->state == "stopped") {
    std::cout << "plane already stopped: " << plane_name << "\n";
    return 0;
  }
  const int superseded = store.SupersedeHostAssignmentsForPlane(
      plane_name,
      "superseded by stop-plane controller lifecycle transition");
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments();
    const auto plane_assignment = FindLatestHostAssignmentForPlane(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root : DefaultArtifactsRoot();
  }();
  store.EnqueueHostAssignments(
      BuildStopPlaneAssignments(
          *desired_state,
          plane->generation,
          artifacts_root,
          availability_overrides),
      "superseded by stop-plane lifecycle transition");
  if (!store.UpdatePlaneState(plane_name, "stopped")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }
  AppendControllerEvent(
      store,
      "plane",
      "stopped",
      "plane lifecycle moved to stopped and stop assignments were queued",
      json{
          {"previous_state", plane->state},
          {"next_state", "stopped"},
          {"superseded_assignments", superseded},
          {"desired_generation", plane->generation},
      },
      plane_name);
  std::cout << "plane stopped: " << plane_name
            << " superseded_assignments=" << superseded
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

int DeletePlane(const std::string& db_path, const std::string& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }
  if (plane->state == "deleting" && CanFinalizeDeletedPlane(store, plane_name)) {
    store.DeletePlane(plane_name);
    AppendControllerEvent(
        store,
        "plane",
        "deleted",
        "plane deleted from controller registry after cleanup convergence",
        json{
            {"plane_name", plane_name},
            {"deleted_generation", plane->generation},
        },
        "");
    std::cout << "plane deleted: " << plane_name
              << " desired_generation=" << plane->generation << "\n";
    return 0;
  }

  const int superseded = store.SupersedeHostAssignmentsForPlane(
      plane_name,
      "superseded by delete-plane controller lifecycle transition");
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments(std::nullopt, std::nullopt, plane_name);
    const auto plane_assignment = FindLatestHostAssignmentForPlane(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root : DefaultArtifactsRoot();
  }();
  if (!store.UpdatePlaneState(plane_name, "deleting")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }
  const auto all_observations = store.LoadHostObservations();
  const auto registered_hosts = store.LoadRegisteredHosts();
  std::set<std::string> cleanup_nodes;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state->nodes) {
    const bool has_observation =
        std::any_of(
            all_observations.begin(),
            all_observations.end(),
            [&](const auto& observation) { return observation.node_name == node.name; });
    const bool connected_host =
        std::any_of(
            registered_hosts.begin(),
            registered_hosts.end(),
            [&](const auto& host) {
              return host.node_name == node.name &&
                     host.registration_state == "registered" &&
                     host.session_state == "connected";
            });
    if (has_observation || connected_host) {
      cleanup_nodes.insert(node.name);
    } else {
      skipped_nodes.push_back(node.name);
    }
  }
  store.ReplaceRolloutActions(plane_name, plane->generation, {});
  store.ClearSchedulerPlaneRuntime(plane_name);
  store.EnqueueHostAssignments(
      [&]() {
        comet::DesiredState cleanup_state = *desired_state;
        std::vector<comet::NodeInventory> nodes;
        for (const auto& node : cleanup_state.nodes) {
          if (cleanup_nodes.count(node.name) > 0) {
            nodes.push_back(node);
          }
        }
        cleanup_state.nodes = std::move(nodes);
        return BuildDeletePlaneAssignments(cleanup_state, plane->generation, artifacts_root);
      }(),
      "superseded by delete-plane lifecycle transition");
  AppendControllerEvent(
      store,
      "plane",
      "delete-requested",
      "plane delete was requested and cleanup assignments were queued",
      json{
          {"previous_state", plane->state},
          {"next_state", "deleting"},
          {"superseded_assignments", superseded},
          {"desired_generation", plane->generation},
          {"cleanup_nodes", cleanup_nodes},
          {"skipped_nodes", skipped_nodes},
      },
      plane_name);
  std::cout << "plane delete started: " << plane_name
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

int RevokeHostd(
    const std::string& db_path,
    const std::string& node_name,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }
  const std::string previous_state = host->registration_state;
  host->registration_state = "revoked";
  host->session_state = "revoked";
  host->session_token.clear();
  host->session_expires_at.clear();
  host->session_host_sequence = 0;
  host->session_controller_sequence = 0;
  host->status_message = status_message.value_or("revoked by operator");
  store.UpsertRegisteredHost(*host);
  AppendControllerEvent(
      store,
      "host-registry",
      "revoked",
      host->status_message,
      json{{"previous_registration_state", previous_state}},
      "",
      node_name,
      "",
      std::nullopt,
      std::nullopt,
      "warning");
  std::cout << "host revoked: " << node_name
            << " previous_registration_state=" << previous_state << "\n";
  return 0;
}

int RotateHostdKey(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& public_key_base64,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }
  const std::string previous_fingerprint =
      host->public_key_base64.empty() ? std::string{} : comet::ComputeKeyFingerprintHex(host->public_key_base64);
  host->public_key_base64 = Trim(public_key_base64);
  host->registration_state = "registered";
  host->session_state = "rotation-pending";
  host->session_token.clear();
  host->session_expires_at.clear();
  host->session_host_sequence = 0;
  host->session_controller_sequence = 0;
  host->status_message = status_message.value_or("host public key rotated by operator");
  store.UpsertRegisteredHost(*host);
  AppendControllerEvent(
      store,
      "host-registry",
      "rotated-key",
      host->status_message,
      json{
          {"previous_fingerprint",
           previous_fingerprint.empty() ? json(nullptr) : json(previous_fingerprint)},
          {"next_fingerprint", comet::ComputeKeyFingerprintHex(host->public_key_base64)},
      },
      "",
      node_name);
  std::cout << "host key rotated: " << node_name
            << " fingerprint=" << comet::ComputeKeyFingerprintHex(host->public_key_base64) << "\n";
  return 0;
}

int ShowDiskState(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadDiskStateViewData(db_path, node_name, plane_name);
  if (!view.desired_state.has_value()) {
    std::cout << "disk-state:\n";
    std::cout << "  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane_filter: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node_filter: " << *view.node_name << "\n";
  }
  PrintDetailedDiskState(*view.desired_state, view.runtime_states, view.observations, view.node_name);
  return 0;
}

int EnsureWebUi(
    const std::string& db_path,
    const std::string& web_ui_root,
    int listen_port,
    const std::string& controller_upstream,
    WebUiComposeMode compose_mode) {
  const std::string image = DefaultWebUiImage();
  const std::string compose_path = WebUiComposePath(web_ui_root);
  WriteTextFile(
      compose_path,
      RenderWebUiComposeYaml(image, listen_port, controller_upstream));

  json state{
      {"image", image},
      {"listen_port", listen_port},
      {"controller_upstream", controller_upstream},
      {"compose_path", compose_path},
      {"web_ui_root", web_ui_root},
      {"materialized", true},
      {"running", false},
      {"status", compose_mode == WebUiComposeMode::Exec ? "starting" : "materialized"},
  };
  if (compose_mode == WebUiComposeMode::Exec) {
    RunCommand(ResolvedDockerCommand() + " compose -f '" + compose_path + "' up -d");
    state["running"] = true;
    state["status"] = "running";
  }
  SaveWebUiStateJson(web_ui_root, state);

  comet::ControllerStore store(db_path);
  store.Initialize();
  AppendControllerEvent(
      store,
      "web-ui",
      "ensured",
      "materialized comet-web-ui sidecar",
      json{
          {"web_ui_root", web_ui_root},
          {"listen_port", listen_port},
          {"controller_upstream", controller_upstream},
          {"compose_mode", compose_mode == WebUiComposeMode::Exec ? "exec" : "skip"},
      });

  std::cout << "web-ui ensured\n";
  std::cout << "root=" << web_ui_root << "\n";
  std::cout << "compose_path=" << compose_path << "\n";
  std::cout << "image=" << image << "\n";
  std::cout << "listen_port=" << listen_port << "\n";
  std::cout << "controller_upstream=" << controller_upstream << "\n";
  std::cout << "compose_mode="
            << (compose_mode == WebUiComposeMode::Exec ? "exec" : "skip") << "\n";
  return 0;
}

int ShowWebUiStatus(const std::string& web_ui_root) {
  const json state = LoadWebUiStateJson(web_ui_root);
  const bool compose_exists = std::filesystem::exists(WebUiComposePath(web_ui_root));
  const bool running = WebUiComposeRunning(web_ui_root);

  std::cout << "web-ui:\n";
  std::cout << "  root=" << web_ui_root << "\n";
  std::cout << "  state_path=" << WebUiStatePath(web_ui_root) << "\n";
  std::cout << "  compose_path=" << WebUiComposePath(web_ui_root) << "\n";
  std::cout << "  materialized=" << (compose_exists ? "yes" : "no") << "\n";
  std::cout << "  running=" << (running ? "yes" : "no") << "\n";
  if (!state.empty()) {
    std::cout << "  image=" << state.value("image", DefaultWebUiImage()) << "\n";
    std::cout << "  listen_port=" << state.value("listen_port", DefaultWebUiPort()) << "\n";
    std::cout << "  controller_upstream="
              << state.value("controller_upstream", DefaultControllerUpstream()) << "\n";
    std::cout << "  status=" << state.value("status", std::string("unknown")) << "\n";
  }
  return 0;
}

int StopWebUi(
    const std::string& db_path,
    const std::string& web_ui_root,
    WebUiComposeMode compose_mode) {
  const std::string compose_path = WebUiComposePath(web_ui_root);
  if (compose_mode == WebUiComposeMode::Exec && std::filesystem::exists(compose_path)) {
    RunCommand(ResolvedDockerCommand() + " compose -f '" + compose_path + "' down --remove-orphans");
  }
  RemoveFileIfExists(compose_path);
  json state = LoadWebUiStateJson(web_ui_root);
  state["materialized"] = false;
  state["running"] = false;
  state["status"] = "stopped";
  SaveWebUiStateJson(web_ui_root, state);

  comet::ControllerStore store(db_path);
  store.Initialize();
  AppendControllerEvent(
      store,
      "web-ui",
      "stopped",
      "stopped comet-web-ui sidecar",
      json{
          {"web_ui_root", web_ui_root},
          {"compose_mode", compose_mode == WebUiComposeMode::Exec ? "exec" : "skip"},
      });

  std::cout << "web-ui stopped\n";
  std::cout << "root=" << web_ui_root << "\n";
  std::cout << "compose_path=" << compose_path << "\n";
  std::cout << "compose_mode="
            << (compose_mode == WebUiComposeMode::Exec ? "exec" : "skip") << "\n";
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

    if (command == "show-hostd-hosts") {
      return ShowRegisteredHosts(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "revoke-hostd") {
      const auto node_name = ParseNodeArg(argc, argv);
      if (!node_name.has_value()) {
        std::cerr << "error: --node is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteRevokeHostdAction(db_path, *node_name, ParseMessageArg(argc, argv)));
    }

    if (command == "rotate-hostd-key") {
      const auto node_name = ParseNodeArg(argc, argv);
      if (!node_name.has_value()) {
        std::cerr << "error: --node is required\n";
        return 1;
      }
      const auto public_key = ParsePublicKeyArg(argc, argv);
      if (!public_key.has_value()) {
        std::cerr << "error: --public-key is required\n";
        return 1;
      }
      return EmitControllerActionResult(
          ExecuteRotateHostdKeyAction(
              db_path,
              *node_name,
              ReadPublicKeyBase64Argument(*public_key),
              ParseMessageArg(argc, argv)));
    }

    if (command == "list-planes") {
      return ListPlanes(db_path);
    }

    if (command == "show-plane") {
      const auto plane_name = ParsePlaneArg(argc, argv);
      if (!plane_name.has_value()) {
        std::cerr << "error: --plane is required\n";
        return 1;
      }
      return ShowPlane(db_path, *plane_name);
    }

    if (command == "start-plane") {
      const auto plane_name = ParsePlaneArg(argc, argv);
      if (!plane_name.has_value()) {
        std::cerr << "error: --plane is required\n";
        return 1;
      }
      return EmitControllerActionResult(ExecuteStartPlaneAction(db_path, *plane_name));
    }

    if (command == "stop-plane") {
      const auto plane_name = ParsePlaneArg(argc, argv);
      if (!plane_name.has_value()) {
        std::cerr << "error: --plane is required\n";
        return 1;
      }
      return EmitControllerActionResult(ExecuteStopPlaneAction(db_path, *plane_name));
    }

    if (command == "delete-plane") {
      const auto plane_name = ParsePlaneArg(argc, argv);
      if (!plane_name.has_value()) {
        std::cerr << "error: --plane is required\n";
        return 1;
      }
      return EmitControllerActionResult(ExecuteDeletePlaneAction(db_path, *plane_name));
    }

    if (command == "ensure-web-ui") {
      return EnsureWebUi(
          db_path,
          ResolveWebUiRoot(ParseWebUiRootArg(argc, argv)),
          ParseListenPortArg(argc, argv).value_or(DefaultWebUiPort()),
          ParseControllerUpstreamArg(argc, argv).value_or(DefaultControllerUpstream()),
          ResolveComposeMode(ParseComposeModeArg(argc, argv)));
    }

    if (command == "show-web-ui-status") {
      return ShowWebUiStatus(ResolveWebUiRoot(ParseWebUiRootArg(argc, argv)));
    }

    if (command == "stop-web-ui") {
      return StopWebUi(
          db_path,
          ResolveWebUiRoot(ParseWebUiRootArg(argc, argv)),
          ResolveComposeMode(ParseComposeModeArg(argc, argv)));
    }

    if (command == "show-host-assignments") {
      return ShowHostAssignments(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "show-host-observations") {
      return ShowHostObservations(
          db_path,
          ParsePlaneArg(argc, argv),
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
      return ShowDiskState(db_path, ParseNodeArg(argc, argv), ParsePlaneArg(argc, argv));
    }

    if (command == "show-rollout-actions") {
      return ShowRolloutActions(
          db_path,
          ParseNodeArg(argc, argv),
          ParsePlaneArg(argc, argv));
    }

    if (command == "show-rebalance-plan") {
      return ShowRebalancePlan(
          db_path,
          ParseNodeArg(argc, argv),
          ParsePlaneArg(argc, argv));
    }

    if (command == "show-events") {
      return ShowEvents(
          db_path,
          ParsePlaneArg(argc, argv),
          ParseNodeArg(argc, argv),
          ParseWorkerArg(argc, argv),
          ParseCategoryArg(argc, argv),
          ParseLimitArg(argc, argv).value_or(100));
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
      std::optional<std::filesystem::path> ui_root;
      if (const auto requested_ui_root = ParseUiRootArg(argc, argv);
          requested_ui_root.has_value()) {
        ui_root = std::filesystem::path(*requested_ui_root);
      } else {
        const std::filesystem::path default_ui_root = DefaultUiRoot();
        if (std::filesystem::exists(default_ui_root)) {
          ui_root = default_ui_root;
        }
      }
      return ServeControllerApi(
          db_path,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          ParseListenHostArg(argc, argv).value_or(DefaultListenHost()),
          ParseListenPortArg(argc, argv).value_or(DefaultListenPort()),
          ui_root);
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  PrintUsage();
  return 1;
}
