#include "app/hostd_controller_transport_support.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

#include <errno.h>
#include <netdb.h>

#include "naim/core/platform_compat.h"

namespace naim::hostd::controller_transport_support {

namespace {

using nlohmann::json;
using SocketHandle = naim::platform::SocketHandle;

std::string SocketErrorMessage() {
  return naim::platform::LastSocketErrorMessage();
}

void CloseSocketHandle(const SocketHandle fd) {
  if (naim::platform::IsSocketValid(fd)) {
    naim::platform::CloseSocket(fd);
  }
}

struct HttpResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
};

struct ControllerEndpointTarget {
  std::string raw;
  std::string host;
  int port = 18080;
  std::string base_path;
};

std::string Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

ControllerEndpointTarget ParseControllerTarget(const std::string& raw_target) {
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
    const std::string& body,
    const std::map<std::string, std::string>& headers) {
  naim::platform::EnsureSocketsInitialized();

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

  SocketHandle fd = naim::platform::kInvalidSocket;
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    fd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (!naim::platform::IsSocketValid(fd)) {
      continue;
    }
    if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    CloseSocketHandle(fd);
    fd = naim::platform::kInvalidSocket;
  }
  freeaddrinfo(results);
  if (!naim::platform::IsSocketValid(fd)) {
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
      const std::string error = SocketErrorMessage();
      CloseSocketHandle(fd);
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
      const std::string error = SocketErrorMessage();
      CloseSocketHandle(fd);
      throw std::runtime_error("failed to read HTTP response: " + error);
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  CloseSocketHandle(fd);
  return ParseHttpResponse(response_text);
}

}  // namespace

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
    --end;
  }
  return value.substr(start, end - start);
}

nlohmann::json SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) {
  const HttpResponse response =
      SendControllerHttpRequest(
          ParseControllerTarget(controller_url),
          method,
          path,
          payload.is_null() ? std::string{} : payload.dump(),
          headers);
  const json body = response.body.empty() ? json::object() : json::parse(response.body);
  if (response.status_code >= 400) {
    throw std::runtime_error(
        body.contains("error") && body["error"].is_object()
            ? body["error"].value("message", "controller request failed")
            : "controller request failed with status " + std::to_string(response.status_code));
  }
  return body;
}

naim::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) {
  naim::HostAssignment assignment;
  assignment.id = payload.value("id", 0);
  assignment.node_name = payload.value("node_name", std::string{});
  assignment.plane_name = payload.value("plane_name", std::string{});
  assignment.desired_generation = payload.value("desired_generation", 0);
  assignment.attempt_count = payload.value("attempt_count", 0);
  assignment.max_attempts = payload.value("max_attempts", 3);
  assignment.assignment_type = payload.value("assignment_type", std::string{});
  assignment.desired_state_json = payload.value("desired_state_json", std::string{});
  assignment.artifacts_root = payload.value("artifacts_root", std::string{});
  assignment.status =
      naim::ParseHostAssignmentStatus(payload.value("status", std::string("pending")));
  assignment.status_message = payload.value("status_message", std::string{});
  if (payload.contains("progress") && !payload.at("progress").is_null()) {
    assignment.progress_json = payload.at("progress").dump();
  }
  return assignment;
}

nlohmann::json BuildHostObservationPayload(const naim::HostObservation& observation) {
  return json{
      {"node_name", observation.node_name},
      {"plane_name", observation.plane_name},
      {"applied_generation",
       observation.applied_generation.has_value() ? json(*observation.applied_generation)
                                                  : json(nullptr)},
      {"last_assignment_id",
       observation.last_assignment_id.has_value() ? json(*observation.last_assignment_id)
                                                  : json(nullptr)},
      {"status", naim::ToString(observation.status)},
      {"status_message", observation.status_message},
      {"observed_state_json", observation.observed_state_json},
      {"runtime_status_json", observation.runtime_status_json},
      {"instance_runtime_json", observation.instance_runtime_json},
      {"gpu_telemetry_json", observation.gpu_telemetry_json},
      {"disk_telemetry_json", observation.disk_telemetry_json},
      {"network_telemetry_json", observation.network_telemetry_json},
      {"cpu_telemetry_json", observation.cpu_telemetry_json},
      {"heartbeat_at", observation.heartbeat_at},
  };
}

nlohmann::json BuildDiskRuntimeStatePayload(const naim::DiskRuntimeState& state) {
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

naim::DiskRuntimeState ParseDiskRuntimeStatePayload(const nlohmann::json& payload) {
  naim::DiskRuntimeState state;
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

}  // namespace naim::hostd::controller_transport_support
