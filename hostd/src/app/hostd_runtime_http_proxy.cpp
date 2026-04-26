#include "app/hostd_runtime_http_proxy.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <sstream>
#include <stdexcept>

#include <netdb.h>

#include "naim/core/platform_compat.h"

namespace naim::hostd {

std::map<std::string, std::string> HostdRuntimeHttpProxy::ParseProxyHeaders(
    const nlohmann::json& headers_json) const {
  std::map<std::string, std::string> headers;
  if (!headers_json.is_array()) {
    return headers;
  }
  for (const auto& item : headers_json) {
    if (!item.is_array() || item.size() != 2 || !item[0].is_string() ||
        !item[1].is_string()) {
      continue;
    }
    const std::string key = item[0].get<std::string>();
    const std::string lowered_key = LowercaseAscii(key);
    if (lowered_key == "host" || lowered_key == "content-length" ||
        lowered_key == "connection") {
      continue;
    }
    headers[key] = item[1].get<std::string>();
  }
  return headers;
}

HostdRuntimeHttpResponse HostdRuntimeHttpProxy::Send(
    const std::string& host,
    int port,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::map<std::string, std::string>& headers,
    HostdRuntimeProxyPolicy policy) const {
  const std::string proxy_label = PolicyLabel(policy);
  if (!IsLoopbackRuntimeHost(host)) {
    throw std::runtime_error(proxy_label + " target host must be loopback");
  }
  if (port <= 0) {
    throw std::runtime_error(proxy_label + " target port is invalid");
  }
  if (!IsAllowedProxyPath(policy, method, path)) {
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
  return ParseHttpResponse(response_text);
}

std::string HostdRuntimeHttpProxy::TrimAscii(std::string value) {
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

std::string HostdRuntimeHttpProxy::LowercaseAscii(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

bool HostdRuntimeHttpProxy::IsLoopbackRuntimeHost(const std::string& host) {
  return host == "127.0.0.1" || host == "localhost" || host == "::1";
}

bool HostdRuntimeHttpProxy::IsAllowedRuntimeProxyPath(
    const std::string& method,
    const std::string& path) {
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

bool HostdRuntimeHttpProxy::IsAllowedProxyPath(
    HostdRuntimeProxyPolicy policy,
    const std::string& method,
    const std::string& path) {
  const std::string route = path.substr(0, path.find('?'));
  if (policy == HostdRuntimeProxyPolicy::KnowledgeVault) {
    if (method == "GET" && route == "/health") {
      return true;
    }
    if (method == "GET" && route == "/v1/status") {
      return true;
    }
    if (method == "POST" &&
        (route == "/v1/context" || route == "/v1/search" ||
         route == "/v1/graph-neighborhood")) {
      return true;
    }
    return false;
  }
  return IsAllowedRuntimeProxyPath(method, path);
}

std::string HostdRuntimeHttpProxy::PolicyLabel(HostdRuntimeProxyPolicy policy) {
  if (policy == HostdRuntimeProxyPolicy::KnowledgeVault) {
    return "knowledge-vault-http";
  }
  return "runtime-direct-http";
}

HostdRuntimeHttpResponse HostdRuntimeHttpProxy::ParseHttpResponse(
    const std::string& response_text) {
  HostdRuntimeHttpResponse response;
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

}  // namespace naim::hostd
