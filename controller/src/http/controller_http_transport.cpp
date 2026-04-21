#include "http/controller_http_transport.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <memory>
#include <sstream>
#include <stdexcept>

#include "infra/controller_network_manager.h"
#include "naim/core/platform_compat.h"

namespace {

using SocketHandle = naim::platform::SocketHandle;
using ControllerNetworkManager = naim::controller::ControllerNetworkManager;

std::string TrimCopy(const std::string& value) {
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

std::string LowercaseCopy(const std::string& value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

SocketHandle ConnectHttpTarget(
    const naim::controller::ControllerEndpointTarget& target) {
  naim::platform::EnsureSocketsInitialized();

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(target.port);
  const int lookup =
      getaddrinfo(target.host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error(
        "failed to resolve controller target '" + target.raw +
        "': " + gai_strerror(lookup));
  }

  SocketHandle fd = naim::platform::kInvalidSocket;
  for (addrinfo* candidate = results; candidate != nullptr;
       candidate = candidate->ai_next) {
    fd = socket(
        candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (!naim::platform::IsSocketValid(fd)) {
      continue;
    }
    if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    ControllerNetworkManager::CloseSocket(fd);
    fd = naim::platform::kInvalidSocket;
  }
  freeaddrinfo(results);
  if (!naim::platform::IsSocketValid(fd)) {
    throw std::runtime_error(
        "failed to connect to controller target '" + target.raw + "'");
  }
  return fd;
}

}  // namespace

naim::controller::ControllerEndpointTarget ParseControllerEndpointTarget(
    const std::string& raw_target) {
  std::string target = TrimCopy(raw_target);
  if (target.empty()) {
    throw std::runtime_error("empty controller target");
  }

  naim::controller::ControllerEndpointTarget parsed;
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
      headers_end == std::string::npos ? response_text
                                       : response_text.substr(0, headers_end);
  response.body = headers_end == std::string::npos
                      ? std::string{}
                      : response_text.substr(headers_end + 4);

  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line =
      line_end == std::string::npos ? header_text
                                    : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  std::string http_version;
  stream >> http_version >> response.status_code;

  std::size_t offset =
      line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = LowercaseCopy(TrimCopy(line.substr(0, colon)));
      const std::string value = TrimCopy(line.substr(colon + 1));
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

std::optional<std::string> FindHttpHeaderValue(
    const std::string& header_text,
    const std::string& header_name) {
  const std::size_t line_end = header_text.find("\r\n");
  std::size_t offset =
      line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = LowercaseCopy(TrimCopy(line.substr(0, colon)));
      if (key == LowercaseCopy(header_name)) {
        return TrimCopy(line.substr(colon + 1));
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return std::nullopt;
}

HttpResponse SendControllerHttpRequest(
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers) {
  const SocketHandle fd = ConnectHttpTarget(target);

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
            [](const auto& header) {
              return LowercaseCopy(header.first) == "content-type";
            })) {
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
      const std::string error = ControllerNetworkManager::SocketErrorMessage();
      ControllerNetworkManager::CloseSocket(fd);
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
      const std::string error = ControllerNetworkManager::SocketErrorMessage();
      ControllerNetworkManager::CloseSocket(fd);
      throw std::runtime_error("failed to read HTTP response: " + error);
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  ControllerNetworkManager::CloseSocket(fd);
  return ParseHttpResponse(response_text);
}

naim::controller::InteractionStreamingUpstreamConnection
OpenInteractionStreamRequest(
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& request_id,
    const std::string& body) {
  auto fd = std::make_shared<SocketHandle>(ConnectHttpTarget(target));
  const auto close_transport = [fd]() {
    if (naim::platform::IsSocketValid(*fd)) {
      ControllerNetworkManager::ShutdownAndCloseSocket(*fd);
      *fd = naim::platform::kInvalidSocket;
    }
  };

  try {
    std::ostringstream upstream_request;
    upstream_request << "POST /v1/chat/completions HTTP/1.1\r\n";
    upstream_request << "Host: " << target.host << ":" << target.port << "\r\n";
    upstream_request << "Connection: close\r\n";
    upstream_request << "Accept: text/event-stream\r\n";
    upstream_request << "X-Naim-Request-Id: " << request_id << "\r\n";
    upstream_request << "Content-Type: application/json\r\n";
    upstream_request << "Content-Length: " << body.size() << "\r\n\r\n";
    upstream_request << body;
    if (!ControllerNetworkManager::SendAll(*fd, upstream_request.str())) {
      throw std::runtime_error("failed to write upstream interaction request");
    }

    std::string response_text;
    std::array<char, 8192> buffer{};
    std::size_t header_end = std::string::npos;
    while (header_end == std::string::npos) {
      const ssize_t read_count = recv(*fd, buffer.data(), buffer.size(), 0);
      if (read_count <= 0) {
        break;
      }
      response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
      header_end = response_text.find("\r\n\r\n");
    }
    if (header_end == std::string::npos) {
      throw std::runtime_error(
          "upstream interaction response ended before headers");
    }

    const std::string header_text = response_text.substr(0, header_end);
    HttpResponse upstream = ParseHttpResponse(response_text);
    const bool chunked_transfer =
        FindHttpHeaderValue(header_text, "transfer-encoding").has_value() &&
        LowercaseCopy(*FindHttpHeaderValue(header_text, "transfer-encoding"))
                .find("chunked") != std::string::npos;
    if (upstream.status_code != 200 ||
        upstream.content_type.find("text/event-stream") == std::string::npos) {
      while (true) {
        const ssize_t read_count = recv(*fd, buffer.data(), buffer.size(), 0);
        if (read_count <= 0) {
          break;
        }
        response_text.append(
            buffer.data(), static_cast<std::size_t>(read_count));
      }
      upstream = ParseHttpResponse(response_text);
      throw std::runtime_error(
          "upstream interaction request failed: " +
          (upstream.body.empty() ? std::to_string(upstream.status_code)
                                 : upstream.body));
    }

    return naim::controller::InteractionStreamingUpstreamConnection{
        chunked_transfer,
        std::move(upstream.body),
        [fd]() -> std::string {
          std::array<char, 8192> buffer{};
          const ssize_t read_count = recv(*fd, buffer.data(), buffer.size(), 0);
          if (read_count <= 0) {
            return {};
          }
          return std::string(
              buffer.data(), static_cast<std::size_t>(read_count));
        },
        close_transport,
    };
  } catch (...) {
    close_transport();
    throw;
  }
}
