#include "devtool/http_client.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace naim::devtool {
namespace {

struct ParsedUrl {
  std::string host;
  std::string port;
  std::string target;
};

struct ParsedUrlResult {
  std::optional<ParsedUrl> value;
  std::string error;

  [[nodiscard]] bool has_value() const { return value.has_value(); }
};

ParsedUrlResult MakeParseUrlError(std::string message) {
  ParsedUrlResult result;
  result.error = std::move(message);
  return result;
}

ParsedUrlResult MakeParseUrlSuccess(ParsedUrl parsed) {
  ParsedUrlResult result;
  result.value = std::move(parsed);
  return result;
}

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string ToLower(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(), [](const unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
      });
  return value;
}

ParsedUrlResult ParseUrl(std::string_view url) {
  constexpr std::string_view kPrefix = "http://";
  if (!url.starts_with(kPrefix)) {
    return MakeParseUrlError("unsupported URL scheme for " + std::string(url));
  }
  const std::string_view remainder = url.substr(kPrefix.size());
  const std::size_t slash = remainder.find('/');
  const std::string_view authority =
      slash == std::string::npos ? remainder : remainder.substr(0, slash);
  const std::string target =
      slash == std::string::npos ? "/" : std::string(remainder.substr(slash));
  const std::size_t colon = authority.rfind(':');
  if (authority.empty()) {
    return MakeParseUrlError("missing host in URL " + std::string(url));
  }
  ParsedUrl parsed;
  parsed.host = colon == std::string::npos ? std::string(authority)
                                           : std::string(authority.substr(0, colon));
  parsed.port = colon == std::string::npos ? "80"
                                           : std::string(authority.substr(colon + 1));
  parsed.target = target.empty() ? "/" : target;
  if (parsed.host.empty() || parsed.port.empty()) {
    return MakeParseUrlError("invalid URL " + std::string(url));
  }
  return MakeParseUrlSuccess(std::move(parsed));
}

std::string ReadExact(
    int socket_fd,
    std::size_t expected_bytes,
    std::string initial = std::string()) {
  std::string body = std::move(initial);
  body.reserve(expected_bytes);
  while (body.size() < expected_bytes) {
    char buffer[8192];
    const std::size_t remaining = expected_bytes - body.size();
    const std::size_t request_size = std::min<std::size_t>(sizeof(buffer), remaining);
    const ssize_t bytes_read = ::recv(socket_fd, buffer, request_size, 0);
    if (bytes_read <= 0) {
      throw std::runtime_error("unexpected EOF while reading HTTP response body");
    }
    body.append(buffer, static_cast<std::size_t>(bytes_read));
  }
  return body;
}

std::string ReadUntilClose(int socket_fd, std::string initial = std::string()) {
  std::string body = std::move(initial);
  for (;;) {
    char buffer[8192];
    const ssize_t bytes_read = ::recv(socket_fd, buffer, sizeof(buffer), 0);
    if (bytes_read == 0) {
      break;
    }
    if (bytes_read < 0) {
      throw std::runtime_error("failed while reading HTTP response body");
    }
    body.append(buffer, static_cast<std::size_t>(bytes_read));
  }
  return body;
}

std::string DecodeChunkedBody(std::string payload) {
  std::string decoded;
  std::size_t offset = 0;
  while (offset < payload.size()) {
    const std::size_t line_end = payload.find("\r\n", offset);
    if (line_end == std::string::npos) {
      throw std::runtime_error("invalid chunked HTTP response");
    }
    const std::string line = payload.substr(offset, line_end - offset);
    const std::size_t chunk_size = std::stoul(line, nullptr, 16);
    offset = line_end + 2;
    if (chunk_size == 0) {
      break;
    }
    if (offset + chunk_size > payload.size()) {
      throw std::runtime_error("truncated chunked HTTP response");
    }
    decoded.append(payload, offset, chunk_size);
    offset += chunk_size;
    if (offset + 2 > payload.size() || payload.substr(offset, 2) != "\r\n") {
      throw std::runtime_error("malformed chunked HTTP response");
    }
    offset += 2;
  }
  return decoded;
}

HttpResponse ParseHttpResponse(const std::string& response_blob, int socket_fd) {
  const std::size_t header_end = response_blob.find("\r\n\r\n");
  if (header_end == std::string::npos) {
    throw std::runtime_error("malformed HTTP response");
  }
  const std::string header_blob = response_blob.substr(0, header_end);
  std::string body = response_blob.substr(header_end + 4);
  std::istringstream stream(header_blob);
  std::string status_line;
  std::getline(stream, status_line);
  if (!status_line.empty() && status_line.back() == '\r') {
    status_line.pop_back();
  }
  std::istringstream status_stream(status_line);
  std::string http_version;
  HttpResponse response;
  status_stream >> http_version >> response.status_code;
  if (response.status_code <= 0) {
    throw std::runtime_error("invalid HTTP status line");
  }
  std::string line;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const std::size_t colon = line.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    response.headers[ToLower(Trim(line.substr(0, colon)))] = Trim(line.substr(colon + 1));
  }
  const auto transfer_encoding = response.headers.find("transfer-encoding");
  if (transfer_encoding != response.headers.end() &&
      ToLower(transfer_encoding->second).contains("chunked")) {
    body = DecodeChunkedBody(ReadUntilClose(socket_fd, body));
  } else if (const auto content_length = response.headers.find("content-length");
             content_length != response.headers.end()) {
    body = ReadExact(socket_fd, std::stoul(content_length->second), body);
  } else {
    body = ReadUntilClose(socket_fd, body);
  }
  response.body = std::move(body);
  return response;
}

}  // namespace

HttpResponse PerformHttpRequest(const HttpRequest& request) {
  const auto url_result = ParseUrl(request.url);
  if (!url_result.has_value()) {
    throw std::runtime_error(url_result.error);
  }
  const ParsedUrl& url = *url_result.value;
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  const int lookup_rc = ::getaddrinfo(url.host.c_str(), url.port.c_str(), &hints, &result);
  if (lookup_rc != 0) {
    throw std::runtime_error("getaddrinfo failed for " + url.host + ":" + url.port);
  }

  int socket_fd = -1;
  for (addrinfo* candidate = result; candidate != nullptr; candidate = candidate->ai_next) {
    socket_fd = ::socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (socket_fd < 0) {
      continue;
    }
    timeval timeout{};
    timeout.tv_sec = request.timeout_seconds;
    timeout.tv_usec = 0;
    ::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (::connect(socket_fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    ::close(socket_fd);
    socket_fd = -1;
  }
  ::freeaddrinfo(result);
  if (socket_fd < 0) {
    throw std::runtime_error("failed to connect to " + request.url);
  }

  std::ostringstream http;
  http << request.method << " " << url.target << " HTTP/1.1\r\n";
  http << "Host: " << url.host;
  if (url.port != "80") {
    http << ":" << url.port;
  }
  http << "\r\n";
  http << "Connection: close\r\n";
  for (const auto& [name, value] : request.headers) {
    http << name << ": " << value << "\r\n";
  }
  if (!request.body.empty()) {
    http << "Content-Length: " << request.body.size() << "\r\n";
  }
  http << "\r\n";
  http << request.body;

  const std::string payload = http.str();
  std::size_t sent = 0;
  while (sent < payload.size()) {
    const ssize_t bytes_written =
        ::send(socket_fd, payload.data() + sent, payload.size() - sent, 0);
    if (bytes_written <= 0) {
      ::close(socket_fd);
      throw std::runtime_error("failed to send HTTP request");
    }
    sent += static_cast<std::size_t>(bytes_written);
  }

  std::string response_blob;
  for (;;) {
    char buffer[8192];
    const ssize_t bytes_read = ::recv(socket_fd, buffer, sizeof(buffer), 0);
    if (bytes_read == 0) {
      break;
    }
    if (bytes_read < 0) {
      ::close(socket_fd);
      throw std::runtime_error("failed to read HTTP response");
    }
    response_blob.append(buffer, static_cast<std::size_t>(bytes_read));
    if (response_blob.contains("\r\n\r\n")) {
      break;
    }
  }
  HttpResponse response = ParseHttpResponse(response_blob, socket_fd);
  ::close(socket_fd);
  return response;
}

}  // namespace naim::devtool
