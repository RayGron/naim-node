#include "../include/controller_network_manager.h"

#include <cctype>
#include <sstream>
#include <stdexcept>

#include "../include/controller_http_transport.h"

namespace comet::controller {

std::string ControllerNetworkManager::LowercaseCopy(const std::string& value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

std::string ControllerNetworkManager::SocketErrorMessage() {
  return comet::platform::LastSocketErrorMessage();
}

void ControllerNetworkManager::CloseSocket(const SocketHandle fd) {
  if (comet::platform::IsSocketValid(fd)) {
    comet::platform::CloseSocket(fd);
  }
}

void ControllerNetworkManager::ShutdownAndCloseSocket(const SocketHandle fd) {
  if (comet::platform::IsSocketValid(fd)) {
    comet::platform::ShutdownSocket(fd);
    comet::platform::CloseSocket(fd);
  }
}

bool ControllerNetworkManager::SendAll(
    const SocketHandle fd,
    const std::string& payload) {
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

bool ControllerNetworkManager::SendSseHeaders(
    const SocketHandle client_fd,
    const std::map<std::string, std::string>& headers) {
  std::ostringstream out;
  out << "HTTP/1.1 200 OK\r\n";
  out << "Content-Type: text/event-stream\r\n";
  out << "Cache-Control: no-cache\r\n";
  for (const auto& [key, value] : headers) {
    if (LowercaseCopy(key) == "content-type") {
      continue;
    }
    out << key << ": " << value << "\r\n";
  }
  out << "Connection: keep-alive\r\n";
  out << "X-Accel-Buffering: no\r\n\r\n";
  return SendAll(client_fd, out.str());
}

bool ControllerNetworkManager::SendSseEventFrame(
    const SocketHandle client_fd,
    const int event_id,
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

bool ControllerNetworkManager::SendSseCommentFrame(
    const SocketHandle client_fd,
    const std::string& message) {
  return SendAll(client_fd, ":" + message + "\n\n");
}

std::string ControllerNetworkManager::ReasonPhrase(const int status_code) {
  switch (status_code) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 409:
      return "Conflict";
    case 422:
      return "Unprocessable Content";
    case 500:
      return "Internal Server Error";
    case 502:
      return "Bad Gateway";
    case 503:
      return "Service Unavailable";
    case 504:
      return "Gateway Timeout";
    default:
      return "Response";
  }
}

void ControllerNetworkManager::SendHttpResponse(
    const SocketHandle client_fd,
    const HttpResponse& response) {
  std::ostringstream out;
  out << "HTTP/1.1 " << response.status_code << " "
      << ReasonPhrase(response.status_code) << "\r\n";
  out << "Content-Type: " << response.content_type << "\r\n";
  for (const auto& [key, value] : response.headers) {
    if (LowercaseCopy(key) == "content-type") {
      continue;
    }
    out << key << ": " << value << "\r\n";
  }
  out << "Content-Length: " << response.body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << response.body;
  SendAll(client_fd, out.str());
}

ControllerNetworkManager::SocketHandle
ControllerNetworkManager::CreateListenSocket(
    const std::string& host,
    const int port) {
  comet::platform::EnsureSocketsInitialized();

  const SocketHandle fd = socket(AF_INET, SOCK_STREAM, 0);
  if (!comet::platform::IsSocketValid(fd)) {
    throw std::runtime_error("failed to create server socket");
  }

  int yes = 1;
#if defined(_WIN32)
  setsockopt(
      fd,
      SOL_SOCKET,
      SO_REUSEADDR,
      reinterpret_cast<const char*>(&yes),
      sizeof(yes));
#else
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#endif

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    CloseSocket(fd);
    throw std::runtime_error("invalid listen host '" + host + "'");
  }

  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    const std::string error = SocketErrorMessage();
    CloseSocket(fd);
    throw std::runtime_error(
        "failed to bind " + host + ":" + std::to_string(port) + ": " +
        error);
  }
  if (listen(fd, 64) != 0) {
    const std::string error = SocketErrorMessage();
    CloseSocket(fd);
    throw std::runtime_error(
        "failed to listen on " + host + ":" + std::to_string(port) + ": " +
        error);
  }
  return fd;
}

}  // namespace comet::controller
