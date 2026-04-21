#pragma once

#include <map>
#include <string>

#include "naim/core/platform_compat.h"

struct HttpResponse;

namespace naim::controller {

class ControllerNetworkManager {
 public:
  using SocketHandle = naim::platform::SocketHandle;

  static std::string SocketErrorMessage();
  static void CloseSocket(SocketHandle fd);
  static void ShutdownAndCloseSocket(SocketHandle fd);
  static bool SendAll(SocketHandle fd, const std::string& payload);
  static bool SendSseHeaders(
      SocketHandle client_fd,
      const std::map<std::string, std::string>& headers = {});
  static bool SendSseEventFrame(
      SocketHandle client_fd,
      int event_id,
      const std::string& event_name,
      const std::string& payload);
  static bool SendSseCommentFrame(
      SocketHandle client_fd,
      const std::string& message);
  static void SendHttpResponse(
      SocketHandle client_fd,
      const HttpResponse& response);
  static SocketHandle CreateListenSocket(const std::string& host, int port);
  static std::string ReasonPhrase(int status_code);

 private:
  static std::string LowercaseCopy(const std::string& value);
};

}  // namespace naim::controller
