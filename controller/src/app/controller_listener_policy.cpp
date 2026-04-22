#include "app/controller_listener_policy.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>

namespace naim::controller::serve_support {

bool ControllerListenerPolicy::WebGatewayRoutesEnabledForListener(
    const std::string& listen_host) const {
  if (IsLoopbackHost(listen_host) || IsPrivateIpv4Host(listen_host)) {
    return true;
  }
  if (const char* internal_host = std::getenv("NAIM_CONTROLLER_INTERNAL_HOST");
      internal_host != nullptr && internal_host[0] != '\0') {
    return listen_host == internal_host;
  }
  return false;
}

std::string ControllerListenerPolicy::LowercaseCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

bool ControllerListenerPolicy::IsLoopbackHost(const std::string& host) {
  const std::string lowered = LowercaseCopy(host);
  return lowered == "127.0.0.1" || lowered == "localhost" || lowered == "::1";
}

bool ControllerListenerPolicy::IsPrivateIpv4Host(const std::string& host) {
  int a = 0;
  int b = 0;
  int c = 0;
  int d = 0;
  char tail = '\0';
  if (std::sscanf(host.c_str(), "%d.%d.%d.%d%c", &a, &b, &c, &d, &tail) != 4) {
    return false;
  }
  return a == 10 || a == 127 || (a == 169 && b == 254) ||
         (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168);
}

}  // namespace naim::controller::serve_support
