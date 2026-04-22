#pragma once

#include <string>

namespace naim::controller::serve_support {

class ControllerListenerPolicy final {
 public:
  bool WebGatewayRoutesEnabledForListener(const std::string& listen_host) const;

 private:
  static std::string LowercaseCopy(std::string value);
  static bool IsLoopbackHost(const std::string& host);
  static bool IsPrivateIpv4Host(const std::string& host);
};

}  // namespace naim::controller::serve_support
