#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace naim::controller::auth {

class WebAuthnService {
 public:
  nlohmann::json Run(
      const std::string& action,
      const nlohmann::json& payload) const;
};

}  // namespace naim::controller::auth
