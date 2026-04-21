#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace naim::controller {

class ControllerHealthService {
 public:
  nlohmann::json BuildPayload(const std::string& db_path) const;
};

}  // namespace naim::controller
