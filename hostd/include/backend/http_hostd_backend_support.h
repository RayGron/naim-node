#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::hostd {

class IHttpHostdBackendSupport {
 public:
  virtual ~IHttpHostdBackendSupport() = default;

  virtual nlohmann::json SendControllerJsonRequest(
      const std::string& controller_url,
      const std::string& method,
      const std::string& path,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const = 0;

  virtual naim::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) const = 0;
  virtual nlohmann::json BuildHostObservationPayload(
      const naim::HostObservation& observation) const = 0;
  virtual nlohmann::json BuildDiskRuntimeStatePayload(
      const naim::DiskRuntimeState& state) const = 0;
  virtual naim::DiskRuntimeState ParseDiskRuntimeStatePayload(
      const nlohmann::json& payload) const = 0;
  virtual std::string Trim(const std::string& value) const = 0;
};

}  // namespace naim::hostd
