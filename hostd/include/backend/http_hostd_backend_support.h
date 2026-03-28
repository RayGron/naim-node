#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"

namespace comet::hostd {

class IHttpHostdBackendSupport {
 public:
  virtual ~IHttpHostdBackendSupport() = default;

  virtual nlohmann::json SendControllerJsonRequest(
      const std::string& controller_url,
      const std::string& method,
      const std::string& path,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const = 0;

  virtual comet::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) const = 0;
  virtual nlohmann::json BuildHostObservationPayload(
      const comet::HostObservation& observation) const = 0;
  virtual nlohmann::json BuildDiskRuntimeStatePayload(
      const comet::DiskRuntimeState& state) const = 0;
  virtual comet::DiskRuntimeState ParseDiskRuntimeStatePayload(
      const nlohmann::json& payload) const = 0;
  virtual std::string Trim(const std::string& value) const = 0;
};

}  // namespace comet::hostd
