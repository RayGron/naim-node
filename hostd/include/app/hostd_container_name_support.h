#pragma once

#include <string>

namespace naim::hostd {

class HostdContainerNameSupport final {
 public:
  std::string KnowledgeVaultContainerName(const std::string& service_id) const;
  std::string KnowledgeVaultStorageSegment(const std::string& service_id) const;

 private:
  static std::string SafeContainerToken(std::string value);
};

}  // namespace naim::hostd
