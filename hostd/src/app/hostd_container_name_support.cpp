#include "app/hostd_container_name_support.h"

#include <cctype>

namespace naim::hostd {

std::string HostdContainerNameSupport::KnowledgeVaultContainerName(
    const std::string& service_id) const {
  return "naim-knowledge-vault-" + SafeContainerToken(service_id);
}

std::string HostdContainerNameSupport::KnowledgeVaultStorageSegment(
    const std::string& service_id) const {
  return SafeContainerToken(service_id);
}

std::string HostdContainerNameSupport::SafeContainerToken(std::string value) {
  for (char& ch : value) {
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (std::isalnum(uch) == 0 && ch != '-' && ch != '_') {
      ch = '-';
    }
  }
  if (value.empty()) {
    return "default";
  }
  return value;
}

}  // namespace naim::hostd
