#include "interaction/interaction_runtime_text_support.h"

#include <cctype>

namespace naim::controller {

std::string InteractionRuntimeTextSupport::TrimCopy(
    const std::string& value) const {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

bool InteractionRuntimeTextSupport::IsBlank(const std::string& value) const {
  return TrimCopy(value).empty();
}

bool InteractionRuntimeTextSupport::IsTimeoutLikeError(
    const std::string& message) const {
  const std::string lowered = LowercaseCopy(message);
  return lowered.find("timed out") != std::string::npos ||
         lowered.find("timeout") != std::string::npos;
}

std::string InteractionRuntimeTextSupport::LowercaseCopy(
    const std::string& value) const {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

}  // namespace naim::controller
