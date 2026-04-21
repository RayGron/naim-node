#pragma once

#include <string>

namespace naim::controller {

class InteractionRuntimeTextSupport final {
 public:
  std::string TrimCopy(const std::string& value) const;
  bool IsBlank(const std::string& value) const;
  bool IsTimeoutLikeError(const std::string& message) const;

 private:
  std::string LowercaseCopy(const std::string& value) const;
};

}  // namespace naim::controller
