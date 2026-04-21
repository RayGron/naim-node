#pragma once

#include <atomic>
#include <string>
#include <string_view>

namespace naim::controller {

class InteractionRequestIdentitySupport final {
 public:
  std::string GenerateRequestId() const;
  std::string GenerateSessionId() const;

 private:
  std::string GenerateTimestampedId(
      std::string_view prefix,
      std::atomic<unsigned long long>* counter) const;
};

}  // namespace naim::controller
