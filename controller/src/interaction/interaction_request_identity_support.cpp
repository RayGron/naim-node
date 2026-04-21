#include "interaction/interaction_request_identity_support.h"

#include <chrono>
#include <stdexcept>

namespace naim::controller {

std::string InteractionRequestIdentitySupport::GenerateRequestId() const {
  static std::atomic<unsigned long long> counter{0};
  return GenerateTimestampedId("req", &counter);
}

std::string InteractionRequestIdentitySupport::GenerateSessionId() const {
  static std::atomic<unsigned long long> counter{0};
  return GenerateTimestampedId("sess", &counter);
}

std::string InteractionRequestIdentitySupport::GenerateTimestampedId(
    std::string_view prefix,
    std::atomic<unsigned long long>* counter) const {
  if (counter == nullptr) {
    throw std::invalid_argument("interaction identity counter is required");
  }
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return std::string(prefix) + "-" + std::to_string(now) + "-" +
         std::to_string(++(*counter));
}

}  // namespace naim::controller
