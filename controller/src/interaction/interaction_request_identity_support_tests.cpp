#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_request_identity_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestRequestIdsUseReqPrefixAndStayUnique() {
  const naim::controller::InteractionRequestIdentitySupport support;
  const std::string first = support.GenerateRequestId();
  const std::string second = support.GenerateRequestId();
  Expect(first.rfind("req-", 0) == 0, "request id should use req- prefix");
  Expect(second.rfind("req-", 0) == 0, "request id should use req- prefix");
  Expect(first != second, "request ids should be unique");
  std::cout << "ok: interaction-request-identity-request-id" << '\n';
}

void TestSessionIdsUseSessPrefixAndStayUnique() {
  const naim::controller::InteractionRequestIdentitySupport support;
  const std::string first = support.GenerateSessionId();
  const std::string second = support.GenerateSessionId();
  Expect(first.rfind("sess-", 0) == 0, "session id should use sess- prefix");
  Expect(second.rfind("sess-", 0) == 0, "session id should use sess- prefix");
  Expect(first != second, "session ids should be unique");
  std::cout << "ok: interaction-request-identity-session-id" << '\n';
}

}  // namespace

int main() {
  try {
    TestRequestIdsUseReqPrefixAndStayUnique();
    TestSessionIdsUseSessPrefixAndStayUnique();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_request_identity_support_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
