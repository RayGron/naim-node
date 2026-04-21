#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_runtime_text_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestTrimCopyRemovesOuterWhitespace() {
  const naim::controller::InteractionRuntimeTextSupport support;
  Expect(
      support.TrimCopy(" \t  hello world \n") == "hello world",
      "trim copy should remove leading and trailing whitespace");
  std::cout << "ok: interaction-runtime-text-trim-copy" << '\n';
}

void TestIsBlankUsesTrimmedView() {
  const naim::controller::InteractionRuntimeTextSupport support;
  Expect(support.IsBlank(" \t \n"), "is blank should treat whitespace as blank");
  Expect(!support.IsBlank(" value "), "is blank should keep non-empty values");
  std::cout << "ok: interaction-runtime-text-is-blank" << '\n';
}

void TestIsTimeoutLikeErrorMatchesCaseInsensitiveTimeouts() {
  const naim::controller::InteractionRuntimeTextSupport support;
  Expect(
      support.IsTimeoutLikeError("Upstream TIMED OUT while waiting"),
      "timeout detector should match timed out messages");
  Expect(
      support.IsTimeoutLikeError("transport timeout after 5s"),
      "timeout detector should match timeout messages");
  Expect(
      !support.IsTimeoutLikeError("connection refused"),
      "timeout detector should ignore unrelated errors");
  std::cout << "ok: interaction-runtime-text-timeout-detection" << '\n';
}

}  // namespace

int main() {
  try {
    TestTrimCopyRemovesOuterWhitespace();
    TestIsBlankUsesTrimmedView();
    TestIsTimeoutLikeErrorMatchesCaseInsensitiveTimeouts();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_runtime_text_support_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
