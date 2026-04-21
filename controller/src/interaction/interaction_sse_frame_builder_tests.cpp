#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_sse_frame_builder.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestBuildEventFrameFormatsEventAndDataLines() {
  const naim::controller::InteractionSseFrameBuilder builder;
  const std::string frame = builder.BuildEventFrame(
      "delta",
      nlohmann::json{{"request_id", "req-1"}, {"delta", "hello"}});
  Expect(
      frame.rfind("event: delta\n", 0) == 0,
      "event frame should start with SSE event header");
  Expect(
      frame.find("data: {\"delta\":\"hello\",\"request_id\":\"req-1\"}\n\n") !=
          std::string::npos,
      "event frame should serialize JSON payload as SSE data");
  std::cout << "ok: interaction-sse-frame-builds-event" << '\n';
}

void TestBuildDoneFrameMatchesSseDoneSentinel() {
  const naim::controller::InteractionSseFrameBuilder builder;
  Expect(
      builder.BuildDoneFrame() == "data: [DONE]\n\n",
      "done frame should match SSE done sentinel");
  std::cout << "ok: interaction-sse-frame-builds-done" << '\n';
}

}  // namespace

int main() {
  try {
    TestBuildEventFrameFormatsEventAndDataLines();
    TestBuildDoneFrameMatchesSseDoneSentinel();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_sse_frame_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
