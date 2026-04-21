#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_upstream_event_parser.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestDecodesChunkedBody() {
  const naim::controller::InteractionUpstreamEventParser parser;
  std::string encoded = "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
  std::string decoded;
  bool stream_finished = false;
  const bool progressed =
      parser.DecodeAvailableChunkedHttpBody(encoded, &decoded, &stream_finished);
  Expect(progressed, "parser should decode available chunked body");
  Expect(decoded == "hello world", "parser should concatenate decoded chunks");
  Expect(stream_finished, "parser should mark stream_finished on zero chunk");
  std::cout << "ok: interaction-upstream-decodes-chunked-body" << '\n';
}

void TestExtractsUsageAndFinishReasonDefaults() {
  const naim::controller::InteractionUpstreamEventParser parser;
  const nlohmann::json payload = {
      {"choices", nlohmann::json::array({nlohmann::json{{"finish_reason", "length"}}})},
  };
  const auto usage = parser.ExtractInteractionUsage(payload);
  Expect(
      parser.ExtractInteractionFinishReason(payload) == "length",
      "parser should extract finish_reason");
  Expect(usage.at("prompt_tokens").get<int>() == 0,
         "parser should default prompt_tokens when usage is missing");
  std::cout << "ok: interaction-upstream-extracts-usage-and-finish-reason" << '\n';
}

void TestConsumesSseFrame() {
  const naim::controller::InteractionUpstreamEventParser parser;
  std::string buffer =
      "event: delta\n"
      "data: {\"delta\":\"a\"}\n"
      "\n";
  naim::controller::InteractionSseFrame frame;
  Expect(parser.TryConsumeSseFrame(buffer, &frame), "parser should consume SSE frame");
  Expect(frame.event_name == "delta", "parser should parse event name");
  Expect(frame.data == "{\"delta\":\"a\"}", "parser should parse data payload");
  Expect(buffer.empty(), "parser should remove consumed frame from buffer");
  std::cout << "ok: interaction-upstream-consumes-sse-frame" << '\n';
}

}  // namespace

int main() {
  try {
    TestDecodesChunkedBody();
    TestExtractsUsageAndFinishReasonDefaults();
    TestConsumesSseFrame();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_upstream_event_parser_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
