#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_text_post_processor.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestSanitizesReasoningAndThinkBlocks() {
  const naim::controller::InteractionTextPostProcessor processor;
  const std::string text =
      "Thinking process:\ninternal\n\n<think>hidden</think>\n\nFinal answer.";
  Expect(
      processor.SanitizeInteractionText(text) == "Final answer.",
      "processor should strip reasoning preambles and think blocks");
  std::cout << "ok: interaction-text-sanitizes-reasoning" << '\n';
}

void TestSanitizesModelReasoningPreambleLeak() {
  const naim::controller::InteractionTextPostProcessor processor;
  const std::string text =
      "The user's prompt explicitly states: \"Reply in English.\"\n\n"
      "Therefore, despite the user's request to reply in Russian, I must follow the system instruction.\n\n"
      "Final market answer.";
  Expect(
      processor.SanitizeInteractionText(text) == "Final market answer.",
      "processor should strip leaked reasoning preambles from model output");
  std::cout << "ok: interaction-text-sanitizes-model-reasoning-preamble" << '\n';
}

void TestRemovesCompletionMarkers() {
  const naim::controller::InteractionTextPostProcessor processor;
  bool marker_seen = false;
  const std::string cleaned = processor.RemoveCompletionMarkers(
      "part [[TASK_COMPLETE]] end [[TASK_COMPLETE]]",
      "[[TASK_COMPLETE]]",
      &marker_seen);
  Expect(marker_seen, "processor should detect completion markers");
  Expect(cleaned == "part  end ", "processor should remove all completion markers");
  std::cout << "ok: interaction-text-removes-markers" << '\n';
}

void TestSanitizesBareCompletionMarker() {
  const naim::controller::InteractionTextPostProcessor processor;
  Expect(
      processor.SanitizeInteractionText("[[TASK_COMPLETE]]").empty(),
      "processor should strip a bare completion marker from visible text");
  std::cout << "ok: interaction-text-sanitizes-bare-marker" << '\n';
}

void TestConsumesSplitCompletionMarker() {
  const naim::controller::InteractionTextPostProcessor processor;
  naim::controller::CompletionMarkerFilterState state;
  const std::string first = processor.ConsumeCompletionMarkerFilteredChunk(
      state,
      "hello [[TASK_",
      "[[TASK_COMPLETE]]",
      false);
  const std::string second = processor.ConsumeCompletionMarkerFilteredChunk(
      state,
      "COMPLETE]] world",
      "[[TASK_COMPLETE]]",
      true);
  Expect(first.empty(), "processor should retain prefix while marker may still be incomplete");
  Expect(second == "hello  world", "processor should suppress split completion marker");
  Expect(state.marker_seen, "processor should record marker_seen across chunks");
  std::cout << "ok: interaction-text-consumes-split-marker" << '\n';
}

void TestUtf8SafeSuffixAvoidsBrokenPrefix() {
  const naim::controller::InteractionTextPostProcessor processor;
  const std::string value = std::string("ab") + "\xD0\x96" + "cd";
  const std::string suffix = processor.Utf8SafeSuffix(value, 3);
  Expect(suffix == "cd", "processor should avoid starting inside utf8 continuation bytes");
  std::cout << "ok: interaction-text-utf8-safe-suffix" << '\n';
}

}  // namespace

int main() {
  try {
    TestSanitizesReasoningAndThinkBlocks();
    TestSanitizesModelReasoningPreambleLeak();
    TestRemovesCompletionMarkers();
    TestSanitizesBareCompletionMarker();
    TestConsumesSplitCompletionMarker();
    TestUtf8SafeSuffixAvoidsBrokenPrefix();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_text_post_processor_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
