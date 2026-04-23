#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_context_compression_service.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::controller::PlaneInteractionResolution BuildResolution(bool enabled) {
  naim::controller::PlaneInteractionResolution resolution;
  resolution.desired_state.plane_name = "compression-plane";
  resolution.desired_state.plane_mode = naim::PlaneMode::Llm;
  resolution.desired_state.inference.max_model_len = 1024;
  if (enabled) {
    naim::ContextCompressionFeatureSpec feature;
    feature.enabled = true;
    resolution.desired_state.context_compression = feature;
    naim::KnowledgeSettings knowledge;
    knowledge.enabled = true;
    knowledge.context_policy.token_budget = 512;
    resolution.desired_state.knowledge = knowledge;
  }
  return resolution;
}

void TestDisabledFeatureNoops() {
  auto resolution = BuildResolution(false);
  naim::controller::InteractionRequestContext request_context;
  request_context.payload["messages"] = json::array({
      json{{"role", "user"}, {"content", "hello"}},
  });

  naim::controller::InteractionContextCompressionService().Apply(
      resolution,
      &request_context);

  Expect(
      !request_context.payload.contains(naim::controller::kInteractionSessionContextStatePayloadKey),
      "disabled feature should not inject context state");
  Expect(
      request_context.payload.at("messages").size() == 1,
      "disabled feature should preserve messages");
  std::cout << "ok: interaction-context-compression-disabled-noop" << '\n';
}

void TestCompressesDialogAndKnowledge() {
  auto resolution = BuildResolution(true);
  naim::controller::InteractionRequestContext request_context;
  json messages = json::array();
  messages.push_back(json{{"role", "system"}, {"content", "base instruction"}});
  for (int index = 0; index < 12; ++index) {
    messages.push_back(json{
        {"role", (index % 2 == 0) ? "user" : "assistant"},
        {"content", "message-" + std::to_string(index) + " " + std::string(180, 'x')},
    });
  }
  request_context.payload["messages"] = messages;
  request_context.payload["__naim_knowledge_context"] = {
      {"context",
       json::array({
           json{{"knowledge_id", "k1"},
                {"block_id", "b1"},
                {"version_id", "v1"},
                {"title", "Alpha"},
                {"text", std::string(1200, 'a')},
                {"score", 0.9}},
           json{{"knowledge_id", "k1"},
                {"block_id", "b1"},
                {"version_id", "v1"},
                {"title", "Alpha duplicate"},
                {"text", std::string(1200, 'b')},
                {"score", 0.8}},
           json{{"knowledge_id", "k2"},
                {"block_id", "b2"},
                {"version_id", "v1"},
                {"title", "Beta"},
                {"text", std::string(1200, 'c')},
                {"score", 0.7}},
       })},
  };

  naim::controller::InteractionContextCompressionService().Apply(
      resolution,
      &request_context);

  const auto context_state = request_context.payload.at(
      naim::controller::kInteractionSessionContextStatePayloadKey);
  Expect(context_state.is_object(), "context state payload should be an object");
  const auto compression = context_state.at("context_compression");
  Expect(compression.at("enabled").get<bool>(), "compression metadata should be enabled");
  Expect(
      compression.at("dialog_estimate_after").get<int>() <
          compression.at("dialog_estimate_before").get<int>(),
      "dialog estimate should shrink");
  Expect(
      compression.at("knowledge_entries_after").get<int>() <
          compression.at("knowledge_entries_before").get<int>(),
      "knowledge entries should dedupe or trim");
  Expect(
      request_context.payload.at("messages").is_array() &&
          request_context.payload.at("messages").size() < messages.size(),
      "compressed messages should be shorter than original");
  const auto knowledge_context = request_context.payload.at("__naim_knowledge_context");
  Expect(
      knowledge_context.at("context").size() < 3 &&
          !knowledge_context.at("context").empty(),
      "knowledge compression should trim to a bounded deduped set");
  Expect(
      request_context.payload.at("__naim_knowledge_system_instruction")
              .get<std::string>()
              .find("Knowledge Base context") != std::string::npos,
      "knowledge instruction should be rebuilt");
  std::cout << "ok: interaction-context-compression-compresses-dialog-and-knowledge" << '\n';
}

void TestCompressesFewButVeryLongTurns() {
  auto resolution = BuildResolution(true);
  resolution.desired_state.inference.max_model_len = 2048;
  naim::controller::InteractionRequestContext request_context;
  request_context.payload["messages"] = json::array({
      json{{"role", "user"}, {"content", "turn-1 " + std::string(1800, 'a')}},
      json{{"role", "assistant"}, {"content", "reply-1 " + std::string(1600, 'b')}},
      json{{"role", "user"}, {"content", "turn-2 " + std::string(1800, 'c')}},
      json{{"role", "assistant"}, {"content", "reply-2 " + std::string(1600, 'd')}},
      json{{"role", "user"}, {"content", "turn-3 " + std::string(1800, 'e')}},
  });

  naim::controller::InteractionContextCompressionService().Apply(
      resolution,
      &request_context);

  const auto compression = request_context.payload
                               .at(naim::controller::kInteractionSessionContextStatePayloadKey)
                               .at("context_compression");
  Expect(
      compression.at("status").get<std::string>() == "compressed",
      "few but very long turns should still be compressed");
  Expect(
      compression.at("dialog_estimate_after").get<int>() <
          compression.at("dialog_estimate_before").get<int>(),
      "compression should reduce dialog estimate even with fewer than six turns");
  Expect(
      request_context.payload.at("messages").size() < 5,
      "compression should replace older long turns with a summary");
  std::cout << "ok: interaction-context-compression-compresses-few-long-turns" << '\n';
}

}  // namespace

int main() {
  try {
    TestDisabledFeatureNoops();
    TestCompressesDialogAndKnowledge();
    TestCompressesFewButVeryLongTurns();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_context_compression_service_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
