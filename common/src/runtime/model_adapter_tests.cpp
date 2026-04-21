#include "naim/runtime/model_adapter.h"

#include <stdexcept>

using naim::runtime::ModelAdapter;
using naim::runtime::ModelFamily;
using naim::runtime::ModelIdentity;
using nlohmann::json;

namespace {

void Expect(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

ModelIdentity Id(std::string model_id = {}, std::string served_model_name = {}) {
  ModelIdentity identity;
  identity.model_id = std::move(model_id);
  identity.served_model_name = std::move(served_model_name);
  return identity;
}

void TestFamilyDetection() {
  Expect(
      ModelAdapter::DetectFamily(Id("google/gemma-4-31B-it")) == ModelFamily::Gemma4,
      "expected gemma family");
  Expect(
      ModelAdapter::DetectFamily(Id({}, "qwen-35b-chat")) == ModelFamily::QwenLegacyChat,
      "expected qwen family");
  Expect(
      ModelAdapter::DetectFamily(Id("meta/llama-3")) == ModelFamily::Generic,
      "expected generic family");
}

void TestGemmaLaunchArgs() {
  std::vector<std::string> args = {"--ctx-size", "4096"};
  ModelIdentity identity = Id("google/gemma-4-31B-it");
  identity.llama_args = {"--threads", "8"};
  ModelAdapter::AdaptLaunchArgs(
      &args, identity);
  Expect(
      std::find(args.begin(), args.end(), "--jinja") != args.end(),
      "expected gemma jinja flag");
  Expect(
      std::find(args.begin(), args.end(), "--reasoning-format") != args.end(),
      "expected gemma reasoning-format");
  Expect(
      std::find(args.begin(), args.end(), "--threads") != args.end(),
      "expected active-model llama args");
}

void TestPromptBuilder() {
  const json payload = {
      {"messages",
       json::array({
           json{{"role", "system"}, {"content", "You are helpful"}},
           json{{"role", "user"}, {"content", "Hello"}},
       })},
  };
  const std::string qwen_prompt = ModelAdapter::BuildLegacyChatPrompt(
      payload, Id("qwen3"));
  Expect(qwen_prompt.contains("<|im_start|>assistant"), "expected qwen prompt");
  const std::string generic_prompt = ModelAdapter::BuildLegacyChatPrompt(
      payload, Id("llama3"));
  Expect(generic_prompt.contains("assistant: "), "expected generic prompt");
}

void TestSanitizers() {
  const std::string gemma = ModelAdapter::SanitizeVisibleText(
      "<|channel>thought<channel|><think>secret</think>assistant: tuned<|im_end|>",
      Id("gemma-4"));
  Expect(gemma == "tuned", "expected gemma visible text sanitation");

  json payload = {
      {"choices",
       json::array({json{
           {"message",
            json{{"role", "assistant"},
                 {"content", "<|channel>thought<channel|>ready"},
                 {"reasoning_content", "hidden"}}},
           {"reasoning_content", "hidden"},
       }})},
  };
  ModelAdapter::SanitizeChatCompletionPayload(
      &payload, Id("gemma-4"));
  Expect(
      payload["choices"][0]["message"]["content"].get<std::string>() == "ready",
      "expected chat payload sanitation");
  Expect(
      !payload["choices"][0]["message"].contains("reasoning_content"),
      "expected reasoning_content removal");
}

}  // namespace

int main() {
  TestFamilyDetection();
  TestGemmaLaunchArgs();
  TestPromptBuilder();
  TestSanitizers();
  return 0;
}
