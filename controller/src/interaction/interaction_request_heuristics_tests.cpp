#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_request_heuristics.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestExtractsLastUserMessage() {
  const naim::controller::InteractionRequestHeuristics heuristics;
  const nlohmann::json payload = {
      {"messages",
       nlohmann::json::array(
           {nlohmann::json{{"role", "system"}, {"content", "sys"}},
            nlohmann::json{{"role", "user"}, {"content", "first"}},
            nlohmann::json{{"role", "assistant"}, {"content", "reply"}},
            nlohmann::json{{"role", "user"}, {"content", "second"}}})},
  };
  Expect(
      heuristics.LastUserMessageContent(payload) == "second",
      "heuristics should return the latest user message");
  std::cout << "ok: interaction-heuristics-last-user-message" << '\n';
}

void TestDetectsLongFormRequest() {
  const naim::controller::InteractionRequestHeuristics heuristics;
  Expect(
      heuristics.LooksLikeLongFormTaskRequest(
          "Please write a detailed analysis of the entire repository architecture."),
      "heuristics should detect repository-wide long-form requests");
  Expect(
      heuristics.LooksLikeLongFormTaskRequest(
          "Разбей ответ на несколько сообщений и подробно опиши архитектуру проекта."),
      "heuristics should detect Russian long-form markers");
  std::cout << "ok: interaction-heuristics-long-form" << '\n';
}

void TestDetectsRepositoryAnalysisRequest() {
  const naim::controller::InteractionRequestHeuristics heuristics;
  Expect(
      heuristics.LooksLikeRepositoryAnalysisRequest(
          "Analyze the repository and explain the project architecture with file references."),
      "heuristics should detect repository analysis wording");
  Expect(
      heuristics.LooksLikeRepositoryAnalysisRequest(
          "Изучи репозиторий и проследи путь по проекту."),
      "heuristics should detect Russian repository-analysis wording");
  std::cout << "ok: interaction-heuristics-repository-analysis" << '\n';
}

void TestCanonicalizesResponseMode() {
  const naim::controller::InteractionRequestHeuristics heuristics;
  Expect(
      heuristics.CanonicalResponseMode("Very-Long") == "very_long",
      "heuristics should lowercase and underscore response modes");
  std::cout << "ok: interaction-heuristics-canonical-response-mode" << '\n';
}

}  // namespace

int main() {
  try {
    TestExtractsLastUserMessage();
    TestDetectsLongFormRequest();
    TestDetectsRepositoryAnalysisRequest();
    TestCanonicalizesResponseMode();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_request_heuristics_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
