#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include <llama-cpp.h>

#include "runtime/infer_runtime_types.h"

namespace comet::infer {

class LlamaLibraryEngine final {
 public:
  struct GenerationResult {
    std::string text;
    int prompt_tokens = 0;
    int completion_tokens = 0;
    std::string finish_reason = "stop";
  };

  LlamaLibraryEngine(const RuntimeConfig& config, const nlohmann::json& active_model);

  std::string ModelPath() const;
  GenerationResult GenerateTextStream(
      const std::string& prompt,
      int max_tokens,
      const std::function<void(const std::string&)>& on_piece);
  GenerationResult GenerateText(const std::string& prompt, int max_tokens);

 private:
  static void EnsureBackendsLoaded();

  std::vector<llama_token> Tokenize(const std::string& text, bool add_bos) const;
  std::string TokenToPiece(llama_token token) const;

  RuntimeConfig config_;
  nlohmann::json active_model_;
  std::optional<std::string> gguf_path_;
  llama_model_ptr model_;
  const llama_vocab* vocab_ = nullptr;
  std::mutex mutex_;
};

}  // namespace comet::infer
