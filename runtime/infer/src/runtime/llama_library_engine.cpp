#include "runtime/llama_library_engine.h"

#include <algorithm>
#include <array>
#include <mutex>
#include <stdexcept>

namespace comet::infer {

namespace {

[[noreturn]] void Throw(const std::string& message) {
  throw std::runtime_error(message);
}

std::optional<std::string> ResolveGgufPath(const nlohmann::json& active_model) {
  const std::string direct_path =
      active_model.value(
          "cached_runtime_model_path",
          active_model.value(
              "cached_local_model_path",
              active_model.value("runtime_model_path", std::string{})));
  if (!direct_path.empty()) {
    return direct_path;
  }
  return std::nullopt;
}

}  // namespace

LlamaLibraryEngine::LlamaLibraryEngine(
    const RuntimeConfig& config,
    const nlohmann::json& active_model)
    : config_(config), active_model_(active_model) {
  gguf_path_ = ResolveGgufPath(active_model_);
  if (!gguf_path_.has_value()) {
    Throw("active model does not resolve to a local GGUF file");
  }
  EnsureBackendsLoaded();
  llama_model_params model_params = llama_model_default_params();
  model_params.n_gpu_layers = config_.llama_gpu_layers;
  model_.reset(llama_model_load_from_file(gguf_path_->c_str(), model_params));
  if (!model_) {
    Throw("failed to load llama model from " + *gguf_path_);
  }
  vocab_ = llama_model_get_vocab(model_.get());
  if (vocab_ == nullptr) {
    Throw("llama model loaded without vocab");
  }
}

std::string LlamaLibraryEngine::ModelPath() const {
  return *gguf_path_;
}

LlamaLibraryEngine::GenerationResult LlamaLibraryEngine::GenerateTextStream(
    const std::string& prompt,
    int max_tokens,
    const std::function<void(const std::string&)>& on_piece) {
  std::lock_guard<std::mutex> guard(mutex_);
  const int bounded_max_tokens = std::max(1, std::min(max_tokens, 1024));
  std::vector<llama_token> prompt_tokens = Tokenize(prompt, true);
  if (prompt_tokens.empty()) {
    Throw("prompt tokenization produced zero tokens");
  }

  llama_context_params ctx_params = llama_context_default_params();
  ctx_params.n_ctx = static_cast<uint32_t>(
      std::max(
          config_.llama_ctx_size,
          static_cast<int>(prompt_tokens.size()) + bounded_max_tokens + 16));
  ctx_params.n_batch = static_cast<uint32_t>(std::min<std::size_t>(prompt_tokens.size(), 512));
  ctx_params.n_threads = static_cast<uint32_t>(std::max(1, config_.llama_threads));
  ctx_params.n_threads_batch = static_cast<uint32_t>(std::max(1, config_.llama_threads));
  ctx_params.no_perf = true;

  llama_context_ptr ctx(llama_init_from_model(model_.get(), ctx_params));
  if (!ctx) {
    Throw("failed to create llama context");
  }

  llama_sampler_chain_params sampler_params = llama_sampler_chain_default_params();
  sampler_params.no_perf = true;
  llama_sampler_ptr sampler(llama_sampler_chain_init(sampler_params));
  if (!sampler) {
    Throw("failed to create llama sampler");
  }
  llama_sampler_chain_add(sampler.get(), llama_sampler_init_greedy());

  llama_batch batch =
      llama_batch_get_one(prompt_tokens.data(), static_cast<int32_t>(prompt_tokens.size()));
  if (llama_model_has_encoder(model_.get())) {
    if (llama_encode(ctx.get(), batch) != 0) {
      Throw("llama_encode failed");
    }
    llama_token decoder_start_token_id = llama_model_decoder_start_token(model_.get());
    if (decoder_start_token_id == LLAMA_TOKEN_NULL) {
      decoder_start_token_id = llama_vocab_bos(vocab_);
    }
    batch = llama_batch_get_one(&decoder_start_token_id, 1);
  }

  std::string output;
  int n_pos = 0;
  int completion_tokens = 0;
  std::string finish_reason = "stop";
  for (; n_pos + batch.n_tokens < static_cast<int>(prompt_tokens.size()) + bounded_max_tokens;) {
    if (llama_decode(ctx.get(), batch) != 0) {
      Throw("llama_decode failed");
    }
    n_pos += batch.n_tokens;
    llama_token next_token = llama_sampler_sample(sampler.get(), ctx.get(), -1);
    if (llama_vocab_is_eog(vocab_, next_token)) {
      break;
    }
    const std::string piece = TokenToPiece(next_token);
    output += piece;
    ++completion_tokens;
    if (on_piece && !piece.empty()) {
      on_piece(piece);
    }
    batch = llama_batch_get_one(&next_token, 1);
    if (completion_tokens >= bounded_max_tokens) {
      finish_reason = "length";
      break;
    }
  }

  return GenerationResult{
      output,
      static_cast<int>(prompt_tokens.size()),
      completion_tokens,
      finish_reason,
  };
}

LlamaLibraryEngine::GenerationResult LlamaLibraryEngine::GenerateText(
    const std::string& prompt,
    int max_tokens) {
  return GenerateTextStream(prompt, max_tokens, {});
}

void LlamaLibraryEngine::EnsureBackendsLoaded() {
  static std::once_flag once;
  std::call_once(once, []() { ggml_backend_load_all(); });
}

std::vector<llama_token> LlamaLibraryEngine::Tokenize(
    const std::string& text,
    bool add_bos) const {
  const int needed = -llama_tokenize(
      vocab_,
      text.c_str(),
      static_cast<int32_t>(text.size()),
      nullptr,
      0,
      add_bos,
      true);
  if (needed <= 0) {
    Throw("failed to size llama tokenization buffer");
  }
  std::vector<llama_token> tokens(static_cast<std::size_t>(needed));
  if (llama_tokenize(
          vocab_,
          text.c_str(),
          static_cast<int32_t>(text.size()),
          tokens.data(),
          static_cast<int32_t>(tokens.size()),
          add_bos,
          true) < 0) {
    Throw("llama_tokenize failed");
  }
  return tokens;
}

std::string LlamaLibraryEngine::TokenToPiece(llama_token token) const {
  std::array<char, 256> buffer{};
  int n = llama_token_to_piece(vocab_, token, buffer.data(), buffer.size(), 0, true);
  if (n < 0) {
    std::string dynamic(static_cast<std::size_t>(-n), '\0');
    n = llama_token_to_piece(vocab_, token, dynamic.data(), dynamic.size(), 0, true);
    if (n < 0) {
      Throw("llama_token_to_piece failed");
    }
    dynamic.resize(static_cast<std::size_t>(n));
    return dynamic;
  }
  return std::string(buffer.data(), static_cast<std::size_t>(n));
}

}  // namespace comet::infer
