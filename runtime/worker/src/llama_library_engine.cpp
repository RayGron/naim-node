#include "llama_library_engine.h"

#include <stdexcept>
#include <string>

namespace naim::worker {

class LlamaLibraryEngine::Impl {};

#if 0
// llama.cpp-backed worker engine is temporarily disabled.
// Keep the original implementation here so it can be restored quickly later.
class LlamaLibraryEngine::Impl {
 public:
  Impl(const std::string& model_path, int ctx_size, int threads, int gpu_layers)
      : model_path_(model_path), ctx_size_(ctx_size), threads_(threads) {
    EnsureBackendsLoaded();
    llama_model_params model_params = llama_model_default_params();
    model_params.n_gpu_layers = gpu_layers;
    model_.reset(llama_model_load_from_file(model_path.c_str(), model_params));
    if (!model_) {
      ThrowWorkerError("failed to load llama model from " + model_path);
    }
    vocab_ = llama_model_get_vocab(model_.get());
    if (vocab_ == nullptr) {
      ThrowWorkerError("llama model loaded without vocab");
    }
  }

  void Tick() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<llama_token> prompt_tokens = Tokenize("worker load tick", true);
    if (prompt_tokens.empty()) {
      ThrowWorkerError("prompt tokenization produced zero tokens");
    }

    llama_context_params ctx_params = llama_context_default_params();
    ctx_params.n_ctx = static_cast<uint32_t>(std::max(ctx_size_, 512));
    ctx_params.n_batch = static_cast<uint32_t>(std::min<std::size_t>(prompt_tokens.size(), 256));
    ctx_params.n_threads = static_cast<uint32_t>(std::max(1, threads_));
    ctx_params.n_threads_batch = static_cast<uint32_t>(std::max(1, threads_));
    ctx_params.no_perf = true;

    llama_context_ptr ctx(llama_init_from_model(model_.get(), ctx_params));
    if (!ctx) {
      ThrowWorkerError("failed to create llama context");
    }

    llama_sampler_chain_params sampler_params = llama_sampler_chain_default_params();
    sampler_params.no_perf = true;
    llama_sampler_ptr sampler(llama_sampler_chain_init(sampler_params));
    if (!sampler) {
      ThrowWorkerError("failed to create llama sampler");
    }
    llama_sampler_chain_add(sampler.get(), llama_sampler_init_greedy());

    llama_batch batch =
        llama_batch_get_one(prompt_tokens.data(), static_cast<int32_t>(prompt_tokens.size()));
    if (llama_model_has_encoder(model_.get())) {
      if (llama_encode(ctx.get(), batch) != 0) {
        ThrowWorkerError("llama_encode failed");
      }
      llama_token decoder_start_token_id = llama_model_decoder_start_token(model_.get());
      if (decoder_start_token_id == LLAMA_TOKEN_NULL) {
        decoder_start_token_id = llama_vocab_bos(vocab_);
      }
      batch = llama_batch_get_one(&decoder_start_token_id, 1);
    }

    int n_pos = 0;
    for (; n_pos + batch.n_tokens < static_cast<int>(prompt_tokens.size()) + 16;) {
      if (llama_decode(ctx.get(), batch) != 0) {
        ThrowWorkerError("llama_decode failed");
      }
      n_pos += batch.n_tokens;
      llama_token next_token = llama_sampler_sample(sampler.get(), ctx.get(), -1);
      if (llama_vocab_is_eog(vocab_, next_token)) {
        break;
      }
      batch = llama_batch_get_one(&next_token, 1);
    }
  }

 private:
  struct LlamaModelDeleter {
    void operator()(llama_model* model) const {
      if (model != nullptr) {
        llama_model_free(model);
      }
    }
  };
  struct LlamaContextDeleter {
    void operator()(llama_context* ctx) const {
      if (ctx != nullptr) {
        llama_free(ctx);
      }
    }
  };
  struct LlamaSamplerDeleter {
    void operator()(llama_sampler* sampler) const {
      if (sampler != nullptr) {
        llama_sampler_free(sampler);
      }
    }
  };

  using llama_model_ptr = std::unique_ptr<llama_model, LlamaModelDeleter>;
  using llama_context_ptr = std::unique_ptr<llama_context, LlamaContextDeleter>;
  using llama_sampler_ptr = std::unique_ptr<llama_sampler, LlamaSamplerDeleter>;

  static void EnsureBackendsLoaded() {
    static std::once_flag once;
    std::call_once(once, []() { ggml_backend_load_all(); });
  }

  std::vector<llama_token> Tokenize(const std::string& text, bool add_bos) const {
    const int needed = -llama_tokenize(
        vocab_,
        text.c_str(),
        static_cast<int32_t>(text.size()),
        nullptr,
        0,
        add_bos,
        true);
    if (needed <= 0) {
      ThrowWorkerError("failed to size llama tokenization buffer");
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
      ThrowWorkerError("llama_tokenize failed");
    }
    return tokens;
  }

  std::string model_path_;
  llama_model_ptr model_;
  const llama_vocab* vocab_ = nullptr;
  int ctx_size_ = 2048;
  int threads_ = 2;
  std::mutex mutex_;
};
#endif

LlamaLibraryEngine::LlamaLibraryEngine(
    const std::string& model_path,
    int ctx_size,
    int threads,
    int gpu_layers) {
  (void)ctx_size;
  (void)threads;
  (void)gpu_layers;
  throw std::runtime_error(
      "llama.cpp worker engine is disabled in this build; requested model: " + model_path);
}

LlamaLibraryEngine::~LlamaLibraryEngine() = default;

void LlamaLibraryEngine::Tick() {
  throw std::runtime_error("llama.cpp worker engine tick requested while disabled");
}

}  // namespace naim::worker
