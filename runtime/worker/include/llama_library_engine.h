#pragma once

#include <memory>
#include <string>

namespace naim::worker {

class LlamaLibraryEngine final {
 public:
  LlamaLibraryEngine(
      const std::string& model_path,
      int ctx_size,
      int threads,
      int gpu_layers);
  ~LlamaLibraryEngine();

  LlamaLibraryEngine(const LlamaLibraryEngine&) = delete;
  LlamaLibraryEngine& operator=(const LlamaLibraryEngine&) = delete;

  void Tick();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace naim::worker
