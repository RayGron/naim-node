#include "comet/runtime_status.h"

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include <llama-cpp.h>

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

std::atomic<bool> g_stop_requested{false};

[[noreturn]] void Throw(const std::string& message) {
  throw std::runtime_error(message);
}

struct WorkerConfig {
  std::string plane_name;
  std::string instance_name;
  std::string instance_role = "worker";
  std::string node_name;
  std::string control_root;
  std::string shared_disk_path;
  std::string private_disk_path;
  std::string status_path;
  std::string model_path;
  std::string gpu_device;
  int llama_ctx_size = 2048;
  int llama_threads = 2;
  int llama_gpu_layers = 99;
  int graceful_stop_timeout_sec = 15;
};

std::string UtcNowIso() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return out.str();
}

std::string GetEnvOr(const char* name, const std::string& fallback = "") {
  const char* value = std::getenv(name);
  if (value == nullptr || std::strlen(value) == 0) {
    return fallback;
  }
  return value;
}

int GetEnvIntOr(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || std::strlen(value) == 0) {
    return fallback;
  }
  return std::stoi(value);
}

std::string ExpandUserPath(const std::string& value) {
  if (value.empty() || value[0] != '~') {
    return value;
  }
  const char* home = std::getenv("HOME");
  if (home == nullptr || std::strlen(home) == 0) {
    return value;
  }
  if (value == "~") {
    return home;
  }
  if (value.size() > 1 && value[1] == '/') {
    return std::string(home) + value.substr(1);
  }
  return value;
}

void TouchReadyFile(bool ready) {
  const fs::path ready_path("/tmp/comet-ready");
  if (ready) {
    fs::create_directories(ready_path.parent_path());
    std::ofstream(ready_path) << "ready\n";
  } else {
    std::error_code error;
    fs::remove(ready_path, error);
  }
}

WorkerConfig LoadConfig() {
  WorkerConfig config;
  config.plane_name = GetEnvOr("COMET_PLANE_NAME", "unknown");
  config.instance_name = GetEnvOr("COMET_INSTANCE_NAME", "worker");
  config.instance_role = GetEnvOr("COMET_INSTANCE_ROLE", "worker");
  config.node_name = GetEnvOr("COMET_NODE_NAME");
  config.control_root = GetEnvOr("COMET_CONTROL_ROOT");
  config.shared_disk_path = GetEnvOr("COMET_SHARED_DISK_PATH", "/comet/shared");
  config.private_disk_path = GetEnvOr("COMET_PRIVATE_DISK_PATH", "/comet/private");
  config.status_path = GetEnvOr(
      "COMET_WORKER_RUNTIME_STATUS_PATH",
      (fs::path(config.private_disk_path) / "worker-runtime-status.json").string());
  config.model_path = GetEnvOr("COMET_WORKER_MODEL_PATH");
  config.gpu_device = GetEnvOr("COMET_GPU_DEVICE", GetEnvOr("COMET_WORKER_GPU_DEVICE"));
  config.llama_ctx_size = GetEnvIntOr("COMET_WORKER_CTX_SIZE", 2048);
  config.llama_threads = GetEnvIntOr("COMET_WORKER_THREADS", 2);
  config.llama_gpu_layers = GetEnvIntOr("COMET_LLAMA_GPU_LAYERS", 99);
  config.graceful_stop_timeout_sec =
      GetEnvIntOr("COMET_WORKER_GRACEFUL_STOP_TIMEOUT_SEC", 15);
  return config;
}

json LoadJsonOrEmpty(const fs::path& path) {
  if (!fs::exists(path)) {
    return json::object();
  }
  std::ifstream input(path);
  if (!input.is_open()) {
    Throw("failed to open json file: " + path.string());
  }
  json value;
  input >> value;
  return value;
}

std::optional<std::string> ResolveGgufPath(const std::string& path_text) {
  if (path_text.empty()) {
    return std::nullopt;
  }
  const fs::path path(ExpandUserPath(path_text));
  if (fs::is_regular_file(path) && path.extension() == ".gguf") {
    return path.string();
  }
  if (!fs::exists(path) || !fs::is_directory(path)) {
    return std::nullopt;
  }
  std::vector<fs::path> candidates;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    if (entry.is_regular_file() && entry.path().extension() == ".gguf") {
      candidates.push_back(entry.path());
    }
  }
  if (candidates.empty()) {
    return std::nullopt;
  }
  std::sort(candidates.begin(), candidates.end());
  return candidates.front().string();
}

std::optional<std::string> ResolveModelPath(const WorkerConfig& config) {
  if (const auto resolved = ResolveGgufPath(config.model_path); resolved.has_value()) {
    return resolved;
  }
  if (config.control_root.empty()) {
    return std::nullopt;
  }
  const json active_model = LoadJsonOrEmpty(fs::path(config.control_root) / "active-model.json");
  if (active_model.empty()) {
    return std::nullopt;
  }
  const std::string runtime_path =
      active_model.value(
          "cached_runtime_model_path",
          active_model.value("runtime_model_path", std::string{}));
  if (const auto resolved = ResolveGgufPath(runtime_path); resolved.has_value()) {
    return resolved;
  }
  return ResolveGgufPath(active_model.value("cached_local_model_path", std::string{}));
}

comet::RuntimeStatus BuildStatus(
    const WorkerConfig& config,
    const std::string& phase,
    bool ready,
    const std::string& started_at,
    const std::string& last_activity_at,
    const std::string& model_path) {
  comet::RuntimeStatus status;
  status.plane_name = config.plane_name;
  status.control_root = config.control_root;
  status.instance_name = config.instance_name;
  status.instance_role = config.instance_role;
  status.node_name = config.node_name;
  status.runtime_backend = "llama-worker";
  status.runtime_phase = phase;
  status.runtime_pid = static_cast<int>(getpid());
  status.engine_pid = static_cast<int>(getpid());
  status.started_at = started_at;
  status.last_activity_at = last_activity_at;
  status.model_path = model_path;
  status.cached_local_model_path = model_path;
  status.gpu_device = config.gpu_device;
  status.ready = ready;
  status.inference_ready = ready;
  status.launch_ready = ready;
  return status;
}

void WriteStatus(const comet::RuntimeStatus& status, const std::string& path) {
  comet::SaveRuntimeStatusJson(status, path);
}

class LlamaLibraryEngine {
 public:
  LlamaLibraryEngine(const std::string& model_path, int ctx_size, int threads, int gpu_layers)
      : model_path_(model_path) {
    EnsureBackendsLoaded();
    llama_model_params model_params = llama_model_default_params();
    model_params.n_gpu_layers = gpu_layers;
    model_.reset(llama_model_load_from_file(model_path.c_str(), model_params));
    if (!model_) {
      Throw("failed to load llama model from " + model_path);
    }
    vocab_ = llama_model_get_vocab(model_.get());
    if (vocab_ == nullptr) {
      Throw("llama model loaded without vocab");
    }
    ctx_size_ = ctx_size;
    threads_ = threads;
  }

  void Tick() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<llama_token> prompt_tokens = Tokenize("worker load tick", true);
    if (prompt_tokens.empty()) {
      Throw("prompt tokenization produced zero tokens");
    }

    llama_context_params ctx_params = llama_context_default_params();
    ctx_params.n_ctx = static_cast<uint32_t>(std::max(ctx_size_, 512));
    ctx_params.n_batch = static_cast<uint32_t>(std::min<std::size_t>(prompt_tokens.size(), 256));
    ctx_params.n_threads = static_cast<uint32_t>(std::max(1, threads_));
    ctx_params.n_threads_batch = static_cast<uint32_t>(std::max(1, threads_));
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

    int n_pos = 0;
    for (; n_pos + batch.n_tokens < static_cast<int>(prompt_tokens.size()) + 16;) {
      if (llama_decode(ctx.get(), batch) != 0) {
        Throw("llama_decode failed");
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

  std::string model_path_;
  llama_model_ptr model_;
  const llama_vocab* vocab_ = nullptr;
  int ctx_size_ = 2048;
  int threads_ = 2;
  std::mutex mutex_;
};

void SignalHandler(int) {
  g_stop_requested.store(true);
}

int RunWorker(const WorkerConfig& config) {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  std::cout << "[comet-workerd] booting plane=" << config.plane_name
            << " instance=" << config.instance_name
            << " node=" << config.node_name
            << " gpu=" << (config.gpu_device.empty() ? "(auto)" : config.gpu_device) << "\n";

  const std::string started_at = UtcNowIso();
  std::optional<std::string> loaded_model_path;
  std::unique_ptr<LlamaLibraryEngine> engine;

  while (!g_stop_requested.load()) {
    try {
      if (!engine) {
        const auto resolved_model_path = ResolveModelPath(config);
        if (!resolved_model_path.has_value()) {
          TouchReadyFile(false);
          WriteStatus(
              BuildStatus(
                  config,
                  "waiting-for-model",
                  false,
                  started_at,
                  "",
                  loaded_model_path.value_or("")),
              config.status_path);
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }
        if (!loaded_model_path.has_value() || *loaded_model_path != *resolved_model_path) {
          engine = std::make_unique<LlamaLibraryEngine>(
              *resolved_model_path,
              config.llama_ctx_size,
              config.llama_threads,
              config.llama_gpu_layers);
          loaded_model_path = *resolved_model_path;
          TouchReadyFile(true);
          WriteStatus(
              BuildStatus(
                  config,
                  "running",
                  true,
                  started_at,
                  UtcNowIso(),
                  *loaded_model_path),
              config.status_path);
        }
      }

      engine->Tick();
      WriteStatus(
          BuildStatus(
              config,
              "running",
              true,
              started_at,
              UtcNowIso(),
              loaded_model_path.value_or("")),
          config.status_path);
      std::this_thread::sleep_for(std::chrono::seconds(2));
    } catch (const std::exception& error) {
      TouchReadyFile(false);
      WriteStatus(
          BuildStatus(
              config,
              "failed",
              false,
              started_at,
              UtcNowIso(),
              loaded_model_path.value_or("")),
          config.status_path);
      std::cerr << "[comet-workerd] " << error.what() << "\n";
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  }

  TouchReadyFile(false);
  WriteStatus(
      BuildStatus(
          config,
          "stopped",
          false,
          started_at,
          UtcNowIso(),
          loaded_model_path.value_or("")),
      config.status_path);
  return 0;
}

}  // namespace

int main() {
  try {
    return RunWorker(LoadConfig());
  } catch (const std::exception& error) {
    std::cerr << "comet-workerd: " << error.what() << "\n";
    return 1;
  }
}
