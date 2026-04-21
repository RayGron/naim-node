#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "http/infer_http_types.h"
#include "http/local_http_server.h"
#include "platform/infer_signal_service.h"
#include "runtime/infer_runtime_types.h"
#include "runtime/llama_library_engine.h"

namespace naim::infer {

class LocalRuntime final {
 public:
  LocalRuntime(
      const RuntimeConfig& config,
      std::string backend,
      std::string started_at,
      std::unique_ptr<LlamaLibraryEngine> engine,
      InferSignalService& signal_service,
      bool dynamic_upstream = false,
      std::optional<UpstreamTarget> upstream = std::nullopt,
      std::function<std::optional<std::uint64_t>()> kv_cache_bytes_provider = {});

  LocalRuntime(const LocalRuntime&) = delete;
  LocalRuntime& operator=(const LocalRuntime&) = delete;

  int Run();

 private:
  bool WorkerGroupReady() const;
  bool InferenceReady() const;
  bool GatewayReady() const;
  bool EnsureReplicaLeadersPrewarmed() const;
  void WriteCurrentRuntimeStatus(const std::string& phase) const;

  RuntimeConfig config_;
  std::string backend_;
  std::string started_at_;
  std::unique_ptr<LlamaLibraryEngine> engine_;
  InferSignalService& signal_service_;
  bool dynamic_upstream_ = false;
  std::optional<UpstreamTarget> upstream_;
  std::function<std::optional<std::uint64_t>()> kv_cache_bytes_provider_;
  LocalHttpServer inference_server_;
  LocalHttpServer gateway_server_;
};

}  // namespace naim::infer
