#include "runtime/local_runtime.h"

#include <chrono>
#include <thread>
#include <unistd.h>

#include "runtime/infer_replica_support.h"
#include "runtime/infer_runtime_support.h"

namespace comet::infer {

LocalRuntime::LocalRuntime(
    const RuntimeConfig& config,
    std::string backend,
    std::string started_at,
    std::unique_ptr<LlamaLibraryEngine> engine,
    InferSignalService& signal_service,
    bool dynamic_upstream,
    std::optional<UpstreamTarget> upstream)
    : config_(config),
      backend_(std::move(backend)),
      started_at_(std::move(started_at)),
      engine_(std::move(engine)),
      signal_service_(signal_service),
      dynamic_upstream_(dynamic_upstream),
      upstream_(std::move(upstream)),
      inference_server_(
          "0.0.0.0",
          config.api_port,
          "comet-inference-local",
          config,
          engine_.get(),
          signal_service_,
          dynamic_upstream_,
          upstream_),
      gateway_server_(
          config.gateway_listen_host,
          config.gateway_listen_port,
          "comet-gateway-local",
          config,
          engine_.get(),
          signal_service_,
          dynamic_upstream_,
          upstream_) {}

int LocalRuntime::Run() {
  signal_service_.RegisterHandlers();
  runtime_support::TouchReadyFile();
  WriteCurrentRuntimeStatus("starting");
  inference_server_.Start();
  gateway_server_.Start();
  WriteCurrentRuntimeStatus("running");
  while (!signal_service_.StopRequested()) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    WriteCurrentRuntimeStatus("running");
  }
  WriteCurrentRuntimeStatus("stopping");
  gateway_server_.Stop();
  inference_server_.Stop();
  WriteCurrentRuntimeStatus("stopped");
  return 0;
}

bool LocalRuntime::WorkerGroupReady() const {
  if (config_.runtime_engine != "vllm") {
    return true;
  }
  const auto topology = replica_support::InspectReplicaTopology(config_);
  if (topology.replica_groups_expected <= 0) {
    return true;
  }
  return topology.replica_groups_ready > 0;
}

bool LocalRuntime::InferenceReady() const {
  if (!WorkerGroupReady()) {
    return false;
  }
  if (dynamic_upstream_ || upstream_.has_value()) {
    const std::string health_url = runtime_support::RuntimeUpstreamHealthUrl(config_);
    const std::string models_url = runtime_support::RuntimeUpstreamModelsUrl(config_);
    return !health_url.empty() && !models_url.empty() && runtime_support::ProbeUrl(health_url) &&
           runtime_support::ProbeModelsUrl(models_url);
  }
  const std::string base_url = "http://127.0.0.1:" + std::to_string(config_.api_port);
  return runtime_support::ProbeUrl(base_url + "/health") &&
         runtime_support::ProbeModelsUrl(base_url + "/v1/models");
}

bool LocalRuntime::GatewayReady() const {
  return runtime_support::ProbeUrl(runtime_support::RuntimeGatewayHealthUrl(config_));
}

void LocalRuntime::WriteCurrentRuntimeStatus(const std::string& phase) const {
  const bool inference_ready = phase == "running" && InferenceReady();
  const bool gateway_ready = phase == "running" && GatewayReady();
  runtime_support::WriteInferRuntimeStatus(
      config_,
      backend_,
      phase,
      inference_ready,
      gateway_ready,
      static_cast<int>(getpid()),
      started_at_);
}

}  // namespace comet::infer
