#include "runtime/local_runtime.h"

#include <chrono>
#include <thread>
#include <unistd.h>

#include "runtime/infer_prewarm_support.h"
#include "runtime/infer_replica_support.h"
#include "runtime/infer_runtime_support.h"

namespace comet::infer {

namespace {

bool UsesRemoteUpstream(bool dynamic_upstream, const std::optional<UpstreamTarget>& upstream) {
  return dynamic_upstream || upstream.has_value();
}

}  // namespace

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
  prewarm_support::ResetPrewarmState(config_);
  WriteCurrentRuntimeStatus("starting");
  const bool remote_upstream = UsesRemoteUpstream(dynamic_upstream_, upstream_);
  if (!remote_upstream) {
    inference_server_.Start();
  }
  while (!signal_service_.StopRequested() && !EnsureReplicaLeadersPrewarmed()) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    WriteCurrentRuntimeStatus("starting");
  }
  if (signal_service_.StopRequested()) {
    WriteCurrentRuntimeStatus("stopping");
    if (!remote_upstream) {
      inference_server_.Stop();
    }
    WriteCurrentRuntimeStatus("stopped");
    return 0;
  }
  gateway_server_.Start();
  runtime_support::TouchReadyFile();
  WriteCurrentRuntimeStatus("running");
  while (!signal_service_.StopRequested()) {
    EnsureReplicaLeadersPrewarmed();
    WriteCurrentRuntimeStatus("running");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  WriteCurrentRuntimeStatus("stopping");
  gateway_server_.Stop();
  if (!remote_upstream) {
    inference_server_.Stop();
  }
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
  return topology.replica_groups_ready >= topology.replica_groups_expected;
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

bool LocalRuntime::EnsureReplicaLeadersPrewarmed() const {
  if (!dynamic_upstream_ && !upstream_.has_value()) {
    return true;
  }
  if (!config_.replica_upstreams.empty()) {
    return true;
  }
  if (!InferenceReady()) {
    return false;
  }
  const auto prewarm_state = prewarm_support::PrewarmReadyReplicaLeaders(config_);
  return prewarm_state.ready_upstreams == 0 ||
         prewarm_state.prewarmed_upstreams >= prewarm_state.ready_upstreams;
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
