#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/infer_http_types.h"
#include "runtime/infer_runtime_types.h"
#include "runtime/llama_library_engine.h"

namespace comet::infer::runtime_support {

int CreateListenSocket(const std::string& host, int port);
std::optional<UpstreamTarget> ResolveRuntimeUpstreamTarget(const RuntimeConfig& config);
bool ProxyHttpRequest(
    int client_fd,
    const std::string& request_data,
    const UpstreamTarget& upstream);
void SendErrorResponse(int client_fd, int status_code, const std::string& message);
bool RequestWantsStream(const HttpRequest& request);
void HandleStreamingChatRequest(
    int client_fd,
    const RuntimeConfig& config,
    const HttpRequest& request,
    LlamaLibraryEngine* engine);
SimpleResponse HandleLocalRequest(
    const RuntimeConfig& config,
    const HttpRequest& request,
    const std::string& service_name,
    LlamaLibraryEngine* engine);
void SendResponse(int client_fd, const SimpleResponse& response);
HttpRequest ParseHttpRequest(const std::string& request_text);
bool ProbeUrl(const std::string& url);
bool ProbeModelsUrl(const std::string& url);
std::string RuntimeUpstreamHealthUrl(const RuntimeConfig& config);
std::string RuntimeUpstreamModelsUrl(const RuntimeConfig& config);
std::string RuntimeGatewayHealthUrl(const RuntimeConfig& config);
nlohmann::json LoadWorkerGroupStatus(const RuntimeConfig& config);
void TouchReadyFile();
void WriteInferRuntimeStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& phase,
    bool inference_ready,
    bool gateway_ready,
    int supervisor_pid,
    const std::string& started_at);

}  // namespace comet::infer::runtime_support
