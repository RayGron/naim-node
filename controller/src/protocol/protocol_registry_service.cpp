#include "protocol/protocol_registry_service.h"

#include <algorithm>
#include <utility>

using nlohmann::json;

namespace naim::controller {
namespace {

constexpr const char* kProtocolVersion = "2026-04-24";

bool StartsWith(const std::string& value, const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

}  // namespace

std::optional<HttpResponse> ProtocolRegistryService::HandleRequest(
    const HttpRequest& request) const {
  constexpr const char* kPrefix = "/api/v1/protocols";
  if (!StartsWith(request.path, kPrefix)) {
    return std::nullopt;
  }
  if (request.method != "GET") {
    return JsonResponse(405, json{{"status", "method_not_allowed"}});
  }
  if (request.path == kPrefix) {
    return JsonResponse(200, BuildPayload());
  }
  const std::string item_prefix = std::string(kPrefix) + "/";
  if (!StartsWith(request.path, item_prefix) ||
      request.path.size() <= item_prefix.size()) {
    return JsonResponse(404, json{{"status", "not_found"}});
  }
  const auto payload = BuildItemPayload(request.path.substr(item_prefix.size()));
  if (payload.is_null()) {
    return JsonResponse(
        404,
        json{{"status", "not_found"},
             {"message", "protocol id is not registered"}});
  }
  return JsonResponse(200, payload);
}

nlohmann::json ProtocolRegistryService::BuildPayload() const {
  json items = json::array();
  int direct_count = 0;
  int relay_count = 0;
  int optional_count = 0;
  for (const auto& item : Items()) {
    if (item.fallback.find("relay") != std::string::npos ||
        item.transport.find("hostd") != std::string::npos) {
      ++relay_count;
    }
    if (item.status == "optional" || item.status == "capability-gated") {
      ++optional_count;
    }
    if (item.transport.find("direct") != std::string::npos ||
        item.capabilities.value("supports_direct_routing", false)) {
      ++direct_count;
    }
    items.push_back(ItemToJson(item));
  }
  return json{
      {"protocol_version", kProtocolVersion},
      {"items", std::move(items)},
      {"summary",
       json{{"total", Items().size()},
            {"direct", direct_count},
            {"relay", relay_count},
            {"optional", optional_count}}},
  };
}

nlohmann::json ProtocolRegistryService::BuildItemPayload(
    const std::string& protocol_id) const {
  const auto items = Items();
  const auto it = std::find_if(
      items.begin(),
      items.end(),
      [&](const ProtocolRegistryItem& item) {
        return item.protocol_id == protocol_id;
      });
  if (it == items.end()) {
    return nullptr;
  }
  return json{
      {"protocol_version", kProtocolVersion},
      {"item", ItemToJson(*it)},
  };
}

std::vector<ProtocolRegistryItem> ProtocolRegistryService::Items() const {
  return {
      {"NAIM-PLANE-EXTERNAL-HTTP",
       "plane",
       "HTTP JSON plus SSE for streams",
       "plane auth or protected-plane session",
       "L1 hot interaction",
       "client retry with idempotency key where provided",
       "request ordered by session",
       "30s first response, stream deadline from plane policy",
       "none for hot path; diagnostic controller relay is explicit degraded mode",
       "direct route selected for normal chat",
       "active",
       json{{"supports_sse", true},
            {"supports_websocket", true},
            {"supports_direct_routing", true}}},
      {"NAIM-CTRL-HTTP",
       "controller",
       "HTTP JSON",
       "controller admin session or SSH API session",
       "L3 control",
       "idempotent actions retryable",
       "per-resource optimistic ordering",
       "15s",
       "operator retry",
       "control APIs avoid hot request relay",
       "active",
       json{{"supports_keep_alive", true}}},
      {"NAIM-INTERACTION-SSE",
       "plane",
       "HTTP text/event-stream",
       "same as interaction request",
       "L1 hot interaction",
       "stream reconnect starts a new request unless session id is reused",
       "ordered event stream",
       "first token target under runtime SLO",
       "non-stream JSON response",
       "token/event delivery for chat UI",
       "active",
       json{{"supports_sse", true}}},
      {"NAIM-EVENT-SSE",
       "controller",
       "HTTP text/event-stream",
       "controller session",
       "L2 near-real UI",
       "client reconnect",
       "best-effort event ordering",
       "continuous",
       "polling /api/v1/events",
       "operator event feed",
       "active",
       json{{"supports_sse", true}}},
      {"NAIM-HOSTD-SESSION",
       "hostd",
       "outbound HTTP poll or long-poll with encrypted envelopes",
       "host session token plus encrypted payload",
       "L3 control",
       "retryable; controller sequence validates replay",
       "per-host sequence",
       "30s long-poll window",
       "short poll",
       "NAT-safe host control",
       "active",
       json{{"supports_long_poll", true},
            {"supports_websocket", true},
            {"requires_hostd_relay", false}}},
      {"NAIM-RUNTIME-HTTP",
       "runtime",
       "direct HTTP JSON/SSE",
       "controller internal routing",
       "L1 hot interaction",
       "retry only before upstream accepts request",
       "request scoped",
       "30s first byte",
       "plane_not_ready for hot path; diagnostic relay only",
       "controller uses direct runtime target",
       "active",
       json{{"supports_keep_alive", true},
            {"supports_direct_routing", true}}},
      {"NAIM-LLAMA-RPC-TCP",
       "infer",
       "framed TCP RPC",
       "node-local or internal trusted network",
       "L0 hot compute",
       "request scoped; cancellation frame",
       "ordered frames per connection",
       "model runtime SLO",
       "runtime HTTP adapter",
       "GPU worker hot compute path",
       "capability-gated",
       json{{"supports_rpc", true}}},
      {"NAIM-KV-HTTP",
       "knowledge-vault",
       "HTTP JSON plus job SSE for long operations",
       "plane auth or controller session",
       "L2/L3 knowledge operations",
       "idempotency by deterministic block/job ids",
       "job progress ordered by job id",
       "operation specific",
       "501 knowledge_job_unsupported when runtime lacks jobs",
       "KV avoids WebSocket as primary job mechanism",
       "active",
       json{{"supports_sse", true}}},
      {"NAIM-SKILLS-HTTP",
       "skills-factory",
       "HTTP JSON",
       "plane-local replica or controller session",
       "L2 enrichment",
       "resolver cache retryable",
       "per request",
       "5s resolver target",
       "no enrichment if no trigger terms match",
       "plane-owned SkillsFactory replica drives trigger checks",
       "active",
       json{{"supports_keep_alive", true}}},
      {"NAIM-WEBGATEWAY-HTTP",
       "webgateway",
       "HTTP JSON",
       "plane auth or internal controller route",
       "L2 enrichment",
       "review/session operations idempotent by request id",
       "per session",
       "15s",
       "disabled enrichment",
       "only used when browsing/enrichment is triggered",
       "active",
       json{{"supports_direct_routing", true}}},
      {"NAIM-BULK-TICKET",
       "hostd",
       "ticketed HTTP chunks",
       "host session token plus ticket validation",
       "L4 bulk",
       "resumable chunks with checksum validation",
       "chunk offset ordering",
       "ticket expiry",
       "renew ticket",
       "artifact transfer is resumable and expiring",
       "active",
       json{{"supports_resumable_transfer", true}}},
      {"NAIM-UDP-OPTIONAL",
       "hostd",
       "UDP beacon",
       "none; non-authoritative hint only",
       "L5 telemetry",
       "lossy; never authoritative",
       "unordered",
       "best effort",
       "HTTP observation / TCP probe confirmation",
       "LAN discovery hint only",
       "optional",
       json{{"supports_udp_discovery", true}}},
      {"NAIM-WS-OPTIONAL",
       "controller",
       "WebSocket JSON envelope",
       "controller or host session",
       "L1/L2 bidirectional live session",
       "request id based",
       "ordered frames per socket",
       "continuous with ping/pong",
       "HTTP/SSE",
       "optional live control and interaction channel",
       "capability-gated",
       json{{"supports_websocket", true}}},
  };
}

HttpResponse ProtocolRegistryService::JsonResponse(
    int status_code,
    const nlohmann::json& payload) {
  return HttpResponse{
      status_code,
      "application/json",
      payload.dump(),
      {},
  };
}

nlohmann::json ProtocolRegistryService::ItemToJson(
    const ProtocolRegistryItem& item) {
  return json{
      {"protocol_id", item.protocol_id},
      {"owner", item.owner},
      {"transport", item.transport},
      {"auth", item.auth},
      {"latency_class", item.latency_class},
      {"retry_semantics", item.retry_semantics},
      {"ordering", item.ordering},
      {"timeout", item.timeout},
      {"fallback", item.fallback},
      {"slo", item.slo},
      {"status", item.status},
      {"capabilities", item.capabilities},
  };
}

}  // namespace naim::controller
