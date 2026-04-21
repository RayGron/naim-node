#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "naim/state/models.h"
#include "naim/core/platform_compat.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ControllerHttpServer {
 public:
  using SocketHandle = naim::platform::SocketHandle;
  using RequestHandlerFn = std::function<HttpResponse(const HttpRequest&)>;
  using StreamInteractionSseFn = std::function<void(
      SocketHandle,
      const std::string&,
      const HttpRequest&)>;
  using ParseInteractionStreamPlaneNameFn = std::function<
      std::optional<std::string>(const std::string&, const std::string&)>;
  using BuildEventPayloadItemFn = std::function<nlohmann::json(
      const naim::EventRecord&)>;

  struct Deps {
    RequestHandlerFn handle_request;
    StreamInteractionSseFn stream_plane_interaction_sse;
    ParseInteractionStreamPlaneNameFn parse_interaction_stream_plane_name;
    BuildEventPayloadItemFn build_event_payload_item;
  };

  struct Config {
    std::string db_path;
    std::string artifacts_root;
    std::string listen_host;
    int listen_port = 0;
    std::optional<std::filesystem::path> ui_root;
    std::string routes_summary;
  };

  explicit ControllerHttpServer(Deps deps);

  int Serve(const Config& config);

 private:
  struct SharedState {
    std::atomic<bool> stop_requested{false};
  };

  static void InstallSignalHandlers(std::atomic<bool>* stop_requested);
  static std::string BuildSseEventName(const naim::EventRecord& event);
  static void StreamEventsSse(
      SocketHandle client_fd,
      const std::string& db_path,
      const HttpRequest& request,
      BuildEventPayloadItemFn build_event_payload_item,
      std::shared_ptr<SharedState> state);

  Deps deps_;
};

}  // namespace naim::controller
