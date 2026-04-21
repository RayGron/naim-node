#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "app/controller_service_interfaces.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

enum class WebUiComposeMode {
  Skip,
  Exec,
};

using WebUiEventSink = std::function<void(
    naim::ControllerStore& store,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload)>;

class WebUiService : public IWebUiService {
 public:
  WebUiService(std::string db_path, WebUiEventSink event_sink);

  static int DefaultWebUiPort();
  static std::string DefaultWebUiRoot();
  static std::string DefaultWebUiImage();
  static std::string DefaultControllerUpstream();
  static std::string ResolveWebUiRoot(const std::optional<std::string>& web_ui_root_arg);
  static WebUiComposeMode ResolveComposeMode(const std::optional<std::string>& compose_mode_arg);

  int Ensure(
      const std::string& web_ui_root,
      int listen_port,
      const std::string& controller_upstream,
      WebUiComposeMode compose_mode) const override;

  int ShowStatus(const std::string& web_ui_root) const override;

  int Stop(const std::string& web_ui_root, WebUiComposeMode compose_mode) const override;

 private:
  std::string db_path_;
  WebUiEventSink event_sink_;
};

}  // namespace naim::controller
