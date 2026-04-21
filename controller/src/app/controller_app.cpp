#include "app/controller_app.h"

#include "app/controller_composition_root.h"
#include "app/controller_main_includes.h"

namespace naim::controller {

ControllerApp::ControllerApp(int argc, char** argv) : cli_(argc, argv) {}

int ControllerApp::Run() {
  if (!cli_.HasCommand()) {
    cli_.PrintUsage(std::cout);
    return 1;
  }

  const std::string& command = cli_.command();
  try {
    const auto db_arg = cli_.db();
    const auto controller_target = ResolveControllerTarget(cli_.controller(), db_arg);
    RemoteControllerCliService remote_controller_cli_service;
    if (controller_target.has_value()) {
      return remote_controller_cli_service.ExecuteCommand(
          ParseControllerEndpointTarget(*controller_target),
          command,
          cli_);
    }

    const std::string db_path = ResolveDbPath(db_arg);
    const ControllerRequestSupport request_support;
    ControllerCompositionRoot composition_root(
        db_path,
        request_support.ResolveArtifactsRoot(
            cli_.artifacts_root(),
            (std::filesystem::path("var") / "artifacts").string()));
    ControllerCli controller_cli = composition_root.BuildCli(cli_);

    if (const auto result = controller_cli.TryRun(); result.has_value()) {
      return *result;
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  cli_.PrintUsage(std::cout);
  return 1;
}

std::string ControllerApp::ResolveDbPath(const std::optional<std::string>& db_arg) {
  if (db_arg.has_value()) {
    return *db_arg;
  }
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

}  // namespace naim::controller
