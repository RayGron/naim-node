#include "app/launcher_app.h"

#include "app/launcher_composition_root.h"
#include "cli/launcher_command_line.h"

#include <iostream>

namespace {

using LauncherCommandLine = naim::launcher::LauncherCommandLine;
}  // namespace

namespace naim::launcher {

LauncherApp::LauncherApp(int argc, char** argv) : argc_(argc), argv_(argv) {}

int LauncherApp::Run() {
  const LauncherCommandLine command_line = LauncherCommandLine::FromArgv(argc_, argv_);
  const LauncherCompositionRoot composition_root;
  return composition_root.Run(command_line, argv_[0]);
}

}  // namespace naim::launcher
