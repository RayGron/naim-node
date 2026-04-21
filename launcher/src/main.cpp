#include "app/launcher_app.h"

int main(int argc, char** argv) {
  naim::launcher::LauncherApp app(argc, argv);
  return app.Run();
}
