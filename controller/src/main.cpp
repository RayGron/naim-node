#include "app/controller_app.h"

int main(int argc, char** argv) {
  naim::controller::ControllerApp app(argc, argv);
  return app.Run();
}
