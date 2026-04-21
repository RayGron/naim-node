#include "app/hostd_app.h"

int main(int argc, char** argv) {
  naim::hostd::HostdApp app(argc, argv);
  return app.Run();
}
