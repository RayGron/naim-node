#include "app/hostd_app.h"

#include "app/hostd_composition_root.h"

namespace naim::hostd {

HostdApp::HostdApp(int argc, char** argv) : argc_(argc), argv_(argv) {}

int HostdApp::Run() {
  HostdCompositionRoot composition_root;
  return composition_root.Run(argc_, argv_);
}

}  // namespace naim::hostd
