#include "worker_app.h"

#include "worker_config_loader.h"
#include "worker_engine_host.h"

#include <exception>
#include <iostream>

namespace comet::worker {

WorkerApp::WorkerApp() = default;

int WorkerApp::Run() {
  try {
    WorkerConfigLoader config_loader;
    WorkerEngineHost engine_host(config_loader.Load());
    return engine_host.Run();
  } catch (const std::exception& error) {
    std::cerr << "comet-workerd: " << error.what() << "\n";
    return 1;
  }
}

}  // namespace comet::worker
