#include "worker_signal_service.h"

#include <csignal>

namespace naim::worker {

WorkerSignalService* WorkerSignalService::instance_ = nullptr;

WorkerSignalService::~WorkerSignalService() {
  if (instance_ == this) {
    instance_ = nullptr;
  }
}

void WorkerSignalService::RegisterHandlers() {
  instance_ = this;
  std::signal(SIGINT, &WorkerSignalService::HandleSignal);
  std::signal(SIGTERM, &WorkerSignalService::HandleSignal);
}

bool WorkerSignalService::StopRequested() const {
  return stop_requested_.load();
}

void WorkerSignalService::HandleSignal(int) {
  if (instance_ != nullptr) {
    instance_->stop_requested_.store(true);
  }
}

}  // namespace naim::worker
