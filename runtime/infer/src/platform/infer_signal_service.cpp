#include "platform/infer_signal_service.h"

#include <csignal>

namespace naim::infer {

InferSignalService* InferSignalService::instance_ = nullptr;

InferSignalService::InferSignalService() = default;

InferSignalService::~InferSignalService() {
  if (instance_ == this) {
    instance_ = nullptr;
  }
}

void InferSignalService::RegisterHandlers() {
  instance_ = this;
  std::signal(SIGINT, &InferSignalService::HandleSignal);
  std::signal(SIGTERM, &InferSignalService::HandleSignal);
}

bool InferSignalService::StopRequested() const {
  return stop_requested_.load();
}

void InferSignalService::HandleSignal(int) {
  if (instance_ != nullptr) {
    instance_->stop_requested_.store(true);
  }
}

}  // namespace naim::infer
