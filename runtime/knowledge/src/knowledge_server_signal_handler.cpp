#include "knowledge/knowledge_server_signal_handler.h"

#include <csignal>

namespace naim::knowledge_runtime {

std::atomic<bool>* KnowledgeServerSignalHandler::stop_requested_ = nullptr;

void KnowledgeServerSignalHandler::Install(std::atomic<bool>& stop_requested) {
  stop_requested_ = &stop_requested;
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
}

void KnowledgeServerSignalHandler::HandleSignal(int) {
  if (stop_requested_ != nullptr) {
    stop_requested_->store(true);
  }
}

}  // namespace naim::knowledge_runtime
