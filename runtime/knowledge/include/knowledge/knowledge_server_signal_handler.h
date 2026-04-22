#pragma once

#include <atomic>

namespace naim::knowledge_runtime {

class KnowledgeServerSignalHandler final {
 public:
  static void Install(std::atomic<bool>& stop_requested);

 private:
  static void HandleSignal(int signal_number);
  static std::atomic<bool>* stop_requested_;
};

}  // namespace naim::knowledge_runtime
