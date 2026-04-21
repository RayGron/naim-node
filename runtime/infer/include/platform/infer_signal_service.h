#pragma once

#include <atomic>

namespace naim::infer {

class InferSignalService final {
 public:
  InferSignalService();
  ~InferSignalService();

  InferSignalService(const InferSignalService&) = delete;
  InferSignalService& operator=(const InferSignalService&) = delete;

  void RegisterHandlers();
  bool StopRequested() const;

 private:
  static void HandleSignal(int signal_number);

  static InferSignalService* instance_;
  std::atomic<bool> stop_requested_{false};
};

}  // namespace naim::infer
