#pragma once

namespace naim::worker {

class WorkerApp final {
 public:
  WorkerApp();

  WorkerApp(const WorkerApp&) = delete;
  WorkerApp& operator=(const WorkerApp&) = delete;

  int Run();
};

}  // namespace naim::worker
