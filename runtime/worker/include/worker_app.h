#pragma once

namespace comet::worker {

class WorkerApp final {
 public:
  WorkerApp();

  WorkerApp(const WorkerApp&) = delete;
  WorkerApp& operator=(const WorkerApp&) = delete;

  int Run();
};

}  // namespace comet::worker
