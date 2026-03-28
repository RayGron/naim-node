#pragma once

namespace comet::infer {

class InferApp final {
 public:
  InferApp(int argc, char** argv);

  InferApp(const InferApp&) = delete;
  InferApp& operator=(const InferApp&) = delete;

  int Run();

 private:
  int argc_;
  char** argv_;
};

}  // namespace comet::infer
