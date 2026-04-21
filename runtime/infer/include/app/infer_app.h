#pragma once

namespace naim::infer {

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

}  // namespace naim::infer
