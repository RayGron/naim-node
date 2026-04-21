#pragma once

namespace naim::hostd {

class HostdApp final {
 public:
  HostdApp(int argc, char** argv);

  HostdApp(const HostdApp&) = delete;
  HostdApp& operator=(const HostdApp&) = delete;

  int Run();

 private:
  int argc_;
  char** argv_;
};

}  // namespace naim::hostd
