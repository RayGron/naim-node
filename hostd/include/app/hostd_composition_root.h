#pragma once

#include <memory>

namespace comet::hostd {

class HostdCompositionRoot final {
 public:
  HostdCompositionRoot();
  ~HostdCompositionRoot();

  HostdCompositionRoot(const HostdCompositionRoot&) = delete;
  HostdCompositionRoot& operator=(const HostdCompositionRoot&) = delete;

  int Run(int argc, char** argv) const;

 private:
  class Components;
  std::unique_ptr<Components> components_;
};

}  // namespace comet::hostd
