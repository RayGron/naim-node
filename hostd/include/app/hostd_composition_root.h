#pragma once

#include <memory>

namespace naim::hostd {

class HostdAppControllerSupport;
class HostdBackendFactory;
class HostdAppObservationSupport;
class HostdObservationService;
class HostdAppAssignmentSupport;
class HostdAssignmentService;
class HostdCli;
class NodeConfigLoader;
class HostdSupportComponentFactory;
class HostdServiceComponentFactory;

class HostdCompositionRoot final {
 public:
  HostdCompositionRoot();
  ~HostdCompositionRoot();

  HostdCompositionRoot(const HostdCompositionRoot&) = delete;
  HostdCompositionRoot& operator=(const HostdCompositionRoot&) = delete;

  int Run(int argc, char** argv) const;

 private:
  std::unique_ptr<HostdSupportComponentFactory> support_factory_;
  std::unique_ptr<HostdServiceComponentFactory> service_factory_;
  std::unique_ptr<HostdAppControllerSupport> backend_support_;
  std::unique_ptr<HostdBackendFactory> backend_factory_;
  std::unique_ptr<HostdAppObservationSupport> observation_support_;
  std::unique_ptr<HostdObservationService> observation_service_;
  std::unique_ptr<HostdAppAssignmentSupport> assignment_support_;
  std::unique_ptr<HostdAssignmentService> assignment_service_;
  std::unique_ptr<HostdCli> cli_;
  std::unique_ptr<NodeConfigLoader> config_loader_;
};

}  // namespace naim::hostd
