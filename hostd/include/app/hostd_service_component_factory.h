#pragma once

#include <memory>

namespace naim::hostd {

class IHttpHostdBackendSupport;
class IHostdBackendFactory;
class HostdBackendFactory;
class IHostdObservationSupport;
class HostdObservationService;
class IHostdAssignmentSupport;
class HostdAssignmentService;
class HostdCli;
class NodeConfigLoader;

class HostdServiceComponentFactory final {
 public:
  std::unique_ptr<HostdBackendFactory> CreateBackendFactory(
      const IHttpHostdBackendSupport& support) const;
  std::unique_ptr<HostdObservationService> CreateObservationService(
      const IHostdBackendFactory& backend_factory,
      const IHostdObservationSupport& support) const;
  std::unique_ptr<HostdAssignmentService> CreateAssignmentService(
      const IHostdBackendFactory& backend_factory,
      const IHostdAssignmentSupport& support,
      const HostdObservationService& observation_service) const;
  std::unique_ptr<HostdCli> CreateCli(
      const HostdAssignmentService& assignment_service,
      const HostdObservationService& observation_service) const;
  std::unique_ptr<NodeConfigLoader> CreateConfigLoader() const;
};

}  // namespace naim::hostd
