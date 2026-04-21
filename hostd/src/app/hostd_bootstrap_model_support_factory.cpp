#include "app/hostd_bootstrap_model_support_factory.h"

namespace naim::hostd {

HostdBootstrapModelSupportFactory::HostdBootstrapModelSupportFactory(
    const HostdDesiredStatePathSupport& path_support,
    const HostdCommandSupport& command_support,
    const HostdFileSupport& file_support,
    const HostdReportingSupport& reporting_support)
    : path_support_(path_support),
      command_support_(command_support),
      file_support_(file_support),
      reporting_support_(reporting_support),
      artifact_support_(path_support_),
      active_model_support_(path_support_, file_support_, artifact_support_),
      transfer_support_(command_support_, file_support_, reporting_support_) {}

HostdBootstrapModelSupport HostdBootstrapModelSupportFactory::Create() const {
  return HostdBootstrapModelSupport(
      artifact_support_,
      active_model_support_,
      transfer_support_,
      command_support_,
      file_support_,
      reporting_support_);
}

}  // namespace naim::hostd
