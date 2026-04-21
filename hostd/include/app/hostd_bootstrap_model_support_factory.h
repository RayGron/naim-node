#pragma once

#include "app/hostd_bootstrap_active_model_support.h"
#include "app/hostd_bootstrap_model_artifact_support.h"
#include "app/hostd_bootstrap_model_support.h"
#include "app/hostd_bootstrap_transfer_support.h"
#include "app/hostd_command_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_file_support.h"
#include "app/hostd_reporting_support.h"

namespace naim::hostd {

class HostdBootstrapModelSupportFactory final {
 public:
  HostdBootstrapModelSupportFactory(
      const HostdDesiredStatePathSupport& path_support,
      const HostdCommandSupport& command_support,
      const HostdFileSupport& file_support,
      const HostdReportingSupport& reporting_support);

  HostdBootstrapModelSupport Create() const;

 private:
  const HostdDesiredStatePathSupport& path_support_;
  const HostdCommandSupport& command_support_;
  const HostdFileSupport& file_support_;
  const HostdReportingSupport& reporting_support_;
  HostdBootstrapModelArtifactSupport artifact_support_;
  HostdBootstrapActiveModelSupport active_model_support_;
  HostdBootstrapTransferSupport transfer_support_;
};

}  // namespace naim::hostd
