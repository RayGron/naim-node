#include "plane/plane_state_presentation_support.h"

namespace naim::controller {

ControllerPlaneStatePresentationSupport::ControllerPlaneStatePresentationSupport(
    const ControllerPrintService& controller_print_service)
    : controller_print_service_(controller_print_service) {}

std::string ControllerPlaneStatePresentationSupport::FormatTimestamp(
    const std::string& value) const {
  return ControllerTimeSupport::FormatDisplayTimestamp(value);
}

void ControllerPlaneStatePresentationSupport::PrintStateSummary(
    const naim::DesiredState& state) const {
  controller_print_service_.PrintStateSummary(state);
}

}  // namespace naim::controller
