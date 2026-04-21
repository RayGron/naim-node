#include "interaction/interaction_sse_frame_builder.h"

#include <sstream>

namespace naim::controller {

std::string InteractionSseFrameBuilder::BuildEventFrame(
    const std::string& event_name,
    const nlohmann::json& payload) const {
  std::ostringstream frame;
  frame << "event: " << event_name << "\n";
  std::stringstream lines(payload.dump().append("\n"));
  std::string line;
  while (std::getline(lines, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    frame << "data: " << line << "\n";
  }
  frame << "\n";
  return frame.str();
}

std::string InteractionSseFrameBuilder::BuildDoneFrame() const {
  return "data: [DONE]\n\n";
}

}  // namespace naim::controller
