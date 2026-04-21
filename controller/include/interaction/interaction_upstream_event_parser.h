#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionUpstreamEventParser {
 public:
  bool DecodeAvailableChunkedHttpBody(
      std::string& encoded,
      std::string* decoded,
      bool* stream_finished) const;

  std::string ExtractInteractionFinishReason(
      const nlohmann::json& payload) const;

  nlohmann::json ExtractInteractionUsage(
      const nlohmann::json& payload) const;

  bool TryConsumeSseFrame(
      std::string& buffer,
      InteractionSseFrame* frame) const;

 private:
  std::string TrimCopy(const std::string& value) const;
};

}  // namespace naim::controller
