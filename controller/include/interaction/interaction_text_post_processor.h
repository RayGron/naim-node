#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionTextPostProcessor {
 public:
  bool StartsWithReasoningPreamble(const std::string& text) const;

  std::string SanitizeInteractionText(std::string text) const;

  std::string Utf8SafeSuffix(
      const std::string& value,
      std::size_t max_bytes) const;

  std::string RemoveCompletionMarkers(
      const std::string& input,
      const std::string& marker,
      bool* marker_seen) const;

  std::string ConsumeCompletionMarkerFilteredChunk(
      CompletionMarkerFilterState& state,
      const std::string& chunk,
      const std::string& marker,
      bool final_flush) const;

  std::string ExtractInteractionText(const nlohmann::json& payload) const;

 private:
  bool IsUtf8ContinuationByte(unsigned char value) const;
  std::size_t Utf8SequenceLength(unsigned char lead) const;
  std::size_t ValidUtf8PrefixLength(const std::string& value) const;
  std::string TrimCopy(const std::string& value) const;
  std::string LowercaseCopy(const std::string& value) const;
  std::string RemoveThinkBlocks(std::string value) const;
  std::vector<std::string> SplitParagraphs(const std::string& value) const;
};

}  // namespace naim::controller
