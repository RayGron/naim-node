#include "interaction/interaction_text_post_processor.h"

#include <algorithm>
#include <cctype>
#include <vector>
#include <sstream>
#include <stdexcept>

namespace naim::controller {

bool InteractionTextPostProcessor::IsUtf8ContinuationByte(
    unsigned char value) const {
  return (value & 0xC0) == 0x80;
}

std::size_t InteractionTextPostProcessor::Utf8SequenceLength(
    unsigned char lead) const {
  if ((lead & 0x80) == 0) {
    return 1;
  }
  if ((lead & 0xE0) == 0xC0) {
    return 2;
  }
  if ((lead & 0xF0) == 0xE0) {
    return 3;
  }
  if ((lead & 0xF8) == 0xF0) {
    return 4;
  }
  return 0;
}

std::size_t InteractionTextPostProcessor::ValidUtf8PrefixLength(
    const std::string& value) const {
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0) {
      break;
    }
    if (index + sequence_length > value.size()) {
      break;
    }
    bool valid = true;
    for (std::size_t offset = 1; offset < sequence_length; ++offset) {
      if (!IsUtf8ContinuationByte(
              static_cast<unsigned char>(value[index + offset]))) {
        valid = false;
        break;
      }
    }
    if (!valid) {
      break;
    }
    index += sequence_length;
  }
  return index;
}

std::string InteractionTextPostProcessor::TrimCopy(const std::string& value) const {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string InteractionTextPostProcessor::LowercaseCopy(
    const std::string& value) const {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

std::string InteractionTextPostProcessor::RemoveThinkBlocks(
    std::string value) const {
  while (true) {
    const std::size_t begin = value.find("<think>");
    if (begin == std::string::npos) {
      return value;
    }
    const std::size_t end = value.find("</think>", begin);
    if (end == std::string::npos) {
      return value.substr(0, begin);
    }
    value.erase(begin, end + std::string("</think>").size() - begin);
  }
}

std::vector<std::string> InteractionTextPostProcessor::SplitParagraphs(
    const std::string& value) const {
  std::vector<std::string> paragraphs;
  std::string current;
  bool last_blank = false;
  std::istringstream input(value);
  std::string line;
  while (std::getline(input, line)) {
    const bool blank = TrimCopy(line).empty();
    if (blank) {
      if (!current.empty()) {
        paragraphs.push_back(TrimCopy(current));
        current.clear();
      }
      last_blank = true;
      continue;
    }
    if (!current.empty()) {
      current += last_blank ? "\n" : "\n";
    }
    current += line;
    last_blank = false;
  }
  if (!current.empty()) {
    paragraphs.push_back(TrimCopy(current));
  }
  return paragraphs;
}

std::string TrimLocal(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::vector<std::string> SplitParagraphsLocal(const std::string& value) {
  std::vector<std::string> paragraphs;
  std::stringstream stream(value);
  std::string line;
  std::string current;
  while (std::getline(stream, line)) {
    if (TrimLocal(line).empty()) {
      if (!TrimLocal(current).empty()) {
        paragraphs.push_back(TrimLocal(current));
        current.clear();
      }
      continue;
    }
    if (!current.empty()) {
      current += '\n';
    }
    current += line;
  }
  if (!TrimLocal(current).empty()) {
    paragraphs.push_back(TrimLocal(current));
  }
  return paragraphs;
}

std::vector<std::string> SplitSentenceLikeUnits(const std::string& value) {
  std::vector<std::string> units;
  std::string current;
  for (char ch : value) {
    current.push_back(ch);
    if (ch == '.' || ch == '!' || ch == '?' || ch == '\n') {
      if (!current.empty()) {
        units.push_back(current);
        current.clear();
      }
    }
  }
  if (!current.empty()) {
    units.push_back(current);
  }
  return units;
}

std::string NormalizeDedupKey(const std::string& value) {
  std::string normalized;
  normalized.reserve(value.size());
  bool previous_space = false;
  for (unsigned char ch : value) {
    if (std::isspace(ch) != 0) {
      if (!previous_space) {
        normalized.push_back(' ');
      }
      previous_space = true;
      continue;
    }
    normalized.push_back(static_cast<char>(std::tolower(ch)));
    previous_space = false;
  }
  while (!normalized.empty() && normalized.front() == ' ') {
    normalized.erase(normalized.begin());
  }
  while (!normalized.empty() && normalized.back() == ' ') {
    normalized.pop_back();
  }
  return normalized;
}

std::string CollapseAdjacentDuplicateUnits(
    const std::vector<std::string>& units,
    const std::string& separator) {
  std::string collapsed;
  std::string previous_key;
  for (const auto& unit : units) {
    const std::string trimmed = TrimLocal(unit);
    if (trimmed.empty()) {
      continue;
    }
    const std::string key = NormalizeDedupKey(trimmed);
    if (!previous_key.empty() && key == previous_key) {
      continue;
    }
    if (!collapsed.empty()) {
      collapsed += separator;
    }
    collapsed += trimmed;
    previous_key = key;
  }
  return collapsed;
}

std::string CollapseAdjacentDuplicates(
    const std::string& value) {
  const auto paragraphs = SplitParagraphsLocal(value);
  if (paragraphs.size() > 1) {
    return CollapseAdjacentDuplicateUnits(paragraphs, "\n\n");
  }
  const auto sentences = SplitSentenceLikeUnits(value);
  if (sentences.size() > 1) {
    return CollapseAdjacentDuplicateUnits(sentences, " ");
  }
  return value;
}

bool InteractionTextPostProcessor::StartsWithReasoningPreamble(
    const std::string& text) const {
  const std::string lowered = LowercaseCopy(TrimCopy(text));
  return lowered.rfind("thinking process:", 0) == 0 ||
         lowered.rfind("reasoning:", 0) == 0 ||
         lowered.rfind("analysis:", 0) == 0 ||
         lowered.rfind("chain of thought:", 0) == 0 ||
         lowered.rfind("the user's prompt explicitly states:", 0) == 0 ||
         lowered.rfind("therefore, despite the user's request", 0) == 0 ||
         lowered.rfind("given the user explicitly requested", 0) == 0 ||
         lowered.rfind("i will present this", 0) == 0 ||
         lowered.rfind("i will structure the response", 0) == 0;
}

std::string InteractionTextPostProcessor::SanitizeInteractionText(
    std::string text) const {
  text = RemoveThinkBlocks(std::move(text));
  text = RemoveCompletionMarkers(text, "[[TASK_COMPLETE]]", nullptr);
  text = TrimCopy(text);
  if (StartsWithReasoningPreamble(text)) {
    const auto paragraphs = SplitParagraphs(text);
    for (auto it = paragraphs.rbegin(); it != paragraphs.rend(); ++it) {
      const std::string candidate = TrimCopy(*it);
      if (candidate.empty()) {
        continue;
      }
      const std::string lowered = LowercaseCopy(candidate);
      if (StartsWithReasoningPreamble(candidate) ||
          lowered.rfind("1.", 0) == 0 ||
          lowered.rfind("2.", 0) == 0 ||
          lowered.rfind("3.", 0) == 0 ||
          lowered.rfind("* ", 0) == 0) {
        continue;
      }
      return candidate;
    }
  }
  return TrimCopy(CollapseAdjacentDuplicates(text));
}

std::string InteractionTextPostProcessor::Utf8SafeSuffix(
    const std::string& value,
    std::size_t max_bytes) const {
  if (value.size() <= max_bytes) {
    return value;
  }
  std::size_t start = value.size() - max_bytes;
  while (start < value.size() &&
         IsUtf8ContinuationByte(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  return value.substr(start);
}

std::string InteractionTextPostProcessor::RemoveCompletionMarkers(
    const std::string& input,
    const std::string& marker,
    bool* marker_seen) const {
  std::string output = input;
  std::size_t position = std::string::npos;
  while ((position = output.find(marker)) != std::string::npos) {
    if (marker_seen != nullptr) {
      *marker_seen = true;
    }
    output.erase(position, marker.size());
  }
  return output;
}

std::string InteractionTextPostProcessor::ConsumeCompletionMarkerFilteredChunk(
    CompletionMarkerFilterState& state,
    const std::string& chunk,
    const std::string& marker,
    bool final_flush) const {
  state.pending += chunk;
  std::string emitted;
  while (true) {
    const std::size_t marker_pos = state.pending.find(marker);
    if (marker_pos != std::string::npos) {
      emitted += state.pending.substr(0, marker_pos);
      state.pending.erase(0, marker_pos + marker.size());
      state.marker_seen = true;
      continue;
    }
    if (final_flush) {
      emitted += state.pending;
      state.pending.clear();
      break;
    }
    if (state.pending.size() > marker.size()) {
      const std::size_t safe_prefix = state.pending.size() - marker.size() + 1;
      const std::string candidate = state.pending.substr(0, safe_prefix);
      const std::size_t valid_prefix = ValidUtf8PrefixLength(candidate);
      if (valid_prefix == 0) {
        break;
      }
      emitted += state.pending.substr(0, valid_prefix);
      state.pending.erase(0, valid_prefix);
    }
    break;
  }
  return emitted;
}

std::string InteractionTextPostProcessor::ExtractInteractionText(
    const nlohmann::json& payload) const {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    throw std::runtime_error(
        "upstream interaction response did not include choices");
  }
  const auto& choice = payload.at("choices").at(0);
  if (choice.contains("message") && choice.at("message").is_object()) {
    return SanitizeInteractionText(
        choice.at("message").value("content", std::string{}));
  }
  return SanitizeInteractionText(choice.value("text", std::string{}));
}

}  // namespace naim::controller
