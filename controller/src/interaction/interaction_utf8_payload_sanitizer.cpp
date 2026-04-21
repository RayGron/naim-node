#include "interaction/interaction_utf8_payload_sanitizer.h"

namespace naim::controller {

bool InteractionUtf8PayloadSanitizer::IsUtf8ContinuationByte(
    unsigned char value) const {
  return (value & 0xC0) == 0x80;
}

std::size_t InteractionUtf8PayloadSanitizer::Utf8SequenceLength(
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

std::string InteractionUtf8PayloadSanitizer::SanitizeString(
    const std::string& value) const {
  std::string sanitized;
  sanitized.reserve(value.size());
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0 || index + sequence_length > value.size()) {
      sanitized.push_back('?');
      ++index;
      continue;
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
      sanitized.push_back('?');
      ++index;
      continue;
    }
    sanitized.append(value, index, sequence_length);
    index += sequence_length;
  }
  return sanitized;
}

nlohmann::json InteractionUtf8PayloadSanitizer::SanitizeJson(
    const nlohmann::json& value) const {
  if (value.is_string()) {
    return SanitizeString(value.get<std::string>());
  }
  if (value.is_array()) {
    nlohmann::json sanitized = nlohmann::json::array();
    for (const auto& item : value) {
      sanitized.push_back(SanitizeJson(item));
    }
    return sanitized;
  }
  if (value.is_object()) {
    nlohmann::json sanitized = nlohmann::json::object();
    for (const auto& [key, item] : value.items()) {
      sanitized[SanitizeString(key)] = SanitizeJson(item);
    }
    return sanitized;
  }
  return value;
}

}  // namespace naim::controller
