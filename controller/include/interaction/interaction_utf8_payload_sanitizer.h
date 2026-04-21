#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace naim::controller {

class InteractionUtf8PayloadSanitizer {
 public:
  std::string SanitizeString(const std::string& value) const;
  nlohmann::json SanitizeJson(const nlohmann::json& value) const;

 private:
  bool IsUtf8ContinuationByte(unsigned char value) const;
  std::size_t Utf8SequenceLength(unsigned char lead) const;
};

}  // namespace naim::controller
