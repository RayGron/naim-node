#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_utf8_payload_sanitizer.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestSanitizesInvalidUtf8Sequences() {
  const naim::controller::InteractionUtf8PayloadSanitizer sanitizer;
  const std::string invalid = std::string("ok ") + char(0xC3) + "x";
  const std::string sanitized = sanitizer.SanitizeString(invalid);
  Expect(sanitized == "ok ?x", "invalid UTF-8 bytes should be replaced with '?'");
  std::cout << "ok: utf8-sanitizes-invalid-sequence" << '\n';
}

void TestSanitizesNestedJsonKeysAndValues() {
  const naim::controller::InteractionUtf8PayloadSanitizer sanitizer;
  nlohmann::json payload = {
      {std::string("ke") + char(0xFF) + "y",
       nlohmann::json::array(
           {std::string("va") + char(0xC3) + "l",
            nlohmann::json{{std::string("in") + char(0xFF), std::string("x") + char(0xFF)}}})}};

  const auto sanitized = sanitizer.SanitizeJson(payload);
  Expect(sanitized.contains("ke?y"), "object keys should be sanitized");
  Expect(sanitized.at("ke?y").at(0).get<std::string>() == "va?l",
         "array string values should be sanitized");
  Expect(sanitized.at("ke?y").at(1).contains("in?"),
         "nested object keys should be sanitized");
  Expect(sanitized.at("ke?y").at(1).at("in?").get<std::string>() == "x?",
         "nested object values should be sanitized");
  std::cout << "ok: utf8-sanitizes-nested-json" << '\n';
}

}  // namespace

int main() {
  try {
    TestSanitizesInvalidUtf8Sequences();
    TestSanitizesNestedJsonKeysAndValues();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_utf8_payload_sanitizer_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
