#include "app/controller_time_support.h"

#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "naim/core/platform_compat.h"

namespace naim::controller {

namespace {

std::time_t ToUtcTime(std::tm* timestamp) {
#if defined(_WIN32)
  return _mkgmtime(timestamp);
#else
  return timegm(timestamp);
#endif
}

std::optional<std::tm> ParseDisplayTimestamp(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

}  // namespace

std::string ControllerTimeSupport::SqlTimestampAfterSeconds(int seconds) {
  const std::time_t future = std::time(nullptr) + seconds;
  std::tm tm{};
  if (!naim::platform::GmTime(&future, &tm)) {
    throw std::runtime_error("failed to format future UTC timestamp");
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::optional<long long> ControllerTimeSupport::HeartbeatAgeSeconds(
    const std::string& heartbeat_at) {
  if (heartbeat_at.empty()) {
    return std::nullopt;
  }

  std::tm heartbeat_tm{};
  std::istringstream input(heartbeat_at);
  input >> std::get_time(&heartbeat_tm, "%Y-%m-%d %H:%M:%S");
  if (input.fail()) {
    return std::nullopt;
  }

  const std::time_t heartbeat_time = ToUtcTime(&heartbeat_tm);
  if (heartbeat_time < 0) {
    return std::nullopt;
  }

  const std::time_t now = std::time(nullptr);
  return static_cast<long long>(now - heartbeat_time);
}

std::optional<long long> ControllerTimeSupport::TimestampAgeSeconds(
    const std::string& timestamp_text) {
  return HeartbeatAgeSeconds(timestamp_text);
}

std::string ControllerTimeSupport::UtcNowSqlTimestamp() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  if (!naim::platform::GmTime(&now, &tm)) {
    throw std::runtime_error("failed to format current UTC timestamp");
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::string ControllerTimeSupport::FormatDisplayTimestamp(const std::string& value) {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

}  // namespace naim::controller
