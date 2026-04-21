#pragma once

#include <optional>
#include <string>

namespace naim::controller {

class ControllerTimeSupport final {
 public:
  static std::string SqlTimestampAfterSeconds(int seconds);
  static std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);
  static std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);
  static std::string UtcNowSqlTimestamp();
  static std::string FormatDisplayTimestamp(const std::string& value);
};

}  // namespace naim::controller
