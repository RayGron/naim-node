#pragma once

#include <optional>
#include <string>

namespace naim::hostd {

class HostdAssignmentLock {
 public:
  HostdAssignmentLock() = default;
  HostdAssignmentLock(const HostdAssignmentLock&) = delete;
  HostdAssignmentLock& operator=(const HostdAssignmentLock&) = delete;
  HostdAssignmentLock(HostdAssignmentLock&& other) noexcept;
  HostdAssignmentLock& operator=(HostdAssignmentLock&& other) noexcept;
  ~HostdAssignmentLock();

  static std::optional<HostdAssignmentLock> TryAcquire(
      const std::string& state_root,
      const std::string& node_name);

  [[nodiscard]] bool acquired() const { return fd_ >= 0; }

 private:
  explicit HostdAssignmentLock(int fd);

  int fd_ = -1;
};

}  // namespace naim::hostd
