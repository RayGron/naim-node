#include "state_apply/hostd_assignment_lock.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <stdexcept>

namespace naim::hostd {

namespace {

std::filesystem::path LockFilePath(
    const std::string& state_root,
    const std::string& node_name) {
  return std::filesystem::path(state_root) / node_name / "assignment.lock";
}

}  // namespace

HostdAssignmentLock::HostdAssignmentLock(const int fd) : fd_(fd) {}

HostdAssignmentLock::HostdAssignmentLock(HostdAssignmentLock&& other) noexcept : fd_(other.fd_) {
  other.fd_ = -1;
}

HostdAssignmentLock& HostdAssignmentLock::operator=(HostdAssignmentLock&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (fd_ >= 0) {
    ::close(fd_);
  }
  fd_ = other.fd_;
  other.fd_ = -1;
  return *this;
}

HostdAssignmentLock::~HostdAssignmentLock() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::optional<HostdAssignmentLock> HostdAssignmentLock::TryAcquire(
    const std::string& state_root,
    const std::string& node_name) {
  const auto lock_path = LockFilePath(state_root, node_name);
  std::filesystem::create_directories(lock_path.parent_path());
  const int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    throw std::runtime_error(
        "failed to open hostd assignment lock '" + lock_path.string() + "'");
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int lock_errno = errno;
    ::close(fd);
    if (lock_errno == EWOULDBLOCK) {
      return std::nullopt;
    }
    throw std::runtime_error(
        "failed to lock hostd assignment lock '" + lock_path.string() + "'");
  }
  return HostdAssignmentLock(fd);
}

}  // namespace naim::hostd
