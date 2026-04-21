#pragma once

#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <ctime>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <direct.h>
#include <io.h>
#include <process.h>

using pid_t = intptr_t;
using ssize_t = SSIZE_T;
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace naim::platform {

#if defined(_WIN32)
using SocketHandle = SOCKET;
using PollFd = WSAPOLLFD;
inline constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
using PollFd = pollfd;
inline constexpr SocketHandle kInvalidSocket = -1;
#endif

inline bool IsSocketValid(const SocketHandle socket_handle) {
  return socket_handle != kInvalidSocket;
}

#if defined(_WIN32)
class SocketRuntime {
 public:
  SocketRuntime() {
    WSADATA data{};
    const int rc = WSAStartup(MAKEWORD(2, 2), &data);
    if (rc != 0) {
      throw std::runtime_error(
          "WSAStartup failed: " + std::to_string(rc));
    }
  }

  ~SocketRuntime() {
    WSACleanup();
  }
};

inline void EnsureSocketsInitialized() {
  static SocketRuntime runtime;
  (void)runtime;
}

inline int CloseSocket(const SocketHandle socket_handle) {
  return closesocket(socket_handle);
}

inline int ShutdownSocket(const SocketHandle socket_handle) {
  return shutdown(socket_handle, SD_BOTH);
}

inline int Poll(PollFd* fds, const unsigned long count, const int timeout_ms) {
  return WSAPoll(fds, count, timeout_ms);
}

inline int LastSocketErrorCode() {
  return WSAGetLastError();
}

inline bool LastSocketErrorWasInterrupted() {
  return WSAGetLastError() == WSAEINTR;
}

inline std::string LastSocketErrorMessage() {
  const int error = WSAGetLastError();
  char* message = nullptr;
  const DWORD size = FormatMessageA(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr,
      static_cast<DWORD>(error),
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      reinterpret_cast<LPSTR>(&message),
      0,
      nullptr);
  std::string rendered =
      size != 0 && message != nullptr ? std::string(message, size) : ("WSA error " + std::to_string(error));
  if (message != nullptr) {
    LocalFree(message);
  }
  while (!rendered.empty() &&
         (rendered.back() == '\r' || rendered.back() == '\n' || rendered.back() == ' ')) {
    rendered.pop_back();
  }
  return rendered;
}

inline int CurrentProcessId() {
  return static_cast<int>(_getpid());
}

inline bool HasElevatedPrivileges() {
  return false;
}

inline FILE* OpenPipe(const char* command, const char* mode) {
  return _popen(command, mode);
}

inline int ClosePipe(FILE* pipe) {
  return _pclose(pipe);
}

inline std::string ExecutablePath() {
  std::string path(MAX_PATH, '\0');
  while (true) {
    const DWORD size = GetModuleFileNameA(
        nullptr,
        path.data(),
        static_cast<DWORD>(path.size()));
    if (size == 0) {
      return {};
    }
    if (size < path.size() - 1) {
      path.resize(size);
      return path;
    }
    path.resize(path.size() * 2);
  }
}

inline bool GmTime(const std::time_t* time_value, std::tm* output) {
  return gmtime_s(output, time_value) == 0;
}
#else
inline void EnsureSocketsInitialized() {
}

inline int CloseSocket(const SocketHandle socket_handle) {
  return close(socket_handle);
}

inline int ShutdownSocket(const SocketHandle socket_handle) {
  return shutdown(socket_handle, SHUT_RDWR);
}

inline int Poll(PollFd* fds, const unsigned long count, const int timeout_ms) {
  return poll(fds, count, timeout_ms);
}

inline int LastSocketErrorCode() {
  return errno;
}

inline bool LastSocketErrorWasInterrupted() {
  return errno == EINTR;
}

inline std::string LastSocketErrorMessage() {
  return std::strerror(errno);
}

inline int CurrentProcessId() {
  return static_cast<int>(getpid());
}

inline bool HasElevatedPrivileges() {
  return geteuid() == 0;
}

inline FILE* OpenPipe(const char* command, const char* mode) {
  return popen(command, mode);
}

inline int ClosePipe(FILE* pipe) {
  return pclose(pipe);
}

inline std::string ExecutablePath() {
  char buffer[4096];
  const ssize_t size = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
  if (size <= 0) {
    return {};
  }
  buffer[static_cast<std::size_t>(size)] = '\0';
  return std::string(buffer);
}

inline bool GmTime(const std::time_t* time_value, std::tm* output) {
  return gmtime_r(time_value, output) != nullptr;
}
#endif

}  // namespace naim::platform
